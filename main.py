import os
import json
import threading
import random
import csv
import io
import time as time_mod
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta, time, date

import discord
from discord import app_commands
from flask import Flask

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials


# =========================================================
# LEME HOLANDÊS BOT (Discord + Google Sheets + Render)
# =========================================================
# CONCEITO NOVO: SEASONS (TEMPORADAS)
#
# Abas:
# - Seasons: controle de temporadas (uma ativa por vez)
# - Players: cadastro permanente do jogador (nick e status)
# - Cycles: ciclos por season (open/locked/completed) + start/deadline
# - Enrollments: inscrição do jogador por cycle+season
# - Decks: deck e decklist por cycle+season (1x por ciclo; ADM/Org podem editar)
# - PodsHistory: pods por cycle+season
# - Matches: confrontos por cycle+season + resultados
# - Standings: ranking por cycle+season (bot escreve do zero por cycle+season)
#
# Regras:
# - Ranking: Pontos > OMW% > GW% > OGW%
# - Piso 33,3% em MWP/GWP antes de calcular OMW/OGW
# - Sempre recalcular tudo do zero
# - Empate em games conta como 0.5 na GWP
# - Rejeição: 48h para rejeitar, depois pode virar confirmed via sweep (/recalcular)
# - Prazo do ciclo (/prazo): pelo maior POD
#   POD 3 -> 5 dias
#   POD 4 -> 8 dias
#   POD 5/6 -> 10 dias
#   Ciclo começa 14:00 (BR) e termina no último dia às 13:59 (BR)
#
# Controles:
# - Abrir/fechar SEASON: somente DONO do servidor
# - Abrir ciclo: ADM/Organizador (ciclo só existe se admin criar)
# - Jogador escolhe o cycle em /deck e /decklist (evita erro)
# =========================================================


# =========================
# HTTP keep-alive (Render)
# =========================
app = Flask(__name__)

@app.get("/")
def home():
    return "LEME HOLANDÊS BOT online", 200

def _run_web():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

def keep_alive():
    t = threading.Thread(target=_run_web, daemon=True)
    t.start()


# =========================
# Configs / Env
# =========================
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")

SHEET_ID = os.getenv("SHEET_ID", "")
SERVICE_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

ROLE_ORGANIZADOR = os.getenv("ROLE_ORGANIZADOR", "Organizador")
ROLE_ADM = os.getenv("ROLE_ADM", "ADM")

RANKING_CHANNEL_ID = int(os.getenv("RANKING_CHANNEL_ID", "0"))  # opcional


# =========================
# Time helpers (BR)
# =========================
BR_TZ = timezone(timedelta(hours=-3))

def now_br_dt() -> datetime:
    return datetime.now(BR_TZ)

def now_br_str() -> str:
    return now_br_dt().strftime("%Y-%m-%d %H:%M:%S")

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_dt(s: str):
    try:
        return datetime.fromisoformat(str(s).strip())
    except Exception:
        return None

def parse_br_dt(s: str):
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=BR_TZ)
    except Exception:
        return None

def fmt_br_dt(dt: datetime) -> str:
    return dt.astimezone(BR_TZ).strftime("%Y-%m-%d %H:%M:%S")


# =========================
# Google Sheets helpers (com retry p/ quota)
# =========================
def get_sheets_client():
    if not SERVICE_JSON:
        return None
    data = json.loads(SERVICE_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    gc = get_sheets_client()
    if not gc or not SHEET_ID:
        raise RuntimeError("Google Sheets não configurado (SHEET_ID ou GOOGLE_SERVICE_ACCOUNT_JSON).")
    return gc.open_by_key(SHEET_ID)

def safe_int(v, default=0):
    try:
        return int(str(v).strip())
    except Exception:
        return default

def as_bool(v):
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "sim")

def floor_333(x: float) -> float:
    return max(x, 1/3)

def pct1(x: float) -> float:
    return round(x * 100.0, 1)

def col_letter(ci_0: int) -> str:
    n = ci_0
    s = ""
    while True:
        s = chr(n % 26 + 65) + s
        n = n // 26 - 1
        if n < 0:
            break
    return s

def ensure_sheet_columns(ws, required_cols: list[str]):
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"Aba '{ws.title}' sem cabeçalho na linha 1.")
    idx = {name: i for i, name in enumerate(header)}
    missing = [c for c in required_cols if c not in idx]
    if missing:
        raise RuntimeError(f"Aba '{ws.title}' sem colunas: {', '.join(missing)}")
    return idx

def ensure_worksheet(sh, title: str, header: list[str], rows: int = 2000, cols: int = 30):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    current = ws.row_values(1)
    if not current:
        ws.append_row(header, value_input_option="USER_ENTERED")
    return ws

def _is_quota_error(e: Exception) -> bool:
    if isinstance(e, APIError):
        # gspread APIError geralmente tem response com status
        try:
            status = getattr(e.response, "status_code", None)
            if status == 429:
                return True
        except Exception:
            pass
        # fallback por texto
        msg = str(e).lower()
        if "quota" in msg or "429" in msg or "read requests" in msg:
            return True
    msg = str(e).lower()
    return ("quota" in msg) or ("read requests" in msg) or ("429" in msg)

def with_retry(fn, *, tries=6, base_sleep=1.0):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if _is_quota_error(e):
                sleep_s = base_sleep * (2 ** i) + random.uniform(0, 0.25)
                time_mod.sleep(min(12.0, sleep_s))
                continue
            raise
    raise last

def ws_update(ws, values, range_name):
    return with_retry(lambda: ws.update(values, range_name=range_name))

def ws_append_row(ws, row):
    return with_retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))

def ws_append_rows(ws, rows):
    return with_retry(lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"))

def ws_get_all_records(ws):
    return with_retry(lambda: ws.get_all_records())

def ws_get_all_values(ws):
    return with_retry(lambda: ws.get_all_values())

def ws_find(ws, query):
    return with_retry(lambda: ws.find(query))


# =========================
# Auth helpers
# =========================
async def has_role(interaction: discord.Interaction, role_name: str) -> bool:
    if not interaction.guild or not interaction.user:
        return False
    guild = interaction.guild
    member = guild.get_member(interaction.user.id)
    if member is None:
        try:
            member = await guild.fetch_member(interaction.user.id)
        except Exception:
            return False
    return any(r.name == role_name for r in member.roles)

async def is_admin_or_organizer(interaction: discord.Interaction) -> bool:
    if interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild:
        return True
    if await has_role(interaction, ROLE_ADM):
        return True
    if await has_role(interaction, ROLE_ORGANIZADOR):
        return True
    return False

async def is_organizer_only(interaction: discord.Interaction) -> bool:
    return await has_role(interaction, ROLE_ORGANIZADOR)

async def is_server_owner(interaction: discord.Interaction) -> bool:
    if not interaction.guild:
        return False
    return interaction.user.id == interaction.guild.owner_id

async def get_access_level(interaction: discord.Interaction) -> str:
    if await has_role(interaction, ROLE_ORGANIZADOR):
        return "organizador"
    if await has_role(interaction, ROLE_ADM):
        return "adm"
    return "jogador"


# =========================
# URL validation (decklist)
# =========================
def validate_decklist_url(url: str) -> tuple[bool, str]:
    raw = str(url).strip()
    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = "https://" + raw

    if " " in raw or len(raw) < 10 or len(raw) > 400:
        return (False, "Link inválido. Envie uma URL completa (sem espaços).")

    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]

    allowed_hosts = {"moxfield.com", "ligamagic.com.br"}
    if host not in allowed_hosts:
        return (False, "Link não permitido. Use apenas moxfield.com ou ligamagic.com.br")

    if host == "moxfield.com":
        if "/decks/" not in (parsed.path or ""):
            return (False, "Link inválido do Moxfield. Exemplo: https://www.moxfield.com/decks/SEU_ID")

    if host == "ligamagic.com.br":
        qs = parse_qs(parsed.query or "")
        deck_id = (qs.get("id", [""])[0] or "").strip()
        if not deck_id.isdigit():
            return (False, "Link inválido da LigaMagic. Exemplo: https://www.ligamagic.com.br/?view=dks/deck&id=123456")

    return (True, raw)


# =========================
# Score helpers (3-partes)
# =========================
def parse_score_3parts(score: str) -> tuple[int, int, int] | None:
    s = str(score).strip().lower().replace(" ", "")
    for sep in ["x", ":", "–", "—"]:
        s = s.replace(sep, "-")
    parts = s.split("-")
    if len(parts) != 3:
        return None
    a = safe_int(parts[0], -1)
    b = safe_int(parts[1], -1)
    d = safe_int(parts[2], -1)
    if a < 0 or b < 0 or d < 0:
        return None
    return (a, b, d)

def validate_3parts_rules(v: int, d: int, e: int) -> tuple[bool, str]:
    total = v + d + e
    if total > 3:
        return (False, "Placar inválido: soma (Vitória+Derrota+Empate) não pode passar de 3.")
    return (True, "")


# =========================
# Sheets schema (headers)
# =========================
SEASONS_HEADER = ["season","status","created_at","updated_at","started_at_br","ended_at_br"]  # status: active|closed
SEASONS_REQUIRED = ["season","status","created_at","updated_at","started_at_br","ended_at_br"]

PLAYERS_HEADER = ["discord_id","nick","status","created_at","updated_at"]
PLAYERS_REQUIRED = ["discord_id","nick","status","created_at","updated_at"]

# season incluído
CYCLES_HEADER = ["season","cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]  # open|locked|completed
CYCLES_REQUIRED = ["season","cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]

ENROLLMENTS_HEADER = ["season","cycle","player_id","status","created_at","updated_at"]
ENROLLMENTS_REQUIRED = ["season","cycle","player_id","status","created_at","updated_at"]

DECKS_HEADER = ["season","cycle","player_id","deck","decklist_url","created_at","updated_at"]
DECKS_REQUIRED = ["season","cycle","player_id","deck","decklist_url","created_at","updated_at"]

PODSHISTORY_HEADER = ["season","cycle","pod","player_id","created_at"]
PODSHISTORY_REQUIRED = ["season","cycle","pod","player_id","created_at"]

MATCHES_REQUIRED_COLS = [
    "season",
    "match_id",
    "cycle",
    "pod",
    "player_a_id",
    "player_b_id",
    "a_games_won",
    "b_games_won",
    "draw_games",
    "result_type",
    "confirmed_status",
    "reported_by_id",
    "confirmed_by_id",
    "message_id",
    "active",
    "created_at",
    "updated_at",
    "auto_confirm_at",
]

STANDINGS_HEADER = [
    "season","cycle","player_id","matches_played","match_points","mwp_percent",
    "game_wins","game_losses","game_draws","games_played","gw_percent",
    "omw_percent","ogw_percent","rank_position","last_recalc_at"
]
STANDINGS_REQUIRED = STANDINGS_HEADER


# =========================
# Season helpers
# =========================
def get_active_season(ws_seasons) -> int | None:
    rows = ws_get_all_records(ws_seasons)
    for r in rows:
        if str(r.get("status","")).strip().lower() == "active":
            s = safe_int(r.get("season",0),0)
            if s > 0:
                return s
    return None

def get_season_row(ws_seasons, season: int) -> int | None:
    vals = ws_get_all_values(ws_seasons)
    if len(vals) <= 1:
        return None
    col = ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        s = r[col["season"]] if col["season"] < len(r) else ""
        if safe_int(s, 0) == season:
            return rown
    return None

def set_season_status(ws_seasons, season: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    rown = get_season_row(ws_seasons, season)
    col = ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
    if rown is None:
        # cria
        started = nowb if status == "active" else ""
        ended = nowb if status == "closed" else ""
        ws_append_row(ws_seasons, [str(season), status, nowb, nowb, started, ended])
        return

    ws_update(ws_seasons, [[status]], f"{col_letter(col['status'])}{rown}")
    ws_update(ws_seasons, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")
    if status == "active":
        ws_update(ws_seasons, [[nowb]], f"{col_letter(col['started_at_br'])}{rown}")
        ws_update(ws_seasons, [[""]], f"{col_letter(col['ended_at_br'])}{rown}")
    if status == "closed":
        ws_update(ws_seasons, [[nowb]], f"{col_letter(col['ended_at_br'])}{rown}")

def close_any_active_season(ws_seasons):
    vals = ws_get_all_values(ws_seasons)
    if len(vals) <= 1:
        return
    col = ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
    nowb = now_br_str()
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        st = (r[col["status"]] if col["status"] < len(r) else "").strip().lower()
        if st == "active":
            ws_update(ws_seasons, [["closed"]], f"{col_letter(col['status'])}{rown}")
            ws_update(ws_seasons, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")
            ws_update(ws_seasons, [[nowb]], f"{col_letter(col['ended_at_br'])}{rown}")

def list_seasons(ws_seasons) -> list[int]:
    rows = ws_get_all_records(ws_seasons)
    out = []
    for r in rows:
        s = safe_int(r.get("season",0),0)
        if s > 0:
            out.append(s)
    return sorted(set(out))


# =========================
# Players helpers
# =========================
def find_player_row(ws_players, discord_id: int):
    try:
        cell = ws_find(ws_players, str(discord_id))
        return cell.row
    except Exception:
        return None

def build_players_nick_map(ws_players) -> dict[str, str]:
    data = ws_get_all_records(ws_players)
    m = {}
    for r in data:
        pid = str(r.get("discord_id", "")).strip()
        nick = str(r.get("nick", "")).strip()
        if pid:
            m[pid] = nick or pid
    return m


# =========================
# Cycle helpers (por season)
# =========================
def get_cycle_row(ws_cycles, season: int, cycle: int) -> int | None:
    rows = ws_get_all_values(ws_cycles)
    if len(rows) <= 1:
        return None
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    for rown in range(2, len(rows)+1):
        r = rows[rown-1]
        s = r[col["season"]] if col["season"] < len(r) else ""
        c = r[col["cycle"]] if col["cycle"] < len(r) else ""
        if safe_int(s,0) == season and safe_int(c,0) == cycle:
            return rown
    return None

def get_cycle_fields(ws_cycles, season: int, cycle: int) -> dict:
    rows = ws_get_all_values(ws_cycles)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    out = {"season": season, "cycle": cycle, "status": None, "start_at_br": "", "deadline_at_br": ""}
    for rown in range(2, len(rows)+1):
        r = rows[rown-1]
        s = r[col["season"]] if col["season"] < len(r) else ""
        c = r[col["cycle"]] if col["cycle"] < len(r) else ""
        if safe_int(s,0) == season and safe_int(c,0) == cycle:
            out["status"] = (r[col["status"]] if col["status"] < len(r) else "").strip().lower()
            out["start_at_br"] = (r[col["start_at_br"]] if col["start_at_br"] < len(r) else "").strip()
            out["deadline_at_br"] = (r[col["deadline_at_br"]] if col["deadline_at_br"] < len(r) else "").strip()
            return out
    return out

def set_cycle_status(ws_cycles, season: int, cycle: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, season, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_append_row(ws_cycles, [str(season), str(cycle), status, "", "", nowb, nowb])
    else:
        ws_update(ws_cycles, [[status]], f"{col_letter(col['status'])}{rown}")
        ws_update(ws_cycles, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")

def set_cycle_times(ws_cycles, season: int, cycle: int, start_at_br: str, deadline_at_br: str):
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, season, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_append_row(ws_cycles, [str(season), str(cycle), "open", start_at_br, deadline_at_br, nowb, nowb])
    else:
        ws_update(ws_cycles, [[start_at_br]], f"{col_letter(col['start_at_br'])}{rown}")
        ws_update(ws_cycles, [[deadline_at_br]], f"{col_letter(col['deadline_at_br'])}{rown}")
        ws_update(ws_cycles, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")

def list_open_cycles(ws_cycles, season: int) -> list[int]:
    rows = ws_get_all_records(ws_cycles)
    out = []
    for r in rows:
        s = safe_int(r.get("season",0),0)
        c = safe_int(r.get("cycle",0),0)
        st = str(r.get("status","")).strip().lower()
        if s == season and c > 0 and st == "open":
            out.append(c)
    return sorted(set(out))


# =========================
# Pods / prazo helpers
# =========================
def round_robin_pairs(players: list[str]):
    pairs = []
    n = len(players)
    for i in range(n):
        for j in range(i + 1, n):
            pairs.append((players[i], players[j]))
    return pairs

def cycle_days_by_max_pod(max_pod_size: int) -> int:
    if max_pod_size <= 3:
        return 5
    if max_pod_size == 4:
        return 8
    return 10

def compute_cycle_start_deadline_br(season: int, cycle: int, ws_pods, ws_cycles) -> tuple[str, str, int, int]:
    fields = get_cycle_fields(ws_cycles, season, cycle)
    if fields.get("start_at_br"):
        start_dt = parse_br_dt(fields["start_at_br"])
    else:
        start_dt = None

    rows = ws_get_all_records(ws_pods)
    pods = {}
    for r in rows:
        if safe_int(r.get("season",0),0) != season:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        pod = str(r.get("pod", "")).strip()
        pid = str(r.get("player_id", "")).strip()
        if pod and pid:
            pods.setdefault(pod, set()).add(pid)

    if not pods:
        return ("", "", 0, 0)

    max_pod_size = max(len(v) for v in pods.values())
    days = cycle_days_by_max_pod(max_pod_size)

    if start_dt is None:
        created_candidates = []
        for r in rows:
            if safe_int(r.get("season",0),0) != season:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            cdt = parse_br_dt(r.get("created_at", ""))
            if cdt:
                created_candidates.append(cdt)
        base_date = (min(created_candidates).astimezone(BR_TZ).date() if created_candidates else now_br_dt().date())
        start_dt = datetime.combine(base_date, time(14, 0), tzinfo=BR_TZ)

    deadline_date = (start_dt.date() + timedelta(days=days))
    deadline_dt = datetime.combine(deadline_date, time(13, 59), tzinfo=BR_TZ)
    return (fmt_br_dt(start_dt), fmt_br_dt(deadline_dt), max_pod_size, days)


# =========================
# Match helpers
# =========================
def new_match_id(season: int, cycle: int, pod: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    rnd = random.randint(1000, 9999)
    return f"S{season}-C{cycle}-P{pod}-{ts}-{rnd}"

def auto_confirm_deadline_iso(created_utc: datetime) -> str:
    return (created_utc + timedelta(hours=48)).isoformat()

def sweep_auto_confirm(sh, season: int, cycle: int) -> int:
    ws_matches = sh.worksheet("Matches")
    col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    rows = ws_get_all_values(ws_matches)
    if len(rows) <= 1:
        return 0

    nowu = utc_now_dt()
    changed = 0

    for rown in range(2, len(rows) + 1):
        r = rows[rown - 1]

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        r_season = safe_int(getc("season"), 0)
        r_cycle = safe_int(getc("cycle"), 0)
        if r_season != season or r_cycle != cycle:
            continue
        if not as_bool(getc("active") or "TRUE"):
            continue
        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            continue

        reported_by = (getc("reported_by_id") or "").strip()
        if not reported_by:
            continue

        ac = parse_iso_dt(getc("auto_confirm_at") or "")
        if not ac:
            continue

        if ac <= nowu:
            ws_update(ws_matches, [["confirmed"]], f"{col_letter(col['confirmed_status'])}{rown}")
            ws_update(ws_matches, [["AUTO"]], f"{col_letter(col['confirmed_by_id'])}{rown}")
            ws_update(ws_matches, [[now_br_str()]], f"{col_letter(col['updated_at'])}{rown}")
            changed += 1

    return changed


# =========================
# Anti-repetição (por season)
# =========================
def get_past_confirmed_pairs(ws_matches, season: int) -> set[frozenset]:
    rows = ws_get_all_records(ws_matches)
    pairs = set()
    for r in rows:
        if safe_int(r.get("season",0),0) != season:
            continue
        if not as_bool(r.get("active", "TRUE")):
            continue
        if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
            continue
        if str(r.get("result_type", "normal")).strip().lower() == "bye":
            continue
        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()
        if a and b:
            pairs.add(frozenset((a, b)))
    return pairs

def score_pods_repeats(pods: list[list[str]], past_pairs: set[frozenset]) -> int:
    penalty = 0
    for pod in pods:
        for a, b in round_robin_pairs(pod):
            if frozenset((a, b)) in past_pairs:
                penalty += 1
    return penalty

def best_shuffle_min_repeats(players: list[str], pod_size: int, past_pairs: set[frozenset], tries: int = 250):
    best = None
    best_score = None
    for _ in range(max(1, tries)):
        cand = players[:]
        random.shuffle(cand)
        pods = []
        for i in range(0, len(cand), pod_size):
            pods.append(cand[i:i+pod_size])
        s = score_pods_repeats(pods, past_pairs)
        if best is None or s < best_score:
            best = pods
            best_score = s
            if best_score == 0:
                break
    return best, best_score


# =========================
# Core: recálculo oficial (por season+cycle)
# =========================
def recalculate_cycle(season: int, cycle: int):
    sh = open_sheet()
    ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)
    ws_matches = sh.worksheet("Matches")
    ws_standings = sh.worksheet("Standings")

    try:
        sweep_auto_confirm(sh, season, cycle)
    except Exception:
        pass

    ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

    players_rows = ws_get_all_records(ws_players)
    all_player_ids = set()
    for r in players_rows:
        pid = str(r.get("discord_id", "")).strip()
        if pid:
            all_player_ids.add(pid)

    stats = {}
    opponents = {}

    def ensure(pid: str):
        if pid not in stats:
            stats[pid] = {
                "match_points": 0,
                "matches_played": 0,
                "game_wins": 0,
                "game_losses": 0,
                "game_draws": 0,
                "games_played": 0,
            }
            opponents[pid] = []

    for pid in list(all_player_ids):
        ensure(pid)

    matches_rows = ws_get_all_records(ws_matches)
    valid = []

    for r in matches_rows:
        if safe_int(r.get("season",0),0) != season:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
            continue
        if not as_bool(r.get("active", "TRUE")):
            continue

        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()
        if not a or not b:
            continue

        result_type = str(r.get("result_type", "normal")).strip().lower()
        if result_type == "bye":
            continue

        ensure(a)
        ensure(b)

        a_gw = safe_int(r.get("a_games_won", 0), 0)
        b_gw = safe_int(r.get("b_games_won", 0), 0)
        d_g = safe_int(r.get("draw_games", 0), 0)
        valid.append((a, b, a_gw, b_gw, d_g))

    for a, b, a_gw, b_gw, d_g in valid:
        stats[a]["matches_played"] += 1
        stats[b]["matches_played"] += 1

        stats[a]["game_wins"] += a_gw
        stats[a]["game_losses"] += b_gw
        stats[a]["game_draws"] += d_g
        stats[a]["games_played"] += (a_gw + b_gw + d_g)

        stats[b]["game_wins"] += b_gw
        stats[b]["game_losses"] += a_gw
        stats[b]["game_draws"] += d_g
        stats[b]["games_played"] += (a_gw + b_gw + d_g)

        if a_gw > b_gw:
            stats[a]["match_points"] += 3
        elif b_gw > a_gw:
            stats[b]["match_points"] += 3
        else:
            stats[a]["match_points"] += 1
            stats[b]["match_points"] += 1

        opponents[a].append(b)
        opponents[b].append(a)

    mwp = {}
    gwp = {}

    for pid, s in stats.items():
        mp = s["match_points"]
        mplayed = s["matches_played"]
        mwp[pid] = 1/3 if mplayed == 0 else floor_333(mp / (3.0 * mplayed))

        gplayed = s["games_played"]
        if gplayed == 0:
            gwp[pid] = 1/3
        else:
            gwp_raw = (s["game_wins"] + 0.5 * s["game_draws"]) / float(gplayed)
            gwp[pid] = floor_333(gwp_raw)

    omw = {}
    ogw = {}

    for pid in stats.keys():
        opps = opponents.get(pid, [])
        if not opps:
            omw[pid] = 1/3
            ogw[pid] = 1/3
        else:
            omw_vals = [mwp.get(oid, 1/3) for oid in opps]
            ogw_vals = [gwp.get(oid, 1/3) for oid in opps]
            omw[pid] = sum(omw_vals) / len(omw_vals)
            ogw[pid] = sum(ogw_vals) / len(ogw_vals)

    rows = []
    for pid, s in stats.items():
        rows.append({
            "season": season,
            "cycle": cycle,
            "player_id": pid,
            "matches_played": s["matches_played"],
            "match_points": s["match_points"],
            "mwp_percent": pct1(mwp[pid]),
            "game_wins": s["game_wins"],
            "game_losses": s["game_losses"],
            "game_draws": s["game_draws"],
            "games_played": s["games_played"],
            "gw_percent": pct1(gwp[pid]),
            "omw_percent": pct1(omw[pid]),
            "ogw_percent": pct1(ogw[pid]),
        })

    rows.sort(
        key=lambda r: (r["match_points"], r["omw_percent"], r["gw_percent"], r["ogw_percent"]),
        reverse=True
    )

    ts = now_iso_utc()
    for i, r in enumerate(rows, start=1):
        r["rank_position"] = i
        r["last_recalc_at"] = ts

    # Reescreve Standings inteiro, mas somente linhas do season+cycle
    # Estratégia simples: limpar tudo e regravar inteiro é ok se você só usa um cycle por vez.
    # Se você quiser manter todos os ciclos/temporadas em Standings, trocamos por "delete rows filter".
    # Aqui vamos manter HISTÓRICO: não vamos limpar tudo. Vamos:
    # - carregar tudo
    # - remover linhas do mesmo season+cycle
    # - anexar novas
    ws = ws_standings
    ensure_sheet_columns(ws, STANDINGS_REQUIRED)

    existing = ws_get_all_values(ws)
    header = existing[0] if existing else STANDINGS_HEADER

    # rebuild
    kept = [header]
    if len(existing) > 1:
        col = {name: i for i, name in enumerate(header)}
        for r in existing[1:]:
            s = r[col["season"]] if col["season"] < len(r) else ""
            c = r[col["cycle"]] if col["cycle"] < len(r) else ""
            if safe_int(s,0) == season and safe_int(c,0) == cycle:
                continue
            kept.append(r)

    # append new
    for r in rows:
        kept.append([
            str(r["season"]), str(r["cycle"]), str(r["player_id"]),
            str(r["matches_played"]), str(r["match_points"]), str(r["mwp_percent"]),
            str(r["game_wins"]), str(r["game_losses"]), str(r["game_draws"]), str(r["games_played"]),
            str(r["gw_percent"]), str(r["omw_percent"]), str(r["ogw_percent"]),
            str(r["rank_position"]), str(r["last_recalc_at"])
        ])

    # rewrite full sheet (cuidado com quota; mas é 1 operação grande)
    with_retry(lambda: ws.clear())
    ws_append_row(ws, kept[0])
    if len(kept) > 1:
        ws_append_rows(ws, kept[1:])

    return rows


# =========================
# Discord Bot
# =========================
class LemeBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

client = LemeBot()


# =========================================================
# Autocomplete
# =========================================================
async def ac_season_any(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        items = list_seasons(ws_seasons)
        cur = str(current).strip()
        if cur:
            items = [s for s in items if str(s).startswith(cur)]
        return [app_commands.Choice(name=f"Season {s}", value=str(s)) for s in items[:25]]
    except Exception:
        return []

async def ac_cycle_open_current_season(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return []

        cycles = list_open_cycles(ws_cycles, season)
        cur = str(current).strip()
        if cur:
            cycles = [c for c in cycles if str(c).startswith(cur)]
        return [app_commands.Choice(name=f"Ciclo {c} (Season {season})", value=str(c)) for c in cycles[:25]]
    except Exception:
        return []

async def ac_cycle_any_current_season(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return []

        rows = ws_get_all_records(ws_cycles)
        cycles = sorted(set(safe_int(r.get("cycle",0),0) for r in rows if safe_int(r.get("season",0),0)==season and safe_int(r.get("cycle",0),0)>0))
        cur = str(current).strip()
        if cur:
            cycles = [c for c in cycles if str(c).startswith(cur)]
        return [app_commands.Choice(name=f"Ciclo {c} (Season {season})", value=str(c)) for c in cycles[:25]]
    except Exception:
        return []

async def ac_match_id_user_pending(interaction: discord.Interaction, current: str):
    try:
        user_id = str(interaction.user.id)
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_get_all_records(ws_matches)
        cur = str(current).strip().lower()

        out = []
        for r in rows:
            if not as_bool(r.get("active","TRUE")):
                continue
            if str(r.get("confirmed_status","")).strip().lower() != "pending":
                continue
            a = str(r.get("player_a_id","")).strip()
            b = str(r.get("player_b_id","")).strip()
            if user_id not in (a,b):
                continue
            mid = str(r.get("match_id","")).strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            pod = str(r.get("pod","")).strip()
            cyc = safe_int(r.get("cycle",0),0)
            seas = safe_int(r.get("season",0),0)
            na = nick_map.get(a,a)
            nb = nick_map.get(b,b)
            label = f"S{seas} C{cyc} Pod {pod}: {na} vs {nb} | {mid}"
            out.append(app_commands.Choice(name=label[:100], value=mid))
        return out[:25]
    except Exception:
        return []

async def ac_match_id_any(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = ws_get_all_records(ws_matches)
        cur = str(current).strip().lower()
        out = []
        for r in rows:
            mid = str(r.get("match_id","")).strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            cyc = safe_int(r.get("cycle",0),0)
            seas = safe_int(r.get("season",0),0)
            pod = str(r.get("pod","")).strip()
            st = str(r.get("confirmed_status","")).strip().lower()
            label = f"S{seas} C{cyc} Pod {pod} [{st}] | {mid}"
            out.append(app_commands.Choice(name=label[:100], value=mid))
        return out[:25]
    except Exception:
        return []


# =========================
# Basics
# =========================
@client.tree.command(name="ping", description="Teste do bot")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("⚓ Pong! Bot online.")

@client.tree.command(name="sheets", description="Teste de conexão com Google Sheets")
async def sheets(interaction: discord.Interaction):
    gc = get_sheets_client()
    if not gc or not SHEET_ID:
        await interaction.response.send_message("⚠️ Sheets não configurado (SHEET_ID ou GOOGLE_SERVICE_ACCOUNT_JSON).")
        return
    try:
        sh = with_retry(lambda: gc.open_by_key(SHEET_ID))
        await interaction.response.send_message(f"✅ Conectado na planilha: **{sh.title}**")
    except Exception as e:
        await interaction.response.send_message(f"❌ Erro ao acessar planilha: `{e}`")


# =========================
# Season: owner-only
# =========================
@client.tree.command(name="season_atual", description="Mostra a Season (temporada) ativa.")
async def season_atual(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        s = get_active_season(ws_seasons)
        if not s:
            return await interaction.followup.send("⚠️ Nenhuma Season ativa. Dono do servidor deve usar `/season_abrir`.", ephemeral=True)
        return await interaction.followup.send(f"✅ Season ativa: **{s}**", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="season_abrir", description="(DONO) Abre uma nova Season e fecha a anterior automaticamente.")
@app_commands.describe(season="Número da season (ex: 2)")
async def season_abrir(interaction: discord.Interaction, season: int):
    if not await is_server_owner(interaction):
        await interaction.response.send_message("❌ Apenas o **DONO do servidor** pode abrir Season.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        if season <= 0:
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)

        # fecha qualquer ativa
        close_any_active_season(ws_seasons)
        # abre a escolhida
        set_season_status(ws_seasons, season, "active")

        await interaction.followup.send(
            f"✅ Season **{season}** aberta como **ACTIVE**.\n"
            "A season anterior (se existia) foi fechada automaticamente.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="season_fechar", description="(DONO) Fecha uma Season específica.")
@app_commands.describe(season="Número da season")
@app_commands.autocomplete(season=ac_season_any)
async def season_fechar(interaction: discord.Interaction, season: str):
    if not await is_server_owner(interaction):
        await interaction.response.send_message("❌ Apenas o **DONO do servidor** pode fechar Season.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        s = safe_int(season, 0)
        if s <= 0:
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)

        set_season_status(ws_seasons, s, "closed")
        await interaction.followup.send(f"✅ Season **{s}** marcada como **CLOSED**.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# MIGRAÇÃO (owner-only)
# =========================
@client.tree.command(name="migrar_season1", description="(DONO) Preenche season=1 onde estiver vazio nas abas principais.")
async def migrar_season1(interaction: discord.Interaction):
    if not await is_server_owner(interaction):
        await interaction.response.send_message("❌ Apenas o DONO do servidor pode migrar.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        # garante seasons
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)

        # se não existir season 1, cria closed
        row1 = get_season_row(ws_seasons, 1)
        if row1 is None:
            set_season_status(ws_seasons, 1, "closed")

        def fill_blank_season(ws, season_col_name="season"):
            vals = ws_get_all_values(ws)
            if len(vals) <= 1:
                return 0
            header = vals[0]
            if season_col_name not in header:
                return 0
            col = header.index(season_col_name)
            changed = 0
            for rown in range(2, len(vals)+1):
                r = vals[rown-1]
                cur = r[col] if col < len(r) else ""
                if str(cur).strip() == "":
                    ws_update(ws, [["1"]], f"{col_letter(col)}{rown}")
                    changed += 1
            return changed

        changed_total = 0

        # Cycles
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        changed_total += fill_blank_season(ws_cycles)

        # Enrollments
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=8000, cols=20)
        changed_total += fill_blank_season(ws_enr)

        # PodsHistory
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=20)
        changed_total += fill_blank_season(ws_pods)

        # Matches (col season)
        ws_matches = sh.worksheet("Matches")
        changed_total += fill_blank_season(ws_matches)

        # Standings
        ws_st = sh.worksheet("Standings")
        changed_total += fill_blank_season(ws_st)

        await interaction.followup.send(
            f"✅ Migração concluída.\n"
            f"Cells season preenchidas com 1: **{changed_total}**\n"
            "Se algo ficou fora (coluna com nome diferente), ajuste manualmente o cabeçalho.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro na migração: {e}", ephemeral=True)


# =========================
# Admin: ciclo (por season ativa)
# =========================
@client.tree.command(name="ciclo_abrir", description="(ADM) Cria/abre um ciclo na Season ativa (status=open).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_abrir(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa. Dono deve usar `/season_abrir`.", ephemeral=True)

        fields = get_cycle_fields(ws_cycles, season, cycle)
        if fields.get("status") == "completed":
            return await interaction.followup.send("❌ Este ciclo está COMPLETED. Não reabrimos por segurança.", ephemeral=True)

        set_cycle_status(ws_cycles, season, cycle, "open")
        await interaction.followup.send(f"✅ Ciclo {cycle} aberto na Season {season} (open).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_encerrar", description="(ADM) Encerra ciclo na Season ativa (status=completed).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_encerrar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa. Dono deve usar `/season_abrir`.", ephemeral=True)

        set_cycle_status(ws_cycles, season, cycle, "completed")
        await interaction.followup.send(f"✅ Ciclo {cycle} encerrado na Season {season} (completed).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_bloquear", description="(ADM) Bloqueia ciclo (status=locked) na Season ativa.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_bloquear(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa. Dono deve usar `/season_abrir`.", ephemeral=True)

        fields = get_cycle_fields(ws_cycles, season, cycle)
        if fields.get("status") == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED não pode virar locked.", ephemeral=True)

        set_cycle_status(ws_cycles, season, cycle, "locked")
        await interaction.followup.send(f"✅ Ciclo {cycle} na Season {season} agora está LOCKED.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Players + Enrollments por ciclo+season
# =========================
@client.tree.command(name="inscrever", description="Inscreve você em um ciclo OPEN (na Season ativa).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_open_current_season)
async def inscrever(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)
        return

    discord_id = interaction.user.id
    nick = interaction.user.display_name
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa. Dono deve abrir Season.", ephemeral=True)

        fields = get_cycle_fields(ws_cycles, season, c)
        st = fields.get("status")

        # IMPORTANTÍSSIMO: não cria ciclo sozinho
        if st is None:
            return await interaction.followup.send(
                f"❌ Ciclo {c} não existe na Season {season}.\n"
                "Um ADM/Organizador precisa abrir com `/ciclo_abrir`.",
                ephemeral=True
            )

        if st == "completed":
            await interaction.followup.send("❌ Este ciclo já foi concluído. Escolha outro.", ephemeral=True)
            return
        if st == "locked":
            await interaction.followup.send("❌ Ciclo LOCKED (inscrição fechada).", ephemeral=True)
            return
        if st != "open":
            await interaction.followup.send(f"❌ Ciclo não está open (status atual: {st}).", ephemeral=True)
            return

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=8000, cols=20)
        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        prow = find_player_row(ws_players, discord_id)
        if prow is None:
            ws_append_row(ws_players, [str(discord_id), nick, "active", nowb, nowb])
        else:
            ws_update(ws_players, [[nick]], f"B{prow}")
            ws_update(ws_players, [["active"]], f"C{prow}")
            ws_update(ws_players, [[nowb]], f"E{prow}")

        data = ws_get_all_values(ws_enr)
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        found_row = None
        for r_i in range(2, len(data) + 1):
            r = data[r_i - 1]
            s = r[col["season"]] if col["season"] < len(r) else ""
            cc = r[col["cycle"]] if col["cycle"] < len(r) else ""
            pid = r[col["player_id"]] if col["player_id"] < len(r) else ""
            if safe_int(s,0) == season and safe_int(cc, 0) == c and str(pid).strip() == str(discord_id):
                found_row = r_i
                break

        if found_row is None:
            ws_append_row(ws_enr, [str(season), str(c), str(discord_id), "active", nowb, nowb])
            await interaction.followup.send(
                f"✅ Inscrito no **Ciclo {c}** (Season {season}).\n"
                "Para registrar deck e decklist, use:\n"
                f"• `/deck cycle:{c} nome:...`\n"
                f"• `/decklist cycle:{c} url:...`",
                ephemeral=True
            )
        else:
            ws_update(ws_enr, [["active"]], f"{col_letter(col['status'])}{found_row}")
            ws_update(ws_enr, [[nowb]], f"{col_letter(col['updated_at'])}{found_row}")
            await interaction.followup.send(f"✅ Inscrição reativada no **Ciclo {c}** (Season {season}).", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscrever: {e}", ephemeral=True)

@client.tree.command(name="drop", description="Sai do ciclo informado (Season ativa) enquanto ciclo OPEN.")
@app_commands.describe(cycle="Número do ciclo (ex: 1)", motivo="Opcional")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def drop(interaction: discord.Interaction, cycle: str, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    discord_id = interaction.user.id
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=8000, cols=20)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        st = get_cycle_fields(ws_cycles, season, c).get("status")
        if st != "open":
            return await interaction.followup.send(f"❌ Drop só permitido com ciclo OPEN. Status atual: {st}", ephemeral=True)

        data = ws_get_all_values(ws_enr)
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        found_row = None
        for r_i in range(2, len(data) + 1):
            r = data[r_i - 1]
            s = r[col["season"]] if col["season"] < len(r) else ""
            cc = r[col["cycle"]] if col["cycle"] < len(r) else ""
            pid = r[col["player_id"]] if col["player_id"] < len(r) else ""
            if safe_int(s,0) == season and safe_int(cc,0) == c and str(pid).strip() == str(discord_id):
                found_row = r_i
                break

        if found_row is None:
            return await interaction.followup.send(f"❌ Você não está inscrito no Ciclo {c}.", ephemeral=True)

        ws_update(ws_enr, [["dropped"]], f"{col_letter(col['status'])}{found_row}")
        ws_update(ws_enr, [[nowb]], f"{col_letter(col['updated_at'])}{found_row}")

        msg = f"✅ Você saiu do **Ciclo {c}** (Season {season})."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)


# =========================
# Deck / Decklist por ciclo+season (1x por ciclo; ADM/Org editam)
# =========================
def _find_deck_row(ws_decks, season: int, cycle: int, player_id: str) -> int | None:
    vals = ws_get_all_values(ws_decks)
    if len(vals) <= 1:
        return None
    col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        s = r[col["season"]] if col["season"] < len(r) else ""
        c = r[col["cycle"]] if col["cycle"] < len(r) else ""
        pid = r[col["player_id"]] if col["player_id"] < len(r) else ""
        if safe_int(s,0) == season and safe_int(c,0) == cycle and str(pid).strip() == str(player_id):
            return rown
    return None

@client.tree.command(name="deck", description="Define seu deck (1 vez por ciclo). Informe o ciclo para evitar erro.")
@app_commands.describe(cycle="Ciclo (ex: 2)", nome="Nome do deck (ex: UR Murktide)")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def deck(interaction: discord.Interaction, cycle: str, nome: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    discord_id = str(interaction.user.id)
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=20)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        st = get_cycle_fields(ws_cycles, season, c).get("status")
        if st is None:
            return await interaction.followup.send("❌ Esse ciclo não existe.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo completed. Deck travado.", ephemeral=True)

        rown = _find_deck_row(ws_decks, season, c, discord_id)
        if rown is None:
            ws_append_row(ws_decks, [str(season), str(c), discord_id, nome, "", nowb, nowb])
            return await interaction.followup.send(f"✅ Deck salvo para **Season {season} / Ciclo {c}**.", ephemeral=True)

        # já existe
        col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
        existing_deck = (ws_decks.cell(rown, col["deck"]+1).value or "").strip()

        if existing_deck and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                "❌ Você já definiu seu deck neste ciclo e não pode alterar.\nPeça para ADM/Organizador se precisar.",
                ephemeral=True
            )

        ws_update(ws_decks, [[nome]], f"{col_letter(col['deck'])}{rown}")
        ws_update(ws_decks, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")
        await interaction.followup.send(f"✅ Deck atualizado para **Season {season} / Ciclo {c}**.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar deck: {e}", ephemeral=True)

@client.tree.command(name="decklist", description="Define sua decklist (1 vez por ciclo). Informe o ciclo para evitar erro.")
@app_commands.describe(cycle="Ciclo (ex: 2)", url="Link (moxfield.com ou ligamagic.com.br)")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def decklist(interaction: discord.Interaction, cycle: str, url: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    ok, val = validate_decklist_url(url)
    if not ok:
        return await interaction.followup.send(f"❌ {val}", ephemeral=True)

    discord_id = str(interaction.user.id)
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=20)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        st = get_cycle_fields(ws_cycles, season, c).get("status")
        if st is None:
            return await interaction.followup.send("❌ Esse ciclo não existe.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo completed. Decklist travada.", ephemeral=True)

        rown = _find_deck_row(ws_decks, season, c, discord_id)
        if rown is None:
            ws_append_row(ws_decks, [str(season), str(c), discord_id, "", val, nowb, nowb])
            return await interaction.followup.send(f"✅ Decklist salva para **Season {season} / Ciclo {c}**.", ephemeral=True)

        col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
        existing_url = (ws_decks.cell(rown, col["decklist_url"]+1).value or "").strip()

        if existing_url and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                "❌ Você já definiu sua decklist neste ciclo e não pode alterar.\nPeça para ADM/Organizador se precisar.",
                ephemeral=True
            )

        ws_update(ws_decks, [[val]], f"{col_letter(col['decklist_url'])}{rown}")
        ws_update(ws_decks, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")
        await interaction.followup.send(f"✅ Decklist atualizada para **Season {season} / Ciclo {c}**.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar decklist: {e}", ephemeral=True)


# =========================
# Pods (gerar/ver) + trava ciclo + calcula prazo
# =========================
@client.tree.command(name="pods_gerar", description="(ADM) Gera pods do ciclo (Season ativa), grava PodsHistory e cria Matches pending.")
@app_commands.describe(cycle="Ciclo (ex: 1)", tamanho="Tamanho do pod (padrão 4)")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def pods_gerar(interaction: discord.Interaction, cycle: str, tamanho: int = 4):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    tamanho = max(2, min(int(tamanho), 8))
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=8000, cols=20)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=20)
        ws_matches = sh.worksheet("Matches")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        fields = get_cycle_fields(ws_cycles, season, c)
        st = fields.get("status")
        if st is None:
            return await interaction.followup.send("❌ Ciclo não existe. Use /ciclo_abrir.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo completed. Não pode gerar pods.", ephemeral=True)
        if st == "locked":
            return await interaction.followup.send("❌ Ciclo locked. Pods já foram gerados.", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Ciclo não está open (status: {st}).", ephemeral=True)

        # impede regeneração
        pods_rows = ws_get_all_records(ws_pods)
        if any(safe_int(r.get("season",0),0) == season and safe_int(r.get("cycle",0),0) == c for r in pods_rows):
            set_cycle_status(ws_cycles, season, c, "locked")
            return await interaction.followup.send(
                "❌ Já existe PodsHistory para esse ciclo. Não é permitido gerar novamente.\nStatus ajustado para LOCKED.",
                ephemeral=True
            )

        enr_rows = ws_get_all_records(ws_enr)
        players = []
        for r in enr_rows:
            if safe_int(r.get("season",0),0) != season:
                continue
            if safe_int(r.get("cycle",0),0) != c:
                continue
            if str(r.get("status","")).strip().lower() != "active":
                continue
            pid = str(r.get("player_id","")).strip()
            if pid:
                players.append(pid)

        if len(players) < 2:
            return await interaction.followup.send("Poucos inscritos ativos para gerar pods.", ephemeral=True)

        past_pairs = get_past_confirmed_pairs(ws_matches, season)
        pods, repeat_score = best_shuffle_min_repeats(players, tamanho, past_pairs, tries=250)

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        nick_map = build_players_nick_map(ws_players)

        # batch append para reduzir chamadas
        pods_to_append = []
        matches_to_append = []

        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"

            for pid in pod_players:
                pods_to_append.append([str(season), str(c), pod_name, pid, nowb])

            for a, b in round_robin_pairs(pod_players):
                mid = new_match_id(season, c, pod_name)
                ac_at = auto_confirm_deadline_iso(utc_now_dt())
                matches_to_append.append([
                    str(season),
                    mid, str(c), pod_name,
                    str(a), str(b),
                    "0", "0", "0",
                    "normal",
                    "pending",
                    "", "", "",
                    "TRUE",
                    nowb, nowb,
                    ac_at
                ])

        if pods_to_append:
            ws_append_rows(ws_pods, pods_to_append)
        if matches_to_append:
            ws_append_rows(ws_matches, matches_to_append)

        set_cycle_status(ws_cycles, season, c, "locked")

        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(season, c, ws_pods, ws_cycles)
        if start_str and deadline_str:
            set_cycle_times(ws_cycles, season, c, start_str, deadline_str)

        lines = [f"🧩 Pods do **Ciclo {c}** (Season {season}) gerados (tamanho base {tamanho})."]
        lines.append(f"♻️ Anti-repetição (Season {season}): penalidade **{repeat_score}**.")
        lines.append("🔒 Ciclo agora está **LOCKED** (inscrição fechada).")
        if start_str and deadline_str:
            lines.append(f"⏳ Prazo (maior POD = {max_pod_size}): **{days} dias**")
            lines.append(f"🕑 Início: **{start_str} (BR)**")
            lines.append(f"🛑 Fim: **{deadline_str} (BR)**")

        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"
            lines.append(f"\n**Pod {pod_name}**")
            for pid in pod_players:
                lines.append(f"• {nick_map.get(pid, pid)} (`{pid}`)")

        lines.append(f"\n✅ Matches criados: **{len(matches_to_append)}** (pending).")
        lines.append("Jogadores: use `/meus_matches cycle:...` para ver IDs.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_gerar: {e}", ephemeral=True)

@client.tree.command(name="pods_ver", description="Mostra pods do ciclo (Season ativa).")
@app_commands.describe(cycle="Ciclo")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def pods_ver(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=20)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        nick_map = build_players_nick_map(ws_players)
        rows = ws_get_all_records(ws_pods)
        rows = [r for r in rows if safe_int(r.get("season",0),0)==season and safe_int(r.get("cycle",0),0)==c]
        if not rows:
            return await interaction.followup.send("Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        pods = {}
        for r in rows:
            pod = str(r.get("pod","")).strip()
            pid = str(r.get("player_id","")).strip()
            pods.setdefault(pod, []).append(pid)

        out = [f"🧩 Pods — **Season {season} / Ciclo {c}**"]
        for pod in sorted(pods.keys()):
            out.append(f"\n**Pod {pod}**")
            for pid in pods[pod]:
                out.append(f"• {nick_map.get(pid,pid)} (`{pid}`)")

        await interaction.followup.send("\n".join(out), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Prazo do ciclo
# =========================
@client.tree.command(name="prazo", description="Mostra o prazo oficial do ciclo (Season ativa).")
@app_commands.describe(cycle="Ciclo")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def prazo(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=20)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(season, c, ws_pods, ws_cycles)
        if not deadline_str:
            return await interaction.followup.send("❌ Ainda não existem pods para esse ciclo.", ephemeral=True)

        set_cycle_times(ws_cycles, season, c, start_str, deadline_str)

        deadline_dt = parse_br_dt(deadline_str)
        nowdt = now_br_dt()
        remaining = deadline_dt - nowdt if deadline_dt else None
        rem_text = "—"
        if remaining:
            secs = int(remaining.total_seconds())
            if secs <= 0:
                rem_text = "EXPIRADO"
            else:
                rem_text = f"{secs//86400}d { (secs%86400)//3600 }h"

        await interaction.followup.send(
            f"⏳ **Prazo — Season {season} / Ciclo {c}**\n"
            f"Maior POD: **{max_pod_size}** → **{days} dias corridos**\n"
            f"Início (BR): **{start_str}**\n"
            f"Fim (BR): **{deadline_str}**\n"
            f"Tempo restante: **{rem_text}**\n\n"
            "Regra: todos os resultados devem ser informados antes do último dia do ciclo.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# /meus_matches
# =========================
@client.tree.command(name="meus_matches", description="Lista seus matches do ciclo (Season ativa).")
@app_commands.describe(cycle="Ciclo")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def meus_matches(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    user_id = str(interaction.user.id)
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_get_all_records(ws_matches)
        my = []
        nowu = utc_now_dt()

        for r in rows:
            if safe_int(r.get("season",0),0) != season:
                continue
            if safe_int(r.get("cycle",0),0) != c:
                continue
            if not as_bool(r.get("active","TRUE")):
                continue

            a = str(r.get("player_a_id","")).strip()
            b = str(r.get("player_b_id","")).strip()
            if user_id not in (a,b):
                continue

            mid = str(r.get("match_id","")).strip()
            pod = str(r.get("pod","")).strip()
            st = str(r.get("confirmed_status","")).strip().lower()
            rep = str(r.get("reported_by_id","")).strip()
            ag = str(r.get("a_games_won","0"))
            bg = str(r.get("b_games_won","0"))
            dg = str(r.get("draw_games","0"))

            ac = parse_iso_dt(r.get("auto_confirm_at","") or "")
            left = "—"
            if st == "pending":
                if not rep:
                    left = "aguardando report"
                elif ac:
                    secs = int((ac - nowu).total_seconds())
                    left = "EXPIRADO" if secs <= 0 else f"{secs//3600}h"

            opp = b if user_id == a else a
            line = f"• `{mid}` | Pod {pod} | vs {nick_map.get(opp, opp)} | {st} | {ag}-{bg}-{dg} | {left}"
            my.append((pod, line))

        if not my:
            return await interaction.followup.send(f"Você não tem matches no Ciclo {c}.", ephemeral=True)

        my.sort(key=lambda x: (x[0], x[1]))
        out = [f"📌 **Seus matches — Season {season} / Ciclo {c}**"]
        out.extend([x[1] for x in my])
        out.append("\nPara reportar: `/resultado match_id:... placar:...`")
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Resultado / Rejeitar
# =========================
@client.tree.command(name="resultado", description="Registra seu resultado (PENDENTE). O oponente tem 48h para rejeitar.")
@app_commands.describe(match_id="ID do match", placar="Vitória-Derrota-Empate (V-D-E) do reportante")
@app_commands.autocomplete(match_id=ac_match_id_user_pending)
@app_commands.choices(
    placar=[
        app_commands.Choice(name="2-0-0", value="2-0-0"),
        app_commands.Choice(name="2-1-0", value="2-1-0"),
        app_commands.Choice(name="1-2-0", value="1-2-0"),
        app_commands.Choice(name="0-2-0", value="0-2-0"),
        app_commands.Choice(name="1-1-1 (empate jogado)", value="1-1-1"),
        app_commands.Choice(name="0-0-0 (empate sem jogo)", value="0-0-0"),
        app_commands.Choice(name="0-0-3 (empate intencional)", value="0-0-3"),
    ]
)
async def resultado(interaction: discord.Interaction, match_id: str, placar: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True)

    reporter_id = str(interaction.user.id)
    nowb = now_br_str()

    sc = parse_score_3parts(placar.value)
    if not sc:
        return await interaction.followup.send("❌ Placar inválido.", ephemeral=True)

    v, d, e = sc
    ok, msg = validate_3parts_rules(v, d, e)
    if not ok:
        return await interaction.followup.send(f"❌ {msg}", ephemeral=True)

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws_find(ws, str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if not as_bool(getc("active") or "TRUE"):
            return await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            return await interaction.followup.send(f"❌ Match não está pending (atual: {status}).", ephemeral=True)

        a_id = (getc("player_a_id") or "").strip()
        b_id = (getc("player_b_id") or "").strip()

        if reporter_id not in (a_id, b_id):
            return await interaction.followup.send("❌ Você não faz parte deste match.", ephemeral=True)

        if reporter_id == a_id:
            a_gw, b_gw, d_g = v, d, e
        else:
            a_gw, b_gw, d_g = d, v, e

        rt = "normal"
        if a_gw == b_gw:
            rt = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            rt = "intentional_draw"

        ac_at = auto_confirm_deadline_iso(utc_now_dt())

        ws_update(ws, [[str(a_gw)]], f"{col_letter(col['a_games_won'])}{rown}")
        ws_update(ws, [[str(b_gw)]], f"{col_letter(col['b_games_won'])}{rown}")
        ws_update(ws, [[str(d_g)]], f"{col_letter(col['draw_games'])}{rown}")
        ws_update(ws, [[rt]], f"{col_letter(col['result_type'])}{rown}")
        ws_update(ws, [[reporter_id]], f"{col_letter(col['reported_by_id'])}{rown}")
        ws_update(ws, [[""]], f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws_update(ws, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")
        ws_update(ws, [[ac_at]], f"{col_letter(col['auto_confirm_at'])}{rown}")

        await interaction.followup.send(
            "✅ Resultado registrado como **PENDENTE**.\n"
            f"Match: **{match_id}**\n"
            f"Seu placar (V-D-E): **{v}-{d}-{e}**\n"
            "Oponente tem **48h** para `/rejeitar`.\n"
            "Se não rejeitar, vira oficial automaticamente (na próxima varredura do `/recalcular`).",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /resultado: {e}", ephemeral=True)

@client.tree.command(name="rejeitar", description="Rejeita um resultado pendente (apenas o oponente, até 48h).")
@app_commands.describe(match_id="ID do match", motivo="Opcional")
async def rejeitar(interaction: discord.Interaction, match_id: str, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)
    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws_find(ws, str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if not as_bool(getc("active") or "TRUE"):
            return await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            return await interaction.followup.send(f"❌ Match não está pending (atual: {status}).", ephemeral=True)

        reported_by = (getc("reported_by_id") or "").strip()
        if not reported_by:
            return await interaction.followup.send("❌ Ainda não existe resultado reportado.", ephemeral=True)

        a_id = (getc("player_a_id") or "").strip()
        b_id = (getc("player_b_id") or "").strip()

        opponent_allowed = a_id if reported_by == b_id else b_id
        if user_id != opponent_allowed:
            return await interaction.followup.send("❌ Apenas o **oponente** pode rejeitar.", ephemeral=True)

        ac = parse_iso_dt(getc("auto_confirm_at") or "")
        if ac and utc_now_dt() > ac:
            return await interaction.followup.send("❌ Prazo expirou (48h). Peça para ADM revisar.", ephemeral=True)

        nowb = now_br_str()
        ws_update(ws, [["rejected"]], f"{col_letter(col['confirmed_status'])}{rown}")
        ws_update(ws, [[user_id]], f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws_update(ws, [[nowb]], f"{col_letter(col['updated_at'])}{rown}")

        msg = "✅ Resultado rejeitado. ADM/Organizador pode corrigir."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================
# Admin: /recalcular, /ranking, /standings_publicar
# =========================
@client.tree.command(name="recalcular", description="(ADM) Auto-confirm (48h) + recalcula ranking do ciclo (Season ativa).")
@app_commands.describe(cycle="Ciclo")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def recalcular(interaction: discord.Interaction, cycle: str):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=True)

        changed = 0
        try:
            changed = sweep_auto_confirm(sh, season, c)
        except Exception:
            pass

        rows = recalculate_cycle(season, c)
        await interaction.followup.send(
            f"✅ Recalculo concluído.\nSeason {season} / Ciclo {c}\n"
            f"Auto-confirm (48h) feitos: **{changed}**\n"
            f"Jogadores no Standings: **{len(rows)}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"⚠️ Erro no recálculo: {e}", ephemeral=True)

@client.tree.command(name="ranking", description="Mostra ranking do ciclo (Season ativa).")
@app_commands.describe(cycle="Ciclo", top="Quantos mostrar (padrão 20)")
@app_commands.autocomplete(cycle=ac_cycle_any_current_season)
async def ranking(interaction: discord.Interaction, cycle: str, top: int = 20):
    await interaction.response.defer(ephemeral=False)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=False)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=10)
        ws_st = sh.worksheet("Standings")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_st, STANDINGS_REQUIRED)

        season = get_active_season(ws_seasons)
        if not season:
            return await interaction.followup.send("❌ Nenhuma Season ativa.", ephemeral=False)

        nick_map = build_players_nick_map(ws_players)
        data = ws_get_all_records(ws_st)
        rows = [r for r in data if safe_int(r.get("season",0),0)==season and safe_int(r.get("cycle",0),0)==c]
        if not rows:
            return await interaction.followup.send("Sem standings. Rode `/recalcular`.", ephemeral=False)

        top = max(5, min(top, 50))
        rows.sort(key=lambda r: safe_int(r.get("rank_position", 9999), 9999))

        out = [f"🏆 **Ranking — Season {season} / Ciclo {c}** (Top {top})"]
        out.append("pos | jogador | pts | OMW | GW | OGW")
        out.append("--- | ------ | --- | --- | --- | ---")

        for r in rows[:top]:
            pid = str(r.get("player_id","")).strip()
            out.append(
                f"{r.get('rank_position','?')} | {nick_map.get(pid,pid)} | {r.get('match_points','')} | "
                f"{r.get('omw_percent','')} | {r.get('gw_percent','')} | {r.get('ogw_percent','')}"
            )

        await interaction.followup.send("\n".join(out), ephemeral=False)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ranking: {e}", ephemeral=False)

@client.tree.command(name="ranking_season", description="Mostra ranking de um ciclo em uma Season passada (consulta histórica).")
@app_commands.describe(season="Season desejada", cycle="Ciclo", top="Top (padrão 20)")
@app_commands.autocomplete(season=ac_season_any)
async def ranking_season(interaction: discord.Interaction, season: str, cycle: int, top: int = 20):
    await interaction.response.defer(ephemeral=False)
    s = safe_int(season,0)
    if s <= 0 or cycle <= 0:
        return await interaction.followup.send("❌ Season ou ciclo inválido.", ephemeral=False)

    try:
        sh = open_sheet()
        ws_st = sh.worksheet("Standings")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=3000, cols=30)
        ensure_sheet_columns(ws_st, STANDINGS_REQUIRED)

        nick_map = build_players_nick_map(ws_players)
        data = ws_get_all_records(ws_st)
        rows = [r for r in data if safe_int(r.get("season",0),0)==s and safe_int(r.get("cycle",0),0)==cycle]
        if not rows:
            return await interaction.followup.send("Sem standings para esse season/cycle.", ephemeral=False)

        top = max(5, min(top, 50))
        rows.sort(key=lambda r: safe_int(r.get("rank_position", 9999), 9999))

        out = [f"🏆 **Ranking — Season {s} / Ciclo {cycle}** (Top {top})"]
        for r in rows[:top]:
            pid = str(r.get("player_id","")).strip()
            out.append(
                f"{r.get('rank_position','?')}. {nick_map.get(pid,pid)} "
                f"| pts {r.get('match_points','')} | OMW {r.get('omw_percent','')} | GW {r.get('gw_percent','')} | OGW {r.get('ogw_percent','')}"
            )
        await interaction.followup.send("\n".join(out), ephemeral=False)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=False)


# =========================
# Start
# =========================
if not DISCORD_TOKEN:
    raise RuntimeError("Faltou a variável DISCORD_TOKEN no ambiente.")

keep_alive()
client.run(DISCORD_TOKEN)
