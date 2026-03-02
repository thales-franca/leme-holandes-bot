# main.py
import os
import json
import threading
import random
import csv
import io
import time as pytime
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta, time, date

import discord
from discord import app_commands
from flask import Flask

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# LEME HOLANDÊS BOT (Discord + Google Sheets + Render)
# =========================================================
# ✅ PRINCIPAIS REGRAS (as suas):
# - Ranking oficial (por CICLO): Pontos > OMW% > GW% > OGW%
# - Piso 33,3% (1/3) em MWP/GWP antes de calcular OMW/OGW
# - Sempre recalcular tudo do zero (sem incremental)
# - Resultado do jogador: V-D-E em GAMES
# - Empate em games conta como 0.5 na GWP
# - Rejeição: 48h para rejeitar (se não, vira confirmed via varredura /recalcular)
# - Prazo do ciclo (/prazo):
#   POD 3 -> 5 dias corridos
#   POD 4 -> 8 dias corridos
#   POD 5 ou 6 -> 10 dias corridos
#   Ciclo começa 14:00 (BR) e termina no último dia às 13:59 (BR)
# - Ciclos NÃO podem abrir sozinhos.
#   ✅ Só abrem por comando (ADM/Organizador): /ciclo_abrir
# - Seasons:
#   ✅ Tudo na MESMA planilha, com histórico.
#   ✅ Nova season e fechar anteriores: APENAS DONO do servidor.
# - Deck/Decklist:
#   ✅ Jogador pode cadastrar 1x por ciclo (não trava para outros ciclos).
#   ✅ Comandos /deck e /decklist pedem o CICLO explicitamente (pra evitar erro).
#
# ✅ ABAS:
# - Players: cadastro permanente (nick, status)
# - Decks: deck/decklist por season+ciclo+jogador (1x por ciclo)
# - Seasons: controle de temporadas
# - Cycles: ciclos por season
# - Enrollments: inscrição por ciclo
# - PodsHistory: pods por ciclo
# - Matches: confrontos + resultados
# - Standings: ranking por ciclo
#
# Observação:
# - Para manter compatibilidade, existe /migrar_season1 que carimba season=1
#   em linhas antigas que não tenham season preenchido.
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
# Small cooldown (reduz spam / quota)
# =========================
_COOLDOWN = {}  # (user_id, cmd) -> epoch seconds
def cooldown_ok(user_id: int, cmd: str, seconds: int = 2) -> bool:
    key = (int(user_id), str(cmd))
    now = pytime.time()
    last = _COOLDOWN.get(key, 0)
    if now - last < seconds:
        return False
    _COOLDOWN[key] = now
    return True


# =========================
# Google Sheets helpers
# =========================
_GC = None
_GC_TS = 0

def get_sheets_client():
    global _GC, _GC_TS
    if not SERVICE_JSON:
        return None
    # cache simples (evita reautenticar toda hora)
    if _GC and (pytime.time() - _GC_TS) < 600:
        return _GC
    data = json.loads(SERVICE_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    _GC = gspread.authorize(creds)
    _GC_TS = pytime.time()
    return _GC

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

def ensure_worksheet(sh, title: str, header: list[str], rows: int = 2000, cols: int = 40):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    current = ws.row_values(1)
    if not current:
        ws.append_row(header)
    else:
        # se faltar coluna nova, atualiza cabeçalho (append ao final)
        missing = [c for c in header if c not in current]
        if missing:
            new_header = current + missing
            ws.update([new_header], range_name=f"A1:{col_letter(len(new_header)-1)}1")
    return ws

def header_index(ws) -> dict[str, int]:
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"Aba '{ws.title}' sem cabeçalho na linha 1.")
    return {name: i for i, name in enumerate(header)}

def require_cols(ws, required_cols: list[str]) -> dict[str, int]:
    idx = header_index(ws)
    missing = [c for c in required_cols if c not in idx]
    if missing:
        raise RuntimeError(f"Aba '{ws.title}' sem colunas: {', '.join(missing)}")
    return idx

def find_row_by_cols(all_values: list[list[str]], col_idx: dict[str,int], cond: dict[str,str]) -> int | None:
    # retorna row number 1-based
    if len(all_values) <= 1:
        return None
    for rown in range(2, len(all_values)+1):
        r = all_values[rown-1]
        ok = True
        for k, v in cond.items():
            ci = col_idx[k]
            cell = r[ci] if ci < len(r) else ""
            if str(cell).strip() != str(v).strip():
                ok = False
                break
        if ok:
            return rown
    return None

def build_players_nick_map(ws_players) -> dict[str, str]:
    data = ws_players.get_all_values()
    idx = require_cols(ws_players, ["discord_id","nick"])
    out = {}
    for i in range(2, len(data)+1):
        r = data[i-1]
        pid = (r[idx["discord_id"]] if idx["discord_id"] < len(r) else "").strip()
        nk  = (r[idx["nick"]] if idx["nick"] < len(r) else "").strip()
        if pid:
            out[pid] = nk or pid
    return out


# =========================
# Auth helpers
# =========================
async def get_member(interaction: discord.Interaction):
    if not interaction.guild or not interaction.user:
        return None
    m = interaction.guild.get_member(interaction.user.id)
    if m:
        return m
    try:
        return await interaction.guild.fetch_member(interaction.user.id)
    except Exception:
        return None

async def has_role(interaction: discord.Interaction, role_name: str) -> bool:
    m = await get_member(interaction)
    if not m:
        return False
    return any(r.name == role_name for r in m.roles)

async def is_admin_or_organizer(interaction: discord.Interaction) -> bool:
    m = await get_member(interaction)
    if not m:
        return False
    if m.guild_permissions.administrator or m.guild_permissions.manage_guild:
        return True
    if await has_role(interaction, ROLE_ADM):
        return True
    if await has_role(interaction, ROLE_ORGANIZADOR):
        return True
    return False

async def is_organizer_only(interaction: discord.Interaction) -> bool:
    return await has_role(interaction, ROLE_ORGANIZADOR)

async def is_owner_only(interaction: discord.Interaction) -> bool:
    if not interaction.guild or not interaction.user:
        return False
    return interaction.user.id == interaction.guild.owner_id


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
# Headers / Schema
# =========================
PLAYERS_HEADER = ["discord_id","nick","status","created_at","updated_at"]

SEASONS_HEADER = ["season","status","created_at","updated_at","closed_at"]  # status: open|closed
CYCLES_HEADER  = ["season","cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]  # status: open|locked|completed
ENROLLMENTS_HEADER = ["season","cycle","player_id","status","created_at","updated_at"]

DECKS_HEADER = ["season","cycle","player_id","deck","decklist_url","created_at","updated_at"]  # 1 por ciclo

PODSHISTORY_HEADER = ["season","cycle","pod","player_id","created_at"]
MATCHES_HEADER = [
    "season","match_id","cycle","pod",
    "player_a_id","player_b_id",
    "a_games_won","b_games_won","draw_games",
    "result_type","confirmed_status",
    "reported_by_id","confirmed_by_id",
    "message_id","active",
    "created_at","updated_at","auto_confirm_at"
]
STANDINGS_HEADER = [
    "season","cycle","player_id","matches_played","match_points","mwp_percent",
    "game_wins","game_losses","game_draws","games_played","gw_percent","omw_percent","ogw_percent",
    "rank_position","last_recalc_at"
]


# =========================
# Season helpers
# =========================
def get_current_season(sh) -> int:
    ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
    vals = ws.get_all_values()
    idx = require_cols(ws, ["season","status"])
    current = 0
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        st = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower()
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        if st == "open" and ss > current:
            current = ss
    if current == 0:
        # se não existir, cria season 1 aberta
        nowb = now_br_str()
        ws.append_row(["1","open",nowb,nowb,""], value_input_option="USER_ENTERED")
        current = 1
    return current

def set_season_status(ws_seasons, season: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    vals = ws_seasons.get_all_values()
    idx = require_cols(ws_seasons, ["season","status","updated_at","closed_at"])
    rown = None
    for i in range(2, len(vals)+1):
        r = vals[i-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        if ss == season:
            rown = i
            break
    if rown is None:
        ws_seasons.append_row([str(season), status, nowb, nowb, (nowb if status=="closed" else "")], value_input_option="USER_ENTERED")
    else:
        ws_seasons.update([[status]], range_name=f"{col_letter(idx['status'])}{rown}")
        ws_seasons.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")
        if status == "closed":
            ws_seasons.update([[nowb]], range_name=f"{col_letter(idx['closed_at'])}{rown}")


# =========================
# Cycle helpers (agora com season)
# =========================
def get_cycle_row(ws_cycles, season: int, cycle: int) -> int | None:
    vals = ws_cycles.get_all_values()
    idx = require_cols(ws_cycles, ["season","cycle"])
    if len(vals) <= 1:
        return None
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
        if ss == season and cc == cycle:
            return rown
    return None

def get_cycle_fields(ws_cycles, season: int, cycle: int) -> dict:
    vals = ws_cycles.get_all_values()
    idx = require_cols(ws_cycles, ["season","cycle","status","start_at_br","deadline_at_br"])
    out = {"season": season, "cycle": cycle, "status": None, "start_at_br": "", "deadline_at_br": ""}
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
        if ss == season and cc == cycle:
            out["status"] = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower()
            out["start_at_br"] = (r[idx["start_at_br"]] if idx["start_at_br"] < len(r) else "").strip()
            out["deadline_at_br"] = (r[idx["deadline_at_br"]] if idx["deadline_at_br"] < len(r) else "").strip()
            return out
    return out

def set_cycle_status(ws_cycles, season: int, cycle: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    idx = require_cols(ws_cycles, ["season","cycle","status","created_at","updated_at","start_at_br","deadline_at_br"])
    rown = get_cycle_row(ws_cycles, season, cycle)
    if rown is None:
        ws_cycles.append_row([str(season), str(cycle), status, "", "", nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[status]], range_name=f"{col_letter(idx['status'])}{rown}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")

def set_cycle_times(ws_cycles, season: int, cycle: int, start_at_br: str, deadline_at_br: str):
    nowb = now_br_str()
    idx = require_cols(ws_cycles, ["season","cycle","start_at_br","deadline_at_br","updated_at","status","created_at"])
    rown = get_cycle_row(ws_cycles, season, cycle)
    if rown is None:
        ws_cycles.append_row([str(season), str(cycle), "open", start_at_br, deadline_at_br, nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[start_at_br]], range_name=f"{col_letter(idx['start_at_br'])}{rown}")
        ws_cycles.update([[deadline_at_br]], range_name=f"{col_letter(idx['deadline_at_br'])}{rown}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")

def list_open_cycles(ws_cycles, season: int) -> list[int]:
    vals = ws_cycles.get_all_values()
    idx = require_cols(ws_cycles, ["season","cycle","status"])
    out = []
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        if ss != season:
            continue
        st = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower()
        cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
        if cc > 0 and st == "open":
            out.append(cc)
    out = sorted(set(out))
    return out


# =========================
# Match helpers
# =========================
def new_match_id(season: int, cycle: int, pod: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    rnd = random.randint(1000, 9999)
    return f"S{season}-C{cycle}-P{pod}-{ts}-{rnd}"

def auto_confirm_deadline_iso(created_utc: datetime) -> str:
    return (created_utc + timedelta(hours=48)).isoformat()

def round_robin_pairs(players: list[str]):
    pairs = []
    n = len(players)
    for i in range(n):
        for j in range(i + 1, n):
            pairs.append((players[i], players[j]))
    return pairs

def sweep_auto_confirm(ws_matches, season: int, cycle: int) -> int:
    vals = ws_matches.get_all_values()
    idx = require_cols(ws_matches, ["season","cycle","active","confirmed_status","reported_by_id","auto_confirm_at","confirmed_by_id","updated_at"])
    changed = 0
    nowu = utc_now_dt()
    nowb = now_br_str()

    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
        if ss != season or cc != cycle:
            continue

        active = (r[idx["active"]] if idx["active"] < len(r) else "TRUE")
        if not as_bool(active):
            continue

        st = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
        if st != "pending":
            continue

        reported_by = (r[idx["reported_by_id"]] if idx["reported_by_id"] < len(r) else "").strip()
        if not reported_by:
            continue

        acs = (r[idx["auto_confirm_at"]] if idx["auto_confirm_at"] < len(r) else "").strip()
        ac = parse_iso_dt(acs)
        if not ac:
            continue

        if ac <= nowu:
            ws_matches.update([["confirmed"]], range_name=f"{col_letter(idx['confirmed_status'])}{rown}")
            ws_matches.update([["AUTO"]], range_name=f"{col_letter(idx['confirmed_by_id'])}{rown}")
            ws_matches.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")
            changed += 1

    return changed


# =========================
# Anti-repetição (heurística)
# =========================
def get_past_confirmed_pairs(ws_matches, season: int) -> set[frozenset]:
    vals = ws_matches.get_all_values()
    idx = require_cols(ws_matches, ["season","active","confirmed_status","result_type","player_a_id","player_b_id"])
    pairs = set()
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        if ss != season:
            continue
        if not as_bool(r[idx["active"]] if idx["active"] < len(r) else "TRUE"):
            continue
        if (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower() != "confirmed":
            continue
        if (r[idx["result_type"]] if idx["result_type"] < len(r) else "").strip().lower() == "bye":
            continue
        a = (r[idx["player_a_id"]] if idx["player_a_id"] < len(r) else "").strip()
        b = (r[idx["player_b_id"]] if idx["player_b_id"] < len(r) else "").strip()
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
# Prazo do ciclo baseado no maior POD
# =========================
def cycle_days_by_max_pod(max_pod_size: int) -> int:
    if max_pod_size <= 3:
        return 5
    if max_pod_size == 4:
        return 8
    return 10

def compute_cycle_start_deadline_br(season: int, cycle: int, ws_pods, ws_cycles) -> tuple[str, str, int, int]:
    fields = get_cycle_fields(ws_cycles, season, cycle)
    start_dt = parse_br_dt(fields["start_at_br"]) if fields.get("start_at_br") else None

    vals = ws_pods.get_all_values()
    idx = require_cols(ws_pods, ["season","cycle","pod","player_id","created_at"])

    pods = {}
    created_candidates = []
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
        if ss != season or cc != cycle:
            continue
        pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
        pid = (r[idx["player_id"]] if idx["player_id"] < len(r) else "").strip()
        if pod and pid:
            pods.setdefault(pod, set()).add(pid)
        cdt = parse_br_dt(r[idx["created_at"]] if idx["created_at"] < len(r) else "")
        if cdt:
            created_candidates.append(cdt)

    if not pods:
        return ("", "", 0, 0)

    max_pod_size = max(len(v) for v in pods.values())
    days = cycle_days_by_max_pod(max_pod_size)

    if start_dt is None:
        base_date = (min(created_candidates).astimezone(BR_TZ).date() if created_candidates else now_br_dt().date())
        start_dt = datetime.combine(base_date, time(14, 0), tzinfo=BR_TZ)

    deadline_date = (start_dt.date() + timedelta(days=days))
    deadline_dt = datetime.combine(deadline_date, time(13, 59), tzinfo=BR_TZ)

    return (fmt_br_dt(start_dt), fmt_br_dt(deadline_dt), max_pod_size, days)


# =========================
# Recalculo oficial (CICLO)
# =========================
def recalculate_cycle(season: int, cycle: int):
    sh = open_sheet()
    ws_players  = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
    ws_matches  = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
    ws_standings = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=20000, cols=40)

    # auto-confirm antes de calcular
    try:
        sweep_auto_confirm(ws_matches, season, cycle)
    except Exception:
        pass

    # players map
    players_vals = ws_players.get_all_values()
    pidx = require_cols(ws_players, ["discord_id"])
    all_player_ids = set()
    for rown in range(2, len(players_vals)+1):
        r = players_vals[rown-1]
        pid = (r[pidx["discord_id"]] if pidx["discord_id"] < len(r) else "").strip()
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

    ws_vals = ws_matches.get_all_values()
    midx = require_cols(ws_matches, ["season","cycle","confirmed_status","active","result_type","player_a_id","player_b_id","a_games_won","b_games_won","draw_games"])
    valid = []

    for rown in range(2, len(ws_vals)+1):
        r = ws_vals[rown-1]
        ss = safe_int(r[midx["season"]] if midx["season"] < len(r) else "0", 0)
        cc = safe_int(r[midx["cycle"]] if midx["cycle"] < len(r) else "0", 0)
        if ss != season or cc != cycle:
            continue
        if not as_bool(r[midx["active"]] if midx["active"] < len(r) else "TRUE"):
            continue
        if (r[midx["confirmed_status"]] if midx["confirmed_status"] < len(r) else "").strip().lower() != "confirmed":
            continue
        if (r[midx["result_type"]] if midx["result_type"] < len(r) else "").strip().lower() == "bye":
            continue

        a = (r[midx["player_a_id"]] if midx["player_a_id"] < len(r) else "").strip()
        b = (r[midx["player_b_id"]] if midx["player_b_id"] < len(r) else "").strip()
        if not a or not b:
            continue

        a_gw = safe_int(r[midx["a_games_won"]] if midx["a_games_won"] < len(r) else "0", 0)
        b_gw = safe_int(r[midx["b_games_won"]] if midx["b_games_won"] < len(r) else "0", 0)
        d_g  = safe_int(r[midx["draw_games"]] if midx["draw_games"] < len(r) else "0", 0)

        ensure(a)
        ensure(b)
        valid.append((a, b, a_gw, b_gw, d_g))

    # acumula
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

    # mwp/gwp com piso
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

    # omw/ogw (média dos oponentes)
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

    # escreve standings (apenas season+cycle)
    all_st = ws_standings.get_all_values()
    sidx = require_cols(ws_standings, ["season","cycle"])
    kept = [all_st[0]]  # header
    for rown in range(2, len(all_st)+1):
        rr = all_st[rown-1]
        ss = safe_int(rr[sidx["season"]] if sidx["season"] < len(rr) else "0", 0)
        cc = safe_int(rr[sidx["cycle"]] if sidx["cycle"] < len(rr) else "0", 0)
        if not (ss == season and cc == cycle):
            kept.append(rr)

    # regrava tudo (mantém histórico dos outros ciclos)
    ws_standings.clear()
    ws_standings.append_row(STANDINGS_HEADER)
    if len(kept) > 1:
        ws_standings.append_rows(kept[1:])

    values = []
    for r in rows:
        values.append([
            r["season"], r["cycle"], r["player_id"], r["matches_played"], r["match_points"], r["mwp_percent"],
            r["game_wins"], r["game_losses"], r["game_draws"], r["games_played"],
            r["gw_percent"], r["omw_percent"], r["ogw_percent"], r["rank_position"], r["last_recalc_at"]
        ])
    if values:
        ws_standings.append_rows(values)

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
async def ac_cycle_open(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        open_cycles = list_open_cycles(ws_cycles, season)
        cur = str(current).strip()
        if cur:
            open_cycles = [c for c in open_cycles if str(c).startswith(cur)]
        return [app_commands.Choice(name=f"Season {season} - Ciclo {c} (aberto)", value=str(c)) for c in open_cycles[:25]]
    except Exception:
        base = [str(i) for i in range(1, 21)]
        cur = str(current).strip()
        if cur:
            base = [x for x in base if x.startswith(cur)]
        return [app_commands.Choice(name=f"Ciclo {x}", value=x) for x in base[:25]]

async def ac_cycle_any(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        vals = ws_cycles.get_all_values()
        idx = require_cols(ws_cycles, ["season","cycle","status"])
        out = []
        cur = str(current).strip()
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            if ss != season:
                continue
            cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            st = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower()
            if cc <= 0:
                continue
            if cur and not str(cc).startswith(cur):
                continue
            out.append(app_commands.Choice(name=f"Season {season} - Ciclo {cc} [{st}]", value=str(cc)))
        out.sort(key=lambda x: safe_int(x.value, 0))
        return out[:25]
    except Exception:
        return []

async def ac_match_id_user_pending(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_matches.get_all_values()
        idx = require_cols(ws_matches, ["season","match_id","cycle","pod","player_a_id","player_b_id","confirmed_status","active","reported_by_id"])
        uid = str(interaction.user.id)
        cur = str(current).strip().lower()

        out = []
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            if ss != season:
                continue
            if not as_bool(r[idx["active"]] if idx["active"] < len(r) else "TRUE"):
                continue
            st = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
            if st != "pending":
                continue
            a = (r[idx["player_a_id"]] if idx["player_a_id"] < len(r) else "").strip()
            b = (r[idx["player_b_id"]] if idx["player_b_id"] < len(r) else "").strip()
            if uid not in (a, b):
                continue
            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
            na = nick_map.get(a, a)
            nb = nick_map.get(b, b)
            label = f"Pod {pod}: {na} vs {nb} | {mid}"
            out.append(app_commands.Choice(name=label[:100], value=mid))
        return out[:25]
    except Exception:
        return []

async def ac_match_id_any(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)

        vals = ws_matches.get_all_values()
        idx = require_cols(ws_matches, ["season","match_id","cycle","pod","confirmed_status"])
        cur = str(current).strip().lower()

        out = []
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            if ss != season:
                continue
            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            cyc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
            st = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
            out.append(app_commands.Choice(name=f"C{cyc} Pod {pod} [{st}] | {mid}"[:100], value=mid))
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
        sh = gc.open_by_key(SHEET_ID)
        await interaction.response.send_message(f"✅ Conectado na planilha: **{sh.title}**")
    except Exception as e:
        await interaction.response.send_message(f"❌ Erro ao acessar planilha: `{e}`")


# =========================
# Season (OWNER ONLY para abrir/fechar)
# =========================
@client.tree.command(name="season_atual", description="Mostra a season atual (aberta).")
async def season_atual(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        s = get_current_season(sh)
        await interaction.followup.send(f"📌 Season atual (aberta): **{s}**", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="season_abrir", description="(DONO) Abre nova season e fecha a anterior.")
@app_commands.describe(season="Número da nova season (ex: 2). Se vazio, abre a próxima.")
async def season_abrir(interaction: discord.Interaction, season: int = 0):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o **DONO do servidor** pode abrir season.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        nowb = now_br_str()

        cur = get_current_season(sh)
        new = season if season > 0 else (cur + 1)

        # fecha a atual
        set_season_status(ws, cur, "closed")
        # abre a nova
        set_season_status(ws, new, "open")

        await interaction.followup.send(f"✅ Season **{new}** aberta. Season anterior (**{cur}**) foi fechada.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="season_fechar", description="(DONO) Fecha uma season específica.")
@app_commands.describe(season="Número da season")
async def season_fechar(interaction: discord.Interaction, season: int):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o **DONO do servidor** pode fechar season.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        set_season_status(ws, season, "closed")
        await interaction.followup.send(f"✅ Season **{season}** fechada.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="migrar_season1", description="(DONO) Carimba season=1 onde estiver vazio (compatibilidade).")
async def migrar_season1(interaction: discord.Interaction):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o **DONO do servidor** pode migrar.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        # garante abas
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_enr   = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)
        ws_pods  = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=30)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        ws_st    = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=20000, cols=40)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=20000, cols=30)

        def fill_season(ws, col_name="season"):
            vals = ws.get_all_values()
            idx = header_index(ws)
            if col_name not in idx:
                return 0
            changed = 0
            for rown in range(2, len(vals)+1):
                r = vals[rown-1]
                cur = (r[idx[col_name]] if idx[col_name] < len(r) else "").strip()
                if cur == "":
                    ws.update([["1"]], range_name=f"{col_letter(idx[col_name])}{rown}")
                    changed += 1
            return changed

        c1 = fill_season(ws_cycles)
        e1 = fill_season(ws_enr)
        p1 = fill_season(ws_pods)
        m1 = fill_season(ws_matches)
        s1 = fill_season(ws_st)
        d1 = fill_season(ws_decks)

        await interaction.followup.send(
            "✅ Migração season=1 concluída.\n"
            f"Cycles: {c1} | Enrollments: {e1} | PodsHistory: {p1} | Matches: {m1} | Standings: {s1} | Decks: {d1}",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Admin: Ciclo (open/locked/completed + reabrir)
# =========================
@client.tree.command(name="ciclo_abrir", description="(ADM/Org) Cria/abre um ciclo (status=open) na season atual.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_abrir(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)

        fields = get_cycle_fields(ws_cycles, season, cycle)
        if fields.get("status") == "completed":
            return await interaction.followup.send("❌ Este ciclo está COMPLETED. Use `/ciclo_reabrir` (dono/força) se necessário.", ephemeral=True)

        set_cycle_status(ws_cycles, season, cycle, "open")
        await interaction.followup.send(f"✅ Season {season} - Ciclo {cycle} aberto (open).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_encerrar", description="(ADM/Org) Encerra ciclo (status=completed) na season atual.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_encerrar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        set_cycle_status(ws_cycles, season, cycle, "completed")
        await interaction.followup.send(f"✅ Season {season} - Ciclo {cycle} encerrado (completed).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_bloquear", description="(ADM/Org) Trava ciclo (status=locked) na season atual.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_bloquear(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        fields = get_cycle_fields(ws_cycles, season, cycle)
        if fields.get("status") == "completed":
            return await interaction.followup.send("❌ Ciclo completed não pode ser bloqueado.", ephemeral=True)
        set_cycle_status(ws_cycles, season, cycle, "locked")
        await interaction.followup.send(f"✅ Season {season} - Ciclo {cycle} travado (locked).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_reabrir", description="(DONO) Reabre um ciclo COMPLETED (força) na season atual.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_reabrir(interaction: discord.Interaction, cycle: int):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o **DONO do servidor** pode reabrir ciclo COMPLETED.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        set_cycle_status(ws_cycles, season, cycle, "open")
        await interaction.followup.send(f"✅ FORÇADO: Season {season} - Ciclo {cycle} reaberto (open).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_status", description="Mostra status detalhado do ciclo (season atual).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def ciclo_status(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)
    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=30)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)

        fields = get_cycle_fields(ws_cycles, season, c)
        st = fields.get("status") or "—"
        start_str = fields.get("start_at_br") or "—"
        deadline_str = fields.get("deadline_at_br") or "—"

        enr_vals = ws_enr.get_all_values()
        eidx = require_cols(ws_enr, ["season","cycle","status"])
        enr_active = 0
        for rown in range(2, len(enr_vals)+1):
            r = enr_vals[rown-1]
            ss = safe_int(r[eidx["season"]] if eidx["season"] < len(r) else "0", 0)
            cc = safe_int(r[eidx["cycle"]] if eidx["cycle"] < len(r) else "0", 0)
            if ss == season and cc == c:
                if (r[eidx["status"]] if eidx["status"] < len(r) else "").strip().lower() == "active":
                    enr_active += 1

        pods_vals = ws_pods.get_all_values()
        pidx = require_cols(ws_pods, ["season","cycle","pod"])
        pod_names = set()
        for rown in range(2, len(pods_vals)+1):
            r = pods_vals[rown-1]
            ss = safe_int(r[pidx["season"]] if pidx["season"] < len(r) else "0", 0)
            cc = safe_int(r[pidx["cycle"]] if pidx["cycle"] < len(r) else "0", 0)
            if ss == season and cc == c:
                pod = (r[pidx["pod"]] if pidx["pod"] < len(r) else "").strip()
                if pod:
                    pod_names.add(pod)

        m_vals = ws_matches.get_all_values()
        midx = require_cols(ws_matches, ["season","cycle","active","confirmed_status","reported_by_id"])
        total = pending = confirmed = rejected = noreport = 0
        for rown in range(2, len(m_vals)+1):
            r = m_vals[rown-1]
            ss = safe_int(r[midx["season"]] if midx["season"] < len(r) else "0", 0)
            cc = safe_int(r[midx["cycle"]] if midx["cycle"] < len(r) else "0", 0)
            if ss != season or cc != c:
                continue
            if not as_bool(r[midx["active"]] if midx["active"] < len(r) else "TRUE"):
                continue
            total += 1
            stt = (r[midx["confirmed_status"]] if midx["confirmed_status"] < len(r) else "").strip().lower()
            rep = (r[midx["reported_by_id"]] if midx["reported_by_id"] < len(r) else "").strip()
            if not rep:
                noreport += 1
            if stt == "pending":
                pending += 1
            elif stt == "confirmed":
                confirmed += 1
            elif stt == "rejected":
                rejected += 1

        await interaction.followup.send(
            "\n".join([
                f"📊 **Season {season} - Status Ciclo {c}**",
                f"Status: **{st}**",
                f"Início (BR): **{start_str}**",
                f"Fim (BR): **{deadline_str}**",
                f"Inscritos ativos: **{enr_active}**",
                f"Pods gerados: **{len(pod_names)}**",
                f"Matches ativos: **{total}**",
                f"Confirmed: **{confirmed}** | Pending: **{pending}** | Rejected: **{rejected}** | Sem report: **{noreport}**",
            ]),
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Players + Enrollments (agora NÃO cria ciclo sozinho)
# =========================
@client.tree.command(name="inscrever", description="Inscreve você em um CICLO ABERTO (season atual).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def inscrever(interaction: discord.Interaction, cycle: str):
    if not cooldown_ok(interaction.user.id, "inscrever", 2):
        return await interaction.response.send_message("⚠️ Aguarde 2s e tente de novo.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    discord_id = interaction.user.id
    nick = interaction.user.display_name
    nowb = now_br_str()

    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        fields = get_cycle_fields(ws_cycles, season, c)
        st = fields.get("status")

        # ✅ aqui é o ponto principal: NÃO cria ciclo automaticamente
        if st is None:
            return await interaction.followup.send(
                f"❌ Esse ciclo não existe na Season {season}.\n"
                "Peça para ADM/Organizador abrir com `/ciclo_abrir`.",
                ephemeral=True
            )

        if st == "completed":
            return await interaction.followup.send("❌ Este ciclo já foi concluído.", ephemeral=True)
        if st == "locked":
            return await interaction.followup.send("❌ Este ciclo está LOCKED (inscrição fechada).", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Ciclo não está aberto (status atual: {st}).", ephemeral=True)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)

        # upsert Players
        pvals = ws_players.get_all_values()
        pidx = require_cols(ws_players, ["discord_id","nick","status","created_at","updated_at"])
        prow = None
        for rown in range(2, len(pvals)+1):
            r = pvals[rown-1]
            pid = (r[pidx["discord_id"]] if pidx["discord_id"] < len(r) else "").strip()
            if pid == str(discord_id):
                prow = rown
                break

        if prow is None:
            ws_players.append_row([str(discord_id), nick, "active", nowb, nowb], value_input_option="USER_ENTERED")
        else:
            ws_players.update([[nick]], range_name=f"{col_letter(pidx['nick'])}{prow}")
            ws_players.update([["active"]], range_name=f"{col_letter(pidx['status'])}{prow}")
            ws_players.update([[nowb]], range_name=f"{col_letter(pidx['updated_at'])}{prow}")

        # upsert Enrollment
        evals = ws_enr.get_all_values()
        eidx = require_cols(ws_enr, ["season","cycle","player_id","status","created_at","updated_at"])
        rown = find_row_by_cols(evals, eidx, {"season": str(season), "cycle": str(c), "player_id": str(discord_id)})

        if rown is None:
            ws_enr.append_row([str(season), str(c), str(discord_id), "active", nowb, nowb], value_input_option="USER_ENTERED")
            await interaction.followup.send(
                f"✅ Inscrito na **Season {season} - Ciclo {c}**.\n"
                "Agora você pode cadastrar **1x** seu deck e decklist nesse ciclo:\n"
                f"• `/deck cycle:{c} nome:...`\n"
                f"• `/decklist cycle:{c} url:...`\n"
                "Se quiser sair: `/drop cycle:...`",
                ephemeral=True
            )
        else:
            ws_enr.update([["active"]], range_name=f"{col_letter(eidx['status'])}{rown}")
            ws_enr.update([[nowb]], range_name=f"{col_letter(eidx['updated_at'])}{rown}")
            await interaction.followup.send(f"✅ Inscrição reativada na **Season {season} - Ciclo {c}**.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscrever: {e}", ephemeral=True)

@client.tree.command(name="drop", description="Sai do ciclo informado (apenas se ciclo OPEN).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)", motivo="Opcional: motivo curto")
async def drop(interaction: discord.Interaction, cycle: int, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)
    discord_id = interaction.user.id
    nowb = now_br_str()
    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        st = get_cycle_fields(ws_cycles, season, cycle).get("status")
        if st != "open":
            return await interaction.followup.send(f"❌ Drop só é permitido com ciclo OPEN. Status atual: {st}", ephemeral=True)

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)
        evals = ws_enr.get_all_values()
        eidx = require_cols(ws_enr, ["season","cycle","player_id","status","updated_at"])
        rown = find_row_by_cols(evals, eidx, {"season": str(season), "cycle": str(cycle), "player_id": str(discord_id)})
        if rown is None:
            return await interaction.followup.send(f"❌ Você não está inscrito na Season {season} - Ciclo {cycle}.", ephemeral=True)

        ws_enr.update([["dropped"]], range_name=f"{col_letter(eidx['status'])}{rown}")
        ws_enr.update([[nowb]], range_name=f"{col_letter(eidx['updated_at'])}{rown}")

        msg = f"✅ Você saiu da **Season {season} - Ciclo {cycle}**."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)


# =========================
# Deck / Decklist (1x POR CICLO) + pede ciclo explicitamente
# =========================
def is_enrolled_active(ws_enr, season: int, cycle: int, player_id: str) -> bool:
    vals = ws_enr.get_all_values()
    idx = require_cols(ws_enr, ["season","cycle","player_id","status"])
    for rown in range(2, len(vals)+1):
        r = vals[rown-1]
        ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
        cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
        pid = (r[idx["player_id"]] if idx["player_id"] < len(r) else "").strip()
        st = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower()
        if ss == season and cc == cycle and pid == str(player_id) and st == "active":
            return True
    return False

def get_deck_row(ws_decks, season: int, cycle: int, player_id: str) -> int | None:
    vals = ws_decks.get_all_values()
    idx = require_cols(ws_decks, ["season","cycle","player_id"])
    return find_row_by_cols(vals, idx, {"season": str(season), "cycle": str(cycle), "player_id": str(player_id)})

@client.tree.command(name="deck", description="Define seu deck (1x por ciclo). ADM/Org podem alterar.")
@app_commands.describe(cycle="Ciclo (ex: 1)", nome="Nome do deck (ex: UR Murktide)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def deck(interaction: discord.Interaction, cycle: str, nome: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    nowb = now_br_str()
    pid = str(interaction.user.id)

    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        st = get_cycle_fields(ws_cycles, season, c).get("status")
        if st is None:
            return await interaction.followup.send("❌ Ciclo não existe. Peça para abrir.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo completed. Não permite editar deck.", ephemeral=True)

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)
        if not is_enrolled_active(ws_enr, season, c, pid):
            return await interaction.followup.send("❌ Você precisa estar inscrito (status active) nesse ciclo para cadastrar deck.", ephemeral=True)

        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=20000, cols=30)
        didx = require_cols(ws_decks, ["season","cycle","player_id","deck","updated_at","created_at"])

        rown = get_deck_row(ws_decks, season, c, pid)
        if rown is None:
            ws_decks.append_row([str(season), str(c), pid, nome, "", nowb, nowb], value_input_option="USER_ENTERED")
            return await interaction.followup.send(f"✅ Deck salvo para **Season {season} - Ciclo {c}**.\nDeck: **{nome}**", ephemeral=True)

        # existe: só deixa trocar se ADM/Org
        vals = ws_decks.row_values(rown)
        cur = (vals[didx["deck"]] if didx["deck"] < len(vals) else "").strip()
        if cur and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send("❌ Você já definiu seu deck nesse ciclo. Peça para ADM/Organizador alterar.", ephemeral=True)

        ws_decks.update([[nome]], range_name=f"{col_letter(didx['deck'])}{rown}")
        ws_decks.update([[nowb]], range_name=f"{col_letter(didx['updated_at'])}{rown}")
        await interaction.followup.send(f"✅ Deck atualizado para **Season {season} - Ciclo {c}**.\nDeck: **{nome}**", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="decklist", description="Define sua decklist (1x por ciclo). ADM/Org podem alterar.")
@app_commands.describe(cycle="Ciclo (ex: 1)", url="Link (moxfield.com ou ligamagic.com.br)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def decklist(interaction: discord.Interaction, cycle: str, url: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    ok, val = validate_decklist_url(url)
    if not ok:
        return await interaction.followup.send(f"❌ {val}", ephemeral=True)

    nowb = now_br_str()
    pid = str(interaction.user.id)

    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        st = get_cycle_fields(ws_cycles, season, c).get("status")
        if st is None:
            return await interaction.followup.send("❌ Ciclo não existe. Peça para abrir.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo completed. Não permite editar decklist.", ephemeral=True)

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)
        if not is_enrolled_active(ws_enr, season, c, pid):
            return await interaction.followup.send("❌ Você precisa estar inscrito (status active) nesse ciclo para cadastrar decklist.", ephemeral=True)

        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=20000, cols=30)
        didx = require_cols(ws_decks, ["season","cycle","player_id","decklist_url","updated_at","created_at"])

        rown = get_deck_row(ws_decks, season, c, pid)
        if rown is None:
            ws_decks.append_row([str(season), str(c), pid, "", val, nowb, nowb], value_input_option="USER_ENTERED")
            return await interaction.followup.send(f"✅ Decklist salva para **Season {season} - Ciclo {c}**.\nLink: {val}", ephemeral=True)

        vals = ws_decks.row_values(rown)
        cur = (vals[didx["decklist_url"]] if didx["decklist_url"] < len(vals) else "").strip()
        if cur and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send("❌ Você já definiu sua decklist nesse ciclo. Peça para ADM/Organizador alterar.", ephemeral=True)

        ws_decks.update([[val]], range_name=f"{col_letter(didx['decklist_url'])}{rown}")
        ws_decks.update([[nowb]], range_name=f"{col_letter(didx['updated_at'])}{rown}")
        await interaction.followup.send(f"✅ Decklist atualizada para **Season {season} - Ciclo {c}**.\nLink: {val}", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# /deck_ver e /decklist_ver
# =========================
@client.tree.command(name="deck_ver", description="Mostra o deck de um jogador no ciclo (season atual).")
@app_commands.describe(cycle="Ciclo", jogador="Jogador (menção ou id)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def deck_ver(interaction: discord.Interaction, cycle: str, jogador: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    # aceita <@id> ou id
    jid = "".join(ch for ch in jogador if ch.isdigit())
    if not jid.isdigit():
        return await interaction.followup.send("❌ Informe o jogador como menção ou discord_id.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=20000, cols=30)
        rown = get_deck_row(ws_decks, season, c, jid)
        if rown is None:
            return await interaction.followup.send("Sem deck registrado para esse jogador nesse ciclo.", ephemeral=True)

        vals = ws_decks.row_values(rown)
        idx = require_cols(ws_decks, ["deck","decklist_url"])
        deck_name = (vals[idx["deck"]] if idx["deck"] < len(vals) else "").strip() or "—"
        await interaction.followup.send(f"📌 Season {season} - Ciclo {c}\nJogador: `{jid}`\nDeck: **{deck_name}**", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="decklist_ver", description="Mostra a decklist de um jogador no ciclo (season atual).")
@app_commands.describe(cycle="Ciclo", jogador="Jogador (menção ou id)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def decklist_ver(interaction: discord.Interaction, cycle: str, jogador: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    jid = "".join(ch for ch in jogador if ch.isdigit())
    if not jid.isdigit():
        return await interaction.followup.send("❌ Informe o jogador como menção ou discord_id.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=20000, cols=30)
        rown = get_deck_row(ws_decks, season, c, jid)
        if rown is None:
            return await interaction.followup.send("Sem decklist registrada para esse jogador nesse ciclo.", ephemeral=True)

        vals = ws_decks.row_values(rown)
        idx = require_cols(ws_decks, ["decklist_url"])
        url = (vals[idx["decklist_url"]] if idx["decklist_url"] < len(vals) else "").strip() or "—"
        await interaction.followup.send(f"📌 Season {season} - Ciclo {c}\nJogador: `{jid}`\nDecklist: {url}", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Pods (gerar/ver/publicar) + trava ciclo + calcula prazo
# =========================
@client.tree.command(name="pods_gerar", description="(ADM/Org) Sorteia pods do ciclo, grava PodsHistory e cria Matches pending.")
@app_commands.describe(cycle="Ciclo (ex: 1)", tamanho="Tamanho do pod (padrão 4)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def pods_gerar(interaction: discord.Interaction, cycle: str, tamanho: int = 4):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    tamanho = max(2, min(int(tamanho), 8))
    nowb = now_br_str()

    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        fields = get_cycle_fields(ws_cycles, season, c)
        st = fields.get("status")
        if st is None:
            return await interaction.followup.send("❌ Ciclo não existe. Abra com /ciclo_abrir.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED. Não pode gerar pods.", ephemeral=True)
        if st == "locked":
            return await interaction.followup.send("❌ Ciclo já está LOCKED. Pods já foram gerados.", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Ciclo não está OPEN (status: {st}).", ephemeral=True)

        ws_enr   = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=30)
        ws_pods  = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=30)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)

        # impede gerar de novo se já existir PodsHistory nesse season+cycle
        pvals = ws_pods.get_all_values()
        pidx = require_cols(ws_pods, ["season","cycle"])
        for rown in range(2, len(pvals)+1):
            r = pvals[rown-1]
            ss = safe_int(r[pidx["season"]] if pidx["season"] < len(r) else "0", 0)
            cc = safe_int(r[pidx["cycle"]] if pidx["cycle"] < len(r) else "0", 0)
            if ss == season and cc == c:
                set_cycle_status(ws_cycles, season, c, "locked")
                return await interaction.followup.send(
                    "❌ Já existe PodsHistory para esse ciclo. Não é permitido gerar novamente.\n"
                    "Status ajustado para LOCKED.",
                    ephemeral=True
                )

        # inscritos ativos
        evals = ws_enr.get_all_values()
        eidx = require_cols(ws_enr, ["season","cycle","player_id","status"])
        players = []
        for rown in range(2, len(evals)+1):
            r = evals[rown-1]
            ss = safe_int(r[eidx["season"]] if eidx["season"] < len(r) else "0", 0)
            cc = safe_int(r[eidx["cycle"]] if eidx["cycle"] < len(r) else "0", 0)
            if ss != season or cc != c:
                continue
            stt = (r[eidx["status"]] if eidx["status"] < len(r) else "").strip().lower()
            if stt != "active":
                continue
            pid = (r[eidx["player_id"]] if eidx["player_id"] < len(r) else "").strip()
            if pid:
                players.append(pid)

        if len(players) < 2:
            return await interaction.followup.send("Poucos inscritos ativos para gerar pods.", ephemeral=True)

        past_pairs = get_past_confirmed_pairs(ws_matches, season)
        pods, repeat_score = best_shuffle_min_repeats(players, tamanho, past_pairs, tries=250)

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        created_matches = 0
        nick_map = build_players_nick_map(ws_players)

        # cria pods + matches
        for idxp, pod_players in enumerate(pods):
            pod_name = letters[idxp] if idxp < len(letters) else f"P{idxp+1}"

            for pid in pod_players:
                ws_pods.append_row([str(season), str(c), pod_name, pid, nowb], value_input_option="USER_ENTERED")

            for a, b in round_robin_pairs(pod_players):
                mid = new_match_id(season, c, pod_name)
                ac_at = auto_confirm_deadline_iso(utc_now_dt())
                row = [
                    str(season), mid, str(c), pod_name,
                    str(a), str(b),
                    "0","0","0",
                    "normal",
                    "pending",
                    "","",
                    "",
                    "TRUE",
                    nowb, nowb,
                    ac_at
                ]
                ws_matches.append_row(row, value_input_option="USER_ENTERED")
                created_matches += 1

        # lock ciclo e grava prazo
        set_cycle_status(ws_cycles, season, c, "locked")
        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(season, c, ws_pods, ws_cycles)
        if start_str and deadline_str:
            set_cycle_times(ws_cycles, season, c, start_str, deadline_str)

        lines = [f"🧩 Pods gerados — **Season {season} - Ciclo {c}** (tamanho base {tamanho})."]
        lines.append(f"♻️ Anti-repetição: penalidade **{repeat_score}** (quanto menor, melhor).")
        lines.append("🔒 Ciclo agora está **LOCKED** (inscrição fechada).")
        if start_str and deadline_str:
            lines.append(f"⏳ Prazo do ciclo (maior POD = {max_pod_size}): **{days} dias**")
            lines.append(f"🕑 Início: **{start_str} (BR)**")
            lines.append(f"🛑 Fim: **{deadline_str} (BR)**")

        for idxp, pod_players in enumerate(pods):
            pod_name = letters[idxp] if idxp < len(letters) else f"P{idxp+1}"
            lines.append(f"\n**Pod {pod_name}**")
            for pid in pod_players:
                lines.append(f"• {nick_map.get(pid, pid)} (`{pid}`)")

        lines.append(f"\n✅ Matches criados: **{created_matches}** (pending).")
        lines.append(f"Jogadores: use `/meus_matches cycle:{c}` para ver seus match_id.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_gerar: {e}", ephemeral=True)

@client.tree.command(name="pods_ver", description="Mostra pods do ciclo (season atual).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def pods_ver(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_pods.get_all_values()
        idx = require_cols(ws_pods, ["season","cycle","pod","player_id"])
        pods = {}
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            if ss != season or cc != c:
                continue
            pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
            pid = (r[idx["player_id"]] if idx["player_id"] < len(r) else "").strip()
            if pod and pid:
                pods.setdefault(pod, []).append(pid)

        if not pods:
            return await interaction.followup.send("Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        out = [f"🧩 Pods — **Season {season} - Ciclo {c}**"]
        for pod in sorted(pods.keys()):
            out.append(f"\n**Pod {pod}**")
            for pid in pods[pod]:
                out.append(f"• {nick_map.get(pid, pid)} (`{pid}`)")
        await interaction.followup.send("\n".join(out), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="pods_publicar", description="(ADM/Org) Publica pods do ciclo no canal.")
@app_commands.describe(cycle="Ciclo", canal="Opcional: canal (id). Se vazio, usa canal atual.")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def pods_publicar(interaction: discord.Interaction, cycle: str, canal: str = ""):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_pods.get_all_values()
        idx = require_cols(ws_pods, ["season","cycle","pod","player_id"])
        pods = {}
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            if ss != season or cc != c:
                continue
            pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
            pid = (r[idx["player_id"]] if idx["player_id"] < len(r) else "").strip()
            if pod and pid:
                pods.setdefault(pod, []).append(pid)

        if not pods:
            return await interaction.followup.send("Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        target = interaction.channel
        if canal.strip().isdigit() and interaction.guild:
            ch = interaction.guild.get_channel(int(canal.strip()))
            if ch:
                target = ch

        msg = [f"🧩 **Pods — Season {season} / Ciclo {c}**"]
        for pod in sorted(pods.keys()):
            msg.append(f"\n**Pod {pod}**")
            for pid in pods[pod]:
                msg.append(f"• {nick_map.get(pid, pid)}")

        await target.send("\n".join(msg))
        await interaction.followup.send("✅ Pods publicados.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Prazo do ciclo
# =========================
@client.tree.command(name="prazo", description="Mostra a data/hora limite do ciclo (baseado no maior POD).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def prazo(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=30)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=30)

        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(season, c, ws_pods, ws_cycles)
        if not deadline_str:
            return await interaction.followup.send("❌ Ainda não existem pods para esse ciclo. Gere os pods primeiro.", ephemeral=True)

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
                rem_text = f"{secs//86400}d {(secs%86400)//3600}h"

        await interaction.followup.send(
            f"⏳ **Prazo — Season {season} / Ciclo {c}**\n"
            f"Maior POD: **{max_pod_size} jogadores** → **{days} dias corridos**\n"
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
@client.tree.command(name="meus_matches", description="Lista seus matches do ciclo (com match_id, pod e prazo 48h).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def meus_matches(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_matches.get_all_values()
        idx = require_cols(ws_matches, ["season","cycle","active","player_a_id","player_b_id","match_id","pod","confirmed_status","reported_by_id","a_games_won","b_games_won","draw_games","auto_confirm_at"])

        my = []
        nowu = utc_now_dt()

        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            if ss != season or cc != c:
                continue
            if not as_bool(r[idx["active"]] if idx["active"] < len(r) else "TRUE"):
                continue

            a = (r[idx["player_a_id"]] if idx["player_a_id"] < len(r) else "").strip()
            b = (r[idx["player_b_id"]] if idx["player_b_id"] < len(r) else "").strip()
            if user_id not in (a, b):
                continue

            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
            st = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
            rep = (r[idx["reported_by_id"]] if idx["reported_by_id"] < len(r) else "").strip()
            ag = (r[idx["a_games_won"]] if idx["a_games_won"] < len(r) else "0").strip()
            bg = (r[idx["b_games_won"]] if idx["b_games_won"] < len(r) else "0").strip()
            dg = (r[idx["draw_games"]] if idx["draw_games"] < len(r) else "0").strip()

            acs = (r[idx["auto_confirm_at"]] if idx["auto_confirm_at"] < len(r) else "").strip()
            ac = parse_iso_dt(acs)
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
            return await interaction.followup.send(f"Você não tem matches no ciclo {c}.", ephemeral=True)

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
        season = get_current_season(sh)
        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        vals = ws.get_all_values()
        idx = require_cols(ws, ["season","match_id","active","confirmed_status","player_a_id","player_b_id","a_games_won","b_games_won","draw_games","result_type","reported_by_id","confirmed_by_id","updated_at","auto_confirm_at"])

        rown = None
        for i in range(2, len(vals)+1):
            r = vals[i-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            if ss == season and mid == str(match_id).strip():
                rown = i
                break
        if rown is None:
            return await interaction.followup.send("❌ Match não encontrado (season atual).", ephemeral=True)

        r = vals[rown-1]
        active = (r[idx["active"]] if idx["active"] < len(r) else "TRUE")
        if not as_bool(active):
            return await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)

        status = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
        if status != "pending":
            return await interaction.followup.send(f"❌ Match não está pending (atual: {status}).", ephemeral=True)

        a_id = (r[idx["player_a_id"]] if idx["player_a_id"] < len(r) else "").strip()
        b_id = (r[idx["player_b_id"]] if idx["player_b_id"] < len(r) else "").strip()
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

        ws.update([[str(a_gw)]], range_name=f"{col_letter(idx['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(idx['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(idx['draw_games'])}{rown}")
        ws.update([[rt]], range_name=f"{col_letter(idx['result_type'])}{rown}")
        ws.update([[reporter_id]], range_name=f"{col_letter(idx['reported_by_id'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(idx['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")
        ws.update([[ac_at]], range_name=f"{col_letter(idx['auto_confirm_at'])}{rown}")

        await interaction.followup.send(
            "✅ Resultado registrado como **PENDENTE**.\n"
            f"Match: **{match_id}**\n"
            f"Seu placar (V-D-E): **{v}-{d}-{e}**\n"
            "Oponente tem **48h** para `/rejeitar`.\n"
            "Se não rejeitar, vira oficial automaticamente (na varredura do `/recalcular`).",
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
        season = get_current_season(sh)
        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        vals = ws.get_all_values()
        idx = require_cols(ws, ["season","match_id","active","confirmed_status","reported_by_id","player_a_id","player_b_id","auto_confirm_at","confirmed_by_id","updated_at"])

        rown = None
        for i in range(2, len(vals)+1):
            r = vals[i-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            if ss == season and mid == str(match_id).strip():
                rown = i
                break
        if rown is None:
            return await interaction.followup.send("❌ Match não encontrado (season atual).", ephemeral=True)

        r = vals[rown-1]
        if not as_bool(r[idx["active"]] if idx["active"] < len(r) else "TRUE"):
            return await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)

        status = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
        if status != "pending":
            return await interaction.followup.send(f"❌ Match não está pending (atual: {status}).", ephemeral=True)

        reported_by = (r[idx["reported_by_id"]] if idx["reported_by_id"] < len(r) else "").strip()
        if not reported_by:
            return await interaction.followup.send("❌ Ainda não existe resultado reportado.", ephemeral=True)

        a_id = (r[idx["player_a_id"]] if idx["player_a_id"] < len(r) else "").strip()
        b_id = (r[idx["player_b_id"]] if idx["player_b_id"] < len(r) else "").strip()

        opponent_allowed = a_id if reported_by == b_id else b_id
        if user_id != opponent_allowed:
            return await interaction.followup.send("❌ Apenas o **oponente** pode rejeitar.", ephemeral=True)

        ac = parse_iso_dt((r[idx["auto_confirm_at"]] if idx["auto_confirm_at"] < len(r) else "").strip())
        if ac and utc_now_dt() > ac:
            return await interaction.followup.send("❌ Prazo expirou (48h). Peça para ADM/Organizador revisar.", ephemeral=True)

        nowb = now_br_str()
        ws.update([["rejected"]], range_name=f"{col_letter(idx['confirmed_status'])}{rown}")
        ws.update([[user_id]], range_name=f"{col_letter(idx['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")

        msg = "✅ Resultado rejeitado. ADM/Organizador pode corrigir."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================
# /resultado_admin (ADM/Org) e /match_cancelar
# =========================
@client.tree.command(name="resultado_admin", description="(ADM/Org) Lança/edita resultado e pode confirmar na hora.")
@app_commands.autocomplete(match_id=ac_match_id_any)
@app_commands.describe(match_id="match_id", placar="Placar V-D-E do player_a (formato do match)", confirmar="Confirmar agora?")
@app_commands.choices(
    placar=[
        app_commands.Choice(name="2-0-0", value="2-0-0"),
        app_commands.Choice(name="2-1-0", value="2-1-0"),
        app_commands.Choice(name="1-2-0", value="1-2-0"),
        app_commands.Choice(name="0-2-0", value="0-2-0"),
        app_commands.Choice(name="1-1-1 (empate jogado)", value="1-1-1"),
        app_commands.Choice(name="0-0-0 (empate sem jogo)", value="0-0-0"),
        app_commands.Choice(name="0-0-3 (empate intencional)", value="0-0-3"),
    ],
    confirmar=[
        app_commands.Choice(name="Sim", value="yes"),
        app_commands.Choice(name="Não", value="no"),
    ]
)
async def resultado_admin(interaction: discord.Interaction, match_id: str, placar: app_commands.Choice[str], confirmar: app_commands.Choice[str]):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    sc = parse_score_3parts(placar.value)
    if not sc:
        return await interaction.followup.send("❌ Placar inválido.", ephemeral=True)
    a_gw, b_gw, d_g = sc
    ok, msg = validate_3parts_rules(a_gw, b_gw, d_g)
    if not ok:
        return await interaction.followup.send(f"❌ {msg}", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        vals = ws.get_all_values()
        idx = require_cols(ws, ["season","match_id","active","a_games_won","b_games_won","draw_games","result_type","confirmed_status","confirmed_by_id","reported_by_id","updated_at"])

        rown = None
        for i in range(2, len(vals)+1):
            r = vals[i-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            if ss == season and mid == str(match_id).strip():
                rown = i
                break
        if rown is None:
            return await interaction.followup.send("❌ Match não encontrado (season atual).", ephemeral=True)

        r = vals[rown-1]
        if not as_bool(r[idx["active"]] if idx["active"] < len(r) else "TRUE"):
            return await interaction.followup.send("❌ Match inativo.", ephemeral=True)

        rt = "normal"
        if a_gw == b_gw:
            rt = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            rt = "intentional_draw"

        nowb = now_br_str()
        ws.update([[str(a_gw)]], range_name=f"{col_letter(idx['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(idx['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(idx['draw_games'])}{rown}")
        ws.update([[rt]], range_name=f"{col_letter(idx['result_type'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")
        ws.update([[str(interaction.user.id)]], range_name=f"{col_letter(idx['reported_by_id'])}{rown}")

        if confirmar.value == "yes":
            ws.update([["confirmed"]], range_name=f"{col_letter(idx['confirmed_status'])}{rown}")
            ws.update([[str(interaction.user.id)]], range_name=f"{col_letter(idx['confirmed_by_id'])}{rown}")
            stx = "confirmed"
        else:
            ws.update([["pending"]], range_name=f"{col_letter(idx['confirmed_status'])}{rown}")
            stx = "pending"

        await interaction.followup.send(
            f"✅ Resultado admin aplicado.\nmatch_id: `{match_id}`\nPlacar (A-B-E): **{a_gw}-{b_gw}-{d_g}**\nStatus: **{stx}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="match_cancelar", description="(ADM/Org) Inativa um match (active=FALSE).")
@app_commands.autocomplete(match_id=ac_match_id_any)
@app_commands.describe(match_id="match_id", motivo="Opcional")
async def match_cancelar(interaction: discord.Interaction, match_id: str, motivo: str = ""):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        vals = ws.get_all_values()
        idx = require_cols(ws, ["season","match_id","active","confirmed_status","confirmed_by_id","updated_at"])

        rown = None
        for i in range(2, len(vals)+1):
            r = vals[i-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            if ss == season and mid == str(match_id).strip():
                rown = i
                break
        if rown is None:
            return await interaction.followup.send("❌ Match não encontrado (season atual).", ephemeral=True)

        nowb = now_br_str()
        ws.update([["FALSE"]], range_name=f"{col_letter(idx['active'])}{rown}")
        ws.update([["canceled"]], range_name=f"{col_letter(idx['confirmed_status'])}{rown}")
        ws.update([[str(interaction.user.id)]], range_name=f"{col_letter(idx['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(idx['updated_at'])}{rown}")

        msg = f"✅ Match cancelado (active=FALSE): `{match_id}`"
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Confrontos pendentes (fecha ciclo)
# =========================
@client.tree.command(name="confrontos_pendentes", description="Lista confrontos do ciclo que ainda não têm report (season atual).")
@app_commands.describe(cycle="Ciclo", mostrar="Quantos mostrar (padrão 30)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def confrontos_pendentes(interaction: discord.Interaction, cycle: str, mostrar: int = 30):
    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_matches.get_all_values()
        idx = require_cols(ws_matches, ["season","cycle","active","reported_by_id","confirmed_status","match_id","pod","player_a_id","player_b_id"])
        pending = []

        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            if ss != season or cc != c:
                continue
            if not as_bool(r[idx["active"]] if idx["active"] < len(r) else "TRUE"):
                continue

            rep = (r[idx["reported_by_id"]] if idx["reported_by_id"] < len(r) else "").strip()
            stt = (r[idx["confirmed_status"]] if idx["confirmed_status"] < len(r) else "").strip().lower()
            if rep:
                continue  # já tem report
            if stt == "canceled":
                continue

            mid = (r[idx["match_id"]] if idx["match_id"] < len(r) else "").strip()
            pod = (r[idx["pod"]] if idx["pod"] < len(r) else "").strip()
            a = (r[idx["player_a_id"]] if idx["player_a_id"] < len(r) else "").strip()
            b = (r[idx["player_b_id"]] if idx["player_b_id"] < len(r) else "").strip()
            pending.append((pod, f"• Pod {pod}: {nick_map.get(a,a)} vs {nick_map.get(b,b)} | `{mid}`"))

        if not pending:
            return await interaction.followup.send("✅ Não há confrontos sem report nesse ciclo.", ephemeral=True)

        pending.sort(key=lambda x: (x[0], x[1]))
        mostrar = max(5, min(mostrar, 80))
        out = [f"📌 Confrontos SEM REPORT — Season {season} / Ciclo {c} (mostrando {mostrar})"]
        out.extend([x[1] for x in pending[:mostrar]])
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Recalcular ranking (CICLO) + Ranking Geral (SEASON)
# =========================
@client.tree.command(name="recalcular", description="(ADM/Org) Auto-confirm (48h) + recalcula ranking do ciclo do zero.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def recalcular(interaction: discord.Interaction, cycle: str):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)

        changed = 0
        try:
            changed = sweep_auto_confirm(ws_matches, season, c)
        except Exception:
            pass

        rows = recalculate_cycle(season, c)
        await interaction.followup.send(
            f"✅ Recalculo concluído.\nSeason {season} - Ciclo {c}\n"
            f"Auto-confirm (48h): **{changed}**\n"
            f"Jogadores no Standings: **{len(rows)}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"⚠️ Erro no recálculo: {e}", ephemeral=True)

@client.tree.command(name="ranking", description="Mostra o ranking do ciclo (público).")
@app_commands.describe(cycle="Ciclo (ex: 1)", top="Quantos mostrar (padrão 20)")
@app_commands.autocomplete(cycle=ac_cycle_any)
async def ranking(interaction: discord.Interaction, cycle: str, top: int = 20):
    await interaction.response.defer(ephemeral=False)
    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=False)

    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_st = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=20000, cols=40)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_st.get_all_values()
        idx = require_cols(ws_st, ["season","cycle","rank_position","player_id","match_points","omw_percent","gw_percent","ogw_percent"])

        rows = []
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            cc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            if ss == season and cc == c:
                rows.append(r)

        if not rows:
            return await interaction.followup.send("Sem standings para esse ciclo. Rode `/recalcular`.", ephemeral=False)

        top = max(5, min(top, 50))
        rows.sort(key=lambda rr: safe_int(rr[idx["rank_position"]] if idx["rank_position"] < len(rr) else "9999", 9999))

        out = [f"🏆 **Ranking — Season {season} / Ciclo {c}** (Top {top})"]
        out.append("pos | jogador | pts | OMW | GW | OGW")
        out.append("--- | ------ | --- | --- | --- | ---")

        for rr in rows[:top]:
            pid = (rr[idx["player_id"]] if idx["player_id"] < len(rr) else "").strip()
            pos = (rr[idx["rank_position"]] if idx["rank_position"] < len(rr) else "").strip()
            pts = (rr[idx["match_points"]] if idx["match_points"] < len(rr) else "").strip()
            omw = (rr[idx["omw_percent"]] if idx["omw_percent"] < len(rr) else "").strip()
            gw  = (rr[idx["gw_percent"]] if idx["gw_percent"] < len(rr) else "").strip()
            ogw = (rr[idx["ogw_percent"]] if idx["ogw_percent"] < len(rr) else "").strip()
            out.append(f"{pos} | {nick_map.get(pid, pid)} | {pts} | {omw} | {gw} | {ogw}")

        await interaction.followup.send("\n".join(out), ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ranking: {e}", ephemeral=False)

@client.tree.command(name="ranking_geral", description="Mostra ranking geral da season (acumulado por pontos).")
@app_commands.describe(top="Quantos mostrar (padrão 30)")
async def ranking_geral(interaction: discord.Interaction, top: int = 30):
    await interaction.response.defer(ephemeral=False)
    try:
        sh = open_sheet()
        season = get_current_season(sh)
        ws_st = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=20000, cols=40)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=30)
        nick_map = build_players_nick_map(ws_players)

        vals = ws_st.get_all_values()
        idx = require_cols(ws_st, ["season","cycle","player_id","match_points","matches_played","omw_percent","gw_percent","ogw_percent"])

        agg = {}  # pid -> stats
        for rown in range(2, len(vals)+1):
            r = vals[rown-1]
            ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
            if ss != season:
                continue
            pid = (r[idx["player_id"]] if idx["player_id"] < len(r) else "").strip()
            if not pid:
                continue
            mp = safe_int(r[idx["match_points"]] if idx["match_points"] < len(r) else "0", 0)
            mj = safe_int(r[idx["matches_played"]] if idx["matches_played"] < len(r) else "0", 0)
            omw = float(str(r[idx["omw_percent"]] if idx["omw_percent"] < len(r) else "33.3").replace(",", "."))
            gw  = float(str(r[idx["gw_percent"]] if idx["gw_percent"] < len(r) else "33.3").replace(",", "."))
            ogw = float(str(r[idx["ogw_percent"]] if idx["ogw_percent"] < len(r) else "33.3").replace(",", "."))

            if pid not in agg:
                agg[pid] = {"points": 0, "matches": 0, "cycles": set(), "omw_sum": 0.0, "gw_sum": 0.0, "ogw_sum": 0.0, "rows": 0}
            agg[pid]["points"] += mp
            agg[pid]["matches"] += mj
            cyc = safe_int(r[idx["cycle"]] if idx["cycle"] < len(r) else "0", 0)
            if cyc > 0:
                agg[pid]["cycles"].add(cyc)
            agg[pid]["omw_sum"] += omw
            agg[pid]["gw_sum"] += gw
            agg[pid]["ogw_sum"] += ogw
            agg[pid]["rows"] += 1

        if not agg:
            return await interaction.followup.send("Sem standings na season atual ainda.", ephemeral=False)

        rows = []
        for pid, a in agg.items():
            n = max(1, a["rows"])
            rows.append({
                "pid": pid,
                "points": a["points"],
                "matches": a["matches"],
                "cycles": len(a["cycles"]),
                "avg_omw": round(a["omw_sum"]/n, 1),
                "avg_gw": round(a["gw_sum"]/n, 1),
                "avg_ogw": round(a["ogw_sum"]/n, 1),
            })

        # ordenação geral: pontos > avg_omw > avg_gw > avg_ogw
        rows.sort(key=lambda x: (x["points"], x["avg_omw"], x["avg_gw"], x["avg_ogw"]), reverse=True)

        top = max(10, min(top, 80))
        out = [f"🏆 **Ranking Geral — Season {season}** (Top {top})"]
        out.append("pos | jogador | pts | ciclos | matches | OMW | GW | OGW")
        out.append("--- | ------ | --- | ----- | ------ | --- | --- | ---")
        for i, r in enumerate(rows[:top], start=1):
            out.append(
                f"{i} | {nick_map.get(r['pid'], r['pid'])} | {r['points']} | {r['cycles']} | {r['matches']} | {r['avg_omw']} | {r['avg_gw']} | {r['avg_ogw']}"
            )

        await interaction.followup.send("\n".join(out), ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ranking_geral: {e}", ephemeral=False)


# =========================
# Export (CSV) — ciclo ou season
# =========================
@client.tree.command(name="export", description="(ADM/Org) Exporta CSV: ciclo (matches+standings) ou season (standings).")
@app_commands.describe(tipo="ciclo ou season", cycle="Se tipo=ciclo, informe o ciclo")
@app_commands.choices(
    tipo=[
        app_commands.Choice(name="ciclo", value="ciclo"),
        app_commands.Choice(name="season", value="season"),
    ]
)
@app_commands.autocomplete(cycle=ac_cycle_any)
async def export(interaction: discord.Interaction, tipo: app_commands.Choice[str], cycle: str = ""):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season = get_current_season(sh)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=20000, cols=40)
        ws_st = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=20000, cols=40)

        buf = io.StringIO()
        w = csv.writer(buf)

        if tipo.value == "season":
            # export standings season
            vals = ws_st.get_all_values()
            idx = require_cols(ws_st, ["season"])
            w.writerow([f"=== STANDINGS (SEASON {season}) ==="])
            w.writerow(ws_st.row_values(1))
            for rown in range(2, len(vals)+1):
                r = vals[rown-1]
                ss = safe_int(r[idx["season"]] if idx["season"] < len(r) else "0", 0)
                if ss == season:
                    w.writerow(r)
            data = buf.getvalue().encode("utf-8")
            file = discord.File(io.BytesIO(data), filename=f"season_{season}_standings.csv")
            return await interaction.followup.send("✅ Export gerado:", file=file, ephemeral=True)

        # tipo=ciclo
        c = safe_int(cycle, 0)
        if c <= 0:
            return await interaction.followup.send("❌ Para export ciclo, informe cycle.", ephemeral=True)

        mv = ws_matches.get_all_values()
        midx = require_cols(ws_matches, ["season","cycle"])
        sv = ws_st.get_all_values()
        sidx = require_cols(ws_st, ["season","cycle"])

        w.writerow([f"=== MATCHES (SEASON {season} / CICLO {c}) ==="])
        w.writerow(ws_matches.row_values(1))
        for rown in range(2, len(mv)+1):
            r = mv[rown-1]
            ss = safe_int(r[midx["season"]] if midx["season"] < len(r) else "0", 0)
            cc = safe_int(r[midx["cycle"]] if midx["cycle"] < len(r) else "0", 0)
            if ss == season and cc == c:
                w.writerow(r)

        w.writerow([])
        w.writerow([f"=== STANDINGS (SEASON {season} / CICLO {c}) ==="])
        w.writerow(ws_st.row_values(1))
        for rown in range(2, len(sv)+1):
            r = sv[rown-1]
            ss = safe_int(r[sidx["season"]] if sidx["season"] < len(r) else "0", 0)
            cc = safe_int(r[sidx["cycle"]] if sidx["cycle"] < len(r) else "0", 0)
            if ss == season and cc == c:
                w.writerow(r)

        data = buf.getvalue().encode("utf-8")
        file = discord.File(io.BytesIO(data), filename=f"season_{season}_ciclo_{c}_export.csv")
        await interaction.followup.send("✅ Export gerado:", file=file, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /export: {e}", ephemeral=True)


# =========================
# Start
# =========================
if not DISCORD_TOKEN:
    raise RuntimeError("Faltou a variável DISCORD_TOKEN no ambiente.")

keep_alive()
client.run(DISCORD_TOKEN)
