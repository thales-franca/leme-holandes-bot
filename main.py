import os
import json
import threading
import random
import csv
import io
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
# Sheets (abas):
# - Players: cadastro permanente
# - Enrollments: inscrição por ciclo (active/dropped)
# - Cycles: status do ciclo (open/locked/completed) + start_at + deadline_at
# - PodsHistory: pods por ciclo
# - Matches: confrontos + resultados
# - Standings: ranking recalculado (bot escreve do zero)
#
# Regras:
# - Ranking: Pontos > OMW% > GW% > OGW%
# - Piso 33,3% em MWP/GWP antes de calcular OMW/OGW
# - Sempre recalcular tudo do zero (sem incremental)
# - Resultado do jogador: V-D-E (Vitória-Derrota-Empate) em GAMES
# - Empate em games conta como 0.5 na GWP
# - Rejeição: 48h para rejeitar (se não, vira confirmed na varredura /recalcular)
# - Prazo do ciclo (/prazo):
#   POD 3 -> 5 dias corridos
#   POD 4 -> 8 dias corridos
#   POD 5 ou 6 -> 10 dias corridos
#   Ciclo começa 14:00 (BR) e termina no último dia às 13:59 (BR)
# - /final (ADM): aplica 0-0-3 para matches sem report
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
    # "YYYY-MM-DD HH:MM:SS" assumed BR
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=BR_TZ)
    except Exception:
        return None


# =========================
# Google Sheets helpers
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
    # A=0, B=1...
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

def ensure_worksheet(sh, title: str, header: list[str], rows: int = 2000, cols: int = 25):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    current = ws.row_values(1)
    if not current:
        ws.append_row(header)
    return ws

def find_player_row(ws_players, discord_id: int):
    try:
        cell = ws_players.find(str(discord_id))
        return cell.row
    except Exception:
        return None

def get_player_fields(ws_players, row: int):
    vals = ws_players.get(f"A{row}:H{row}")
    if not vals or not vals[0]:
        return {}
    r = vals[0]
    while len(r) < 8:
        r.append("")
    return {
        "discord_id": str(r[0]).strip(),
        "nick": str(r[1]).strip(),
        "deck": str(r[2]).strip(),
        "decklist_url": str(r[3]).strip(),
        "status": str(r[4]).strip(),
        "reports_unique": str(r[5]).strip(),
        "created_at": str(r[6]).strip(),
        "updated_at": str(r[7]).strip(),
    }

def build_players_nick_map(ws_players) -> dict[str, str]:
    data = ws_players.get_all_records()
    m = {}
    for r in data:
        pid = str(r.get("discord_id", "")).strip()
        nick = str(r.get("nick", "")).strip()
        if pid:
            m[pid] = nick or pid
    return m


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
PLAYERS_HEADER = ["discord_id","nick","deck","decklist_url","status","reports_unique","created_at","updated_at"]

ENROLLMENTS_HEADER = ["cycle","player_id","status","created_at","updated_at"]
ENROLLMENTS_REQUIRED = ["cycle","player_id","status","created_at","updated_at"]

PODSHISTORY_HEADER = ["cycle","pod","player_id","created_at"]
PODSHISTORY_REQUIRED = ["cycle","pod","player_id","created_at"]

# Cycles: adicionamos start_at_br e deadline_at_br para /prazo e /final
CYCLES_HEADER = ["cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]  # status: open|locked|completed
CYCLES_REQUIRED = ["cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]

MATCHES_REQUIRED_COLS = [
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


# =========================
# Cycle helpers
# =========================
def get_cycle_row(ws_cycles, cycle: int) -> int | None:
    rows = ws_cycles.get_all_values()
    if len(rows) <= 1:
        return None
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    for r_i in range(2, len(rows) + 1):
        r = rows[r_i-1]
        c = r[col["cycle"]] if col["cycle"] < len(r) else ""
        if safe_int(c, 0) == cycle:
            return r_i
    return None

def get_cycle_fields(ws_cycles, cycle: int) -> dict:
    rows = ws_cycles.get_all_values()
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    out = {"cycle": cycle, "status": None, "start_at_br": "", "deadline_at_br": ""}
    for r_i in range(2, len(rows) + 1):
        r = rows[r_i-1]
        c = r[col["cycle"]] if col["cycle"] < len(r) else ""
        if safe_int(c, 0) == cycle:
            out["status"] = (r[col["status"]] if col["status"] < len(r) else "").strip().lower()
            out["start_at_br"] = (r[col["start_at_br"]] if col["start_at_br"] < len(r) else "").strip()
            out["deadline_at_br"] = (r[col["deadline_at_br"]] if col["deadline_at_br"] < len(r) else "").strip()
            return out
    return out

def set_cycle_status(ws_cycles, cycle: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_cycles.append_row([str(cycle), status, "", "", nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[status]], range_name=f"{col_letter(col['status'])}{rown}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

def set_cycle_times(ws_cycles, cycle: int, start_at_br: str, deadline_at_br: str):
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_cycles.append_row([str(cycle), "open", start_at_br, deadline_at_br, nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[start_at_br]], range_name=f"{col_letter(col['start_at_br'])}{rown}")
        ws_cycles.update([[deadline_at_br]], range_name=f"{col_letter(col['deadline_at_br'])}{rown}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

def list_cycles(ws_cycles) -> list[tuple[int, str]]:
    out = []
    for r in ws_cycles.get_all_records():
        c = safe_int(r.get("cycle", 0), 0)
        st = str(r.get("status", "")).strip().lower()
        if c > 0:
            out.append((c, st))
    out.sort(key=lambda x: x[0])
    return out

def suggest_open_cycles(ws_cycles, limit: int = 20) -> list[int]:
    items = list_cycles(ws_cycles)
    if not items:
        return [1]
    max_cycle = max(c for c, _ in items)
    open_cycles = [c for c, st in items if st == "open"]
    if (max_cycle + 1) not in open_cycles:
        open_cycles.append(max_cycle + 1)
    open_cycles = sorted(set(open_cycles))
    return open_cycles[:limit]


# =========================
# Match helpers
# =========================
def new_match_id(cycle: int, pod: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    rnd = random.randint(1000, 9999)
    return f"C{cycle}-P{pod}-{ts}-{rnd}"

def auto_confirm_deadline_iso(created_utc: datetime) -> str:
    return (created_utc + timedelta(hours=48)).isoformat()

def round_robin_pairs(players: list[str]):
    pairs = []
    n = len(players)
    for i in range(n):
        for j in range(i + 1, n):
            pairs.append((players[i], players[j]))
    return pairs

def sweep_auto_confirm(sh, cycle: int) -> int:
    ws_matches = sh.worksheet("Matches")
    col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    rows = ws_matches.get_all_values()
    if len(rows) <= 1:
        return 0

    now = utc_now_dt()
    changed = 0

    for rown in range(2, len(rows) + 1):
        r = rows[rown - 1]

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        r_cycle = safe_int(getc("cycle"), 0)
        if r_cycle != cycle:
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

        if ac <= now:
            ws_matches.update([["confirmed"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
            ws_matches.update([["AUTO"]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
            ws_matches.update([[now_br_str()]], range_name=f"{col_letter(col['updated_at'])}{rown}")
            changed += 1

    return changed


# =========================
# Anti-repetição (heurística)
# =========================
def get_past_confirmed_pairs(ws_matches) -> set[frozenset]:
    rows = ws_matches.get_all_records()
    pairs = set()
    for r in rows:
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
# Prazo do ciclo baseado no maior POD
# =========================
def cycle_days_by_max_pod(max_pod_size: int) -> int:
    if max_pod_size <= 3:
        return 5
    if max_pod_size == 4:
        return 8
    # 5 ou 6 (ou mais)
    return 10

def compute_cycle_start_deadline_br(cycle: int, ws_pods, ws_cycles) -> tuple[str, str, int, int]:
    """
    Retorna (start_at_br_str, deadline_at_br_str, max_pod_size, days)
    - start_at: 14:00 BR do dia que o ciclo foi "travado" (geração de pods)
      (se já existir no Cycles, reaproveita)
    - deadline_at: (start_date + days) às 13:59 BR
    """
    fields = get_cycle_fields(ws_cycles, cycle)
    if fields.get("start_at_br"):
        start_dt = parse_br_dt(fields["start_at_br"])
    else:
        start_dt = None

    # Descobrir max_pod_size pelo PodsHistory
    rows = ws_pods.get_all_records()
    pods = {}
    for r in rows:
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        pod = str(r.get("pod", "")).strip()
        pid = str(r.get("player_id", "")).strip()
        if pod and pid:
            pods.setdefault(pod, set()).add(pid)

    if not pods:
        # sem pods ainda
        max_pod_size = 0
        days = 0
        return ("", "", max_pod_size, days)

    max_pod_size = max(len(v) for v in pods.values())
    days = cycle_days_by_max_pod(max_pod_size)

    # Se não havia start_dt, usamos o dia do primeiro registro do pod (created_at) ou hoje
    if start_dt is None:
        created_candidates = []
        for r in rows:
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            c = parse_br_dt(r.get("created_at", ""))
            if c:
                created_candidates.append(c)
        base_date = (min(created_candidates).astimezone(BR_TZ).date() if created_candidates else now_br_dt().date())
        start_dt = datetime.combine(base_date, time(14, 0), tzinfo=BR_TZ)

    deadline_date = (start_dt.date() + timedelta(days=days))
    deadline_dt = datetime.combine(deadline_date, time(13, 59), tzinfo=BR_TZ)

    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    deadline_str = deadline_dt.strftime("%Y-%m-%d %H:%M:%S")
    return (start_str, deadline_str, max_pod_size, days)


# =========================
# Core: recálculo oficial
# =========================
def recalculate_cycle(cycle: int):
    sh = open_sheet()
    ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
    ws_matches = sh.worksheet("Matches")
    ws_standings = sh.worksheet("Standings")

    try:
        sweep_auto_confirm(sh, cycle)
    except Exception:
        pass

    players_rows = ws_players.get_all_records()
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

    matches_rows = ws_matches.get_all_records()
    valid = []

    for r in matches_rows:
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

    header = [
        "cycle","player_id","matches_played","match_points","mwp_percent",
        "game_wins","game_losses","game_draws","games_played","gw_percent",
        "omw_percent","ogw_percent","rank_position","last_recalc_at"
    ]

    ws_standings.clear()
    ws_standings.append_row(header)

    values = []
    for r in rows:
        values.append([
            r["cycle"], r["player_id"], r["matches_played"], r["match_points"], r["mwp_percent"],
            r["game_wins"], r["game_losses"], r["game_draws"], r["games_played"], r["gw_percent"],
            r["omw_percent"], r["ogw_percent"], r["rank_position"], r["last_recalc_at"]
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
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        open_cycles = suggest_open_cycles(ws_cycles, limit=25)
        cur = str(current).strip()
        if cur:
            open_cycles = [c for c in open_cycles if str(c).startswith(cur)]

        return [app_commands.Choice(name=f"Ciclo {c} (aberto)", value=str(c)) for c in open_cycles[:25]]
    except Exception:
        base = [str(i) for i in range(1, 11)]
        cur = str(current).strip()
        if cur:
            base = [x for x in base if x.startswith(cur)]
        return [app_commands.Choice(name=f"Ciclo {x}", value=x) for x in base[:25]]

async def ac_match_id_user_pending(interaction: discord.Interaction, current: str):
    try:
        user_id = str(interaction.user.id)
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        cur = str(current).strip().lower()

        out = []
        for r in rows:
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "pending":
                continue
            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if user_id not in (a, b):
                continue
            mid = str(r.get("match_id", "")).strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            pod = str(r.get("pod", "")).strip()
            na = nick_map.get(a, a)
            nb = nick_map.get(b, b)
            label = f"Pod {pod}: {na} vs {nb} | {mid}"
            out.append(app_commands.Choice(name=label[:100], value=mid))
        return out[:25]
    except Exception:
        return []

async def ac_match_id_any(interaction: discord.Interaction, current: str):
    # Para admin editar/cancelar: qualquer match_id
    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = ws_matches.get_all_records()
        cur = str(current).strip().lower()
        out = []
        for r in rows:
            mid = str(r.get("match_id", "")).strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            cyc = safe_int(r.get("cycle", 0), 0)
            pod = str(r.get("pod", "")).strip()
            st = str(r.get("confirmed_status", "")).strip().lower()
            label = f"C{cyc} Pod {pod} [{st}] | {mid}"
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

@client.tree.command(name="sheets", description="Teste de conexão com Google Sheets (apenas leitura)")
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

@client.tree.command(name="forcesync", description="Força sincronização dos comandos (ADM/Organizador)")
async def forcesync(interaction: discord.Interaction):
    if not await is_admin_or_organizer(interaction):
        try:
            await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        except discord.errors.NotFound:
            pass
        return
    if not GUILD_ID:
        try:
            await interaction.response.send_message("⚠️ DISCORD_GUILD_ID não configurado.", ephemeral=True)
        except discord.errors.NotFound:
            pass
        return

    try:
        await interaction.response.defer(ephemeral=True)
    except discord.errors.NotFound:
        return

    try:
        guild = discord.Object(id=GUILD_ID)
        await client.tree.sync(guild=guild)
        await interaction.followup.send("🔄 Comandos sincronizados com sucesso.", ephemeral=True)
    except Exception as e:
        try:
            await interaction.followup.send(f"⚠️ Falha ao sincronizar: {type(e).__name__}", ephemeral=True)
        except discord.errors.NotFound:
            pass


# =========================
# Admin: ciclo
# =========================
@client.tree.command(name="ciclo_abrir", description="(ADM) Cria/abre um ciclo (status=open).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_abrir(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        fields = get_cycle_fields(ws_cycles, cycle)
        if fields.get("status") == "completed":
            await interaction.followup.send("❌ Este ciclo está COMPLETED. Não reabrimos por segurança.", ephemeral=True)
            return

        set_cycle_status(ws_cycles, cycle, "open")
        await interaction.followup.send(f"✅ Ciclo {cycle} aberto (open).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="ciclo_encerrar", description="(ADM) Encerra ciclo (status=completed).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_encerrar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        set_cycle_status(ws_cycles, cycle, "completed")
        await interaction.followup.send(f"✅ Ciclo {cycle} encerrado (completed).", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Players + Enrollments por ciclo
# =========================
@client.tree.command(name="inscrever", description="Inscreve você em um CICLO (somente ciclo aberto).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_open)
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

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        fields = get_cycle_fields(ws_cycles, c)
        st = fields.get("status")

        if st is None:
            # cria automaticamente open
            ws_cycles.append_row([str(c), "open", "", "", nowb, nowb], value_input_option="USER_ENTERED")
            st = "open"

        if st == "completed":
            await interaction.followup.send("❌ Este ciclo já foi concluído. Escolha outro ciclo.", ephemeral=True)
            return
        if st == "locked":
            await interaction.followup.send("❌ Este ciclo já teve pods gerados e está LOCKED (inscrição fechada).", ephemeral=True)
            return

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)

        # Players
        prow = find_player_row(ws_players, discord_id)
        if prow is None:
            ws_players.append_row([str(discord_id), nick, "", "", "active", "0", nowb, nowb], value_input_option="USER_ENTERED")
        else:
            ws_players.update([[nick]], range_name=f"B{prow}")
            ws_players.update([["active"]], range_name=f"E{prow}")
            ws_players.update([[nowb]], range_name=f"H{prow}")

        # Enrollments
        data = ws_enr.get_all_values()
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        found_row = None
        for r_i in range(2, len(data) + 1):
            r = data[r_i - 1]
            cc = r[col["cycle"]] if col["cycle"] < len(r) else ""
            pid = r[col["player_id"]] if col["player_id"] < len(r) else ""
            if safe_int(cc, 0) == c and str(pid).strip() == str(discord_id):
                found_row = r_i
                break

        if found_row is None:
            ws_enr.append_row([str(c), str(discord_id), "active", nowb, nowb], value_input_option="USER_ENTERED")
            await interaction.followup.send(
                f"✅ Inscrito no **Ciclo {c}**.\n"
                "Você pode definir seu deck e decklist **apenas 1 vez** com `/deck` e `/decklist`.\n"
                "Se quiser sair do ciclo: `/drop cycle:...`",
                ephemeral=True
            )
        else:
            ws_enr.update([["active"]], range_name=f"{col_letter(col['status'])}{found_row}")
            ws_enr.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{found_row}")
            await interaction.followup.send(f"✅ Inscrição no **Ciclo {c}** reativada.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscrever: {e}", ephemeral=True)

@client.tree.command(name="drop", description="Sai do ciclo informado (somente enquanto ciclo open).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)", motivo="Opcional: motivo curto")
async def drop(interaction: discord.Interaction, cycle: int, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    nowb = now_br_str()

    try:
        sh = open_sheet()

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        st = get_cycle_fields(ws_cycles, cycle).get("status")

        if st == "locked":
            await interaction.followup.send("❌ Ciclo LOCKED (pods já gerados). Drop não permitido.", ephemeral=True)
            return
        if st == "completed":
            await interaction.followup.send("❌ Ciclo COMPLETED. Drop não permitido.", ephemeral=True)
            return

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)
        data = ws_enr.get_all_values()
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        found_row = None
        for r_i in range(2, len(data) + 1):
            r = data[r_i - 1]
            c = r[col["cycle"]] if col["cycle"] < len(r) else ""
            pid = r[col["player_id"]] if col["player_id"] < len(r) else ""
            if safe_int(c, 0) == cycle and str(pid).strip() == str(discord_id):
                found_row = r_i
                break

        if found_row is None:
            await interaction.followup.send(f"❌ Você não está inscrito no Ciclo {cycle}.", ephemeral=True)
            return

        ws_enr.update([["dropped"]], range_name=f"{col_letter(col['status'])}{found_row}")
        ws_enr.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{found_row}")

        msg = f"✅ Você saiu do **Ciclo {cycle}**.\nPara jogar outro ciclo, use `/inscrever cycle:...`"
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)

@client.tree.command(name="deck", description="Define seu deck (1 vez). ADM/Organizador podem alterar.")
@app_commands.describe(nome="Nome do deck (ex: UR Murktide)")
async def deck(interaction: discord.Interaction, nome: str):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        row = find_player_row(ws, discord_id)
        if row is None:
            await interaction.followup.send("❌ Use `/inscrever` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current = (fields.get("deck") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu seu deck e não pode alterar.\nPeça para ADM/Organizador se precisar.",
                ephemeral=True
            )
            return

        ws.update([[nome]], range_name=f"C{row}")
        ws.update([[nowb]], range_name=f"H{row}")
        await interaction.followup.send(f"✅ Deck salvo.\nDeck: **{nome}**", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar deck: {e}", ephemeral=True)

@client.tree.command(name="decklist", description="Define sua decklist (1 vez). ADM/Organizador podem alterar.")
@app_commands.describe(url="Link (moxfield.com ou ligamagic.com.br)")
async def decklist(interaction: discord.Interaction, url: str):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    nowb = now_br_str()

    ok, val = validate_decklist_url(url)
    if not ok:
        await interaction.followup.send(f"❌ {val}", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        row = find_player_row(ws, discord_id)
        if row is None:
            await interaction.followup.send("❌ Use `/inscrever` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current = (fields.get("decklist_url") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu sua decklist e não pode alterar.\nPeça para ADM/Organizador se precisar.",
                ephemeral=True
            )
            return

        ws.update([[val]], range_name=f"D{row}")
        ws.update([[nowb]], range_name=f"H{row}")
        await interaction.followup.send(f"✅ Decklist salva.\nLink: {val}", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar decklist: {e}", ephemeral=True)


# =========================
# Pods (gerar/ver) + trava ciclo + calcula prazo
# =========================
@client.tree.command(name="pods_gerar", description="(ADM) Sorteia pods do ciclo, grava PodsHistory e cria Matches pending.")
@app_commands.describe(cycle="Ciclo (ex: 1)", tamanho="Tamanho do pod (padrão 4)")
async def pods_gerar(interaction: discord.Interaction, cycle: int, tamanho: int = 4):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    tamanho = max(2, min(int(tamanho), 8))
    nowb = now_br_str()

    try:
        sh = open_sheet()

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        fields = get_cycle_fields(ws_cycles, cycle)
        st = fields.get("status")

        if st is None:
            ws_cycles.append_row([str(cycle), "open", "", "", nowb, nowb], value_input_option="USER_ENTERED")
            st = "open"

        if st == "completed":
            await interaction.followup.send("❌ Ciclo COMPLETED. Não pode gerar pods.", ephemeral=True)
            return
        if st == "locked":
            await interaction.followup.send("❌ Pods já foram gerados. Ciclo está LOCKED. Não pode gerar novamente.", ephemeral=True)
            return

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=10000, cols=20)
        ws_matches = sh.worksheet("Matches")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # se já existe PodsHistory no ciclo -> bloqueia
        pods_rows = ws_pods.get_all_records()
        if any(safe_int(r.get("cycle", 0), 0) == cycle for r in pods_rows):
            set_cycle_status(ws_cycles, cycle, "locked")
            await interaction.followup.send(
                f"❌ Já existe PodsHistory para o Ciclo {cycle}. Não é permitido gerar novamente.\n"
                "Status do ciclo ajustado para LOCKED.",
                ephemeral=True
            )
            return

        # inscritos ativos
        enr_rows = ws_enr.get_all_records()
        players = []
        for r in enr_rows:
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if str(r.get("status", "")).strip().lower() != "active":
                continue
            pid = str(r.get("player_id", "")).strip()
            if pid:
                players.append(pid)

        if len(players) < 2:
            await interaction.followup.send("Poucos inscritos ativos no ciclo para gerar pods.", ephemeral=True)
            return

        past_pairs = get_past_confirmed_pairs(ws_matches)
        pods, repeat_score = best_shuffle_min_repeats(players, tamanho, past_pairs, tries=250)

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        created_matches = 0
        nick_map = build_players_nick_map(ws_players)

        # grava PodsHistory + cria matches
        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"

            for pid in pod_players:
                ws_pods.append_row([str(cycle), pod_name, pid, nowb], value_input_option="USER_ENTERED")

            for a, b in round_robin_pairs(pod_players):
                mid = new_match_id(cycle, pod_name)
                ac_at = auto_confirm_deadline_iso(utc_now_dt())
                row = [
                    mid, str(cycle), pod_name,
                    str(a), str(b),
                    "0", "0", "0",
                    "normal",
                    "pending",
                    "", "", "",
                    "TRUE",
                    nowb, nowb,
                    ac_at
                ]
                ws_matches.append_row(row, value_input_option="USER_ENTERED")
                created_matches += 1

        # trava ciclo
        set_cycle_status(ws_cycles, cycle, "locked")

        # calcula e grava prazo do ciclo no Cycles
        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(cycle, ws_pods, ws_cycles)
        if start_str and deadline_str:
            set_cycle_times(ws_cycles, cycle, start_str, deadline_str)

        lines = [f"🧩 Pods do **Ciclo {cycle}** gerados (tamanho base {tamanho})."]
        lines.append(f"♻️ Anti-repetição: penalidade final **{repeat_score}** (quanto menor, melhor).")
        lines.append("🔒 Ciclo agora está **LOCKED** (inscrição fechada).")
        if start_str and deadline_str:
            lines.append(f"⏳ Prazo do ciclo (maior POD = {max_pod_size}): **{days} dias**")
            lines.append(f"🕑 Início: **{start_str} (BR)**")
            lines.append(f"🛑 Fim: **{deadline_str} (BR)**")

        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"
            lines.append(f"\n**Pod {pod_name}**")
            for pid in pod_players:
                lines.append(f"• {nick_map.get(pid, pid)} (`{pid}`)")

        lines.append(f"\n✅ Matches criados: **{created_matches}** (pending).")
        lines.append("Jogadores: use `/meus_matches cycle:X` para ver seus match_id.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_gerar: {e}", ephemeral=True)

@client.tree.command(name="pods_ver", description="Mostra pods do ciclo (com nomes).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def pods_ver(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=10000, cols=20)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        nick_map = build_players_nick_map(ws_players)
        rows = ws_pods.get_all_records()
        rows = [r for r in rows if safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            await interaction.followup.send("Nenhum pod encontrado para esse ciclo.", ephemeral=True)
            return

        pods = {}
        for r in rows:
            pod = str(r.get("pod", "")).strip()
            pid = str(r.get("player_id", "")).strip()
            pods.setdefault(pod, []).append(pid)

        out = [f"🧩 Pods do **Ciclo {cycle}**"]
        for pod in sorted(pods.keys()):
            out.append(f"\n**Pod {pod}**")
            for pid in pods[pod]:
                out.append(f"• {nick_map.get(pid, pid)} (`{pid}`)")

        await interaction.followup.send("\n".join(out), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Prazo do ciclo
# =========================
@client.tree.command(name="prazo", description="Mostra a data/hora limite do ciclo (baseado no maior POD).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def prazo(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=10000, cols=20)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(cycle, ws_pods, ws_cycles)
        if not deadline_str:
            await interaction.followup.send("❌ Ainda não existem pods para esse ciclo. Gere os pods primeiro.", ephemeral=True)
            return

        # grava para manter consistente
        set_cycle_times(ws_cycles, cycle, start_str, deadline_str)

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
            f"⏳ **Prazo do Ciclo {cycle}**\n"
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
async def meus_matches(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        my = []
        nowu = utc_now_dt()

        for r in rows:
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if not as_bool(r.get("active", "TRUE")):
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if user_id not in (a, b):
                continue

            mid = str(r.get("match_id", "")).strip()
            pod = str(r.get("pod", "")).strip()
            st = str(r.get("confirmed_status", "")).strip().lower()
            rep = str(r.get("reported_by_id", "")).strip()
            ag = str(r.get("a_games_won", "0"))
            bg = str(r.get("b_games_won", "0"))
            dg = str(r.get("draw_games", "0"))

            ac = parse_iso_dt(r.get("auto_confirm_at", "") or "")
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
            await interaction.followup.send(f"Você não tem matches no Ciclo {cycle}.", ephemeral=True)
            return

        my.sort(key=lambda x: (x[0], x[1]))
        out = [f"📌 **Seus matches - Ciclo {cycle}**"]
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
        await interaction.followup.send("❌ Placar inválido.", ephemeral=True)
        return

    v, d, e = sc
    ok, msg = validate_3parts_rules(v, d, e)
    if not ok:
        await interaction.followup.send(f"❌ {msg}", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)
            return

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if not as_bool(getc("active") or "TRUE"):
            await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)
            return

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            await interaction.followup.send(f"❌ Match não está pending (atual: {status}).", ephemeral=True)
            return

        a_id = (getc("player_a_id") or "").strip()
        b_id = (getc("player_b_id") or "").strip()

        if reporter_id not in (a_id, b_id):
            await interaction.followup.send("❌ Você não faz parte deste match.", ephemeral=True)
            return

        # grava no formato A/B do match
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

        ws.update([[str(a_gw)]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(col['draw_games'])}{rown}")
        ws.update([[rt]], range_name=f"{col_letter(col['result_type'])}{rown}")
        ws.update([[reporter_id]], range_name=f"{col_letter(col['reported_by_id'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")
        ws.update([[ac_at]], range_name=f"{col_letter(col['auto_confirm_at'])}{rown}")

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

        cell = ws.find(str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)
            return

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if not as_bool(getc("active") or "TRUE"):
            await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)
            return

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            await interaction.followup.send(f"❌ Match não está pending (atual: {status}).", ephemeral=True)
            return

        reported_by = (getc("reported_by_id") or "").strip()
        if not reported_by:
            await interaction.followup.send("❌ Ainda não existe resultado reportado.", ephemeral=True)
            return

        a_id = (getc("player_a_id") or "").strip()
        b_id = (getc("player_b_id") or "").strip()

        opponent_allowed = a_id if reported_by == b_id else b_id
        if user_id != opponent_allowed:
            await interaction.followup.send("❌ Apenas o **oponente** pode rejeitar.", ephemeral=True)
            return

        ac = parse_iso_dt(getc("auto_confirm_at") or "")
        if ac and utc_now_dt() > ac:
            await interaction.followup.send(
                "❌ Prazo expirou (48h). Peça para um ADM/Organizador revisar.",
                ephemeral=True
            )
            return

        nowb = now_br_str()
        ws.update([["rejected"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
        ws.update([[user_id]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        msg = "✅ Resultado rejeitado. ADM/Organizador pode corrigir."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================
# Admin: editar/cancelar resultado (ESSENCIAL)
# =========================
@client.tree.command(name="admin_resultado_editar", description="(ADM) Edita resultado de um match e opcionalmente confirma.")
@app_commands.autocomplete(match_id=ac_match_id_any)
@app_commands.describe(match_id="match_id", placar="Placar V-D-E do player_a (formato do match)", confirmar="confirmar agora?")
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
        app_commands.Choice(name="Sim (confirmar)", value="yes"),
        app_commands.Choice(name="Não (manter pending)", value="no"),
    ]
)
async def admin_resultado_editar(interaction: discord.Interaction, match_id: str, placar: app_commands.Choice[str], confirmar: app_commands.Choice[str]):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    sc = parse_score_3parts(placar.value)
    if not sc:
        await interaction.followup.send("❌ Placar inválido.", ephemeral=True)
        return
    a_gw, b_gw, d_g = sc
    ok, msg = validate_3parts_rules(a_gw, b_gw, d_g)
    if not ok:
        await interaction.followup.send(f"❌ {msg}", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        rown = cell.row
        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if not as_bool(getc("active") or "TRUE"):
            await interaction.followup.send("❌ Match inativo.", ephemeral=True)
            return

        rt = "normal"
        if a_gw == b_gw:
            rt = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            rt = "intentional_draw"

        nowb = now_br_str()
        ws.update([[str(a_gw)]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(col['draw_games'])}{rown}")
        ws.update([[rt]], range_name=f"{col_letter(col['result_type'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        if confirmar.value == "yes":
            ws.update([["confirmed"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
            ws.update([[str(interaction.user.id)]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")

        await interaction.followup.send(
            f"✅ Match atualizado.\n"
            f"match_id: `{match_id}`\n"
            f"Placar (A-B-E): **{a_gw}-{b_gw}-{d_g}**\n"
            f"Status: **{'confirmed' if confirmar.value=='yes' else 'pending'}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="admin_resultado_cancelar", description="(ADM) Cancela um match (active=FALSE).")
@app_commands.autocomplete(match_id=ac_match_id_any)
@app_commands.describe(match_id="match_id", motivo="Opcional")
async def admin_resultado_cancelar(interaction: discord.Interaction, match_id: str, motivo: str = ""):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        rown = cell.row

        nowb = now_br_str()
        ws.update([["FALSE"]], range_name=f"{col_letter(col['active'])}{rown}")
        ws.update([["canceled"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
        ws.update([[str(interaction.user.id)]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        msg = f"✅ Match cancelado (active=FALSE): `{match_id}`"
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# /deadline (48h) - recomendado forte
# =========================
@client.tree.command(name="deadline", description="Lista resultados pendentes próximos de expirar (48h).")
@app_commands.describe(cycle="Ciclo (ex: 1)", horas="Janela (ex: 12)")
async def deadline(interaction: discord.Interaction, cycle: int, horas: int = 12):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        nowu = utc_now_dt()
        hours = max(1, min(horas, 48))
        limit_dt = nowu + timedelta(hours=hours)

        rows = ws.get_all_records()
        items = []
        for r in rows:
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "pending":
                continue
            if not str(r.get("reported_by_id", "")).strip():
                continue
            ac = parse_iso_dt(r.get("auto_confirm_at", "") or "")
            if not ac:
                continue
            if ac <= limit_dt:
                a = str(r.get("player_a_id", "")).strip()
                b = str(r.get("player_b_id", "")).strip()
                pod = str(r.get("pod", "")).strip()
                mid = str(r.get("match_id", "")).strip()
                items.append((ac, f"• `{mid}` Pod {pod}: {nick_map.get(a,a)} vs {nick_map.get(b,b)} | expira {ac.isoformat()} UTC"))

        if not items:
            await interaction.followup.send(f"✅ Nenhum pending expira nas próximas {hours}h (Ciclo {cycle}).", ephemeral=True)
            return

        items.sort(key=lambda x: x[0])
        out = [f"⏰ Pendências (Ciclo {cycle}) que expiram em até {hours}h:"]
        out.extend([x[1] for x in items[:40]])
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# /final (ADM) - aplica 0-0-3 em matches sem report
# =========================
@client.tree.command(name="final", description="(ADM) Aplica 0-0-3 em todos os matches sem resultado reportado no ciclo.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def final(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=10000, cols=20)
        ws_matches = sh.worksheet("Matches")

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # valida prazo (se já expirou)
        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(cycle, ws_pods, ws_cycles)
        if not deadline_str:
            await interaction.followup.send("❌ Este ciclo ainda não tem pods/prazo.", ephemeral=True)
            return
        set_cycle_times(ws_cycles, cycle, start_str, deadline_str)

        deadline_dt = parse_br_dt(deadline_str)
        if deadline_dt and now_br_dt() < deadline_dt:
            await interaction.followup.send(
                f"❌ Ainda não chegou o fim do ciclo.\nFim: **{deadline_str} (BR)**\nUse `/prazo` para ver.",
                ephemeral=True
            )
            return

        rows = ws_matches.get_all_values()
        if len(rows) <= 1:
            await interaction.followup.send("Nada para finalizar (Matches vazio).", ephemeral=True)
            return

        nowb = now_br_str()
        changed = 0

        for rown in range(2, len(rows) + 1):
            r = rows[rown - 1]
            def getc(name: str) -> str:
                idx = col[name]
                return r[idx] if idx < len(r) else ""

            if safe_int(getc("cycle"), 0) != cycle:
                continue
            if not as_bool(getc("active") or "TRUE"):
                continue

            rep = (getc("reported_by_id") or "").strip()
            if rep:
                continue  # já tem report

            # aplicar 0-0-3 e confirmar como oficial
            ws_matches.update([["0"]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
            ws_matches.update([["0"]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
            ws_matches.update([["3"]], range_name=f"{col_letter(col['draw_games'])}{rown}")
            ws_matches.update([["intentional_draw"]], range_name=f"{col_letter(col['result_type'])}{rown}")
            ws_matches.update([["confirmed"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
            ws_matches.update([["FINAL"]], range_name=f"{col_letter(col['reported_by_id'])}{rown}")
            ws_matches.update([["FINAL"]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
            ws_matches.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")
            changed += 1

        await interaction.followup.send(
            f"✅ Finalização aplicada no Ciclo {cycle}.\n"
            f"Matches sem report que receberam **0-0-3**: **{changed}**\n"
            "Agora rode `/recalcular` para atualizar o ranking.",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /final: {e}", ephemeral=True)


# =========================
# Recalcular ranking
# =========================
@client.tree.command(name="recalcular", description="(ADM) Auto-confirm (48h) + recalcula ranking do ciclo do zero.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def recalcular(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        changed = 0
        try:
            changed = sweep_auto_confirm(sh, cycle)
        except Exception:
            pass

        rows = recalculate_cycle(cycle)
        await interaction.followup.send(
            f"✅ Recalculo concluído. Ciclo {cycle} atualizado.\n"
            f"Auto-confirm (48h) feitos: **{changed}**\n"
            f"Jogadores no Standings: **{len(rows)}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"⚠️ Erro no recálculo: {e}", ephemeral=True)


# =========================
# Ranking público (ESSENCIAL)
# =========================
@client.tree.command(name="ranking", description="Mostra o ranking do ciclo (público).")
@app_commands.describe(cycle="Ciclo (ex: 1)", top="Quantos mostrar (padrão 20)")
async def ranking(interaction: discord.Interaction, cycle: int, top: int = 20):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        ws_st = sh.worksheet("Standings")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        data = ws_st.get_all_records()
        rows = [r for r in data if safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            await interaction.followup.send("Sem standings para esse ciclo. Rode `/recalcular`.", ephemeral=False)
            return

        top = max(5, min(top, 50))
        rows.sort(key=lambda r: safe_int(r.get("rank_position", 9999), 9999))

        out = [f"🏆 **Ranking - Ciclo {cycle}** (Top {top})"]
        out.append("pos | jogador | pts | OMW | GW | OGW")
        out.append("--- | ------ | --- | --- | --- | ---")

        for r in rows[:top]:
            pid = str(r.get("player_id", "")).strip()
            pos = str(r.get("rank_position", ""))
            pts = str(r.get("match_points", ""))
            omw = str(r.get("omw_percent", ""))
            gw = str(r.get("gw_percent", ""))
            ogw = str(r.get("ogw_percent", ""))
            out.append(f"{pos} | {nick_map.get(pid, pid)} | {pts} | {omw} | {gw} | {ogw}")

        await interaction.followup.send("\n".join(out), ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ranking: {e}", ephemeral=False)

@client.tree.command(name="standings_publicar", description="(ADM) Publica o ranking no canal configurado (ou no atual).")
@app_commands.describe(cycle="Ciclo (ex: 1)", top="Quantos mostrar (padrão 20)")
async def standings_publicar(interaction: discord.Interaction, cycle: int, top: int = 20):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_st = sh.worksheet("Standings")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        data = ws_st.get_all_records()
        rows = [r for r in data if safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            await interaction.followup.send("Sem standings. Rode `/recalcular`.", ephemeral=True)
            return

        top = max(5, min(top, 50))
        rows.sort(key=lambda r: safe_int(r.get("rank_position", 9999), 9999))

        msg = [f"🏆 **Ranking - Ciclo {cycle}** (Top {top})"]
        for r in rows[:top]:
            pid = str(r.get("player_id", "")).strip()
            msg.append(
                f"{r.get('rank_position','?')}. {nick_map.get(pid,pid)} "
                f"| pts {r.get('match_points','')} | OMW {r.get('omw_percent','')} | GW {r.get('gw_percent','')} | OGW {r.get('ogw_percent','')}"
            )

        channel = interaction.channel
        if RANKING_CHANNEL_ID and interaction.guild:
            ch = interaction.guild.get_channel(RANKING_CHANNEL_ID)
            if ch:
                channel = ch

        await channel.send("\n".join(msg))
        await interaction.followup.send("✅ Ranking publicado.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Status do ciclo (recomendado forte)
# =========================
@client.tree.command(name="status_ciclo", description="Mostra status geral do ciclo (inscritos, pods, matches, pendências).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def status_ciclo(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=10000, cols=20)
        ws_matches = sh.worksheet("Matches")

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        fields = get_cycle_fields(ws_cycles, cycle)
        st = fields.get("status") or "—"

        enr = ws_enr.get_all_records()
        enr_active = [r for r in enr if safe_int(r.get("cycle", 0), 0) == cycle and str(r.get("status","")).strip().lower() == "active"]

        pods = ws_pods.get_all_records()
        pods_cycle = [r for r in pods if safe_int(r.get("cycle", 0), 0) == cycle]

        matches = ws_matches.get_all_records()
        m_cycle = [r for r in matches if safe_int(r.get("cycle", 0), 0) == cycle and as_bool(r.get("active","TRUE"))]

        pending = [r for r in m_cycle if str(r.get("confirmed_status","")).strip().lower() == "pending"]
        confirmed = [r for r in m_cycle if str(r.get("confirmed_status","")).strip().lower() == "confirmed"]
        rejected = [r for r in m_cycle if str(r.get("confirmed_status","")).strip().lower() == "rejected"]

        # pods count
        pod_names = set(str(r.get("pod","")).strip() for r in pods_cycle if str(r.get("pod","")).strip())
        out = [
            f"📊 **Status Ciclo {cycle}**",
            f"Status: **{st}**",
            f"Inscritos ativos: **{len(enr_active)}**",
            f"Pods gerados: **{len(pod_names)}**",
            f"Matches ativos: **{len(m_cycle)}**",
            f"Confirmed: **{len(confirmed)}** | Pending: **{len(pending)}** | Rejected: **{len(rejected)}**",
        ]
        await interaction.followup.send("\n".join(out), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Luxo competitivo: exportar ciclo (CSV)
# =========================
@client.tree.command(name="exportar_ciclo", description="(ADM) Exporta CSV do ciclo (Matches e Standings).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def exportar_ciclo(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ws_st = sh.worksheet("Standings")

        matches = ws_matches.get_all_records()
        m_cycle = [r for r in matches if safe_int(r.get("cycle", 0), 0) == cycle]

        standings = ws_st.get_all_records()
        s_cycle = [r for r in standings if safe_int(r.get("cycle", 0), 0) == cycle]

        # CSV em memória
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["=== MATCHES ==="])
        if m_cycle:
            w.writerow(list(m_cycle[0].keys()))
            for r in m_cycle:
                w.writerow([r.get(k, "") for k in m_cycle[0].keys()])
        else:
            w.writerow(["(vazio)"])

        w.writerow([])
        w.writerow(["=== STANDINGS ==="])
        if s_cycle:
            w.writerow(list(s_cycle[0].keys()))
            for r in s_cycle:
                w.writerow([r.get(k, "") for k in s_cycle[0].keys()])
        else:
            w.writerow(["(vazio)"])

        data = buf.getvalue().encode("utf-8")
        file = discord.File(io.BytesIO(data), filename=f"ciclo_{cycle}_export.csv")

        await interaction.followup.send("✅ Export gerado:", file=file, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Luxo: fechar resultados atrasados (administrativo)
# =========================
@client.tree.command(name="fechar_resultados_atrasados", description="(ADM) Fecha matches pendentes expirados (48h) e aplica auto-confirm.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def fechar_resultados_atrasados(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        changed = sweep_auto_confirm(sh, cycle)
        await interaction.followup.send(f"✅ Auto-confirm aplicado. Alterados: **{changed}**", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Luxo: substituir jogador
# - troca player_id no ciclo em PodsHistory e Matches (somente matches SEM report)
# =========================
@client.tree.command(name="substituir_jogador", description="(ADM) Substitui um jogador no ciclo (somente matches sem report).")
@app_commands.describe(cycle="Ciclo", antigo_id="Discord ID antigo", novo_id="Discord ID novo")
async def substituir_jogador(interaction: discord.Interaction, cycle: int, antigo_id: str, novo_id: str):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    antigo_id = str(antigo_id).strip()
    novo_id = str(novo_id).strip()
    if not antigo_id.isdigit() or not novo_id.isdigit():
        await interaction.followup.send("❌ Informe discord_id numérico para antigo e novo.", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=10000, cols=20)
        ws_matches = sh.worksheet("Matches")

        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # PodsHistory: substituir
        pods_vals = ws_pods.get_all_values()
        pods_col = ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)

        pods_changed = 0
        for rown in range(2, len(pods_vals) + 1):
            r = pods_vals[rown - 1]
            c = r[pods_col["cycle"]] if pods_col["cycle"] < len(r) else ""
            pid = r[pods_col["player_id"]] if pods_col["player_id"] < len(r) else ""
            if safe_int(c, 0) == cycle and str(pid).strip() == antigo_id:
                ws_pods.update([[novo_id]], range_name=f"{col_letter(pods_col['player_id'])}{rown}")
                pods_changed += 1

        # Matches: substituir SOMENTE se sem report
        mv = ws_matches.get_all_values()
        mcol = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        matches_changed = 0

        for rown in range(2, len(mv) + 1):
            r = mv[rown - 1]
            def getc(name: str) -> str:
                idx = mcol[name]
                return r[idx] if idx < len(r) else ""

            if safe_int(getc("cycle"), 0) != cycle:
                continue
            if not as_bool(getc("active") or "TRUE"):
                continue
            if (getc("reported_by_id") or "").strip():
                continue  # não mexe se já tem report

            a = (getc("player_a_id") or "").strip()
            b = (getc("player_b_id") or "").strip()
            if a == antigo_id:
                ws_matches.update([[novo_id]], range_name=f"{col_letter(mcol['player_a_id'])}{rown}")
                matches_changed += 1
            if b == antigo_id:
                ws_matches.update([[novo_id]], range_name=f"{col_letter(mcol['player_b_id'])}{rown}")
                matches_changed += 1

        await interaction.followup.send(
            f"✅ Substituição concluída (Ciclo {cycle}).\n"
            f"PodsHistory alterados: **{pods_changed}**\n"
            f"Matches alterados (sem report): **{matches_changed}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Recomendado forte: histórico confronto + estatísticas
# =========================
@client.tree.command(name="historico_confronto", description="Mostra histórico de confrontos confirmados entre dois jogadores.")
@app_commands.describe(jogador_a_id="Discord ID A", jogador_b_id="Discord ID B")
async def historico_confronto(interaction: discord.Interaction, jogador_a_id: str, jogador_b_id: str):
    await interaction.response.defer(ephemeral=True)

    a_id = str(jogador_a_id).strip()
    b_id = str(jogador_b_id).strip()
    if not a_id.isdigit() or not b_id.isdigit():
        await interaction.followup.send("❌ Informe discord_id numérico para A e B.", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        hits = []
        for r in rows:
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
                continue
            pa = str(r.get("player_a_id", "")).strip()
            pb = str(r.get("player_b_id", "")).strip()
            if set([pa, pb]) != set([a_id, b_id]):
                continue
            cyc = safe_int(r.get("cycle", 0), 0)
            pod = str(r.get("pod","")).strip()
            ag = str(r.get("a_games_won","0"))
            bg = str(r.get("b_games_won","0"))
            dg = str(r.get("draw_games","0"))
            hits.append((cyc, pod, f"C{cyc} Pod {pod}: {ag}-{bg}-{dg}"))

        if not hits:
            await interaction.followup.send("Nenhum confronto confirmado encontrado entre esses dois jogadores.", ephemeral=True)
            return

        hits.sort(key=lambda x: x[0])
        out = [
            f"📚 Histórico: **{nick_map.get(a_id,a_id)}** vs **{nick_map.get(b_id,b_id)}**",
            *[f"• {x[2]}" for x in hits[:40]]
        ]
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="estatisticas", description="Mostra estatísticas gerais confirmadas do jogador.")
@app_commands.describe(jogador_id="Discord ID do jogador")
async def estatisticas(interaction: discord.Interaction, jogador_id: str):
    await interaction.response.defer(ephemeral=True)
    pid = str(jogador_id).strip()
    if not pid.isdigit():
        await interaction.followup.send("❌ Informe discord_id numérico.", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        mp = 0
        matches = 0
        gw = 0
        gl = 0
        gd = 0

        for r in rows:
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("confirmed_status","")).strip().lower() != "confirmed":
                continue
            a = str(r.get("player_a_id","")).strip()
            b = str(r.get("player_b_id","")).strip()
            if pid not in (a, b):
                continue

            a_gw = safe_int(r.get("a_games_won",0),0)
            b_gw = safe_int(r.get("b_games_won",0),0)
            d_g = safe_int(r.get("draw_games",0),0)

            matches += 1
            gd += d_g

            # ponto de match
            if a_gw > b_gw:
                winner = a
            elif b_gw > a_gw:
                winner = b
            else:
                winner = None

            if winner is None:
                mp += 1
            else:
                if winner == pid:
                    mp += 3

            # games
            if pid == a:
                gw += a_gw
                gl += b_gw
            else:
                gw += b_gw
                gl += a_gw

        if matches == 0:
            await interaction.followup.send("Sem estatísticas (nenhum match confirmado para esse jogador).", ephemeral=True)
            return

        mwp = floor_333(mp / (3.0 * matches))
        gplayed = gw + gl + gd
        gwp = floor_333((gw + 0.5*gd) / float(gplayed)) if gplayed > 0 else 1/3

        await interaction.followup.send(
            f"📈 Estatísticas: **{nick_map.get(pid,pid)}**\n"
            f"Matches confirmados: **{matches}**\n"
            f"Match Points: **{mp}** | MWP: **{pct1(mwp)}**\n"
            f"Games: W {gw} / L {gl} / D {gd} | GWP: **{pct1(gwp)}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Start
# =========================
if not DISCORD_TOKEN:
    raise RuntimeError("Faltou a variável DISCORD_TOKEN no ambiente.")

keep_alive()
client.run(DISCORD_TOKEN)
