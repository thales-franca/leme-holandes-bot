# =========================================================
# [BLOCO 1/8] — IMPORTS + CONFIG + HELPERS BASE
# (Cole os 8 blocos em sequência, 1 abaixo do outro)
# =========================================================

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
# LEME HOLANDÊS BOT — Discord + Google Sheets + Render
# =========================================================
# PRINCÍPIOS DO PROJETO (fixos):
# - Tudo na MESMA planilha, com histórico completo.
# - Season existe e separa os dados (season_id).
# - Ciclos existem DENTRO de uma season (cycle_number).
# - Somente o DONO do servidor controla Season (abrir/fechar).
# - ADM/Organizador controlam ciclo e operação do torneio.
# - Deck e Decklist: 1 vez POR CICLO (não trava ciclo seguinte).
# - Ranking oficial: Pontos > OMW% > GW% > OGW%
# - Piso 33,3% aplicado em MWP e GWP antes de calcular OMW/OGW
# - Recalcular ranking sempre do zero (nunca incremental)
# - Resultado: V-D-E em GAMES (Vitória/Derrota/Empate)
# - Empate em games conta como 0.5 na GWP
# - Report vira PENDENTE; oponente tem 48h para rejeitar
# - Se não rejeitar, varredura /recalcular auto-confirma
# - Prazo do ciclo (/prazo): depende do maior POD
#   POD 3 -> 5 dias corridos
#   POD 4 -> 8 dias corridos
#   POD 5 ou 6 -> 10 dias corridos
#   Ciclo começa 14:00 (BR) e termina no último dia às 13:59 (BR)
# - /final (ADM): aplica 0-0-3 (ID) em matches sem report após prazo
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
ROLE_JOGADOR = os.getenv("ROLE_JOGADOR", "Jogador")  # nome do cargo "Jogador"

# Canal de ranking público (opcional)
RANKING_CHANNEL_ID = int(os.getenv("RANKING_CHANNEL_ID", "0"))

# Canal de log administrativo (você já configurou)
LOG_ADMIN_CHANNEL_ID = int(os.getenv("LOG_ADMIN_CHANNEL_ID", "0"))


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

def fmt_br_dt(dt: datetime) -> str:
    return dt.astimezone(BR_TZ).strftime("%Y-%m-%d %H:%M:%S")


# =========================
# Helpers gerais
# =========================
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

def ensure_sheet_columns(ws, required_cols: list[str]):
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"Aba '{ws.title}' sem cabeçalho na linha 1.")
    idx = {name: i for i, name in enumerate(header)}  # 0-based
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


# =========================
# LOG admin (canal do Render)
# =========================
async def log_admin(interaction: discord.Interaction | None, message: str):
    """
    Envia log no canal LOG_ADMIN_CHANNEL_ID (se configurado).
    Não quebra fluxo se falhar.
    """
    try:
        guild = interaction.guild if interaction else None
        if not guild:
            return
        if not LOG_ADMIN_CHANNEL_ID:
            return
        ch = guild.get_channel(LOG_ADMIN_CHANNEL_ID)
        if not ch:
            try:
                ch = await guild.fetch_channel(LOG_ADMIN_CHANNEL_ID)
            except Exception:
                ch = None
        if not ch:
            return

        who = "SYSTEM"
        if interaction and interaction.user:
            who = f"{interaction.user} ({interaction.user.id})"
        ts = now_br_str()
        await ch.send(f"🧾 **LOG** [{ts} BR]\n**Quem:** {who}\n**Ação:** {message}")
    except Exception:
        pass


# =========================
# Auth helpers (permissões)
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

async def is_owner_only(interaction: discord.Interaction) -> bool:
    # Dono real do servidor (sem cargo manual)
    if not interaction.guild or not interaction.user:
        return False
    return interaction.guild.owner_id == interaction.user.id

async def is_admin_or_organizer(interaction: discord.Interaction) -> bool:
    # Admin do Discord OU cargos ADM/Organizador
    if interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild:
        return True
    if await has_role(interaction, ROLE_ADM):
        return True
    if await has_role(interaction, ROLE_ORGANIZADOR):
        return True
    return False

async def is_organizer_only(interaction: discord.Interaction) -> bool:
    return await has_role(interaction, ROLE_ORGANIZADOR)

async def get_access_level(interaction: discord.Interaction) -> str:
    # "owner" | "organizador" | "adm" | "jogador"
    if await is_owner_only(interaction):
        return "owner"
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
# Score helpers (V-D-E)
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


# =========================================================
# [BLOCO 1/8 termina aqui]
# No BLOCO 2/8 eu trago:
# - Schema completo da planilha (Seasons, SeasonState, Players, Enrollments...)
# - Funções de season/ciclo (get_current_season, abrir/fechar season, etc.)
# =========================================================


# =========================================================
# [BLOCO 2/8] — SCHEMA (ABAS) + SEASON STATE + FUNÇÕES DE SEASON/CICLO
# =========================================================

# =========================
# Sheets schema (headers)
# =========================
# Observação: Mantemos tudo na MESMA planilha.
# A season atual é controlada por SeasonState (1 linha).

SEASONSTATE_HEADER = ["key", "value", "updated_at"]  # key="current_season_id"

SEASONS_HEADER = ["season_id", "status", "name", "created_at", "updated_at"]  # status: open|closed

PLAYERS_HEADER = ["discord_id","nick","status","created_at","updated_at"]
# status em Players é só "active"/"banned" etc (não é por ciclo)

# Deck/Decklist agora são por ciclo (e por season), para permitir 1 vez POR CICLO
DECKS_HEADER = ["season_id","cycle","player_id","deck","decklist_url","created_at","updated_at"]
DECKS_REQUIRED = ["season_id","cycle","player_id","deck","decklist_url","created_at","updated_at"]

ENROLLMENTS_HEADER = ["season_id","cycle","player_id","status","created_at","updated_at"]
ENROLLMENTS_REQUIRED = ["season_id","cycle","player_id","status","created_at","updated_at"]

CYCLES_HEADER = ["season_id","cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]
# status: open|locked|completed
CYCLES_REQUIRED = ["season_id","cycle","status","start_at_br","deadline_at_br","created_at","updated_at"]

PODSHISTORY_HEADER = ["season_id","cycle","pod","player_id","created_at"]
PODSHISTORY_REQUIRED = ["season_id","cycle","pod","player_id","created_at"]

MATCHES_REQUIRED_COLS = [
    "match_id",
    "season_id",
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
    "season_id","cycle","player_id",
    "matches_played","match_points","mwp_percent",
    "game_wins","game_losses","game_draws","games_played","gw_percent",
    "omw_percent","ogw_percent",
    "rank_position","last_recalc_at"
]
STANDINGS_REQUIRED = STANDINGS_HEADER[:]  # mesmas


# =========================
# Ensure abas existem
# =========================
def ensure_all_sheets(sh):
    ensure_worksheet(sh, "SeasonState", SEASONSTATE_HEADER, rows=20, cols=10)
    ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
    ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=20)
    ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=20)
    ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=20)
    ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=20)
    # Matches e Standings assumimos que já existem, mas garantimos:
    ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
    ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30)


# =========================
# Season helpers
# =========================
def get_current_season_id(sh) -> int:
    ws = sh.worksheet("SeasonState")
    rows = ws.get_all_records()
    for r in rows:
        if str(r.get("key","")).strip() == "current_season_id":
            return safe_int(r.get("value", 0), 0)
    return 0

def set_current_season_id(sh, season_id: int):
    ws = sh.worksheet("SeasonState")
    vals = ws.get_all_values()
    nowb = now_br_str()

    # procura key
    found = None
    for i in range(2, len(vals)+1):
        row = vals[i-1]
        if len(row) >= 1 and str(row[0]).strip() == "current_season_id":
            found = i
            break

    if found is None:
        ws.append_row(["current_season_id", str(season_id), nowb], value_input_option="USER_ENTERED")
    else:
        ws.update([[str(season_id)]], range_name=f"B{found}")
        ws.update([[nowb]], range_name=f"C{found}")

def season_exists(sh, season_id: int) -> bool:
    ws = sh.worksheet("Seasons")
    rows = ws.get_all_records()
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) == season_id:
            return True
    return False

def get_season_status(sh, season_id: int) -> str:
    ws = sh.worksheet("Seasons")
    rows = ws.get_all_records()
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) == season_id:
            return str(r.get("status","")).strip().lower()
    return ""

def set_season_status(sh, season_id: int, status: str, name: str | None = None):
    status = str(status).strip().lower()
    ws = sh.worksheet("Seasons")
    data = ws.get_all_values()
    nowb = now_br_str()

    # header: season_id,status,name,created_at,updated_at
    found = None
    for i in range(2, len(data)+1):
        row = data[i-1]
        sid = safe_int(row[0] if len(row)>0 else 0, 0)
        if sid == season_id:
            found = i
            break

    if found is None:
        nm = name or f"Temporada {season_id}"
        ws.append_row([str(season_id), status, nm, nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws.update([[status]], range_name=f"B{found}")
        if name is not None:
            ws.update([[name]], range_name=f"C{found}")
        ws.update([[nowb]], range_name=f"E{found}")

def close_all_other_seasons(sh, keep_open_id: int):
    ws = sh.worksheet("Seasons")
    data = ws.get_all_values()
    if len(data) <= 1:
        return 0
    nowb = now_br_str()
    changed = 0
    for i in range(2, len(data)+1):
        row = data[i-1]
        sid = safe_int(row[0] if len(row)>0 else 0, 0)
        if sid <= 0 or sid == keep_open_id:
            continue
        st = (row[1] if len(row)>1 else "").strip().lower()
        if st != "closed":
            ws.update([["closed"]], range_name=f"B{i}")
            ws.update([[nowb]], range_name=f"E{i}")
            changed += 1
    return changed


# =========================
# Cycle helpers (por season)
# =========================
def get_cycle_row(ws_cycles, season_id: int, cycle: int) -> int | None:
    rows = ws_cycles.get_all_values()
    if len(rows) <= 1:
        return None
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    for r_i in range(2, len(rows) + 1):
        r = rows[r_i-1]
        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        c = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)
        if s == season_id and c == cycle:
            return r_i
    return None

def get_cycle_fields(ws_cycles, season_id: int, cycle: int) -> dict:
    rows = ws_cycles.get_all_values()
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    out = {"season_id": season_id, "cycle": cycle, "status": None, "start_at_br": "", "deadline_at_br": ""}
    for r_i in range(2, len(rows) + 1):
        r = rows[r_i-1]
        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        c = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)
        if s == season_id and c == cycle:
            out["status"] = (r[col["status"]] if col["status"] < len(r) else "").strip().lower()
            out["start_at_br"] = (r[col["start_at_br"]] if col["start_at_br"] < len(r) else "").strip()
            out["deadline_at_br"] = (r[col["deadline_at_br"]] if col["deadline_at_br"] < len(r) else "").strip()
            return out
    return out

def set_cycle_status(ws_cycles, season_id: int, cycle: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, season_id, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_cycles.append_row([str(season_id), str(cycle), status, "", "", nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[status]], range_name=f"{col_letter(col['status'])}{rown}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

def set_cycle_times(ws_cycles, season_id: int, cycle: int, start_at_br: str, deadline_at_br: str):
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, season_id, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_cycles.append_row([str(season_id), str(cycle), "open", start_at_br, deadline_at_br, nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[start_at_br]], range_name=f"{col_letter(col['start_at_br'])}{rown}")
        ws_cycles.update([[deadline_at_br]], range_name=f"{col_letter(col['deadline_at_br'])}{rown}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

def list_cycles(ws_cycles, season_id: int) -> list[tuple[int, str]]:
    out = []
    for r in ws_cycles.get_all_records():
        s = safe_int(r.get("season_id", 0), 0)
        if s != season_id:
            continue
        c = safe_int(r.get("cycle", 0), 0)
        st = str(r.get("status", "")).strip().lower()
        if c > 0:
            out.append((c, st))
    out.sort(key=lambda x: x[0])
    return out

def suggest_open_cycles(ws_cycles, season_id: int, limit: int = 25) -> list[int]:
    items = list_cycles(ws_cycles, season_id)
    if not items:
        return [1]
    max_cycle = max(c for c, _ in items)
    open_cycles = [c for c, st in items if st == "open"]
    # sugestão inclui também o próximo ciclo (ainda inexistente), MAS ele só poderá existir via /ciclo_abrir (ADM/Org)
    if (max_cycle + 1) not in open_cycles:
        open_cycles.append(max_cycle + 1)
    open_cycles = sorted(set(open_cycles))
    return open_cycles[:limit]


# =========================
# Players helpers
# =========================
def find_player_row(ws_players, discord_id: int):
    try:
        cell = ws_players.find(str(discord_id))
        return cell.row
    except Exception:
        return None

def get_player_nick_map(ws_players) -> dict[str, str]:
    data = ws_players.get_all_records()
    m = {}
    for r in data:
        pid = str(r.get("discord_id","")).strip()
        nick = str(r.get("nick","")).strip()
        if pid:
            m[pid] = nick or pid
    return m


# =========================
# Deck helpers (1 vez POR CICLO)
# =========================
def get_deck_row(ws_decks, season_id: int, cycle: int, player_id: str) -> int | None:
    data = ws_decks.get_all_values()
    if len(data) <= 1:
        return None
    col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
    for i in range(2, len(data)+1):
        r = data[i-1]
        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        c = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)
        p = str(r[col["player_id"]] if col["player_id"] < len(r) else "").strip()
        if s == season_id and c == cycle and p == str(player_id).strip():
            return i
    return None

def get_deck_fields(ws_decks, row: int) -> dict:
    vals = ws_decks.row_values(row)
    while len(vals) < len(DECKS_HEADER):
        vals.append("")
    return {
        "season_id": safe_int(vals[0], 0),
        "cycle": safe_int(vals[1], 0),
        "player_id": str(vals[2]).strip(),
        "deck": str(vals[3]).strip(),
        "decklist_url": str(vals[4]).strip(),
        "created_at": str(vals[5]).strip(),
        "updated_at": str(vals[6]).strip(),
    }


# =========================================================
# [BLOCO 2/8 termina aqui]
# No BLOCO 3/8 eu trago:
# - Match helpers (IDs, auto-confirm, anti-repetição)
# - Prazo do ciclo (dias por maior POD) e compute start/deadline
# =========================================================


# =========================================================
# [BLOCO 3/8] — MATCH HELPERS + ANTI-REPETIÇÃO + PRAZO DO CICLO
# =========================================================

# =========================
# Match helpers
# =========================
def new_match_id(season_id: int, cycle: int, pod: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    rnd = random.randint(1000, 9999)
    return f"S{season_id}-C{cycle}-P{pod}-{ts}-{rnd}"

def auto_confirm_deadline_iso(created_utc: datetime) -> str:
    return (created_utc + timedelta(hours=48)).isoformat()

def round_robin_pairs(players: list[str]):
    pairs = []
    n = len(players)
    for i in range(n):
        for j in range(i + 1, n):
            pairs.append((players[i], players[j]))
    return pairs

def sweep_auto_confirm(sh, season_id: int, cycle: int) -> int:
    """
    Auto-confirm: Matches pending com reported_by_id preenchido e auto_confirm_at <= agora (UTC)
    """
    ws_matches = sh.worksheet("Matches")
    col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    rows = ws_matches.get_all_values()
    if len(rows) <= 1:
        return 0

    nowu = utc_now_dt()
    changed = 0

    for rown in range(2, len(rows) + 1):
        r = rows[rown - 1]

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        r_season = safe_int(getc("season_id"), 0)
        r_cycle = safe_int(getc("cycle"), 0)
        if r_season != season_id or r_cycle != cycle:
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
            ws_matches.update([["confirmed"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
            ws_matches.update([["AUTO"]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
            ws_matches.update([[now_br_str()]], range_name=f"{col_letter(col['updated_at'])}{rown}")
            changed += 1

    return changed


# =========================
# Anti-repetição (heurística)
# =========================
def get_past_confirmed_pairs(ws_matches) -> set[frozenset]:
    """
    Cria um conjunto de pares (A,B) já enfrentados em matches CONFIRMED (qualquer season/cycle),
    exceto BYE e matches inativos.
    """
    rows = ws_matches.get_all_records()
    pairs: set[frozenset] = set()
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
    # Regra oficial definida:
    # POD 3 -> 5 dias
    # POD 4 -> 8 dias
    # POD 5 ou 6 -> 10 dias
    if max_pod_size <= 3:
        return 5
    if max_pod_size == 4:
        return 8
    return 10

def compute_cycle_start_deadline_br(season_id: int, cycle: int, ws_pods, ws_cycles) -> tuple[str, str, int, int]:
    """
    Retorna (start_at_br_str, deadline_at_br_str, max_pod_size, days)

    - start_at: 14:00 BR do dia que o ciclo foi "travado" (geração de pods)
      (se já existir em Cycles, reaproveita)
    - deadline_at: (start_date + days) às 13:59 BR
    """
    fields = get_cycle_fields(ws_cycles, season_id, cycle)

    if fields.get("start_at_br"):
        start_dt = parse_br_dt(fields["start_at_br"])
    else:
        start_dt = None

    rows = ws_pods.get_all_records()
    pods = {}
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
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
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            c = parse_br_dt(r.get("created_at", ""))
            if c:
                created_candidates.append(c)

        base_date = (min(created_candidates).astimezone(BR_TZ).date() if created_candidates else now_br_dt().date())
        start_dt = datetime.combine(base_date, time(14, 0), tzinfo=BR_TZ)

    deadline_date = (start_dt.date() + timedelta(days=days))
    deadline_dt = datetime.combine(deadline_date, time(13, 59), tzinfo=BR_TZ)

    return (fmt_br_dt(start_dt), fmt_br_dt(deadline_dt), max_pod_size, days)


# =========================================================
# [BLOCO 3/8 termina aqui]
# No BLOCO 4/8 eu trago:
# - Recálculo oficial (Pontos > OMW% > GW% > OGW%, piso 33,3%)
# - Escrita do Standings SEM incremental (zera e escreve do zero por season+ciclo)
# =========================================================


# =========================================================
# [BLOCO 4/8] — RECÁLCULO OFICIAL (MWP/OMW/GWP/OGW) + STANDINGS (ZERADO)
# =========================================================

def recalculate_cycle(season_id: int, cycle: int):
    """
    Recalcula o ranking do ciclo (SEMPRE do zero):
    - Piso 33,3% primeiro (MWP/GWP)
    - Ranking: Pontos > OMW% > GW% > OGW%
    - Match points: Win=3, Draw=1, Loss=0 (por match)
    - GWP: (W + 0.5*D) / GamesPlayed (com piso)
    - OMW: média do MWP (já com piso) dos oponentes enfrentados
    - OGW: média do GWP (já com piso) dos oponentes enfrentados
    - Considera apenas matches:
      active=TRUE e confirmed_status=confirmed e result_type != bye
    """
    sh = open_sheet()
    ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
    ws_matches = sh.worksheet("Matches")
    ws_standings = sh.worksheet("Standings")

    # auto-confirm "silencioso" (opcional) antes do cálculo
    try:
        sweep_auto_confirm(sh, season_id, cycle)
    except Exception:
        pass

    # Base de jogadores (Players) — ranking inclui TODOS cadastrados
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

    # Filtra matches válidos (season+cycle)
    col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    matches_rows = ws_matches.get_all_records()

    valid = []
    for r in matches_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
            continue
        if not as_bool(r.get("active", "TRUE")):
            continue

        result_type = str(r.get("result_type", "normal")).strip().lower()
        if result_type == "bye":
            continue

        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()
        if not a or not b:
            continue

        ensure(a)
        ensure(b)

        a_gw = safe_int(r.get("a_games_won", 0), 0)
        b_gw = safe_int(r.get("b_games_won", 0), 0)
        d_g = safe_int(r.get("draw_games", 0), 0)

        valid.append((a, b, a_gw, b_gw, d_g))

    # Atualiza estatísticas
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

        # Match points
        if a_gw > b_gw:
            stats[a]["match_points"] += 3
        elif b_gw > a_gw:
            stats[b]["match_points"] += 3
        else:
            stats[a]["match_points"] += 1
            stats[b]["match_points"] += 1

        opponents[a].append(b)
        opponents[b].append(a)

    # Calcula MWP/GWP com piso 33,3%
    mwp = {}
    gwp = {}

    for pid, s in stats.items():
        mp = s["match_points"]
        mplayed = s["matches_played"]
        mwp[pid] = (1/3) if mplayed == 0 else floor_333(mp / (3.0 * mplayed))

        gplayed = s["games_played"]
        if gplayed == 0:
            gwp[pid] = 1/3
        else:
            gwp_raw = (s["game_wins"] + 0.5 * s["game_draws"]) / float(gplayed)
            gwp[pid] = floor_333(gwp_raw)

    # Calcula OMW/OGW como média dos oponentes
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

    # Monta linhas do standings
    out_rows = []
    for pid, s in stats.items():
        out_rows.append({
            "season_id": season_id,
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

    # Ordena: Pontos > OMW > GW > OGW
    out_rows.sort(
        key=lambda r: (r["match_points"], r["omw_percent"], r["gw_percent"], r["ogw_percent"]),
        reverse=True
    )

    ts = now_iso_utc()
    for i, r in enumerate(out_rows, start=1):
        r["rank_position"] = i
        r["last_recalc_at"] = ts

    # Grava no Standings:
    # - Não limpar planilha inteira (pois pode ter outras seasons/ciclos)
    # - Estratégia: reescrever TUDO filtrando: remove linhas da season+cycle e reinsere
    # Obs: gspread não tem "delete by filter" eficiente em massa sem API avançada,
    # então aqui usamos a estratégia robusta:
    #   1) lê tudo
    #   2) mantém header
    #   3) filtra fora season_id+cycle
    #   4) escreve novamente (clear + append)
    # É mais pesado, mas BLINDADO e simples.
    header = [
        "season_id","cycle","player_id","matches_played","match_points","mwp_percent",
        "game_wins","game_losses","game_draws","games_played","gw_percent",
        "omw_percent","ogw_percent","rank_position","last_recalc_at"
    ]

    existing = ws_standings.get_all_values()
    kept = []
    if existing and len(existing) > 1:
        # detecta header atual; se diferente, força header padrão
        existing_header = existing[0]
        if existing_header != header:
            # se header não bate, descartamos e reescrevemos
            kept = []
        else:
            for r in existing[1:]:
                # garante tamanho mínimo
                while len(r) < len(header):
                    r.append("")
                r_season = safe_int(r[0], 0)
                r_cycle = safe_int(r[1], 0)
                if r_season == season_id and r_cycle == cycle:
                    continue
                kept.append(r)

    # Agora reescreve tudo
    ws_standings.clear()
    ws_standings.append_row(header)

    if kept:
        ws_standings.append_rows(kept, value_input_option="USER_ENTERED")

    values = []
    for r in out_rows:
        values.append([
            r["season_id"], r["cycle"], r["player_id"],
            r["matches_played"], r["match_points"], r["mwp_percent"],
            r["game_wins"], r["game_losses"], r["game_draws"], r["games_played"], r["gw_percent"],
            r["omw_percent"], r["ogw_percent"], r["rank_position"], r["last_recalc_at"]
        ])

    if values:
        ws_standings.append_rows(values, value_input_option="USER_ENTERED")

    return out_rows


# =========================================================
# [BLOCO 4/8 termina aqui]
# No BLOCO 5/8 eu trago:
# - Discord Client + setup_hook
# - Autocompletes
# - /comando (catálogo completo e por permissão)
# - Gate de entrada novo usuário (Participar/Assistir) + cargo automático Jogador
# =========================================================


# =========================================================
# [BLOCO 5/8 — CORRIGIDO] — DISCORD CORE + AUTOCOMPLETE + /COMANDO + ONBOARDING (Participar/Assistir)
# (cole este BLOCO 5/8 por cima do seu BLOCO 5 atual)
# IMPORTANTE:
# - Este BLOCO 5 fica como a ÚNICA implementação de onboarding (on_member_join).
# - No BLOCO 8 você VAI REMOVER o onboarding duplicado (eu corrijo no BLOCO 8 depois).
# =========================================================

# =========================
# Discord Bot (Client)
# =========================
class LemeBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # necessário para on_member_join + cargos
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        # sync dos comandos (guild-scoped quando possível)
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

client = LemeBot()


# =========================
# Logs (canal admin)
# =========================
LOG_ADMIN_CHANNEL_ID = int(os.getenv("LOG_ADMIN_CHANNEL_ID", "0"))  # você passou: 1478023275597791253
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "0"))      # opcional (fallback se DM falhar)

ROLE_JOGADOR = os.getenv("ROLE_JOGADOR", "Jogador")  # nome do cargo
ROLE_SEASON_CURRENT = os.getenv("ROLE_SEASON_CURRENT", "J")  # seu pedido: cargo "J" (season atual)

async def log_admin(guild: discord.Guild, text: str):
    if not guild:
        return
    if not LOG_ADMIN_CHANNEL_ID:
        return
    ch = guild.get_channel(LOG_ADMIN_CHANNEL_ID)
    if ch:
        try:
            await ch.send(text)
        except Exception:
            pass


# =========================================================
# Autocomplete helpers (season/cycle/match)
# =========================================================
async def ac_season_active(interaction: discord.Interaction, current: str):
    """
    Retorna seasons disponíveis (preferindo a current/active).
    """
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)

        rows = ws_seasons.get_all_records()
        cur = str(current).strip()
        choices = []
        for r in rows:
            sid = safe_int(r.get("season_id", 0), 0)
            st = str(r.get("status", "")).strip().lower()
            if sid <= 0:
                continue
            name = str(r.get("name", "")).strip() or f"Season {sid}"
            label = f"{name} (id {sid}, {st})"
            if cur and cur not in str(sid):
                continue
            choices.append(app_commands.Choice(name=label[:100], value=str(sid)))

        # coloca active primeiro
        def keyf(c: app_commands.Choice):
            v = safe_int(c.value, 0)
            return (0 if ("active" in c.name.lower()) else 1, v)

        choices.sort(key=keyf)
        return choices[:25]
    except Exception:
        base = [str(i) for i in range(1, 21)]
        cur = str(current).strip()
        if cur:
            base = [x for x in base if x.startswith(cur)]
        return [app_commands.Choice(name=f"Season {x}", value=x) for x in base[:25]]


async def ac_cycle_open(interaction: discord.Interaction, current: str):
    """
    Ciclos abertos da SEASON atual (current season).
    """
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        open_cycles = suggest_open_cycles(ws_cycles, season_id=season_id, limit=25)

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
    """
    Match_id pending do usuário, na season atual (independente do ciclo).
    """
    try:
        user_id = str(interaction.user.id)
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        season_id = get_current_season_id(ws_seasons)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        cur = str(current).strip().lower()

        out = []
        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
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
            cyc = safe_int(r.get("cycle", 0), 0)
            na = nick_map.get(a, a)
            nb = nick_map.get(b, b)
            label = f"S{season_id} C{cyc} Pod {pod}: {na} vs {nb} | {mid}"
            out.append(app_commands.Choice(name=label[:100], value=mid))

        return out[:25]
    except Exception:
        return []


async def ac_match_id_any(interaction: discord.Interaction, current: str):
    """
    Qualquer match_id (season atual), qualquer status.
    """
    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        season_id = get_current_season_id(ws_seasons)

        rows = ws_matches.get_all_records()
        cur = str(current).strip().lower()
        out = []
        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            mid = str(r.get("match_id", "")).strip()
            if not mid:
                continue
            if cur and cur not in mid.lower():
                continue
            cyc = safe_int(r.get("cycle", 0), 0)
            pod = str(r.get("pod", "")).strip()
            st = str(r.get("confirmed_status", "")).strip().lower()
            label = f"S{season_id} C{cyc} Pod {pod} [{st}] | {mid}"
            out.append(app_commands.Choice(name=label[:100], value=mid))
        return out[:25]
    except Exception:
        return []


# =========================================================
# Catálogo de comandos (para /comando)
# IMPORTANTE: listar TODOS, mesmo se alguns forem "luxo"
# =========================================================
COMMANDS_CATALOG = [
    # Jogador
    ("jogador", "/inscrever", "Se inscreve em um ciclo aberto (com escolha de ciclo)."),
    ("jogador", "/drop", "Sai do ciclo (somente enquanto estiver open)."),
    ("jogador", "/deck", "Define seu deck (1 vez POR CICLO, escolhendo o ciclo)."),
    ("jogador", "/decklist", "Define decklist (1 vez POR CICLO, escolhendo o ciclo)."),
    ("jogador", "/deck_ver", "Mostra o deck do jogador em um ciclo."),
    ("jogador", "/decklist_ver", "Mostra a decklist do jogador em um ciclo."),
    ("jogador", "/pods_ver", "Mostra pods do ciclo."),
    ("jogador", "/meus_matches", "Lista seus matches do ciclo (IDs, status e prazo 48h)."),
    ("jogador", "/confrontos_pendentes", "Lista SEUS matches pendentes (sem report / pending)."),
    ("jogador", "/resultado", "Reporta resultado V-D-E (menu)."),
    ("jogador", "/rejeitar", "Rejeita resultado pendente dentro de 48h."),
    ("jogador", "/ranking", "Mostra ranking público do ciclo."),
    ("jogador", "/ranking_geral", "Mostra ranking geral da season (ou por season escolhida)."),
    ("jogador", "/prazo", "Mostra o prazo oficial do ciclo."),
    ("jogador", "/comando", "Mostra os comandos que você tem acesso."),

    # ADM
    ("adm", "/forcesync", "Sincroniza comandos no servidor (rápido)."),
    ("adm", "/ciclo_abrir", "Cria/abre um ciclo (status=open)."),
    ("adm", "/ciclo_reabrir", "Reabre um ciclo (somente se NÃO estiver completed)."),
    ("adm", "/ciclo_fechar", "Fecha ciclo aberto por engano (status=locked)."),
    ("adm", "/ciclo_encerrar", "Encerra ciclo (status=completed)."),
    ("adm", "/ciclo_status", "Resumo do ciclo (equivalente /status_ciclo)."),
    ("adm", "/pods_gerar", "Gera pods + matches e trava ciclo (locked)."),
    ("adm", "/pods_publicar", "Publica pods do ciclo em um canal (texto)."),
    ("adm", "/deadline", "Lista pendências (pending) próximas de expirar (48h)."),
    ("adm", "/confrontos_pendentes_admin", "Lista TODOS os pendentes do ciclo (visão ADM)."),
    ("adm", "/recalcular", "Auto-confirm (48h) + recalcula ranking do zero."),
    ("adm", "/standings_publicar", "Publica ranking em canal configurado."),
    ("adm", "/final", "Aplica 0-0-3 em matches sem report após prazo."),
    ("adm", "/resultado_admin", "Edita/força resultado de match (admin)."),
    ("adm", "/match_cancelar", "Inativa um match (active=FALSE)."),
    ("adm", "/substituir_jogador", "Substitui jogador no ciclo (somente matches sem report)."),
    ("adm", "/historico_confronto", "Histórico confirmado entre dois jogadores."),
    ("adm", "/estatisticas", "Estatísticas gerais confirmadas do jogador."),
    ("adm", "/export", "Exporta CSV (cycle/season)."),

    # Organizador
    ("organizador", "/estender", "Estende o prazo do ciclo em X dias corridos (exceção)."),

    # Dono do servidor (Owner-only)
    ("owner", "/season_abrir", "Cria/abre nova season e fecha a anterior (mantém histórico)."),
    ("owner", "/season_fechar", "Fecha a season ativa (não apaga dados)."),
    ("owner", "/season_status", "Mostra seasons e qual está ativa."),
]

def level_allows(user_level: str, cmd_level: str) -> bool:
    order = {"jogador": 1, "adm": 2, "organizador": 3, "owner": 4}
    return order.get(user_level, 1) >= order.get(cmd_level, 1)

async def get_access_level(interaction: discord.Interaction) -> str:
    if not interaction.guild or not interaction.user:
        return "jogador"

    # Owner-only
    if interaction.guild.owner_id == interaction.user.id:
        return "owner"

    if interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild:
        return "adm"

    if await has_role(interaction, ROLE_ORGANIZADOR):
        return "organizador"
    if await has_role(interaction, ROLE_ADM):
        return "adm"
    return "jogador"


# =========================
# /comando (menu/lista)
# =========================
@client.tree.command(name="comando", description="Mostra seus comandos disponíveis (de acordo com seu cargo).")
async def comando(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        user_level = await get_access_level(interaction)
        lines = [f"📌 **Seus comandos disponíveis ({user_level.upper()})**\n"]

        for lvl, cmd, desc in COMMANDS_CATALOG:
            if level_allows(user_level, lvl):
                lines.append(f"• **{cmd}** — {desc}")

        await interaction.followup.send("\n".join(lines), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /comando: {e}", ephemeral=True)


# =========================================================
# ONBOARDING — Pergunta automática ao entrar no servidor
# - DM com botões (Participar/Assistir)
# - fallback: mensagem no canal WELCOME_CHANNEL_ID (se configurado)
# IMPORTANTE:
# - Este é o ÚNICO on_member_join do projeto.
# - No BLOCO 8 NÃO deve existir outro on_member_join.
# =========================================================
class OnboardingView(discord.ui.View):
    def __init__(self, member_id: int, timeout: int = 3600):
        super().__init__(timeout=timeout)
        self.member_id = member_id

    async def _get_member(self, interaction: discord.Interaction):
        if not interaction.guild:
            return None
        try:
            m = interaction.guild.get_member(self.member_id)
            if m is None:
                m = await interaction.guild.fetch_member(self.member_id)
            return m
        except Exception:
            return None

    @discord.ui.button(label="Participar", style=discord.ButtonStyle.success)
    async def participar(self, interaction: discord.Interaction, button: discord.ui.Button):
        # garante que só o próprio clique
        if interaction.user.id != self.member_id:
            return await interaction.response.send_message("❌ Este menu não é para você.", ephemeral=True)

        m = await self._get_member(interaction)
        if not m:
            return await interaction.response.send_message("⚠️ Não consegui localizar seu usuário no servidor.", ephemeral=True)

        roles_to_add = []
        role_jog = discord.utils.get(m.guild.roles, name=ROLE_JOGADOR)
        if role_jog:
            roles_to_add.append(role_jog)

        role_j = discord.utils.get(m.guild.roles, name=ROLE_SEASON_CURRENT)
        if role_j:
            roles_to_add.append(role_j)

        try:
            if roles_to_add:
                await m.add_roles(*roles_to_add, reason="Onboarding: escolheu Participar")
        except Exception:
            return await interaction.response.send_message(
                "❌ Eu não tenho permissão para atribuir cargos. Peça para um ADM ajustar minhas permissões/cargo do bot.",
                ephemeral=True
            )

        # desabilita botões depois de escolher
        for c in self.children:
            c.disabled = True
        try:
            await interaction.response.edit_message(
                content=(
                    "✅ Perfeito. Você entrou como **Jogador**.\n"
                    "Próximo passo: use `/inscrever` e depois `/deck` e `/decklist` (cada um 1 vez por ciclo)."
                ),
                view=self
            )
        except Exception:
            await interaction.response.send_message(
                "✅ Perfeito. Você entrou como **Jogador**.\nUse `/comando` para ver tudo.",
                ephemeral=True
            )

        try:
            await log_admin(m.guild, f"👤 Onboarding: {m.mention} escolheu **Participar** (cargo aplicado).")
        except Exception:
            pass

        self.stop()

    @discord.ui.button(label="Assistir", style=discord.ButtonStyle.secondary)
    async def assistir(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.member_id:
            return await interaction.response.send_message("❌ Este menu não é para você.", ephemeral=True)

        m = await self._get_member(interaction)
        for c in self.children:
            c.disabled = True

        try:
            await interaction.response.edit_message(
                content=(
                    "✅ Tranquilo. Você entrou como **Assistir**.\n"
                    "Para jogar depois, peça para um ADM/Organizador aplicar o cargo **Jogador**."
                ),
                view=self
            )
        except Exception:
            await interaction.response.send_message(
                "✅ Tranquilo. Você entrou como **Assistir**.\nPara jogar depois, peça cargo **Jogador** a um ADM.",
                ephemeral=True
            )

        try:
            if m and m.guild:
                await log_admin(m.guild, f"👀 Onboarding: {m.mention} escolheu **Assistir** (sem cargo).")
        except Exception:
            pass

        self.stop()


async def send_onboarding(member: discord.Member):
    text = (
        "⚓ **Bem-vindo ao Leme Holandês!**\n\n"
        "Você pretende:\n"
        "✅ **Participar** (jogar a liga)\n"
        "👀 **Assistir** (somente acompanhar)\n\n"
        "Escolha abaixo:"
    )

    view = OnboardingView(member.id)

    # 1) tenta DM
    try:
        await member.send(text, view=view)
        try:
            await log_admin(member.guild, f"📩 Onboarding enviado por DM para {member.mention}.")
        except Exception:
            pass
        return
    except Exception:
        pass

    # 2) fallback em canal
    if WELCOME_CHANNEL_ID and member.guild:
        ch = member.guild.get_channel(WELCOME_CHANNEL_ID)
        if ch:
            try:
                await ch.send(f"{member.mention}\n{text}", view=view)
                try:
                    await log_admin(member.guild, f"💬 Onboarding enviado no canal {ch.mention} para {member.mention}.")
                except Exception:
                    pass
                return
            except Exception:
                pass

    # 3) se tudo falhar, loga
    try:
        await log_admin(member.guild, f"⚠️ Onboarding falhou (DM e canal). Usuário: {member.mention}")
    except Exception:
        pass


@client.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return
    await send_onboarding(member)


# =========================================================
# Comandos básicos que sempre ajudam
# =========================================================
@client.tree.command(name="ping", description="Teste do bot")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("⚓ Pong! Bot online.")


@client.tree.command(name="forcesync", description="(ADM) Força sincronização dos comandos no servidor")
async def forcesync(interaction: discord.Interaction):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    if not GUILD_ID:
        return await interaction.response.send_message("⚠️ DISCORD_GUILD_ID não configurado.", ephemeral=True)

    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        guild = discord.Object(id=GUILD_ID)
        await client.tree.sync(guild=guild)
        await interaction.followup.send("🔄 Comandos sincronizados com sucesso.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ Falha ao sincronizar: {type(e).__name__} — {e}", ephemeral=True)


# =========================================================
# [BLOCO 5/8 — CORRIGIDO termina aqui]
# No BLOCO 6/8 você mantém inscrição/drop/deck/decklist (ok).
# No BLOCO 7/8 eu vou corrigir /confrontos_pendentes (jogador) + criar /confrontos_pendentes_admin (adm).
# No BLOCO 8/8 eu vou REMOVER onboarding duplicado e também blindar match_id (resultado/rejeitar).
# =========================================================


# =========================================================
# [BLOCO 6/8 — CORRIGIDO] — INSCRIÇÃO + DROP + DECK/DECKLIST (1x POR CICLO) + VERIFICAÇÃO
# (cole este BLOCO 6/8 por cima do seu BLOCO 6 atual)
# Ajustes nesta correção:
# - /deck e /decklist agora BLOQUEIAM ciclo COMPLETED (reduz erro humano)
# - Mantém 1x por ciclo (jogador), ADM/Organizador podem alterar
# - Mantém escolha explícita do ciclo em /inscrever /deck /decklist /deck_ver /decklist_ver
# =========================================================

# =========================================================
# PlayerCycles helpers (deck/decklist por ciclo)
# =========================================================
PLAYERCYCLES_HEADER = ["season_id","cycle","player_id","deck","decklist_url","created_at","updated_at"]
PLAYERCYCLES_REQUIRED = ["season_id","cycle","player_id","deck","decklist_url","created_at","updated_at"]

def get_playercycle_row(ws_pc, season_id: int, cycle: int, player_id: str) -> int | None:
    vals = ws_pc.get_all_values()
    if len(vals) <= 1:
        return None
    col = ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)
    for rown in range(2, len(vals) + 1):
        r = vals[rown - 1]
        sid = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else "0", 0)
        cyc = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else "0", 0)
        pid = str(r[col["player_id"]] if col["player_id"] < len(r) else "").strip()
        if sid == season_id and cyc == cycle and pid == str(player_id).strip():
            return rown
    return None

def get_playercycle_fields(ws_pc, rown: int) -> dict:
    vals = ws_pc.get(f"A{rown}:G{rown}")
    if not vals or not vals[0]:
        return {}
    r = vals[0]
    while len(r) < 7:
        r.append("")
    return {
        "season_id": safe_int(r[0], 0),
        "cycle": safe_int(r[1], 0),
        "player_id": str(r[2]).strip(),
        "deck": str(r[3]).strip(),
        "decklist_url": str(r[4]).strip(),
        "created_at": str(r[5]).strip(),
        "updated_at": str(r[6]).strip(),
    }

def ensure_playercycle_row(ws_pc, season_id: int, cycle: int, player_id: str) -> int:
    nowb = now_br_str()
    rown = get_playercycle_row(ws_pc, season_id, cycle, player_id)
    col = ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)
    if rown is None:
        ws_pc.append_row(
            [str(season_id), str(cycle), str(player_id), "", "", nowb, nowb],
            value_input_option="USER_ENTERED"
        )
        return len(ws_pc.get_all_values())
    else:
        ws_pc.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")
        return rown

def is_player_enrolled_active(ws_enr, season_id: int, cycle: int, player_id: str) -> bool:
    rows = ws_enr.get_all_records()
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        if str(r.get("player_id", "")).strip() != str(player_id).strip():
            continue
        if str(r.get("status", "")).strip().lower() == "active":
            return True
    return False


# =========================================================
# /inscrever — EXIGE que o ciclo exista e esteja OPEN
# =========================================================
@client.tree.command(name="inscrever", description="Se inscreve em um ciclo aberto (ciclo precisa existir).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def inscrever(interaction: discord.Interaction, cycle: str):
    await interaction.response.defer(ephemeral=True)

    c = safe_int(cycle, 0)
    if c <= 0:
        return await interaction.followup.send("❌ Ciclo inválido.", ephemeral=True)

    discord_id = str(interaction.user.id)
    nick = interaction.user.display_name
    nowb = now_br_str()

    try:
        sh = open_sheet()

        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=25)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa. Peça ao DONO do servidor para abrir uma season.", ephemeral=True)

        fields = get_cycle_fields(ws_cycles, season_id, c)
        st = (fields.get("status") or "").strip().lower()
        if fields.get("status") is None:
            return await interaction.followup.send(
                f"❌ O **Ciclo {c}** não existe na **Season {season_id}**.\n"
                "Peça para um ADM criar/abrir o ciclo com `/ciclo_abrir`.",
                ephemeral=True
            )

        if st == "completed":
            return await interaction.followup.send("❌ Este ciclo já foi concluído (COMPLETED).", ephemeral=True)
        if st == "locked":
            return await interaction.followup.send("❌ Este ciclo está LOCKED (inscrição fechada).", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Este ciclo não está OPEN (status atual: {st}).", ephemeral=True)

        # garante player
        prow = find_player_row(ws_players, int(discord_id))
        if prow is None:
            ws_players.append_row([discord_id, nick, "", "", "active", "0", nowb, nowb], value_input_option="USER_ENTERED")
        else:
            ws_players.update([[nick]], range_name=f"B{prow}")
            ws_players.update([["active"]], range_name=f"E{prow}")
            ws_players.update([[nowb]], range_name=f"H{prow}")

        # enroll por season+ciclo
        data = ws_enr.get_all_values()
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        found_row = None
        for rown in range(2, len(data) + 1):
            r = data[rown - 1]
            sid = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else "0", 0)
            cyc = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else "0", 0)
            pid = str(r[col["player_id"]] if col["player_id"] < len(r) else "").strip()
            if sid == season_id and cyc == c and pid == discord_id:
                found_row = rown
                break

        if found_row is None:
            ws_enr.append_row([str(season_id), str(c), discord_id, "active", nowb, nowb], value_input_option="USER_ENTERED")
            await interaction.followup.send(
                f"✅ Inscrito no **Ciclo {c}** (Season {season_id}).\n"
                "Agora defina:\n"
                "• `/deck` (1 vez POR CICLO)\n"
                "• `/decklist` (1 vez POR CICLO)\n"
                "Se quiser sair: `/drop`",
                ephemeral=True
            )
        else:
            ws_enr.update([["active"]], range_name=f"{col_letter(col['status'])}{found_row}")
            ws_enr.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{found_row}")
            await interaction.followup.send(f"✅ Inscrição reativada no **Ciclo {c}** (Season {season_id}).", ephemeral=True)

        try:
            await log_admin(interaction.guild, f"📝 Inscrição: {interaction.user.mention} no S{season_id} C{c}.")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscrever: {e}", ephemeral=True)


# =========================================================
# /drop — só enquanto ciclo OPEN
# =========================================================
@client.tree.command(name="drop", description="Sai do ciclo informado (somente enquanto ciclo open).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)", motivo="Opcional: motivo curto")
async def drop(interaction: discord.Interaction, cycle: int, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)

    discord_id = str(interaction.user.id)
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=25)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        fields = get_cycle_fields(ws_cycles, season_id, cycle)
        st = (fields.get("status") or "").strip().lower()
        if fields.get("status") is None:
            return await interaction.followup.send(f"❌ Ciclo {cycle} não existe na Season {season_id}.", ephemeral=True)

        if st == "locked":
            return await interaction.followup.send("❌ Ciclo LOCKED (pods já gerados). Drop não permitido.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED. Drop não permitido.", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Ciclo não está OPEN (status: {st}).", ephemeral=True)

        data = ws_enr.get_all_values()
        found_row = None
        for rown in range(2, len(data) + 1):
            r = data[rown - 1]
            sid = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else "0", 0)
            cyc = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else "0", 0)
            pid = str(r[col["player_id"]] if col["player_id"] < len(r) else "").strip()
            if sid == season_id and cyc == cycle and pid == discord_id:
                found_row = rown
                break

        if found_row is None:
            return await interaction.followup.send(f"❌ Você não está inscrito no Ciclo {cycle}.", ephemeral=True)

        ws_enr.update([["dropped"]], range_name=f"{col_letter(col['status'])}{found_row}")
        ws_enr.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{found_row}")

        msg = f"✅ Você saiu do **Ciclo {cycle}** (Season {season_id})."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

        try:
            await log_admin(interaction.guild, f"🚪 Drop: {interaction.user.mention} no S{season_id} C{cycle}.")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)


# =========================================================
# /deck — 1 vez POR CICLO (jogador), ADM/Organizador podem alterar
# (com escolha explícita de ciclo, e BLOQUEIA ciclo COMPLETED)
# =========================================================
@client.tree.command(name="deck", description="Define seu deck (1 vez POR CICLO).")
@app_commands.describe(cycle="Ciclo do deck (ex: 2)", nome="Nome do deck (ex: UR Murktide)")
async def deck(interaction: discord.Interaction, cycle: int, nome: str):
    await interaction.response.defer(ephemeral=True)

    pid = str(interaction.user.id).strip()
    nowb = now_br_str()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=25)
        ws_pc = ensure_worksheet(sh, "PlayerCycles", PLAYERCYCLES_HEADER, rows=10000, cols=20)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        st = (cf.get("status") or "").strip().lower()
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ Ciclo {cycle} não existe na Season {season_id}.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Este ciclo está COMPLETED. Não é permitido registrar/alterar deck.", ephemeral=True)

        if not is_player_enrolled_active(ws_enr, season_id, cycle, pid) and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                "❌ Para definir deck você precisa estar **inscrito (active)** nesse ciclo.\nUse `/inscrever`.",
                ephemeral=True
            )

        rown = ensure_playercycle_row(ws_pc, season_id, cycle, pid)
        col = ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)
        fields = get_playercycle_fields(ws_pc, rown)
        current = (fields.get("deck") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                f"❌ Você já definiu deck para o **Ciclo {cycle}** e não pode alterar.\n"
                "Peça para um ADM/Organizador se precisar.",
                ephemeral=True
            )

        ws_pc.update([[nome]], range_name=f"{col_letter(col['deck'])}{rown}")
        ws_pc.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send(f"✅ Deck salvo para **Ciclo {cycle}**.\nDeck: **{nome}**", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /deck: {e}", ephemeral=True)


# =========================================================
# /decklist — 1 vez POR CICLO (jogador), ADM/Organizador podem alterar
# (com escolha explícita de ciclo, e BLOQUEIA ciclo COMPLETED)
# =========================================================
@client.tree.command(name="decklist", description="Define sua decklist (1 vez POR CICLO).")
@app_commands.describe(cycle="Ciclo da decklist (ex: 2)", url="Link (moxfield.com ou ligamagic.com.br)")
async def decklist(interaction: discord.Interaction, cycle: int, url: str):
    await interaction.response.defer(ephemeral=True)

    pid = str(interaction.user.id).strip()
    nowb = now_br_str()

    ok, val = validate_decklist_url(url)
    if not ok:
        return await interaction.followup.send(f"❌ {val}", ephemeral=True)

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=25)
        ws_pc = ensure_worksheet(sh, "PlayerCycles", PLAYERCYCLES_HEADER, rows=10000, cols=20)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        st = (cf.get("status") or "").strip().lower()
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ Ciclo {cycle} não existe na Season {season_id}.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Este ciclo está COMPLETED. Não é permitido registrar/alterar decklist.", ephemeral=True)

        if not is_player_enrolled_active(ws_enr, season_id, cycle, pid) and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                "❌ Para definir decklist você precisa estar **inscrito (active)** nesse ciclo.\nUse `/inscrever`.",
                ephemeral=True
            )

        rown = ensure_playercycle_row(ws_pc, season_id, cycle, pid)
        col = ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)
        fields = get_playercycle_fields(ws_pc, rown)
        current = (fields.get("decklist_url") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                f"❌ Você já definiu decklist para o **Ciclo {cycle}** e não pode alterar.\n"
                "Peça para um ADM/Organizador se precisar.",
                ephemeral=True
            )

        ws_pc.update([[val]], range_name=f"{col_letter(col['decklist_url'])}{rown}")
        ws_pc.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send(f"✅ Decklist salva para **Ciclo {cycle}**.\nLink: {val}", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /decklist: {e}", ephemeral=True)


# =========================================================
# /deck_ver e /decklist_ver — visível para qualquer jogador
# =========================================================
@client.tree.command(name="deck_ver", description="Mostra o deck do jogador no ciclo informado.")
@app_commands.describe(cycle="Ciclo (ex: 2)", jogador="Opcional: jogador para consultar")
async def deck_ver(interaction: discord.Interaction, cycle: int, jogador: discord.Member | None = None):
    await interaction.response.defer(ephemeral=True)

    target = jogador or interaction.user
    pid = str(target.id).strip()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_pc = ensure_worksheet(sh, "PlayerCycles", PLAYERCYCLES_HEADER, rows=10000, cols=20)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        rown = get_playercycle_row(ws_pc, season_id, cycle, pid)
        if rown is None:
            return await interaction.followup.send(f"Sem deck registrado para **Ciclo {cycle}** (Season {season_id}).", ephemeral=True)

        fields = get_playercycle_fields(ws_pc, rown)
        deck_name = (fields.get("deck") or "").strip()
        if not deck_name:
            return await interaction.followup.send(f"Deck ainda não definido para **Ciclo {cycle}**.", ephemeral=True)

        await interaction.followup.send(
            f"🧾 Deck — **{target.display_name}**\n"
            f"Season {season_id} | Ciclo {cycle}\n"
            f"**{deck_name}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


@client.tree.command(name="decklist_ver", description="Mostra a decklist do jogador no ciclo informado.")
@app_commands.describe(cycle="Ciclo (ex: 2)", jogador="Opcional: jogador para consultar")
async def decklist_ver(interaction: discord.Interaction, cycle: int, jogador: discord.Member | None = None):
    await interaction.response.defer(ephemeral=True)

    target = jogador or interaction.user
    pid = str(target.id).strip()

    try:
        sh = open_sheet()
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
        ws_pc = ensure_worksheet(sh, "PlayerCycles", PLAYERCYCLES_HEADER, rows=10000, cols=20)

        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        ensure_sheet_columns(ws_pc, PLAYERCYCLES_REQUIRED)

        season_id = get_current_season_id(ws_seasons)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        rown = get_playercycle_row(ws_pc, season_id, cycle, pid)
        if rown is None:
            return await interaction.followup.send(f"Sem decklist registrada para **Ciclo {cycle}** (Season {season_id}).", ephemeral=True)

        fields = get_playercycle_fields(ws_pc, rown)
        url = (fields.get("decklist_url") or "").strip()
        if not url:
            return await interaction.followup.send(f"Decklist ainda não definida para **Ciclo {cycle}**.", ephemeral=True)

        await interaction.followup.send(
            f"🔗 Decklist — **{target.display_name}**\n"
            f"Season {season_id} | Ciclo {cycle}\n"
            f"{url}",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# [BLOCO 6/8 — CORRIGIDO termina aqui]
# Próximo: BLOCO 7/8 (eu vou corrigir sem duplicar comandos e alinhado ao /comando)
# =========================================================


# =========================================================
# [BLOCO 7/8 — CORRIGIDO] — PODS + MATCHES (gerar/ver/publicar) + PENDÊNCIAS + RESULTADO/REJEITAR
# (cole este BLOCO 7/8 por cima do seu BLOCO 7 atual)
#
# Correções aplicadas aqui (importantes):
# 1) /confrontos_pendentes agora é VISÃO DO PRÓPRIO JOGADOR (como você queria na lista).
#    - Mostra pending do jogador no ciclo escolhido.
#    - Se for ADM/Organizador, também mostra (sem bloquear).
#
# 2) Ajuste de texto e consistência: /meus_matches já lista tudo, mas /confrontos_pendentes
#    foca somente em matches "pending" do usuário.
#
# 3) Mantém: anti-repetição (todas as seasons), LOCK do ciclo ao gerar pods, prazo do ciclo.
# =========================================================

def require_current_season(sh) -> int:
    ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
    ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
    sid = get_current_season_id(ws_seasons)
    if sid <= 0:
        raise RuntimeError("Não existe season ativa.")
    return sid

def require_cycle_exists_and_get_status(ws_cycles, season_id: int, cycle: int) -> str:
    fields = get_cycle_fields(ws_cycles, season_id, cycle)
    if fields.get("status") is None:
        raise RuntimeError(f"Ciclo {cycle} não existe na Season {season_id}.")
    return (fields.get("status") or "").strip().lower()


# =========================================================
# PODS: gerar/ver/publicar + trava ciclo + calcula prazo
# =========================================================
@client.tree.command(name="pods_gerar", description="(ADM) Sorteia pods do ciclo, grava PodsHistory e cria Matches pending.")
@app_commands.describe(cycle="Ciclo (ex: 1)", tamanho="Tamanho do pod (padrão 4)")
async def pods_gerar(interaction: discord.Interaction, cycle: int, tamanho: int = 4):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    tamanho = max(2, min(int(tamanho), 8))
    nowb = now_br_str()

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=25)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        st = require_cycle_exists_and_get_status(ws_cycles, season_id, cycle)

        if st == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED. Não pode gerar pods.", ephemeral=True)
        if st == "locked":
            return await interaction.followup.send("❌ Pods já foram gerados. Ciclo está LOCKED.", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Ciclo não está OPEN (status: {st}).", ephemeral=True)

        # proteção contra regenerar pods
        pods_rows = ws_pods.get_all_records()
        if any(safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle for r in pods_rows):
            set_cycle_status(ws_cycles, season_id, cycle, "locked")
            return await interaction.followup.send(
                f"❌ Já existe PodsHistory para S{season_id} C{cycle}. Não é permitido gerar novamente.\n"
                "Status ajustado para LOCKED.",
                ephemeral=True
            )

        # inscritos ativos no ciclo
        enr_rows = ws_enr.get_all_records()
        players = []
        for r in enr_rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if str(r.get("status", "")).strip().lower() != "active":
                continue
            pid = str(r.get("player_id", "")).strip()
            if pid:
                players.append(pid)

        if len(players) < 2:
            return await interaction.followup.send("❌ Poucos inscritos ativos para gerar pods.", ephemeral=True)

        # anti-repetição baseado em matches confirmados de TODAS as seasons (histórico inteiro)
        past_pairs = get_past_confirmed_pairs(ws_matches)
        pods, repeat_score = best_shuffle_min_repeats(players, tamanho, past_pairs, tries=250)

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        created_matches = 0
        nick_map = build_players_nick_map(ws_players)

        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"

            # grava PodHistory
            for pid in pod_players:
                ws_pods.append_row([str(season_id), str(cycle), pod_name, pid, nowb], value_input_option="USER_ENTERED")

            # cria Matches round-robin pending
            for a, b in round_robin_pairs(pod_players):
                mid = new_match_id(season_id, cycle, pod_name)
                ac_at = auto_confirm_deadline_iso(utc_now_dt())
                row = [
                    mid, str(season_id), str(cycle), pod_name,
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

        set_cycle_status(ws_cycles, season_id, cycle, "locked")

        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)
        if start_str and deadline_str:
            set_cycle_times(ws_cycles, season_id, cycle, start_str, deadline_str)

        lines = [
            f"🧩 Pods do **Ciclo {cycle}** (Season {season_id}) gerados (tamanho base {tamanho}).",
            f"♻️ Anti-repetição: penalidade final **{repeat_score}** (quanto menor, melhor).",
            "🔒 Ciclo agora está **LOCKED** (inscrição fechada)."
        ]
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
        lines.append("Jogadores: use `/meus_matches` para ver seus match_id.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

        try:
            await log_admin(interaction.guild, f"🧩 Pods gerados: S{season_id} C{cycle} | matches {created_matches} | repeat_score {repeat_score}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_gerar: {e}", ephemeral=True)


@client.tree.command(name="pods_ver", description="Mostra pods do ciclo (com nomes).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def pods_ver(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=25)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        nick_map = build_players_nick_map(ws_players)
        rows = ws_pods.get_all_records()
        rows = [r for r in rows if safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            return await interaction.followup.send("Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        pods = {}
        for r in rows:
            pod = str(r.get("pod", "")).strip()
            pid = str(r.get("player_id", "")).strip()
            pods.setdefault(pod, []).append(pid)

        out = [f"🧩 Pods do **Ciclo {cycle}** (Season {season_id})"]
        for pod in sorted(pods.keys()):
            out.append(f"\n**Pod {pod}**")
            for pid in pods[pod]:
                out.append(f"• {nick_map.get(pid, pid)} (`{pid}`)")

        await interaction.followup.send("\n".join(out), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


@client.tree.command(name="pods_publicar", description="(ADM) Publica os pods no canal atual (ou canal de ranking, se configurado).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def pods_publicar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=25)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)

        nick_map = build_players_nick_map(ws_players)

        rows = ws_pods.get_all_records()
        rows = [r for r in rows if safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            return await interaction.followup.send("❌ Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        pods = {}
        for r in rows:
            pod = str(r.get("pod", "")).strip()
            pid = str(r.get("player_id", "")).strip()
            pods.setdefault(pod, []).append(pid)

        msg = [f"🧩 **PODS — Season {season_id} | Ciclo {cycle}**"]
        for pod in sorted(pods.keys()):
            msg.append(f"\n**Pod {pod}**")
            for pid in pods[pod]:
                msg.append(f"• {nick_map.get(pid, pid)}")

        channel = interaction.channel
        if RANKING_CHANNEL_ID and interaction.guild:
            ch = interaction.guild.get_channel(RANKING_CHANNEL_ID)
            if ch:
                channel = ch

        await channel.send("\n".join(msg))
        await interaction.followup.send("✅ Pods publicados.", ephemeral=True)

        try:
            await log_admin(interaction.guild, f"📣 Pods publicados: S{season_id} C{cycle}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_publicar: {e}", ephemeral=True)


# =========================================================
# /meus_matches (lista todos os matches do ciclo)
# =========================================================
@client.tree.command(name="meus_matches", description="Lista seus matches do ciclo (com match_id, pod e prazo 48h).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def meus_matches(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        my = []
        nowu = utc_now_dt()

        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
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
                    left = "EXPIRADO" if secs <= 0 else f"{max(0, secs)//3600}h"

            opp = b if user_id == a else a
            line = f"• `{mid}` | Pod {pod} | vs {nick_map.get(opp, opp)} | {st} | {ag}-{bg}-{dg} | {left}"
            my.append((pod, line))

        if not my:
            return await interaction.followup.send(f"Você não tem matches no Ciclo {cycle}.", ephemeral=True)

        my.sort(key=lambda x: (x[0], x[1]))
        out = [f"📌 **Seus matches — Season {season_id} | Ciclo {cycle}**"]
        out.extend([x[1] for x in my])
        out.append("\nPara reportar: `/resultado match_id:... placar:...`")
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /confrontos_pendentes (JOGADOR) — apenas PENDING do usuário no ciclo
# =========================================================
@client.tree.command(name="confrontos_pendentes", description="Lista seus matches pendentes (pending) do ciclo.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def confrontos_pendentes(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        items = []
        nowu = utc_now_dt()

        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "pending":
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if user_id not in (a, b):
                continue

            mid = str(r.get("match_id", "")).strip()
            pod = str(r.get("pod", "")).strip()
            rep = str(r.get("reported_by_id", "")).strip()

            ac = parse_iso_dt(r.get("auto_confirm_at", "") or "")
            left = "—"
            if rep and ac:
                secs = int((ac - nowu).total_seconds())
                left = "EXPIRADO" if secs <= 0 else f"{max(0, secs)//3600}h"
            elif not rep:
                left = "aguardando report"

            opp = b if user_id == a else a
            status_extra = "sem report" if not rep else f"reportado (expira {left})"
            items.append((pod, f"• Pod {pod} — `{mid}` — vs {nick_map.get(opp, opp)} — {status_extra}"))

        if not items:
            return await interaction.followup.send(f"✅ Você não tem pendências em S{season_id} C{cycle}.", ephemeral=True)

        items.sort(key=lambda x: (x[0], x[1]))
        out = [f"🧾 **Seus confrontos pendentes — Season {season_id} | Ciclo {cycle}**"]
        out.extend([x[1] for x in items[:80]])
        if len(items) > 80:
            out.append(f"\n(+{len(items)-80} não exibidos)")
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# Resultado / Rejeitar
# =========================================================
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
        season_id = require_current_season(sh)

        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if safe_int(getc("season_id"), 0) != season_id:
            return await interaction.followup.send("❌ Match não pertence à season ativa.", ephemeral=True)

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

        try:
            await log_admin(interaction.guild, f"🧾 Resultado PENDENTE: {interaction.user.mention} reportou `{match_id}` ({a_gw}-{b_gw}-{d_g}).")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /resultado: {e}", ephemeral=True)


@client.tree.command(name="rejeitar", description="Rejeita um resultado pendente (apenas o oponente, até 48h).")
@app_commands.describe(match_id="ID do match", motivo="Opcional")
async def rejeitar(interaction: discord.Interaction, match_id: str, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)
    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if safe_int(getc("season_id"), 0) != season_id:
            return await interaction.followup.send("❌ Match não pertence à season ativa.", ephemeral=True)

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
            return await interaction.followup.send(
                "❌ Prazo expirou (48h). Peça para um ADM/Organizador revisar.",
                ephemeral=True
            )

        nowb = now_br_str()
        ws.update([["rejected"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
        ws.update([[user_id]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        msg = "✅ Resultado rejeitado. ADM/Organizador pode corrigir."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

        try:
            await log_admin(interaction.guild, f"⛔ Rejeição: {interaction.user.mention} rejeitou `{match_id}`. Motivo: {motivo.strip() or '—'}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================================================
# [BLOCO 7/8 — CORRIGIDO termina aqui]
# Próximo: BLOCO 8/8 (corrigir duplicidade de onboarding e manter somente 1 on_member_join)
# =========================================================


# =========================================================
# [BLOCO 8/8] — ADMIN/LUXO + RANKINGS + EXPORT + START (REVISADO / SEM DUPLICATAS)
# (cole abaixo do BLOCO 7/8)
# =========================================================

# =========================================================
# Batch helpers (reduz chamadas ao Google Sheets)
# =========================================================
def _chunk_list(items, size: int):
    size = max(1, int(size))
    for i in range(0, len(items), size):
        yield items[i:i+size]


def ws_batch_update(ws, updates: list[dict], chunk: int = 400):
    """
    updates: [{"range": "A2", "values": [["x"]]}, ...]
    Faz batch_update em chunks para evitar limite de payload.
    """
    if not updates:
        return
    for part in _chunk_list(updates, chunk):
        ws.batch_update(part, value_input_option="USER_ENTERED")


# =========================================================
# Auto-confirm (48h) — versão batch (substitui/otimiza)
# - Confirma PENDING que já expirou auto_confirm_at
# - Só se houver reported_by_id (ou seja, alguém reportou)
# =========================================================
def sweep_auto_confirm(sh, cycle: int) -> int:
    season_id = require_current_season(sh)
    ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
    col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

    vals = ws.get_all_values()
    if len(vals) <= 1:
        return 0

    nowu = utc_now_dt()
    nowb = now_br_str()
    updates = []
    changed = 0

    # map coluna -> letra uma vez
    c_a = col_letter(col["a_games_won"])
    c_b = col_letter(col["b_games_won"])
    c_d = col_letter(col["draw_games"])
    c_rt = col_letter(col["result_type"])
    c_cs = col_letter(col["confirmed_status"])
    c_rep = col_letter(col["reported_by_id"])
    c_conf = col_letter(col["confirmed_by_id"])
    c_upd = col_letter(col["updated_at"])
    c_ac = col_letter(col["auto_confirm_at"])
    c_sid = col_letter(col["season_id"])
    c_cyc = col_letter(col["cycle"])
    c_act = col_letter(col["active"])

    for rown in range(2, len(vals) + 1):
        r = vals[rown - 1]

        def getv(key: str) -> str:
            idx = col[key]
            return r[idx] if idx < len(r) else ""

        if safe_int(getv("season_id"), 0) != season_id:
            continue
        if safe_int(getv("cycle"), 0) != cycle:
            continue
        if not as_bool(getv("active") or "TRUE"):
            continue

        st = (getv("confirmed_status") or "").strip().lower()
        if st != "pending":
            continue

        rep = (getv("reported_by_id") or "").strip()
        if not rep:
            continue

        ac = parse_iso_dt(getv("auto_confirm_at") or "")
        if not ac:
            continue

        if nowu <= ac:
            continue

        # expirou -> confirma
        updates.append({"range": f"{c_cs}{rown}", "values": [["confirmed"]]})
        updates.append({"range": f"{c_conf}{rown}", "values": [[rep]]})
        updates.append({"range": f"{c_upd}{rown}", "values": [[nowb]]})
        changed += 1

    ws_batch_update(ws, updates, chunk=400)
    return changed


# =========================================================
# ADMIN: editar resultado + ALIAS /resultado_admin
# =========================================================
@client.tree.command(name="resultado_admin", description="(ADM) Edita resultado de um match e opcionalmente confirma (alias).")
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
async def resultado_admin(interaction: discord.Interaction, match_id: str, placar: app_commands.Choice[str], confirmar: app_commands.Choice[str]):
    return await admin_resultado_editar(interaction, match_id, placar, confirmar)


# =========================================================
# ADMIN: cancelar/inativar match + ALIAS /match_cancelar
# =========================================================
@client.tree.command(name="match_cancelar", description="(ADM) Inativa um match (active=FALSE) (alias).")
@app_commands.autocomplete(match_id=ac_match_id_any)
@app_commands.describe(match_id="match_id", motivo="Opcional")
async def match_cancelar(interaction: discord.Interaction, match_id: str, motivo: str = ""):
    return await admin_resultado_cancelar(interaction, match_id, motivo)


# =========================================================
# /deadline (pendências próximas de expirar)
# =========================================================
@client.tree.command(name="deadline", description="Lista resultados pendentes próximos de expirar (48h).")
@app_commands.describe(cycle="Ciclo (ex: 1)", horas="Janela (ex: 12)")
async def deadline(interaction: discord.Interaction, cycle: int, horas: int = 12):
    await interaction.response.defer(ephemeral=True)
    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        nowu = utc_now_dt()
        hours = max(1, min(horas, 48))
        limit_dt = nowu + timedelta(hours=hours)

        rows = ws.get_all_records()
        items = []
        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
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
            return await interaction.followup.send(f"✅ Nenhum pending expira nas próximas {hours}h (S{season_id} C{cycle}).", ephemeral=True)

        items.sort(key=lambda x: x[0])
        out = [f"⏰ Pendências (Season {season_id} / Ciclo {cycle}) que expiram em até {hours}h:"]
        out.extend([x[1] for x in items[:40]])
        await interaction.followup.send("\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /fechar_resultados_atrasados (comando separado)
# =========================================================
@client.tree.command(name="fechar_resultados_atrasados", description="(ADM) Força auto-confirm expirados (48h) no ciclo.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def fechar_resultados_atrasados(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        _ = require_current_season(sh)
        changed = sweep_auto_confirm(sh, cycle)
        await interaction.followup.send(f"✅ Auto-confirm aplicado. Alterados: **{changed}**", ephemeral=True)

        try:
            season_id = require_current_season(sh)
            await log_admin(interaction.guild, f"🕒 Auto-confirm manual: S{season_id} C{cycle} | alterados {changed}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /final — versão batch (blindado p/ até 90 matches e além)
# Aplica 0-0-3 em matches sem report após prazo do ciclo.
# =========================================================
@client.tree.command(name="final", description="(ADM) Aplica 0-0-3 em todos os matches sem resultado reportado no ciclo (após prazo).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def final(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=25)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        start_str, deadline_str, max_pod_size, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)
        if not deadline_str:
            return await interaction.followup.send("❌ Este ciclo ainda não tem pods/prazo.", ephemeral=True)

        set_cycle_times(ws_cycles, season_id, cycle, start_str, deadline_str)

        deadline_dt = parse_br_dt(deadline_str)
        if deadline_dt and now_br_dt() < deadline_dt:
            return await interaction.followup.send(
                f"❌ Ainda não chegou o fim do ciclo.\nFim: **{deadline_str} (BR)**\nUse `/prazo` para ver.",
                ephemeral=True
            )

        vals = ws_matches.get_all_values()
        if len(vals) <= 1:
            return await interaction.followup.send("Nada para finalizar (Matches vazio).", ephemeral=True)

        nowb = now_br_str()
        updates = []
        changed = 0

        # colunas -> letras
        c_sid = col_letter(col["season_id"])
        c_cyc = col_letter(col["cycle"])
        c_act = col_letter(col["active"])
        c_rep = col_letter(col["reported_by_id"])

        c_agw = col_letter(col["a_games_won"])
        c_bgw = col_letter(col["b_games_won"])
        c_drw = col_letter(col["draw_games"])
        c_rt = col_letter(col["result_type"])
        c_cs = col_letter(col["confirmed_status"])
        c_cb = col_letter(col["confirmed_by_id"])
        c_upd = col_letter(col["updated_at"])

        for rown in range(2, len(vals) + 1):
            r = vals[rown - 1]

            def getc(name: str) -> str:
                idx = col[name]
                return r[idx] if idx < len(r) else ""

            if safe_int(getc("season_id"), 0) != season_id:
                continue
            if safe_int(getc("cycle"), 0) != cycle:
                continue
            if not as_bool(getc("active") or "TRUE"):
                continue

            rep = (getc("reported_by_id") or "").strip()
            if rep:
                continue

            # aplica 0-0-3 + confirma
            updates.append({"range": f"{c_agw}{rown}", "values": [["0"]]})
            updates.append({"range": f"{c_bgw}{rown}", "values": [["0"]]})
            updates.append({"range": f"{c_drw}{rown}", "values": [["3"]]})
            updates.append({"range": f"{c_rt}{rown}", "values": [["intentional_draw"]]})
            updates.append({"range": f"{c_cs}{rown}", "values": [["confirmed"]]})
            updates.append({"range": f"{c_rep}{rown}", "values": [["FINAL"]]})
            updates.append({"range": f"{c_cb}{rown}", "values": [["FINAL"]]})
            updates.append({"range": f"{c_upd}{rown}", "values": [[nowb]]})
            changed += 1

        ws_batch_update(ws_matches, updates, chunk=400)

        await interaction.followup.send(
            f"✅ Finalização aplicada no **Ciclo {cycle}** (Season {season_id}).\n"
            f"Matches sem report que receberam **0-0-3**: **{changed}**\n"
            "Agora rode `/recalcular` para atualizar o ranking.",
            ephemeral=True
        )

        try:
            await log_admin(interaction.guild, f"🏁 FINAL (batch): S{season_id} C{cycle} | 0-0-3 aplicados {changed}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /final: {e}", ephemeral=True)


# =========================================================
# /recalcular (auto-confirm + ranking)
# =========================================================
@client.tree.command(name="recalcular", description="(ADM) Auto-confirm (48h) + recalcula ranking do ciclo do zero.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def recalcular(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        _ = require_current_season(sh)

        changed = sweep_auto_confirm(sh, cycle)
        rows = recalculate_cycle(cycle)

        await interaction.followup.send(
            f"✅ Recalculo concluído. Ciclo {cycle} atualizado.\n"
            f"Auto-confirm (48h) feitos: **{changed}**\n"
            f"Jogadores no Standings: **{len(rows)}**",
            ephemeral=True
        )

        try:
            season_id = require_current_season(sh)
            await log_admin(interaction.guild, f"🔁 RECALC: S{season_id} C{cycle} | auto-confirm {changed} | standings {len(rows)}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"⚠️ Erro no recálculo: {e}", ephemeral=True)


# =========================================================
# /ranking (público) + /standings_publicar
# =========================================================
@client.tree.command(name="ranking", description="Mostra o ranking do ciclo (público).")
@app_commands.describe(cycle="Ciclo (ex: 1)", top="Quantos mostrar (padrão 20)")
async def ranking(interaction: discord.Interaction, cycle: int, top: int = 20):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_st = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=10000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        data = ws_st.get_all_records()
        rows = [r for r in data if safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            return await interaction.followup.send("Sem standings para esse ciclo. Rode `/recalcular`.", ephemeral=False)

        top = max(5, min(top, 50))
        rows.sort(key=lambda r: safe_int(r.get("rank_position", 9999), 9999))

        out = [f"🏆 **Ranking — Season {season_id} | Ciclo {cycle}** (Top {top})"]
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
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_st = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=10000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        data = ws_st.get_all_records()
        rows = [r for r in data if safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            return await interaction.followup.send("Sem standings. Rode `/recalcular`.", ephemeral=True)

        top = max(5, min(top, 50))
        rows.sort(key=lambda r: safe_int(r.get("rank_position", 9999), 9999))

        msg = [f"🏆 **Ranking — Season {season_id} | Ciclo {cycle}** (Top {top})"]
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

        try:
            await log_admin(interaction.guild, f"📣 Ranking publicado: S{season_id} C{cycle} top {top}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /ranking_geral — SEASON atual (todos os ciclos somados)
# =========================================================
@client.tree.command(name="ranking_geral", description="Mostra ranking geral da SEASON atual (todos os ciclos somados).")
@app_commands.describe(top="Quantos mostrar (padrão 30)")
async def ranking_geral(interaction: discord.Interaction, top: int = 30):
    await interaction.response.defer(ephemeral=False)
    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()

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

        valid = []
        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
                continue
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("result_type", "normal")).strip().lower() == "bye":
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if not a or not b:
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

        if not stats:
            return await interaction.followup.send("Sem matches confirmados na season atual.", ephemeral=False)

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

        table = []
        for pid, s in stats.items():
            table.append({
                "pid": pid,
                "pts": s["match_points"],
                "omw": pct1(omw[pid]),
                "gw": pct1(gwp[pid]),
                "ogw": pct1(ogw[pid]),
                "j": s["matches_played"],
            })

        table.sort(key=lambda r: (r["pts"], r["omw"], r["gw"], r["ogw"]), reverse=True)

        top = max(10, min(top, 60))
        out = [f"🏆 **Ranking Geral — Season {season_id}** (Top {top})"]
        out.append("pos | jogador | pts | OMW | GW | OGW | J")
        out.append("--- | ------ | --- | --- | --- | --- | ---")
        for i, r in enumerate(table[:top], start=1):
            out.append(f"{i} | {nick_map.get(r['pid'], r['pid'])} | {r['pts']} | {r['omw']} | {r['gw']} | {r['ogw']} | {r['j']}")

        await interaction.followup.send("\n".join(out), ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ranking_geral: {e}", ephemeral=False)


# =========================================================
# /export (alias do exportar_ciclo) — mantém seu nome preferido
# =========================================================
@client.tree.command(name="export", description="(ADM) Exporta CSV do ciclo (Matches e Standings).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def export_cmd(interaction: discord.Interaction, cycle: int):
    return await exportar_ciclo(interaction, cycle)


# =========================================================
# /ciclo_status (alias) — você pediu esse nome
# =========================================================
@client.tree.command(name="ciclo_status", description="Mostra status geral do ciclo (alias).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_status(interaction: discord.Interaction, cycle: int):
    return await status_ciclo(interaction, cycle)


# =========================================================
# /ciclo_fechar e /ciclo_reabrir (blindado contra erro humano)
# =========================================================
@client.tree.command(name="ciclo_fechar", description="(ADM) Fecha um ciclo OPEN por engano (status=locked).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_fechar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        st = require_cycle_exists_and_get_status(ws_cycles, season_id, cycle)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED não pode ser fechado/reaberto por este comando.", ephemeral=True)

        set_cycle_status(ws_cycles, season_id, cycle, "locked")
        await interaction.followup.send(f"✅ Ciclo {cycle} (Season {season_id}) fechado: status = LOCKED.", ephemeral=True)

        try:
            await log_admin(interaction.guild, f"🔒 Ciclo fechado manualmente: S{season_id} C{cycle} (por {interaction.user.mention})")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


@client.tree.command(name="ciclo_reabrir", description="(ADM) Reabre ciclo para OPEN (apenas se não houver PodsHistory e não estiver completed).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def ciclo_reabrir(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=20000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)

        st = require_cycle_exists_and_get_status(ws_cycles, season_id, cycle)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED não pode ser reaberto.", ephemeral=True)

        pods_rows = ws_pods.get_all_records()
        has_pods = any(safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle for r in pods_rows)
        if has_pods:
            return await interaction.followup.send(
                "❌ Não posso reabrir: este ciclo já tem PodsHistory.\n"
                "Se precisa corrigir algo, use comandos administrativos (cancelar match, substituir jogador, etc).",
                ephemeral=True
            )

        set_cycle_status(ws_cycles, season_id, cycle, "open")
        await interaction.followup.send(f"✅ Ciclo {cycle} (Season {season_id}) reaberto: status = OPEN.", ephemeral=True)

        try:
            await log_admin(interaction.guild, f"🔓 Ciclo reaberto: S{season_id} C{cycle} (por {interaction.user.mention})")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# START
# =========================
if not DISCORD_TOKEN:
    raise RuntimeError("Faltou a variável DISCORD_TOKEN no ambiente.")

keep_alive()
client.run(DISCORD_TOKEN)

# =========================================================
# [BLOCO 8/8 termina aqui]
# =========================================================
