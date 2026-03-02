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

# [BLOCO 2/8] — SCHEMA (ABAS) + SEASON STATE + FUNÇÕES DE SEASON/CICLO (REVISADO v2)
# AJUSTES FEITOS NESTA REVISÃO:
# - Reintroduz ABA "Decks" (DECKS_HEADER/DECKS_REQUIRED) pois BLOCO 6 usa.
# - Adiciona helpers: get_deck_row / get_deck_fields (usados no BLOCO 6).
# - ensure_all_sheets agora garante também a aba Decks.
# - Mantém Players no formato longo (A..H) como você está usando.
# - Mantém lógica de season atual (SeasonState -> fallback Seasons OPEN).
# =========================================================

# =========================
# Sheets schema (headers)
# =========================
SEASONSTATE_HEADER = ["key", "value", "updated_at"]  # key="current_season_id"

SEASONS_HEADER = ["season_id", "status", "name", "created_at", "updated_at"]  # status: open|closed
SEASONS_REQUIRED = ["season_id", "status", "name", "created_at", "updated_at"]

# A: discord_id | B: nick | C: name | D: notes | E: status | F: rating | G: created_at | H: updated_at
PLAYERS_HEADER = ["discord_id", "nick", "name", "notes", "status", "rating", "created_at", "updated_at"]
PLAYERS_REQUIRED = PLAYERS_HEADER[:]

# Deck/Decklist: 1 vez POR CICLO (por season + cycle + player_id)
DECKS_HEADER = ["season_id", "cycle", "player_id", "deck", "decklist_url", "created_at", "updated_at"]
DECKS_REQUIRED = DECKS_HEADER[:]

ENROLLMENTS_HEADER = ["season_id", "cycle", "player_id", "status", "created_at", "updated_at"]
ENROLLMENTS_REQUIRED = ["season_id", "cycle", "player_id", "status", "created_at", "updated_at"]

CYCLES_HEADER = ["season_id", "cycle", "status", "start_at_br", "deadline_at_br", "created_at", "updated_at"]
# status: open|locked|completed
CYCLES_REQUIRED = ["season_id", "cycle", "status", "start_at_br", "deadline_at_br", "created_at", "updated_at"]

PODSHISTORY_HEADER = ["season_id", "cycle", "pod", "player_id", "created_at"]
PODSHISTORY_REQUIRED = ["season_id", "cycle", "pod", "player_id", "created_at"]

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
MATCHES_HEADER = MATCHES_REQUIRED_COLS[:]

STANDINGS_HEADER = [
    "season_id", "cycle", "player_id",
    "matches_played", "match_points", "mwp_percent",
    "game_wins", "game_losses", "game_draws", "games_played", "gw_percent",
    "omw_percent", "ogw_percent",
    "rank_position", "last_recalc_at"
]
STANDINGS_REQUIRED = STANDINGS_HEADER[:]


# =========================
# Ensure abas existem
# =========================
def ensure_all_sheets(sh):
    ensure_worksheet(sh, "SeasonState", SEASONSTATE_HEADER, rows=20, cols=10)
    ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
    ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)

    # Decks por ciclo
    ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)

    ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
    ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
    ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
    ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30)


# =========================
# Season helpers
# =========================
def _infer_open_season_id_from_seasons_ws(ws_seasons) -> int:
    rows = ws_seasons.get_all_records()
    open_ids = []
    for r in rows:
        sid = safe_int(r.get("season_id", 0), 0)
        st = str(r.get("status", "")).strip().lower()
        if sid > 0 and st == "open":
            open_ids.append(sid)
    if not open_ids:
        return 0
    return max(open_ids)

def get_current_season_id(arg) -> int:
    """
    - Se receber worksheet "Seasons": retorna a season com status=open.
    - Se receber spreadsheet (sh): tenta SeasonState; se falhar, infere pela aba Seasons.
    """
    # Caso 1: worksheet "Seasons"
    try:
        if hasattr(arg, "title") and str(arg.title).strip().lower() == "seasons":
            ensure_sheet_columns(arg, SEASONS_REQUIRED)
            return _infer_open_season_id_from_seasons_ws(arg)
    except Exception:
        pass

    # Caso 2: spreadsheet sh
    sh = arg
    try:
        ws_state = sh.worksheet("SeasonState")
        rows = ws_state.get_all_records()
        for r in rows:
            if str(r.get("key", "")).strip() == "current_season_id":
                sid = safe_int(r.get("value", 0), 0)
                if sid > 0:
                    return sid
    except Exception:
        pass

    # fallback: infere pela aba Seasons
    try:
        ws_seasons = sh.worksheet("Seasons")
        ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)
        return _infer_open_season_id_from_seasons_ws(ws_seasons)
    except Exception:
        return 0

def set_current_season_id(sh, season_id: int):
    ws = ensure_worksheet(sh, "SeasonState", SEASONSTATE_HEADER, rows=20, cols=10)
    vals = ws.get_all_values()
    nowb = now_br_str()

    found = None
    for i in range(2, len(vals) + 1):
        row = vals[i - 1]
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
            return str(r.get("status", "")).strip().lower()
    return ""

def set_season_status(sh, season_id: int, status: str, name: str | None = None):
    status = str(status).strip().lower()
    ws = sh.worksheet("Seasons")
    data = ws.get_all_values()
    nowb = now_br_str()

    found = None
    for i in range(2, len(data) + 1):
        row = data[i - 1]
        sid = safe_int(row[0] if len(row) > 0 else 0, 0)
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
    for i in range(2, len(data) + 1):
        row = data[i - 1]
        sid = safe_int(row[0] if len(row) > 0 else 0, 0)
        if sid <= 0 or sid == keep_open_id:
            continue
        st = (row[1] if len(row) > 1 else "").strip().lower()
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
        r = rows[r_i - 1]
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
        r = rows[r_i - 1]
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
        pid = str(r.get("discord_id", "")).strip()
        nick = str(r.get("nick", "")).strip()
        if pid:
            m[pid] = nick or pid
    return m

# Alias usado nos BLOCOS 5/7/8
def build_players_nick_map(ws_players) -> dict[str, str]:
    return get_player_nick_map(ws_players)


# =========================
# Deck helpers (1 vez POR CICLO)
# =========================
def get_deck_row(ws_decks, season_id: int, cycle: int, player_id: str) -> int | None:
    """
    Localiza a linha (1-based) do registro do jogador na aba Decks para (season,cycle,player).
    """
    data = ws_decks.get_all_values()
    if len(data) <= 1:
        return None
    col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
    pid = str(player_id).strip()

    for i in range(2, len(data) + 1):
        r = data[i - 1]
        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        c = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)
        p = str(r[col["player_id"]] if col["player_id"] < len(r) else "").strip()
        if s == season_id and c == cycle and p == pid:
            return i
    return None

def get_deck_fields(ws_decks, row: int) -> dict:
    """
    Lê e retorna os campos do Decks naquela linha (1-based).
    """
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
# =========================================================
# =========================================================
# =========================================================
# [BLOCO 5/8] — DISCORD CORE + AUTOCOMPLETE + /COMANDO + ONBOARDING
# (VERSÃO FINAL — Modal Nome/Sobrenome + Cache Autocomplete + Onboarding via CANAL + /cadastro backup)
# =========================================================

import asyncio
from time import time as _time


# =========================
# Discord Bot (Client)
# =========================
class LemeBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # necessário para on_member_join + fetch_member
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        try:
            if GUILD_ID:
                guild = discord.Object(id=GUILD_ID)
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
            else:
                await self.tree.sync()
        except Exception:
            pass


client = LemeBot()


# =========================
# Config extra (onboarding)
# =========================
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "0"))


# =========================
# Logs (admin)
# =========================
async def log_admin_guild(guild: discord.Guild | None, text: str):
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
    if ch:
        try:
            await ch.send(text)
        except Exception:
            pass


# =========================================================
# Helper: split de mensagens (limite Discord 2000 chars)
# =========================================================
def split_text_lines(text: str, limit: int = 1900) -> list[str]:
    lines = str(text).split("\n")
    chunks: list[str] = []
    buf = ""

    for ln in lines:
        piece = ln + "\n"
        if len(buf) + len(piece) > limit:
            if buf.strip():
                chunks.append(buf.rstrip("\n"))
            buf = piece
        else:
            buf += piece

    if buf.strip():
        chunks.append(buf.rstrip("\n"))

    safe_chunks: list[str] = []
    for c in chunks:
        if len(c) <= limit:
            safe_chunks.append(c)
            continue
        for i in range(0, len(c), limit):
            safe_chunks.append(c[i:i + limit])

    return safe_chunks


async def send_followup_chunks(interaction: discord.Interaction, text: str, ephemeral: bool = True):
    chunks = split_text_lines(text, limit=1900)
    if not chunks:
        return

    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(chunks[0], ephemeral=ephemeral)
        else:
            await interaction.followup.send(chunks[0], ephemeral=ephemeral)
    except Exception:
        return

    for c in chunks[1:]:
        try:
            await interaction.followup.send(c, ephemeral=ephemeral)
        except Exception:
            break


# =========================================================
# Localizar membro: funciona em clique no servidor OU em DM
# =========================================================
async def find_member_anywhere(interaction: discord.Interaction, member_id: int) -> tuple[discord.Guild | None, discord.Member | None]:
    # 1) Se clique veio do servidor, melhor caso
    if interaction.guild:
        try:
            m = interaction.guild.get_member(member_id)
            if m is None:
                m = await interaction.guild.fetch_member(member_id)
            return interaction.guild, m
        except Exception:
            return interaction.guild, None

    # 2) DM: tenta pelo GUILD_ID fixo
    if GUILD_ID:
        try:
            g = client.get_guild(GUILD_ID)
            if g is None:
                g = await client.fetch_guild(GUILD_ID)
            try:
                m = g.get_member(member_id)
                if m is None:
                    m = await g.fetch_member(member_id)
                return g, m
            except Exception:
                pass
        except Exception:
            pass

    # 3) varre todos os guilds do bot
    for g in list(client.guilds):
        try:
            m = g.get_member(member_id)
            if m is None:
                m = await g.fetch_member(member_id)
            return g, m
        except Exception:
            continue

    return None, None


# =========================================================
# Players upsert (Sheets)
# =========================================================
def upsert_player(ws_players, discord_id: str, nickname: str):
    did = str(discord_id).strip()
    nick = str(nickname).strip()
    if not did or not nick:
        return

    col = ensure_sheet_columns(ws_players, PLAYERS_HEADER)
    rows = ws_players.get_all_values()

    did_col = col.get("discord_id", 0)
    found_row = None
    for i in range(2, len(rows) + 1):
        r = rows[i - 1]
        v = r[did_col] if did_col < len(r) else ""
        if str(v).strip() == did:
            found_row = i
            break

    nowc = now_br_str()

    if found_row:
        try:
            ws_players.update([[nick]], range_name=f"{col_letter(col['nick'])}{found_row}")
        except Exception:
            pass
        if "updated_at" in col:
            try:
                ws_players.update([[nowc]], range_name=f"{col_letter(col['updated_at'])}{found_row}")
            except Exception:
                pass
        return

    row = [""] * len(PLAYERS_HEADER)
    row[col.get("discord_id", 0)] = did
    row[col.get("nick", 1)] = nick
    if "status" in col:
        row[col["status"]] = "active"
    if "created_at" in col:
        row[col["created_at"]] = nowc
    if "updated_at" in col:
        row[col["updated_at"]] = nowc

    ws_players.append_row(row, value_input_option="USER_ENTERED")


# =========================================================
# Autocomplete Cache (Ciclos OPEN)
# =========================================================
_OPEN_CYCLES_CACHE = {"ts": 0.0, "season_id": 0, "cycles": []}

def _load_open_cycles_from_sheets(sh):
    season_id = get_current_season_id(sh)
    if season_id <= 0:
        return 0, []

    ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

    rows = ws_cycles.get_all_records()
    open_cycles: list[int] = []
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if str(r.get("status", "")).strip().lower() != "open":
            continue
        cyc = safe_int(r.get("cycle", 0), 0)
        if cyc > 0:
            open_cycles.append(cyc)

    return season_id, sorted(set(open_cycles))


def get_open_cycles_cached(max_age_seconds: int = 60):
    now = _time()
    if (_OPEN_CYCLES_CACHE["season_id"] > 0) and ((now - _OPEN_CYCLES_CACHE["ts"]) < max_age_seconds):
        return _OPEN_CYCLES_CACHE["season_id"], list(_OPEN_CYCLES_CACHE["cycles"])

    try:
        sh = open_sheet()
        sid, cycles = _load_open_cycles_from_sheets(sh)
        _OPEN_CYCLES_CACHE["ts"] = now
        _OPEN_CYCLES_CACHE["season_id"] = sid
        _OPEN_CYCLES_CACHE["cycles"] = cycles
        return sid, cycles
    except Exception:
        return _OPEN_CYCLES_CACHE.get("season_id", 0), list(_OPEN_CYCLES_CACHE.get("cycles", []))


async def ac_cycle_open(interaction: discord.Interaction, current: str):
    try:
        _, cycles = get_open_cycles_cached(max_age_seconds=60)
        cur = str(current or "").strip()
        if cur:
            cycles = [c for c in cycles if str(c).startswith(cur)]
        return [app_commands.Choice(name=f"Ciclo {c} (open)", value=str(c)) for c in cycles[:25]]
    except Exception:
        return []


# =========================================================
# /comando (catálogo)
# =========================================================
COMMANDS_CATALOG = [
    ("jogador", "/inscrever", "Se inscreve em um ciclo aberto (com escolha de ciclo)."),
    ("jogador", "/deck", "Define seu deck (1 vez POR CICLO, escolhendo o ciclo)."),
    ("jogador", "/decklist", "Define decklist (1 vez POR CICLO, escolhendo o ciclo)."),
    ("jogador", "/resultado", "Reporta resultado V-D-E (menu)."),
    ("jogador", "/ranking", "Mostra ranking público do ciclo."),
    ("jogador", "/ranking_geral", "Mostra ranking geral da season."),
    ("jogador", "/prazo", "Mostra o prazo oficial do ciclo."),
    ("jogador", "/cadastro", "Abre o cadastro do jogador (Nome/Sobrenome)."),
    ("jogador", "/comando", "Mostra os comandos que você tem acesso."),
    ("adm", "/forcesync", "Sincroniza comandos no servidor (rápido)."),
]

def level_allows(user_level: str, cmd_level: str) -> bool:
    order = {"jogador": 1, "adm": 2, "organizador": 3, "owner": 4}
    return order.get(user_level, 1) >= order.get(cmd_level, 1)


@client.tree.command(name="comando", description="Mostra seus comandos disponíveis.")
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

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True)

    except Exception as e:
        try:
            await interaction.followup.send(f"❌ Erro no /comando: {e}", ephemeral=True)
        except Exception:
            pass


# =========================================================
# /forcesync (ADM) — resposta garantida
# =========================================================
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
        await asyncio.wait_for(client.tree.sync(guild=guild), timeout=15)
        await interaction.followup.send("🔄 Comandos sincronizados com sucesso.", ephemeral=True)
    except asyncio.TimeoutError:
        await interaction.followup.send("⚠️ Timeout no Discord. Tente novamente em 30s.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ Falha ao sincronizar: {type(e).__name__} — {e}", ephemeral=True)


# =========================================================
# MODAL — Nome e Sobrenome
# =========================================================
class NicknameModal(discord.ui.Modal, title="Cadastro do Jogador"):
    nome = discord.ui.TextInput(
        label="Insira seu Nome e Sobrenome igual no WhatsApp, para facilitar a localização das Matchs",
        required=True,
        max_length=32,
    )

    def __init__(self, member_id: int):
        super().__init__()
        self.member_id = member_id

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.nome.value).strip()

        if len(raw.split()) < 2:
            return await interaction.response.send_message(
                "⚠️ Informe Nome e Sobrenome.\nExemplo: João Silva",
                ephemeral=True
            )

        guild, member = await find_member_anywhere(interaction, self.member_id)

        # salvar no Sheets
        try:
            sh = open_sheet()
            ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
            ensure_sheet_columns(ws_players, PLAYERS_HEADER)
            upsert_player(ws_players, str(self.member_id), raw)
        except Exception:
            pass

        # tentar alterar nickname
        nick_ok = False
        if member:
            try:
                await member.edit(nick=raw, reason="Onboarding - Nome informado pelo jogador")
                nick_ok = True
            except Exception:
                nick_ok = False

        if nick_ok:
            await interaction.response.send_message(
                f"✅ Cadastro concluído!\nSeu nome foi definido como **{raw}**.\n\nAgora use `/inscrever`.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"✅ Nome salvo: **{raw}**.\n\n"
                "⚠️ Não consegui alterar seu apelido automaticamente.\n"
                "Verifique permissão do bot: **Gerenciar Apelidos** e cargo do bot acima do Jogador.\n\n"
                "Agora use `/inscrever`.",
                ephemeral=True
            )

        try:
            await log_admin_guild(guild, f"📝 Cadastro: <@{self.member_id}> -> {raw} (nick_ok={nick_ok})")
        except Exception:
            pass


# =========================================================
# /cadastro (backup) — funciona dentro do servidor
# =========================================================
@client.tree.command(name="cadastro", description="Cadastro do jogador (Nome e Sobrenome).")
async def cadastro(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message(
            "⚠️ Use este comando dentro do servidor (não funciona por DM).",
            ephemeral=True
        )
    try:
        await interaction.response.send_modal(NicknameModal(interaction.user.id))
    except Exception:
        try:
            await interaction.response.send_message("⚠️ Não consegui abrir o formulário agora. Tente novamente.", ephemeral=True)
        except Exception:
            pass


# =========================================================
# ONBOARDING (Participar / Assistir)
# =========================================================
class OnboardingView(discord.ui.View):
    def __init__(self, member_id: int):
        super().__init__(timeout=3600)
        self.member_id = member_id

    @discord.ui.button(label="Participar", style=discord.ButtonStyle.success)
    async def participar(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild, member = await find_member_anywhere(interaction, self.member_id)

        if not member or not guild:
            return await interaction.response.send_message(
                "⚠️ Não consegui localizar seu usuário no servidor.\n"
                "Entre no servidor e use `/cadastro` no canal de comandos.",
                ephemeral=True
            )

        role_jog = discord.utils.get(guild.roles, name=ROLE_JOGADOR)
        if not role_jog:
            return await interaction.response.send_message(
                "⚠️ Cargo **Jogador** não encontrado. Crie o cargo com esse nome exato ou ajuste ROLE_JOGADOR no Render.",
                ephemeral=True
            )

        try:
            await member.add_roles(role_jog, reason="Onboarding - Participar")
        except Exception:
            return await interaction.response.send_message(
                "❌ Não consegui aplicar o cargo.\n"
                "Verifique permissão do bot: **Gerenciar Cargos** e cargo do bot acima do cargo Jogador.",
                ephemeral=True
            )

        # abre modal
        try:
            await interaction.response.send_modal(NicknameModal(member.id))
        except Exception:
            try:
                await interaction.followup.send("⚠️ Não consegui abrir o formulário agora. Use `/cadastro`.", ephemeral=True)
            except Exception:
                pass

        self.stop()

    @discord.ui.button(label="Assistir", style=discord.ButtonStyle.secondary)
    async def assistir(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "✅ Você entrou como **Assistir**.\nPara jogar depois, peça para um ADM aplicar o cargo **Jogador**.",
            ephemeral=True
        )
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

    # 1) PRIORIDADE: enviar no canal do servidor (evita erro de DM)
    try:
        if WELCOME_CHANNEL_ID and member.guild:
            ch = member.guild.get_channel(WELCOME_CHANNEL_ID)
            if not ch:
                ch = await member.guild.fetch_channel(WELCOME_CHANNEL_ID)
            if ch:
                await ch.send(f"{member.mention}\n{text}", view=view)
                try:
                    await log_admin_guild(member.guild, f"💬 Onboarding enviado no canal {ch.mention} para {member.mention}.")
                except Exception:
                    pass
                return
    except Exception:
        pass

    # 2) fallback: DM
    try:
        await member.send(text, view=view)
        try:
            await log_admin_guild(member.guild, f"📩 Onboarding enviado por DM para {member.mention}.")
        except Exception:
            pass
        return
    except Exception:
        pass

    # 3) se falhar, loga
    try:
        await log_admin_guild(member.guild, f"⚠️ Onboarding falhou (canal e DM). Usuário: {member.mention}")
    except Exception:
        pass


@client.event
async def on_member_join(member: discord.Member):
    if getattr(member, "bot", False):
        return
    await send_onboarding(member)


# =========================================================
# Comando básico
# =========================================================
@client.tree.command(name="ping", description="Teste do bot")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("⚓ Pong! Bot online.")


# =========================================================
# [BLOCO 5/8 termina aqui]
# =========================================================

# =========================================================
# [BLOCO 6/8] — INSCRIÇÃO + DROP + DECK/DECKLIST (1x POR CICLO) + VERIFICAÇÃO
# (REVISADO v2)
# AJUSTES FEITOS NESTA REVISÃO:
# - Corrige o update do Players: agora escreve nas colunas CERTAS do seu PLAYERS_HEADER (Bloco 2).
#   PLAYERS_HEADER atual: ["discord_id","nick","name","notes","status","rating","created_at","updated_at"]
#   => update: B(nick) / E(status) / H(updated_at) e mantém created_at.
# - /inscrever: não cria ciclo, mas valida existência e status OPEN (mantido).
# - /deck e /decklist: mantém 1x por ciclo, e ADM/Org pode sobrescrever (mantido).
# - Tudo usa aba "Decks" (Decks por season+ciclo+player) — confirmado.
# =========================================================

# =========================================================
# Decks helpers (1x por ciclo)
# - Baseado na aba: "Decks" (DECKS_HEADER / DECKS_REQUIRED do BLOCO 2)
# =========================================================
def ensure_deck_row(ws_decks, season_id: int, cycle: int, player_id: str) -> int:
    """
    Garante que exista uma linha para (season_id, cycle, player_id) na aba Decks.
    Retorna o número da linha (1-based).
    """
    nowb = now_br_str()
    rown = get_deck_row(ws_decks, season_id, cycle, player_id)
    col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

    if rown is None:
        ws_decks.append_row(
            [str(season_id), str(cycle), str(player_id), "", "", nowb, nowb],
            value_input_option="USER_ENTERED",
        )
        return len(ws_decks.get_all_values())
    else:
        # só "toca" no updated_at
        ws_decks.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")
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
# Players upsert helper (alinhado ao PLAYERS_HEADER do BLOCO 2)
# A:discord_id B:nick C:name D:notes E:status F:rating G:created_at H:updated_at
# =========================================================
def upsert_player(ws_players, discord_id: str, nick: str):
    nowb = now_br_str()
    prow = find_player_row(ws_players, int(discord_id))
    if prow is None:
        # preenche o header novo corretamente
        ws_players.append_row(
            [discord_id, nick, "", "", "active", "", nowb, nowb],
            value_input_option="USER_ENTERED"
        )
        return

    # updates alinhados
    ws_players.update([[nick]], range_name=f"B{prow}")       # nick
    ws_players.update([["active"]], range_name=f"E{prow}")   # status
    ws_players.update([[nowb]], range_name=f"H{prow}")       # updated_at


# =========================================================
# /inscrever — EXIGE ciclo existir e estar OPEN
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

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send(
                "❌ Não existe season ativa. Peça ao DONO do servidor para abrir uma season.",
                ephemeral=True
            )

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

        # garante player em Players (header novo)
        upsert_player(ws_players, discord_id, nick)

        # enroll por season+ciclo (ativa/reativa)
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
            await log_admin(interaction, f"📝 Inscrição: {interaction.user.mention} no S{season_id} C{c}.")
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
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        season_id = get_current_season_id(sh)
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
            await log_admin(interaction, f"🚪 Drop: {interaction.user.mention} no S{season_id} C{cycle}. Motivo: {motivo.strip() or '—'}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)


# =========================================================
# /deck — 1 vez POR CICLO (jogador), ADM/Organizador podem alterar
# =========================================================
@client.tree.command(name="deck", description="Define seu deck (1 vez POR CICLO).")
@app_commands.describe(cycle="Ciclo do deck (ex: 2)", nome="Nome do deck (ex: UR Murktide)")
async def deck(interaction: discord.Interaction, cycle: int, nome: str):
    await interaction.response.defer(ephemeral=True)

    pid = str(interaction.user.id).strip()
    nowb = now_br_str()

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        # ciclo precisa existir na season
        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ Ciclo {cycle} não existe na Season {season_id}.", ephemeral=True)

        # precisa estar inscrito ativo (exceto ADM/Org)
        if not is_player_enrolled_active(ws_enr, season_id, cycle, pid) and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                "❌ Para definir deck você precisa estar **inscrito (active)** nesse ciclo.\nUse `/inscrever`.",
                ephemeral=True
            )

        rown = ensure_deck_row(ws_decks, season_id, cycle, pid)
        col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
        fields = get_deck_fields(ws_decks, rown)
        current = (fields.get("deck") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                f"❌ Você já definiu deck para o **Ciclo {cycle}** e não pode alterar.\n"
                "Peça para um ADM/Organizador se precisar.",
                ephemeral=True
            )

        ws_decks.update([[nome]], range_name=f"{col_letter(col['deck'])}{rown}")
        ws_decks.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send(f"✅ Deck salvo para **Ciclo {cycle}**.\nDeck: **{nome}**", ephemeral=True)

        try:
            await log_admin(interaction, f"🧾 deck: {interaction.user.mention} S{season_id} C{cycle} -> {nome}")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /deck: {e}", ephemeral=True)


# =========================================================
# /decklist — 1 vez POR CICLO (jogador), ADM/Organizador podem alterar
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
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ Ciclo {cycle} não existe na Season {season_id}.", ephemeral=True)

        if not is_player_enrolled_active(ws_enr, season_id, cycle, pid) and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                "❌ Para definir decklist você precisa estar **inscrito (active)** nesse ciclo.\nUse `/inscrever`.",
                ephemeral=True
            )

        rown = ensure_deck_row(ws_decks, season_id, cycle, pid)
        col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
        fields = get_deck_fields(ws_decks, rown)
        current = (fields.get("decklist_url") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            return await interaction.followup.send(
                f"❌ Você já definiu decklist para o **Ciclo {cycle}** e não pode alterar.\n"
                "Peça para um ADM/Organizador se precisar.",
                ephemeral=True
            )

        ws_decks.update([[val]], range_name=f"{col_letter(col['decklist_url'])}{rown}")
        ws_decks.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send(f"✅ Decklist salva para **Ciclo {cycle}**.\nLink: {val}", ephemeral=True)

        try:
            await log_admin(interaction, f"🔗 decklist: {interaction.user.mention} S{season_id} C{cycle} -> {val}")
        except Exception:
            pass

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
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        rown = get_deck_row(ws_decks, season_id, cycle, pid)
        if rown is None:
            return await interaction.followup.send(
                f"Sem deck registrado para **Ciclo {cycle}** (Season {season_id}).",
                ephemeral=True
            )

        fields = get_deck_fields(ws_decks, rown)
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
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        rown = get_deck_row(ws_decks, season_id, cycle, pid)
        if rown is None:
            return await interaction.followup.send(
                f"Sem decklist registrada para **Ciclo {cycle}** (Season {season_id}).",
                ephemeral=True
            )

        fields = get_deck_fields(ws_decks, rown)
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
# [BLOCO 6/8 termina aqui]
# =========================================================
# =========================================================
# [BLOCO 7/8] — PODS + MATCHES + RESULTADOS (REVISADO v2)
# AJUSTES FEITOS NESTA REVISÃO:
# - IMPLEMENTA /pods_gerar e /pods_publicar (estavam faltando no seu bloco atual, mas existem no catálogo).
# - /resultado agora:
#   - valida V-D-E (usa parse_score_3parts + validate_3parts_rules do BLOCO 1)
#   - só permite reportar se o usuário estiver no match
#   - grava auto_confirm_at (agora+48h)
#   - escreve o placar no sentido CORRETO (se reportou como player_b, inverte a/b)
#   - não deixa sobrescrever pending já reportado (exceto ADM/Org via /resultado_admin no BLOCO 8)
# - /rejeitar agora:
#   - só o oponente pode rejeitar
#   - só dentro da janela de 48h (auto_confirm_at)
#   - limpa campos e volta match para "em aberto" (confirmed_status vazio)
# - /meus_matches mostra prazo/expiração quando estiver pending
# =========================================================


# =========================================================
# /pods_gerar (ADM) — gera PodsHistory + Matches e trava ciclo (locked)
# =========================================================
@client.tree.command(name="pods_gerar", description="(ADM) Gera pods + matches e trava o ciclo (locked).")
@app_commands.describe(cycle="Ciclo (ex: 1)", pod_size="Tamanho alvo do pod (3..6)")
@app_commands.choices(
    pod_size=[
        app_commands.Choice(name="3", value=3),
        app_commands.Choice(name="4 (padrão)", value=4),
        app_commands.Choice(name="5", value=5),
        app_commands.Choice(name="6", value=6),
    ]
)
async def pods_gerar(interaction: discord.Interaction, cycle: int, pod_size: app_commands.Choice[int] = None):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        colm = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # ciclo precisa existir e estar OPEN
        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        st = (cf.get("status") or "").strip().lower()
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ Ciclo {cycle} não existe na Season {season_id}.", ephemeral=True)
        if st == "completed":
            return await interaction.followup.send("❌ Ciclo COMPLETED. Não posso gerar pods.", ephemeral=True)
        if st == "locked":
            return await interaction.followup.send("⚠️ Ciclo já está LOCKED. Pods já deveriam existir.", ephemeral=True)
        if st != "open":
            return await interaction.followup.send(f"❌ Ciclo não está OPEN (status: {st}).", ephemeral=True)

        # não gerar se já existe PodsHistory nesse ciclo (blindagem contra duplicação)
        pods_rows = ws_pods.get_all_records()
        if any(safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle for r in pods_rows):
            return await interaction.followup.send(
                "❌ PodsHistory já existe para esse ciclo. Para corrigir, use comandos administrativos (cancelar match, etc).",
                ephemeral=True
            )

        # lista inscritos ativos
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

        players = sorted(set(players))
        if len(players) < 3:
            return await interaction.followup.send("❌ Poucos inscritos (mínimo 3) para gerar pods.", ephemeral=True)

        ps = int(pod_size.value) if pod_size else 4
        ps = max(3, min(ps, 6))

        # anti-repetição: usa matches confirmados do histórico inteiro
        past_pairs = get_past_confirmed_pairs(ws_matches)
        pods, score = best_shuffle_min_repeats(players, ps, past_pairs, tries=350)

        # grava PodsHistory
        nowb = now_br_str()
        pod_letter = "A"
        for pod in pods:
            for pid in pod:
                ws_pods.append_row([str(season_id), str(cycle), pod_letter, str(pid), nowb], value_input_option="USER_ENTERED")
            pod_letter = chr(ord(pod_letter) + 1)

        # cria matches (round-robin) para cada pod
        created_matches = 0
        pod_letter = "A"
        for pod in pods:
            pairs = round_robin_pairs(pod)
            for a, b in pairs:
                mid = new_match_id(season_id, cycle, pod_letter)
                # Match row no MESMO formato de MATCHES_REQUIRED_COLS
                row = [
                    mid,                               # match_id
                    str(season_id),                     # season_id
                    str(cycle),                         # cycle
                    str(pod_letter),                    # pod
                    str(a),                             # player_a_id
                    str(b),                             # player_b_id
                    "",                                 # a_games_won
                    "",                                 # b_games_won
                    "",                                 # draw_games
                    "normal",                           # result_type
                    "",                                 # confirmed_status (vazio = "não reportado ainda")
                    "",                                 # reported_by_id
                    "",                                 # confirmed_by_id
                    "",                                 # message_id
                    "TRUE",                             # active
                    nowb,                               # created_at
                    nowb,                               # updated_at
                    "",                                 # auto_confirm_at
                ]
                ws_matches.append_row(row, value_input_option="USER_ENTERED")
                created_matches += 1
            pod_letter = chr(ord(pod_letter) + 1)

        # trava ciclo e grava start/deadline
        set_cycle_status(ws_cycles, season_id, cycle, "locked")
        # calcula e grava prazo baseado no maior POD
        start_br, end_br, max_pod, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)
        if start_br and end_br:
            set_cycle_times(ws_cycles, season_id, cycle, start_br, end_br)

        await interaction.followup.send(
            f"✅ Pods gerados e ciclo travado (LOCKED).\n"
            f"Season {season_id} | Ciclo {cycle}\n"
            f"Inscritos: **{len(players)}** | Pod alvo: **{ps}** | Penalidade repetição: **{score}**\n"
            f"Matches criados: **{created_matches}**",
            ephemeral=True
        )
        await log_admin(interaction, f"pods_gerar: S{season_id} C{cycle} | players {len(players)} | pod_size {ps} | repeats {score} | matches {created_matches}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_gerar: {e}", ephemeral=True)


# =========================================================
# /pods_ver (público)
# =========================================================
@client.tree.command(name="pods_ver", description="Mostra os pods do ciclo.")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
async def pods_ver(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)

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
                pods.setdefault(pod, []).append(pid)

        if not pods:
            return await interaction.followup.send("❌ Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        msg = [f"📦 Pods — Season {season_id} | Ciclo {cycle}\n"]
        for pod in sorted(pods.keys()):
            names = sorted([nick_map.get(p, p) for p in pods[pod]], key=lambda x: x.lower())
            msg.append(f"**Pod {pod}**")
            for n in names:
                msg.append(f"• {n}")
            msg.append("")

        await interaction.followup.send("\n".join(msg), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_ver: {e}", ephemeral=True)


# =========================================================
# /pods_publicar (ADM) — publica pods em um canal (ou no atual)
# =========================================================
@client.tree.command(name="pods_publicar", description="(ADM) Publica os pods do ciclo em um canal.")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def pods_publicar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)

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
                pods.setdefault(pod, []).append(pid)

        if not pods:
            return await interaction.followup.send("❌ Nenhum pod encontrado para esse ciclo.", ephemeral=True)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        lines = [f"📦 **PODS — Season {season_id} | Ciclo {cycle}**\n"]
        for pod in sorted(pods.keys()):
            names = sorted([nick_map.get(p, p) for p in pods[pod]], key=lambda x: x.lower())
            lines.append(f"**Pod {pod}**")
            for n in names:
                lines.append(f"• {n}")
            lines.append("")

        await interaction.channel.send("\n".join(lines))
        await interaction.followup.send("✅ Pods publicados no canal atual.", ephemeral=True)
        await log_admin(interaction, f"pods_publicar: S{season_id} C{cycle} no canal {interaction.channel.id}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_publicar: {e}", ephemeral=True)


# =========================================================
# /meus_matches (público)
# =========================================================
@client.tree.command(name="meus_matches", description="Lista seus matches no ciclo.")
@app_commands.describe(cycle="Número do ciclo")
async def meus_matches(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        pid = str(interaction.user.id)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)

        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        found = []

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
            if pid not in (a, b):
                continue

            mid = str(r.get("match_id", "")).strip()
            status = str(r.get("confirmed_status", "")).strip().lower() or "open"
            opp = b if pid == a else a

            extra = ""
            if status == "pending":
                ac = parse_iso_dt(r.get("auto_confirm_at", "") or "")
                if ac:
                    hrs = (ac - nowu).total_seconds() / 3600.0
                    if hrs < 0:
                        extra = " | expiração: **já passou**"
                    else:
                        extra = f" | expira em ~{int(hrs)}h (UTC)"

            found.append(f"• `{mid}` vs {nick_map.get(opp, opp)} | status: {status}{extra}")

        if not found:
            return await interaction.followup.send("Você não possui matches nesse ciclo.", ephemeral=True)

        msg = f"🎮 Seus matches — Season {season_id} | Ciclo {cycle}\n\n" + "\n".join(found[:60])
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /resultado — reporta V-D-E (do ponto de vista de QUEM REPORTA)
# - grava pending + auto_confirm_at agora+48h
# - salva placar invertendo se reportou como player_b
# =========================================================
@client.tree.command(name="resultado", description="Reporta resultado do match (V-D-E em games).")
@app_commands.autocomplete(match_id=ac_match_id_user_pending)
@app_commands.describe(match_id="Match ID", placar="Formato V-D-E (ex: 2-1-0)")
async def resultado(interaction: discord.Interaction, match_id: str, placar: str):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        if not cell:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        rown = cell.row
        if rown <= 1:
            return await interaction.followup.send("❌ Match inválido.", ephemeral=True)

        r = ws.row_values(rown)

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        if safe_int(getc("season_id"), 0) != season_id:
            return await interaction.followup.send("❌ Match não pertence à season ativa.", ephemeral=True)

        if not as_bool(getc("active") or "TRUE"):
            return await interaction.followup.send("❌ Match inativo/cancelado.", ephemeral=True)

        current_status = (getc("confirmed_status") or "").strip().lower()
        if current_status == "confirmed":
            return await interaction.followup.send("❌ Match já está CONFIRMED.", ephemeral=True)

        # não sobrescreve pending já reportado
        if current_status == "pending" and str(getc("reported_by_id") or "").strip():
            return await interaction.followup.send(
                "❌ Já existe um resultado PENDENTE aguardando confirmação.\n"
                "Se você é o oponente e discorda, use `/rejeitar`.",
                ephemeral=True
            )

        a = str(getc("player_a_id") or "").strip()
        b = str(getc("player_b_id") or "").strip()
        uid = str(interaction.user.id)

        if uid not in (a, b):
            return await interaction.followup.send("❌ Você não faz parte deste match.", ephemeral=True)

        sc = parse_score_3parts(placar)
        if not sc:
            return await interaction.followup.send("❌ Formato inválido. Use V-D-E (ex: 2-1-0).", ephemeral=True)

        v, d, e = sc
        ok, msg = validate_3parts_rules(v, d, e)
        if not ok:
            return await interaction.followup.send(f"❌ {msg}", ephemeral=True)

        # converte para o formato do match (player_a)
        if uid == a:
            a_gw, b_gw, d_g = v, d, e
        else:
            # reportou como player_b => inverte para salvar do ponto de vista do player_a
            a_gw, b_gw, d_g = d, v, e

        rt = "normal"
        if a_gw == b_gw:
            rt = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            rt = "intentional_draw"

        ws.update([[str(a_gw)]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(col['draw_games'])}{rown}")
        ws.update([[rt]], range_name=f"{col_letter(col['result_type'])}{rown}")

        ws.update([["pending"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
        ws.update([[uid]], range_name=f"{col_letter(col['reported_by_id'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[auto_confirm_deadline_iso(utc_now_dt())]], range_name=f"{col_letter(col['auto_confirm_at'])}{rown}")
        ws.update([[now_br_str()]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send(
            "✅ Resultado reportado e ficou **PENDENTE**.\n"
            "O oponente tem **48h** para rejeitar. Se não rejeitar, o `/recalcular` auto-confirma.",
            ephemeral=True
        )

        try:
            await log_admin(interaction, f"resultado: `{match_id}` by {interaction.user.mention} -> {v}-{d}-{e} (pending)")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /resultado: {e}", ephemeral=True)


# =========================================================
# /rejeitar — só o OPONENTE pode rejeitar dentro de 48h
# =========================================================
@client.tree.command(name="rejeitar", description="Rejeita resultado pendente (somente o oponente, dentro de 48h).")
@app_commands.autocomplete(match_id=ac_match_id_user_pending)
async def rejeitar(interaction: discord.Interaction, match_id: str):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return await interaction.followup.send("❌ Não existe season ativa.", ephemeral=True)

        ws = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        if not cell:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        rown = cell.row
        if rown <= 1:
            return await interaction.followup.send("❌ Match inválido.", ephemeral=True)

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
            return await interaction.followup.send("❌ Este match não está PENDENTE.", ephemeral=True)

        reported_by = str(getc("reported_by_id") or "").strip()
        if not reported_by:
            return await interaction.followup.send("❌ Não existe report registrado para rejeitar.", ephemeral=True)

        uid = str(interaction.user.id)
        if uid == reported_by:
            return await interaction.followup.send("❌ Quem reportou não pode rejeitar o próprio report.", ephemeral=True)

        a = str(getc("player_a_id") or "").strip()
        b = str(getc("player_b_id") or "").strip()
        if uid not in (a, b):
            return await interaction.followup.send("❌ Você não faz parte deste match.", ephemeral=True)

        ac = parse_iso_dt(getc("auto_confirm_at") or "")
        if ac and utc_now_dt() > ac:
            return await interaction.followup.send("❌ Prazo de rejeição expirou (48h). Peça para um ADM ajustar.", ephemeral=True)

        # limpa e volta para "open"
        ws.update([[""]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['draw_games'])}{rown}")
        ws.update([["normal"]], range_name=f"{col_letter(col['result_type'])}{rown}")

        ws.update([[""]], range_name=f"{col_letter(col['reported_by_id'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['auto_confirm_at'])}{rown}")
        ws.update([[now_br_str()]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send("✅ Resultado rejeitado. O match voltou para **em aberto** e pode ser reportado novamente.", ephemeral=True)

        try:
            await log_admin(interaction, f"rejeitar: `{match_id}` por {interaction.user.mention} (rejeitou report de {reported_by})")
        except Exception:
            pass

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================================================
# [BLOCO 7/8 termina aqui]
# =========================================================

# =========================================================
# [BLOCO 8/8] — ADMIN FINAL + PRAZO + RANKINGS + EXPORT + START
# (REVISADO v3 — CONSOLIDADO E ALINHADO AOS BLOCOS 2,6,7)
#
# AJUSTES NESTA VERSÃO:
# - /prazo usa compute_cycle_start_deadline_br + set_cycle_times (consistente)
# - /deadline respeita season ativa + confirmed_status=pending
# - /final só aplica 0-0-3 após deadline BR
# - /recalcular chama sweep_auto_confirm + recalculate_cycle
# - /ranking_geral usa apenas MATCHES CONFIRMED e active=TRUE
# - Ordenação oficial: pts > OMW% > GW% > OGW%
# - Coluna J = matches_played
# - START protegido
# =========================================================


# =========================================================
# Helpers obrigatórios
# =========================================================
def require_current_season(sh) -> int:
    sid = get_current_season_id(sh)
    if sid <= 0:
        raise RuntimeError("Não existe season ativa.")
    return sid


# =========================================================
# /prazo (público)
# =========================================================
@client.tree.command(name="prazo", description="Mostra o prazo oficial do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
async def prazo(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        start_br, end_br, max_pod_size, days = compute_cycle_start_deadline_br(
            season_id, cycle, ws_pods, ws_cycles
        )

        if not start_br or not end_br:
            return await interaction.followup.send(
                "⚠️ Ciclo ainda não possui prazo definido (pods não gerados).",
                ephemeral=False
            )

        await interaction.followup.send(
            f"⏳ **Season {season_id} | Ciclo {cycle}**\n"
            f"Início: **{start_br} (BR)**\n"
            f"Fim: **{end_br} (BR)**\n"
            f"Regra aplicada (maior POD={max_pod_size}): **{days} dias**",
            ephemeral=False
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /prazo: {e}", ephemeral=False)


# =========================================================
# /deadline (ADM)
# =========================================================
@client.tree.command(name="deadline", description="(ADM) Lista matches pending próximos de expirar.")
@app_commands.describe(cycle="Número do ciclo", horas="Janela em horas (1..48)")
async def deadline(interaction: discord.Interaction, cycle: int, horas: int = 12):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
        ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        nowu = utc_now_dt()
        limit = nowu + timedelta(hours=max(1, min(horas, 48)))

        rows = ws.get_all_records()
        items = []

        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "pending":
                continue
            if not as_bool(r.get("active", "TRUE")):
                continue

            ac = parse_iso_dt(r.get("auto_confirm_at", "") or "")
            if ac and ac <= limit:
                items.append(f"`{r.get('match_id')}` expira {ac.isoformat()} UTC")

        if not items:
            return await interaction.followup.send("✅ Nenhuma pendência na janela.", ephemeral=True)

        await interaction.followup.send(
            "⏰ Pendências próximas de expirar:\n" + "\n".join(items[:40]),
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /final (ADM)
# =========================================================
@client.tree.command(name="final", description="(ADM) Aplica 0-0-3 após deadline do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
async def final(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)

        start_br, end_br, _, _ = compute_cycle_start_deadline_br(
            season_id, cycle, ws_pods, ws_cycles
        )

        if not end_br:
            return await interaction.followup.send("❌ Ciclo sem prazo definido.", ephemeral=True)

        deadline_dt = parse_br_dt(end_br)
        if deadline_dt and now_br_dt() < deadline_dt:
            return await interaction.followup.send("❌ Deadline ainda não chegou.", ephemeral=True)

        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = ws_matches.get_all_values()

        changed = 0
        for rown in range(2, len(rows) + 1):
            r = rows[rown - 1]

            def getc(name):
                idx = col[name]
                return r[idx] if idx < len(r) else ""

            if safe_int(getc("season_id"), 0) != season_id:
                continue
            if safe_int(getc("cycle"), 0) != cycle:
                continue
            if str(getc("reported_by_id")).strip():
                continue
            if not as_bool(getc("active") or "TRUE"):
                continue

            ws_matches.update([["0"]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
            ws_matches.update([["0"]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
            ws_matches.update([["3"]], range_name=f"{col_letter(col['draw_games'])}{rown}")
            ws_matches.update([["intentional_draw"]], range_name=f"{col_letter(col['result_type'])}{rown}")
            ws_matches.update([["confirmed"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
            ws_matches.update([[now_br_str()]], range_name=f"{col_letter(col['updated_at'])}{rown}")
            changed += 1

        await interaction.followup.send(f"✅ FINAL aplicado. {changed} matches ajustados.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /recalcular (ADM)
# =========================================================
@client.tree.command(name="recalcular", description="(ADM) Auto-confirm + recalcula standings.")
@app_commands.describe(cycle="Número do ciclo")
async def recalcular(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        changed = sweep_auto_confirm(sh, season_id, cycle)
        rows = recalculate_cycle(season_id, cycle)

        await interaction.followup.send(
            f"✅ Recalculo concluído.\nAuto-confirm: {changed}\nStandings: {len(rows)}",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /ranking_geral
# =========================================================
@client.tree.command(name="ranking_geral", description="Mostra ranking geral da season.")
@app_commands.describe(top="Quantos mostrar")
async def ranking_geral(interaction: discord.Interaction, top: int = 30):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        rows = ws_matches.get_all_records()

        stats = {}
        opponents = {}

        def ensure(pid):
            if pid not in stats:
                stats[pid] = {
                    "pts": 0,
                    "matches": 0,
                    "gw": 0,
                    "gl": 0,
                    "gd": 0,
                    "gp": 0,
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
            stats[a]["matches"] += 1
            stats[b]["matches"] += 1

            stats[a]["gw"] += a_gw
            stats[a]["gl"] += b_gw
            stats[a]["gd"] += d_g
            stats[a]["gp"] += a_gw + b_gw + d_g

            stats[b]["gw"] += b_gw
            stats[b]["gl"] += a_gw
            stats[b]["gd"] += d_g
            stats[b]["gp"] += a_gw + b_gw + d_g

            if a_gw > b_gw:
                stats[a]["pts"] += 3
            elif b_gw > a_gw:
                stats[b]["pts"] += 3
            else:
                stats[a]["pts"] += 1
                stats[b]["pts"] += 1

            opponents[a].append(b)
            opponents[b].append(a)

        if not stats:
            return await interaction.followup.send("Sem matches confirmados.", ephemeral=False)

        mwp = {}
        gwp = {}
        for pid, s in stats.items():
            mp = s["pts"]
            mplayed = s["matches"]
            mwp[pid] = 1/3 if mplayed == 0 else floor_333(mp / (3 * mplayed))

            if s["gp"] == 0:
                gwp[pid] = 1/3
            else:
                gwp[pid] = floor_333((s["gw"] + 0.5 * s["gd"]) / s["gp"])

        omw = {}
        ogw = {}
        for pid in stats.keys():
            opps = opponents.get(pid, [])
            if not opps:
                omw[pid] = 1/3
                ogw[pid] = 1/3
            else:
                omw[pid] = sum(mwp[o] for o in opps) / len(opps)
                ogw[pid] = sum(gwp[o] for o in opps) / len(opps)

        table = []
        for pid, s in stats.items():
            table.append({
                "pid": pid,
                "pts": s["pts"],
                "omw": pct1(omw[pid]),
                "gw": pct1(gwp[pid]),
                "ogw": pct1(ogw[pid]),
                "j": s["matches"],
            })

        table.sort(key=lambda r: (r["pts"], r["omw"], r["gw"], r["ogw"]), reverse=True)

        top = max(10, min(top, 60))
        out = [f"🏆 Ranking Geral — Season {season_id} (Top {top})"]
        out.append("pos | jogador | pts | OMW | GW | OGW | J")
        out.append("--- | ------ | --- | --- | --- | --- | ---")

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        nick_map = build_players_nick_map(ws_players)

        for i, r in enumerate(table[:top], 1):
            out.append(
                f"{i} | {nick_map.get(r['pid'], r['pid'])} | {r['pts']} | {r['omw']} | {r['gw']} | {r['ogw']} | {r['j']}"
            )

        await interaction.followup.send("\n".join(out), ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=False)


# =========================================================
# START
# =========================================================
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN não configurado.")

keep_alive()
client.run(DISCORD_TOKEN)

# =========================================================
# [BLOCO 8/8 termina aqui]
# =========================================================
