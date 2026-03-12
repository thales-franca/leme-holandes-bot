# =================================================
# BLOCO ORIGINAL: BLOCO 1/8
# SUB-BLOCO: A
# REVISÃO FINAL — cache + RAM index + locks globais
# =================================================

import os
import json
import time
import threading
import random
import csv
import io

from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta, time as dtime, date

import discord
from discord import app_commands
from flask import Flask

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# LEME HOLANDÊS BOT
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
    t = threading.Thread(
        target=_run_web,
        daemon=True
    )
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
ROLE_JOGADOR = os.getenv("ROLE_JOGADOR", "Jogador")

RANKING_CHANNEL_ID = int(os.getenv("RANKING_CHANNEL_ID", "0"))
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

        raw = str(s).strip()

        if not raw:
            return None

        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"

        dt = datetime.fromisoformat(raw)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None


def parse_br_dt(s: str):

    try:
        return datetime.strptime(
            str(s).strip(),
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=BR_TZ)

    except Exception:
        return None


def fmt_br_dt(dt: datetime) -> str:
    return dt.astimezone(BR_TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


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

    n = ci_0
    s = ""

    while True:

        s = chr(n % 26 + 65) + s

        n = n // 26 - 1

        if n < 0:
            break

    return s


# =========================================================
# CACHE (Sheets)
# =========================================================

_SHEETS_CACHE: dict[str, dict] = {}
_SHEETS_CACHE_LOCK = threading.Lock()


def _cache_now():
    return time.monotonic()


def _ws_cache_prefix(ws):

    try:
        sid = ws.spreadsheet.id
    except Exception:
        sid = SHEET_ID or "unknown_sheet"

    try:
        wid = str(ws.id)
    except Exception:
        wid = ""

    try:
        title = str(ws.title)
    except Exception:
        title = "unknown_ws"

    if wid:
        return f"{sid}:{wid}:{title}:"

    return f"{sid}:{title}:"


def _cache_key(ws, kind: str):
    return _ws_cache_prefix(ws) + kind


def cache_get(ws, kind, ttl_seconds):

    key = _cache_key(ws, kind)

    now = _cache_now()

    with _SHEETS_CACHE_LOCK:

        item = _SHEETS_CACHE.get(key)

        if not item:
            return None

        ts = item.get("ts")

        if ts is None:
            _SHEETS_CACHE.pop(key, None)
            return None

        if now - ts <= ttl_seconds:
            return item.get("data")

        _SHEETS_CACHE.pop(key, None)

    return None


def cache_set(ws, kind, data):

    key = _cache_key(ws, kind)

    now = _cache_now()

    prefix = _ws_cache_prefix(ws)

    with _SHEETS_CACHE_LOCK:

        _SHEETS_CACHE[key] = {
            "ts": now,
            "data": data
        }

        expired = []

        for k, item in _SHEETS_CACHE.items():

            if not k.startswith(prefix):
                continue

            ts = item.get("ts")

            if ts is None:
                expired.append(k)
                continue

            if now - ts > 120:
                expired.append(k)

        for k in expired:
            _SHEETS_CACHE.pop(k, None)


def cache_invalidate(ws, kind=None):

    prefix = _ws_cache_prefix(ws)

    with _SHEETS_CACHE_LOCK:

        if kind is None:

            keys = [
                k for k in _SHEETS_CACHE
                if k.startswith(prefix)
            ]

            for k in keys:
                _SHEETS_CACHE.pop(k, None)

        else:

            _SHEETS_CACHE.pop(
                prefix + kind,
                None
            )


# =========================================================
# RAM INDEX (NOVO)
# =========================================================

_MATCH_RAM_INDEX = None
_MATCH_RAM_LOCK = threading.Lock()

_PLAYER_RAM_INDEX = None
_PLAYER_RAM_LOCK = threading.Lock()

_CYCLE_RAM_INDEX = None
_CYCLE_RAM_LOCK = threading.Lock()


def invalidate_match_ram_index():

    global _MATCH_RAM_INDEX

    with _MATCH_RAM_LOCK:
        _MATCH_RAM_INDEX = None


def invalidate_player_ram_index():

    global _PLAYER_RAM_INDEX

    with _PLAYER_RAM_LOCK:
        _PLAYER_RAM_INDEX = None


def invalidate_cycle_ram_index():

    global _CYCLE_RAM_INDEX

    with _CYCLE_RAM_LOCK:
        _CYCLE_RAM_INDEX = None


# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 1/8
# SUB-BLOCO: B
# RESUMO: Final do sistema de cache do Google Sheets, helpers de integração com Google Sheets,
# criação e validação de abas, sistema de log administrativo no Discord, helpers de autorização
# (owner, ADM, organizador, jogador), validação de URLs de decklist (Moxfield e LigaMagic) e
# utilitários de parsing e validação de placar V-D-E.
# REVISÃO: reforço dos helpers de cache, redução de leitura direta do cabeçalho
# e manutenção da mesma lógica funcional do projeto.
# =================================================

def cached_get_all_values(ws, ttl_seconds: int = 10):
    cached = cache_get(ws, "all_values", ttl_seconds)
    if cached is not None:
        return cached

    data = ws.get_all_values()
    cache_set(ws, "all_values", data)
    return data

def cached_get_all_records(ws, ttl_seconds: int = 10):
    cached = cache_get(ws, "all_records", ttl_seconds)
    if cached is not None:
        return cached

    data = ws.get_all_records()
    cache_set(ws, "all_records", data)
    return data


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
    vals = cached_get_all_values(ws, ttl_seconds=10)
    header = vals[0] if vals else []
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

    current = cached_get_all_values(ws, ttl_seconds=10)
    if not current:
        ws.append_row(header)
        cache_invalidate(ws)  # header criado agora, limpa cache dessa aba
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
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 2/8
# SUB-BLOCO: A
# RESUMO: Definição do schema das abas do Google Sheets, cabeçalhos e colunas obrigatórias,
# garantia de criação das abas, helpers de season, leitura e atualização da season atual,
# status de temporada e fechamento das demais seasons.
# REVISÃO: adoção de cache nos helpers centrais de season e redução de escritas
# isoladas com batch_update, mantendo a mesma lógica funcional.
# =================================================

# [BLOCO 2/8] — SCHEMA (ABAS) + SEASON STATE + FUNÇÕES DE SEASON/CICLO (REVISADO v2)
# AJUSTES FEITOS NESTA REVISÃO:
# - Reintroduz ABA "Decks" (DECKS_HEADER/DECKS_REQUIRED) pois BLOCO 6 usa.
# - Adiciona helpers: get_deck_row / get_deck_fields (usados no BLOCO 6).
# - (ADICIONADO AGORA) ensure_deck_row (BLOCO 6 chama esse helper).
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
    rows = cached_get_all_records(ws_seasons, ttl_seconds=10)
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
        rows = cached_get_all_records(ws_state, ttl_seconds=10)
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
    vals = cached_get_all_values(ws, ttl_seconds=10)
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
        ws.batch_update([
            {
                "range": f"B{found}",
                "values": [[str(season_id)]]
            },
            {
                "range": f"C{found}",
                "values": [[nowb]]
            },
        ])

    cache_invalidate(ws)

def season_exists(sh, season_id: int) -> bool:
    ws = sh.worksheet("Seasons")
    rows = cached_get_all_records(ws, ttl_seconds=10)
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) == season_id:
            return True
    return False

def get_season_status(sh, season_id: int) -> str:
    ws = sh.worksheet("Seasons")
    rows = cached_get_all_records(ws, ttl_seconds=10)
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) == season_id:
            return str(r.get("status", "")).strip().lower()
    return ""

def set_season_status(sh, season_id: int, status: str, name: str | None = None):
    status = str(status).strip().lower()
    ws = sh.worksheet("Seasons")
    data = cached_get_all_values(ws, ttl_seconds=10)
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
        updates = [
            {
                "range": f"B{found}",
                "values": [[status]]
            },
            {
                "range": f"E{found}",
                "values": [[nowb]]
            },
        ]
        if name is not None:
            updates.append({
                "range": f"C{found}",
                "values": [[name]]
            })
        ws.batch_update(updates)

    cache_invalidate(ws)

def close_all_other_seasons(sh, keep_open_id: int):
    ws = sh.worksheet("Seasons")
    data = cached_get_all_values(ws, ttl_seconds=10)
    if len(data) <= 1:
        return 0

    nowb = now_br_str()
    changed = 0
    updates = []

    for i in range(2, len(data) + 1):
        row = data[i - 1]
        sid = safe_int(row[0] if len(row) > 0 else 0, 0)
        if sid <= 0 or sid == keep_open_id:
            continue

        st = (row[1] if len(row) > 1 else "").strip().lower()
        if st != "closed":
            updates.append({
                "range": f"B{i}",
                "values": [["closed"]]
            })
            updates.append({
                "range": f"E{i}",
                "values": [[nowb]]
            })
            changed += 1

    if updates:
        ws.batch_update(updates)
        cache_invalidate(ws)

    return changed

# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 2/8
# SUB-BLOCO: B
# RESUMO: Helpers de ciclos (busca, status, tempos, listagem e sugestões de ciclos),
# helpers de jogadores (localização e mapa de nicknames) e helpers de decks por ciclo
# incluindo localização de linha, criação garantida de registro e leitura dos campos.
# REVISÃO: substituição de leituras diretas por cache, remoção de ws.find()
# e melhor reaproveitamento de dados em memória.
# =================================================

# =========================
# Cycle helpers (por season)
# =========================
def get_cycle_row(ws_cycles, season_id: int, cycle: int) -> int | None:
    rows = cached_get_all_values(ws_cycles, ttl_seconds=10)
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
    rows = cached_get_all_values(ws_cycles, ttl_seconds=10)
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
        ws_cycles.batch_update([
            {
                "range": f"{col_letter(col['status'])}{rown}",
                "values": [[status]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{rown}",
                "values": [[nowb]]
            },
        ])
    cache_invalidate(ws_cycles)

def set_cycle_times(ws_cycles, season_id: int, cycle: int, start_at_br: str, deadline_at_br: str):
    nowb = now_br_str()
    rown = get_cycle_row(ws_cycles, season_id, cycle)
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    if rown is None:
        ws_cycles.append_row([str(season_id), str(cycle), "open", start_at_br, deadline_at_br, nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.batch_update([
            {
                "range": f"{col_letter(col['start_at_br'])}{rown}",
                "values": [[start_at_br]]
            },
            {
                "range": f"{col_letter(col['deadline_at_br'])}{rown}",
                "values": [[deadline_at_br]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{rown}",
                "values": [[nowb]]
            },
        ])
    cache_invalidate(ws_cycles)

def list_cycles(ws_cycles, season_id: int) -> list[tuple[int, str]]:
    out = []
    for r in cached_get_all_records(ws_cycles, ttl_seconds=10):
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
    rows = cached_get_all_values(ws_players, ttl_seconds=10)
    if len(rows) <= 1:
        return None

    col = ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
    did_col = col.get("discord_id", 0)
    target = str(discord_id).strip()

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]
        v = r[did_col] if did_col < len(r) else ""
        if str(v).strip() == target:
            return i
    return None

def get_player_nick_map(ws_players) -> dict[str, str]:
    data = cached_get_all_records(ws_players, ttl_seconds=10)
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
    data = cached_get_all_values(ws_decks, ttl_seconds=10)
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

# (ADICIONADO) BLOCO 6 usa ensure_deck_row(...)
def ensure_deck_row(ws_decks, season_id: int, cycle: int, player_id: str) -> int:
    """
    Garante que exista uma linha em Decks para (season,cycle,player).
    Retorna a linha 1-based.
    """
    rown = get_deck_row(ws_decks, season_id, cycle, player_id)
    if rown is not None:
        return rown

    nowb = now_br_str()
    ws_decks.append_row(
        [str(season_id), str(cycle), str(player_id).strip(), "", "", nowb, nowb],
        value_input_option="USER_ENTERED"
    )
    cache_invalidate(ws_decks)

    # linha criada é a última
    try:
        return len(cached_get_all_values(ws_decks, ttl_seconds=5))
    except Exception:
        # fallback seguro
        return (get_deck_row(ws_decks, season_id, cycle, player_id) or 2)

def get_deck_fields(ws_decks, row: int) -> dict:
    """
    Lê e retorna os campos do Decks naquela linha (1-based).
    """
    vals_all = cached_get_all_values(ws_decks, ttl_seconds=10)
    vals = vals_all[row - 1][:] if 0 < row <= len(vals_all) else []
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
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 3/8
# SUB-BLOCO: A
# RESUMO: Cabeçalho do bloco, helpers de matches, geração de IDs, prazo de auto-confirmação,
# round robin, varredura de auto-confirmação, heurística anti-repetição entre jogadores e
# cálculo de prazo do ciclo com base no maior POD.
# REVISÃO: substituição de leituras diretas por cache, redução de updates isolados
# em auto-confirmação, integração com invalidação dos índices RAM de matches/autocomplete
# e padronização de timestamp UTC.
# =================================================

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
    rows = cached_get_all_values(ws_matches, ttl_seconds=10)
    if len(rows) <= 1:
        return 0

    nowu = utc_now_dt()
    changed = 0
    updates = []
    updated_at = now_iso_utc()

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
            updates.extend([
                {
                    "range": f"{col_letter(col['confirmed_status'])}{rown}",
                    "values": [["confirmed"]]
                },
                {
                    "range": f"{col_letter(col['confirmed_by_id'])}{rown}",
                    "values": [["AUTO"]]
                },
                {
                    "range": f"{col_letter(col['updated_at'])}{rown}",
                    "values": [[updated_at]]
                },
            ])
            changed += 1

    if updates:
        ws_matches.batch_update(updates)
        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

    return changed


# =========================
# Anti-repetição (heurística)
# =========================
def get_past_confirmed_pairs(ws_matches) -> set[frozenset]:
    """
    Cria um conjunto de pares (A,B) já enfrentados em matches CONFIRMED (qualquer season/cycle),
    exceto BYE e matches inativos.
    """
    rows = cached_get_all_records(ws_matches, ttl_seconds=10)
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

    rows = cached_get_all_records(ws_pods, ttl_seconds=10)
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
        start_dt = datetime.combine(base_date, dtime(14, 0), tzinfo=BR_TZ)

    deadline_date = (start_dt.date() + timedelta(days=days))
    deadline_dt = datetime.combine(deadline_date, dtime(13, 59), tzinfo=BR_TZ)

    return (fmt_br_dt(start_dt), fmt_br_dt(deadline_dt), max_pod_size, days)

# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 3/8
# SUB-BLOCO: B/2
# RESUMO: Funções de autocomplete que devem ficar acima dos decorators dos comandos,
# incluindo autocomplete de ciclos, ciclos abertos, match_id relevante ao usuário
# e sugestões de placar V-D-E.
# REVISÃO: integração com o índice RAM de MATCHES e com os índices RAM de CYCLES,
# mantendo autocomplete instantâneo de /resultado e /rejeitar, exibindo OPONENTE
# + status e mantendo match_id como valor interno, com debounce leve para reduzir
# rajadas de chamadas do Discord sem alterar a UX.
# =================================================

# =========================
# DEBOUNCE LEVE — AUTOCOMPLETE
# =========================
_AC_DEBOUNCE_STATE: dict[str, float] = {}
_AC_DEBOUNCE_LOCK = threading.Lock()
_AC_DEBOUNCE_WINDOW_SECONDS = 0.08

def _ac_should_skip(interaction: discord.Interaction, ac_name: str, window_seconds: float = _AC_DEBOUNCE_WINDOW_SECONDS) -> bool:
    """
    Throttle leve por usuário + autocomplete.
    Reduz rajadas muito próximas disparadas pelo Discord/mobile.
    """
    try:
        uid = str(interaction.user.id)
    except Exception:
        uid = "0"

    key = f"{ac_name}:{uid}"
    now = _cache_now()

    with _AC_DEBOUNCE_LOCK:
        last = _AC_DEBOUNCE_STATE.get(key, 0.0)
        _AC_DEBOUNCE_STATE[key] = now

        # limpeza oportunista simples
        expired = [k for k, ts in _AC_DEBOUNCE_STATE.items() if now - ts > 30]
        for k in expired:
            _AC_DEBOUNCE_STATE.pop(k, None)

    return (now - last) < window_seconds


# =========================
# AUTOCOMPLETE FUNCTIONS (DEVEM FICAR ANTES DOS COMMANDS)
# =========================
async def ac_cycle_open(interaction: discord.Interaction, current: str):
    """
    Lista ciclos existentes na aba Cycles (season ativa),
    com label mostrando status.
    """
    try:
        if _ac_should_skip(interaction, "ac_cycle_open"):
            return []

        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return []

        q = str(current or "").strip().lower()
        items = get_cycle_choices_fast(
            sh,
            season_id=season_id,
            query=q,
            only_open=False,
            limit=25
        )

        out: list[app_commands.Choice[str]] = []
        for item in items:
            label = str(item.get("label", "")).strip()
            value = str(item.get("value", "")).strip()
            if not label or not value:
                continue
            out.append(app_commands.Choice(name=label[:100], value=value))

        return out[:25]
    except Exception:
        return []


async def ac_cycle_only_open(interaction: discord.Interaction, current: str):
    """
    Lista SOMENTE ciclos com status OPEN.
    Uso principal: /inscrever
    """
    try:
        if _ac_should_skip(interaction, "ac_cycle_only_open"):
            return []

        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return []

        q = str(current or "").strip().lower()
        items = get_cycle_choices_fast(
            sh,
            season_id=season_id,
            query=q,
            only_open=True,
            limit=25
        )

        out: list[app_commands.Choice[str]] = []
        for item in items:
            label = str(item.get("label", "")).strip()
            value = str(item.get("value", "")).strip()
            if not label or not value:
                continue
            out.append(app_commands.Choice(name=label[:100], value=value))

        return out[:25]
    except Exception:
        return []


async def ac_match_id_user_pending(interaction: discord.Interaction, current: str):
    """
    Sugere matches relevantes ao usuário:
    - Mostra apenas matches do usuário na season ativa e active=TRUE
    - Considera apenas ciclos LOCKED
    - Exibe no formato "Oponente | POD X | pendente/registrado"
    - Mantém o match_id como valor interno da escolha
    - Pode abrir automaticamente sem digitação, pois usa índice em memória
    """
    try:
        if _ac_should_skip(interaction, "ac_match_id_user_pending"):
            return []

        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return []

        uid = str(interaction.user.id).strip()
        q = str(current or "").strip().lower()

        nick_map = get_player_nick_map_fast(sh)
        matches = get_matches_for_player_fast(
            sh,
            player_id=uid,
            season_id=season_id,
            cycle=None,
            only_active=True
        )

        if not matches:
            return []

        # identifica ciclos locked da season ativa
        locked_items = get_cycle_choices_fast(
            sh,
            season_id=season_id,
            query="locked",
            only_open=False,
            limit=25
        )
        locked_cycles = {
            safe_int(item.get("value", 0), 0)
            for item in locked_items
            if "fechado" in str(item.get("label", "")).lower()
        }

        out: list[app_commands.Choice[str]] = []
        seen = set()

        def visual_status_from_match(r: dict) -> str:
            status = str(r.get("confirmed_status", "")).strip().lower()
            reported_by = str(r.get("reported_by_id", "")).strip()
            return "registrado" if (status == "pending" and reported_by) or status == "confirmed" else "pendente"

        for r in matches:
            cyc = safe_int(r.get("cycle", 0), 0)
            if cyc not in locked_cycles:
                continue

            mid = str(r.get("match_id", "")).strip()
            if not mid or mid in seen:
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            opp = b if uid == a else a
            opp_name = nick_map.get(opp, opp)
            pod = str(r.get("pod", "")).strip()
            visual_status = visual_status_from_match(r)

            if pod:
                label = f"{opp_name} | POD {pod} | {visual_status}"
            else:
                label = f"{opp_name} | {visual_status}"

            search_blob = f"{mid} {opp_name} {pod} {visual_status}".lower()
            if q and q not in search_blob:
                continue

            out.append(app_commands.Choice(name=label[:100], value=mid))
            seen.add(mid)

            if len(out) >= 25:
                break

        return out
    except Exception:
        return []


async def ac_score_vde(interaction: discord.Interaction, current: str):
    """
    Autocomplete de placar V-D-E (padrão estilo Melee).
    Mantém liberdade do usuário digitar manualmente.
    """
    try:
        if _ac_should_skip(interaction, "ac_score_vde"):
            return []

        q = str(current or "").strip().replace(" ", "")

        options = [
            "2-0-0",
            "2-1-0",
            "0-2-0",
            "1-2-0",
            "0-1-0",
            "0-0-1",
            "1-0-0",
            "1-0-1",
            "1-1-0",
            "1-1-1",
            "0-0-3",
        ]

        out = []
        for s in options:
            if q and q not in s:
                continue
            out.append(app_commands.Choice(name=s, value=s))
        return out[:25]
    except Exception:
        return []


# =========================================================
# [BLOCO 3/8 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 4/8
# SUB-BLOCO: ÚNICA
# RESUMO: Função de recálculo oficial do ciclo, incluindo auto-confirmação silenciosa,
# leitura de jogadores e matches válidos, montagem de estatísticas, cálculo de MWP, GWP,
# OMW e OGW com piso, ordenação do ranking, reconstrução da aba Standings do zero e retorno
# das linhas finais do ranking recalculado.
# REVISÃO: substituição de leituras diretas por cache, padronização de acesso às abas
# e invalidação explícita de cache após reconstrução da aba Standings.
# =================================================

def recalculate_cycle(sh, season_id: int, cycle: int):
    """
    Recalcula o ranking do ciclo (SEMPRE do zero):
    - Piso 33,3% primeiro (MWP/GWP)
    - Ranking: MWP% > OMW% > GW% > OGW% > Pontos
    - Match points: Win=3, Draw=1, Loss=0 (por match)
    - GWP: (W + 0.5*D) / GamesPlayed (com piso)
    - OMW: média do MWP (já com piso) dos oponentes enfrentados
    - OGW: média do GWP (já com piso) dos oponentes enfrentados
    - Considera apenas matches:
      active=TRUE e confirmed_status=confirmed e result_type != bye
    """
    ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
    ws_matches = sh.worksheet("Matches")
    ws_standings = sh.worksheet("Standings")

    # auto-confirm "silencioso" (opcional) antes do cálculo
    try:
        sweep_auto_confirm(sh, season_id, cycle)
    except Exception:
        pass

    # Base de jogadores do ciclo — ranking inclui apenas inscritos no ciclo
    ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
    enr_rows = cached_get_all_records(ws_enr, ttl_seconds=10)
    all_player_ids = set()
    for r in enr_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        pid = str(r.get("player_id", "")).strip()
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
    ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    matches_rows = cached_get_all_records(ws_matches, ttl_seconds=10)

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

    # Ordena: MWP > OMW > GW > OGW > Pontos
    out_rows.sort(
        key=lambda r: (r["mwp_percent"], r["omw_percent"], r["gw_percent"], r["ogw_percent"], r["match_points"]),
        reverse=True
    )

    ts = now_iso_utc()
    for i, r in enumerate(out_rows, start=1):
        r["rank_position"] = i
        r["last_recalc_at"] = ts

    header = [
        "season_id","cycle","player_id","matches_played","match_points","mwp_percent",
        "game_wins","game_losses","game_draws","games_played","gw_percent",
        "omw_percent","ogw_percent","rank_position","last_recalc_at"
    ]

    existing = cached_get_all_values(ws_standings, ttl_seconds=10)
    kept = []
    if existing and len(existing) > 1:
        existing_header = existing[0]
        if existing_header != header:
            kept = []
        else:
            for r in existing[1:]:
                while len(r) < len(header):
                    r.append("")
                r_season = safe_int(r[0], 0)
                r_cycle = safe_int(r[1], 0)
                if r_season == season_id and r_cycle == cycle:
                    continue
                kept.append(r)

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

    cache_invalidate(ws_standings)
    return out_rows

# =========================================================
# [BLOCO 4/8 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO ÚNICA
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 5/8
# SUB-BLOCO: A
# RESUMO: Cabeçalho do bloco, import adicional de asyncio e tasks, definição do cliente
# Discord, setup inicial do bot, configuração de onboarding, logs administrativos,
# rotina automática de limpeza do canal de boas-vindas e preparação do before_loop.
# REVISÃO: remoção de espera redundante no loop de limpeza, ajuste de comentário
# para refletir o comportamento real configurado e warm cache dos índices RAM
# no boot do bot para melhorar a performance do primeiro uso.
# =================================================

import asyncio
from discord.ext import tasks

# =========================================================
# Warm cache dos índices RAM
# =========================================================
async def warm_ram_indexes():
    """
    Pré-carrega os índices RAM no boot do bot para evitar que o primeiro
    autocomplete/comando faça leituras pesadas no Google Sheets.
    """
    try:
        sh = open_sheet()

        try:
            ensure_player_ram_index(sh)
        except Exception:
            pass

        try:
            ensure_cycle_ram_index(sh)
        except Exception:
            pass

        try:
            ensure_season_ram_index(sh)
        except Exception:
            pass

        try:
            ensure_match_ram_index(sh)
        except Exception:
            pass

    except Exception:
        pass


# =========================
# Discord Bot (Client)
# =========================
class LemeBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # necessário para fetch_member / on_member_join

        # MESSAGE CONTENT INTENT já estava ativado no portal (histórico do projeto).
        intents.message_content = True

        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        # Views persistentes (funcionam após restart quando o bot volta online)
        try:
            self.add_view(OnboardingStartView())
            self.add_view(OnboardingConfirmIdView())
            self.add_view(OnboardingChoiceView())
        except Exception:
            pass

        # inicia limpeza automática do canal de boas-vindas
        try:
            if not cleanup_welcome_channel.is_running():
                cleanup_welcome_channel.start()
        except Exception:
            pass

        # Sync commands (guild-scoped quando possível)
        try:
            if GUILD_ID:
                guild = discord.Object(id=GUILD_ID)
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
            else:
                await self.tree.sync()
        except Exception:
            pass

        # Warm cache dos índices RAM
        try:
            await warm_ram_indexes()
        except Exception:
            pass


client = LemeBot()


# =========================
# Config extra (onboarding)
# =========================
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "0"))


# =========================
# Logs (admin) — canal do servidor
# =========================
async def log_admin_guild(guild: discord.Guild | None, text: str):
    if not guild or not LOG_ADMIN_CHANNEL_ID:
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
# Limpeza automática do canal de boas-vindas
# - apaga TODAS as mensagens do canal a cada 60 minutos
# - inclui mensagens do bot, owner e qualquer interação
# - usa purge/bulk delete para mensagens recentes
# - mensagens com mais de 14 dias são apagadas individualmente
# =========================================================
@tasks.loop(minutes=60)
async def cleanup_welcome_channel():
    try:
        if not WELCOME_CHANNEL_ID:
            return

        channel = client.get_channel(WELCOME_CHANNEL_ID)
        if channel is None:
            try:
                channel = await client.fetch_channel(WELCOME_CHANNEL_ID)
            except Exception:
                channel = None

        if channel is None:
            return

        # coleta mensagens
        msgs = []
        async for m in channel.history(limit=500):
            msgs.append(m)

        if not msgs:
            return

        nowu = utc_now_dt()
        recent = []
        old = []

        for m in msgs:
            try:
                age = nowu - m.created_at
                if age <= timedelta(days=14):
                    recent.append(m)
                else:
                    old.append(m)
            except Exception:
                old.append(m)

        # apaga recentes em lote
        try:
            if recent:
                await channel.delete_messages(recent)
        except Exception:
            # fallback: apaga uma a uma
            for m in recent:
                try:
                    await m.delete()
                    await asyncio.sleep(0.35)
                except Exception:
                    pass

        # apaga antigas uma a uma
        for m in old:
            try:
                await m.delete()
                await asyncio.sleep(0.35)
            except Exception:
                pass

    except Exception:
        pass


@cleanup_welcome_channel.before_loop
async def before_cleanup_welcome_channel():
    try:
        await client.wait_until_ready()
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

    # fallback: quebra dura se ainda exceder
    safe_chunks: list[str] = []
    for c in chunks:
        if len(c) <= limit:
            safe_chunks.append(c)
        else:
            for i in range(0, len(c), limit):
                safe_chunks.append(c[i:i + limit])

    return safe_chunks

# =================================================
# FIM DO SUB-BLOCO A/3
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 5/8
# SUB-BLOCO: B
# RESUMO: Funções auxiliares para envio de mensagens longas no Discord,
# upsert de jogadores no Google Sheets com blindagem contra erro 429,
# catálogo oficial de comandos da liga e implementação dos comandos
# /comando e /tutorial.
# REVISÃO: inclusão do /tutorial e ajuste do catálogo/listagem por nível.
# =================================================

async def send_followup_chunks(interaction: discord.Interaction, text: str, ephemeral: bool = True, limit: int = 1900):
    """
    Regra do projeto: quando resposta for grande, enviar em múltiplas mensagens
    para não estourar o limite do Discord.
    """
    chunks = split_text_lines(text, limit=limit)
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
# Players upsert (Sheets) — mínimo necessário
# + BLINDAGEM 429: invalida cache após escrita
# =========================================================
def upsert_player(ws_players, discord_id: str, nickname: str):
    did = str(discord_id).strip()
    nick = str(nickname).strip()
    if not did or not nick:
        return

    col = ensure_sheet_columns(ws_players, PLAYERS_HEADER)
    rows = cached_get_all_values(ws_players, ttl_seconds=10)  # usa cache para reduzir 429

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
            if "nick" in col:
                ws_players.update([[nick]], range_name=f"{col_letter(col['nick'])}{found_row}")
            if "updated_at" in col:
                ws_players.update([[nowc]], range_name=f"{col_letter(col['updated_at'])}{found_row}")
            cache_invalidate(ws_players)  # <<< blindagem 429 (dados mudaram)
        except Exception:
            pass
        return

    row = [""] * len(PLAYERS_HEADER)
    if "discord_id" in col:
        row[col["discord_id"]] = did
    if "nick" in col:
        row[col["nick"]] = nick
    if "status" in col:
        row[col["status"]] = "active"
    if "created_at" in col:
        row[col["created_at"]] = nowc
    if "updated_at" in col:
        row[col["updated_at"]] = nowc

    try:
        ws_players.append_row(row, value_input_option="USER_ENTERED")
        cache_invalidate(ws_players)  # <<< blindagem 429 (dados mudaram)
    except Exception:
        pass


# =========================================================
# /comando (catálogo) — CORRIGIDO (sem SyntaxError)
# =========================================================
# OBS: aqui é catálogo (listagem). A implementação real dos comandos fica nos BLOCOS 6/7/8.
# A intenção é: /comando sempre refletir TUDO que a liga considera “comandos oficiais”.
COMMANDS_CATALOG = [
    # Jogador
    ("jogador", "/meuid", "Mostra seu ID do Discord (suporte)."),
    ("jogador", "/tutorial", "Mostra um guia rápido de como usar o bot."),
    ("jogador", "/inscrever", "Se inscreve com season, ciclo, deck e decklist válidos."),
    ("jogador", "/drop", "Sai do ciclo (marca dropped)."),
    ("jogador", "/pods_ver", "Mostra todos os PODs no ciclo atual + deck + decklist."),
    ("jogador", "/meus_matches", "Lista seus matches do ciclo."),
    ("jogador", "/resultado", "Reporta resultado V-D-E (games) para um match."),
    ("jogador", "/rejeitar", "Rejeita um resultado pendente (janela 48h)."),
    ("jogador", "/ranking", "Mostra ranking do ciclo."),
    ("jogador", "/ranking_geral", "Mostra ranking geral da season."),
    ("jogador", "/prazo", "Mostra o prazo oficial do ciclo."),
    ("jogador", "/comando", "Mostra os comandos que você tem acesso."),

    # Administrativo (ADM/Organizador)
    ("adm", "/deck", "Define ou altera deck do jogador no ciclo."),
    ("adm", "/decklist", "Define ou altera decklist do jogador no ciclo."),
    ("adm", "/forcesync", "Sincroniza comandos no servidor."),
    ("adm", "/onboarding", "Reposta o botão de onboarding no canal atual."),
    ("adm", "/ciclo_abrir", "Abre um ciclo para inscrições."),
    ("adm", "/ciclo_fechar", "Fecha inscrições do ciclo."),
    ("adm", "/ciclo_encerrar", "Encerra ciclo (completed)."),
    ("adm", "/start_cycle", "Gera pods + matches e trava ciclo (locked)."),
    ("adm", "/deadline", "Lista pendências próximas do vencimento (48h)."),
    ("adm", "/recalcular", "Auto-confirma pendentes e recalcula ranking do ciclo."),
    ("adm", "/standings_publicar", "Publica standings (canal configurado)."),
    ("adm", "/final", "Finaliza ciclo: aplica 0-0-3 (ID) nos matches sem report após prazo."),
    ("adm", "/admin_resultado_editar", "Edita resultado de um match (admin)."),
    ("adm", "/admin_resultado_cancelar", "Cancela um resultado (admin)."),
    ("adm", "/status_ciclo", "Mostra status do ciclo atual."),
    ("adm", "/exportar_ciclo", "Exporta dados do ciclo (admin)."),
    ("adm", "/fechar_resultados_atrasados", "Força fechamento de pendências antigas (admin)."),
    ("adm", "/substituir_jogador", "Substitui jogador (admin)."),
    ("adm", "/historico_confronto", "Mostra histórico de confrontos (admin)."),
    ("adm", "/estatisticas", "Estatísticas da liga (admin)."),
    ("adm", "/inscritos", "Lista inscritos, deck/decklist e pendências do ciclo."),
    ("adm", "/cadastrar_player", "Cadastra player manualmente com season, ciclo, deck e decklist."),

     # Owner (Owner)
    ("owner", "/startseason", "Abre uma nova season e define como ativa (owner)."),
    ("owner", "/closeseason", "Fecha a season atual (owner)."),
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

        # Lista real de slash commands registrados (evita "comandos fantasma")
        try:
            real_cmds = {f"/{c.name}" for c in client.tree.get_commands()}
        except Exception:
            real_cmds = set()

        titulo_nivel = {
            "jogador": "JOGADOR",
            "adm": "ADM",
            "organizador": "ORGANIZADOR",
            "owner": "OWNER",
        }.get(user_level, user_level.upper())

        lines = [f"📌 **Seus comandos disponíveis ({titulo_nivel})**\n"]

        for lvl, cmd, desc in COMMANDS_CATALOG:
            if not level_allows(user_level, lvl):
                continue

            # Se conseguimos detectar os comandos reais, não mostramos os que não existem ainda
            if real_cmds and cmd not in real_cmds:
                continue

            lines.append(f"• **{cmd}** — {desc}")

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True, limit=1500)

    except Exception as e:
        try:
            await interaction.followup.send(f"❌ Erro no /comando: {e}", ephemeral=True)
        except Exception:
            pass


# =========================================================
# /tutorial
# =========================================================
@client.tree.command(name="tutorial", description="Mostra um tutorial rápido de como usar o bot.")
async def tutorial(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        lines = [
            "📘 **Tutorial rápido do jogador**",
            "",
            "**1. Faça seu cadastro**",
            "• Entre pelo onboarding do servidor e confirme seus dados.",
            "",
            "**2. Veja seus comandos**",
            "• Use **/comando** para ver o que está disponível para você.",
            "",
            "**3. Inscreva-se no ciclo**",
            "• Use **/inscrever** informando a season, o ciclo, o nome do deck e a decklist.",
            "",
            "**4. Consulte os PODs**",
            "• Use **/pods_ver** para ver os grupos do ciclo.",
            "",
            "**5. Veja seus matches**",
            "• Use **/meus_matches** para consultar suas partidas.",
            "",
            "**6. Envie seu resultado**",
            "• Use **/resultado** no formato **V-D-E**. Exemplo: **2-1-0**.",
            "",
            "**7. Confirmação do oponente**",
            "• O oponente pode confirmar ou rejeitar o resultado.",
            "• Se não houver rejeição em até **48h**, o sistema pode auto-confirmar.",
            "",
            "**8. Acompanhe o campeonato**",
            "• Use **/ranking** para ver o ranking do ciclo.",
            "• Use **/ranking_geral** para ver o ranking geral da season.",
            "• Use **/prazo** para consultar o prazo oficial do ciclo.",
            "",
            "**Comandos mais usados na prática**",
            "• **/comando**",
            "• **/inscrever**",
            "• **/pods_ver**",
            "• **/meus_matches**",
            "• **/resultado**",
            "• **/ranking**",
            "• **/prazo**",
        ]

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True, limit=1500)

    except Exception as e:
        try:
            await interaction.followup.send(f"❌ Erro no /tutorial: {e}", ephemeral=True)
        except Exception:
            pass


# =========================================================
# Comandos básicos do onboarding (BLOCO 5)
# =========================================================
@client.tree.command(name="meuid", description="Mostra seu ID do Discord.")
async def meuid(interaction: discord.Interaction):
    try:
        await interaction.response.send_message(f"Seu ID é: `{interaction.user.id}`", ephemeral=True)
    except Exception:
        pass

# =================================================
# FIM DO SUB-BLOCO B/3
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 5/8
# SUB-BLOCO: C/3
# RESUMO: Implementação completa do sistema de onboarding do bot,
# incluindo Modal para cadastro de nome/sobrenome, Views persistentes
# com botões (start, confirmar, participar/assistir), aplicação do cargo
# Jogador, evento on_member_join para postagem automática do onboarding
# e comando administrativo /onboarding para repostar o fluxo.
# REVISÃO: integração com MATCH RAM INDEX, além de blindagem do fluxo de confirmação/
# rejeição por botão em DM para evitar "command error" / timeout de interação.
# =================================================

# =========================================================
# ONBOARDING — Views persistentes + Modal
# =========================================================
class NicknameModal(discord.ui.Modal, title="Cadastro do Jogador"):
    nome = discord.ui.TextInput(
        label="Insira seu Nome e Sobrenome sem abreviações",
        required=True,
        max_length=32,
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            return

        raw = " ".join(str(self.nome.value or "").strip().split())
        parts = raw.split()

        # Regra: exatamente 2 palavras
        if len(parts) != 2:
            try:
                await interaction.followup.send(
                    "⚠️ Informe apenas **Nome e Sobrenome**.\nExemplo: **Thales França**",
                    ephemeral=True
                )
            except Exception:
                pass
            return

        # Salva no Sheets (Players)
        try:
            sh = open_sheet()
            ensure_all_sheets(sh)
            ws_players = sh.worksheet("Players")
            upsert_player(ws_players, str(interaction.user.id), raw)
        except Exception:
            try:
                await interaction.followup.send(
                    "⚠️ Não consegui salvar seu cadastro agora. Tente novamente em instantes.",
                    ephemeral=True
                )
            except Exception:
                pass
            return

        try:
            await interaction.followup.send(
                "✅ Cadastro salvo. Agora escolha uma opção abaixo:",
                ephemeral=True,
                view=OnboardingChoiceView()
            )
        except Exception:
            pass


class OnboardingStartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Iniciar cadastro", style=discord.ButtonStyle.success, custom_id="lhb_onb_start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_message(
                "Confirme abaixo para prosseguir com seu cadastro.",
                ephemeral=True,
                view=OnboardingConfirmIdView()
            )
        except Exception:
            pass


class OnboardingConfirmIdView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Confirmar ID", style=discord.ButtonStyle.primary, custom_id="lhb_onb_confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(NicknameModal())
        except Exception:
            try:
                await interaction.response.send_message(
                    "⚠️ Não consegui abrir o formulário agora. Tente novamente.",
                    ephemeral=True
                )
            except Exception:
                pass


class OnboardingChoiceView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Participar", style=discord.ButtonStyle.success, custom_id="lhb_onb_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        # aplica cargo Jogador (se existir) e ajusta nickname para Nome Sobrenome salvo no Sheets
        try:
            guild = interaction.guild
            nome_salvo = ""

            try:
                sh = open_sheet()
                ensure_all_sheets(sh)
                ws_players = sh.worksheet("Players")
                rows = cached_get_all_records(ws_players, ttl_seconds=10)
                uid = str(interaction.user.id).strip()
                for r in rows:
                    if str(r.get("discord_id", "")).strip() == uid:
                        nome_salvo = str(r.get("nick", "")).strip()
                        break
            except Exception:
                nome_salvo = ""

            if not nome_salvo or len(nome_salvo.split()) != 2:
                return await interaction.response.send_message(
                    "⚠️ Seu cadastro não está com Nome e Sobrenome válidos. Refaça o onboarding ou procure um ADM.",
                    ephemeral=True
                )

            if guild:
                member = guild.get_member(interaction.user.id) or await guild.fetch_member(interaction.user.id)

                try:
                    await member.edit(nick=nome_salvo, reason="Onboarding Leme Holandês")
                except Exception:
                    return await interaction.response.send_message(
                        "⚠️ Não consegui aplicar seu Nome e Sobrenome no servidor. Verifique se o bot tem permissão de gerenciar apelidos e tente novamente.",
                        ephemeral=True
                    )

                role = discord.utils.get(guild.roles, name=ROLE_JOGADOR)
                if role:
                    try:
                        await member.add_roles(role, reason="Onboarding Leme Holandês")
                    except Exception:
                        pass

            await interaction.response.send_message("✅ Perfeito. Você está marcado como **Jogador**.", ephemeral=True)
        except Exception:
            try:
                await interaction.response.send_message("✅ Cadastro finalizado.", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Assistir", style=discord.ButtonStyle.secondary, custom_id="lhb_onb_watch")
    async def watch(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_message("✅ Tudo certo. Bem-vindo(a)!", ephemeral=True)
        except Exception:
            pass


# =========================================================
# RESULTADO — View de confirmação por DM
# =========================================================
class ResultConfirmView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Confirmar", style=discord.ButtonStyle.success, custom_id="lhb_result_confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # responde imediatamente para evitar timeout/"interaction failed"
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass

            message = interaction.message
            if not message or not message.embeds:
                return await interaction.followup.send(
                    "❌ Não consegui identificar o resultado para confirmação.",
                    ephemeral=True
                )

            embed = message.embeds[0]
            match_id = ""
            if embed.footer and embed.footer.text:
                footer = str(embed.footer.text).strip()
                if footer.lower().startswith("match_id:"):
                    match_id = footer.split(":", 1)[1].strip()

            if not match_id:
                return await interaction.followup.send(
                    "❌ Match não identificado nesta mensagem.",
                    ephemeral=True
                )

            sh = open_sheet()
            ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
            col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

            match_row = get_match_by_id_fast(sh, match_id)

            found = None
            row_data = None
            rows = cached_get_all_values(ws_matches, ttl_seconds=10)

            for idx in range(1, len(rows)):
                r = rows[idx]
                val = r[col["match_id"]] if col["match_id"] < len(r) else ""
                if str(val).strip() == match_id:
                    found = idx + 1
                    row_data = r
                    break

            if not found:
                return await interaction.followup.send(
                    "❌ Match não encontrado.",
                    ephemeral=True
                )

            def getc(name: str) -> str:
                if match_row is not None and name in match_row:
                    return str(match_row.get(name, ""))
                ci = col[name]
                return row_data[ci] if row_data is not None and ci < len(row_data) else ""

            a = str(getc("player_a_id")).strip()
            b = str(getc("player_b_id")).strip()
            uid = str(interaction.user.id).strip()

            if uid not in (a, b):
                return await interaction.followup.send(
                    "❌ Você não participa deste match.",
                    ephemeral=True
                )

            reported_by = str(getc("reported_by_id")).strip()
            if not reported_by:
                return await interaction.followup.send(
                    "❌ Este match não possui resultado pendente.",
                    ephemeral=True
                )

            if uid == reported_by:
                return await interaction.followup.send(
                    "❌ Quem reportou não pode confirmar o próprio resultado.",
                    ephemeral=True
                )

            status = str(getc("confirmed_status")).strip().lower()
            if status != "pending":
                return await interaction.followup.send(
                    "❌ Este match não está pendente.",
                    ephemeral=True
                )

            updated_at = now_iso_utc()
            ws_matches.batch_update([
                {
                    "range": f"{col_letter(col['confirmed_status'])}{found}",
                    "values": [["confirmed"]]
                },
                {
                    "range": f"{col_letter(col['confirmed_by_id'])}{found}",
                    "values": [[uid]]
                },
                {
                    "range": f"{col_letter(col['updated_at'])}{found}",
                    "values": [[updated_at]]
                },
            ])
            cache_invalidate(ws_matches)
            invalidate_match_ram_index()
            invalidate_match_ac_index()

            season_id = safe_int(getc("season_id"), 0)
            cycle = safe_int(getc("cycle"), 0)
            try:
                recalculate_cycle(sh, season_id, cycle)
            except Exception:
                pass

            try:
                for child in self.children:
                    child.disabled = True
                await interaction.message.edit(view=self)
            except Exception:
                pass

            try:
                await interaction.followup.send(
                    "✅ Resultado confirmado com sucesso.",
                    ephemeral=True
                )
            except Exception:
                pass

        except Exception as e:
            try:
                await interaction.followup.send(f"❌ Erro ao confirmar: {e}", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Rejeitar", style=discord.ButtonStyle.danger, custom_id="lhb_result_reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # responde imediatamente para evitar timeout/"interaction failed"
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass

            message = interaction.message
            if not message or not message.embeds:
                return await interaction.followup.send(
                    "❌ Não consegui identificar o resultado para rejeição.",
                    ephemeral=True
                )

            embed = message.embeds[0]
            match_id = ""
            if embed.footer and embed.footer.text:
                footer = str(embed.footer.text).strip()
                if footer.lower().startswith("match_id:"):
                    match_id = footer.split(":", 1)[1].strip()

            if not match_id:
                return await interaction.followup.send(
                    "❌ Match não identificado nesta mensagem.",
                    ephemeral=True
                )

            sh = open_sheet()
            ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
            col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

            match_row = get_match_by_id_fast(sh, match_id)

            found = None
            row_data = None
            rows = cached_get_all_values(ws_matches, ttl_seconds=10)

            for idx in range(1, len(rows)):
                r = rows[idx]
                val = r[col["match_id"]] if col["match_id"] < len(r) else ""
                if str(val).strip() == match_id:
                    found = idx + 1
                    row_data = r
                    break

            if not found:
                return await interaction.followup.send(
                    "❌ Match não encontrado.",
                    ephemeral=True
                )

            def getc(name: str) -> str:
                if match_row is not None and name in match_row:
                    return str(match_row.get(name, ""))
                ci = col[name]
                return row_data[ci] if row_data is not None and ci < len(row_data) else ""

            a = str(getc("player_a_id")).strip()
            b = str(getc("player_b_id")).strip()
            uid = str(interaction.user.id).strip()

            if uid not in (a, b):
                return await interaction.followup.send(
                    "❌ Você não participa deste match.",
                    ephemeral=True
                )

            reported_by = str(getc("reported_by_id")).strip()
            if not reported_by:
                return await interaction.followup.send(
                    "❌ Este match não possui resultado pendente.",
                    ephemeral=True
                )

            if uid == reported_by:
                return await interaction.followup.send(
                    "❌ Quem reportou não pode rejeitar o próprio resultado.",
                    ephemeral=True
                )

            status = str(getc("confirmed_status")).strip().lower()
            if status != "pending":
                return await interaction.followup.send(
                    "❌ Este match não está pendente.",
                    ephemeral=True
                )

            updated_at = now_iso_utc()
            ws_matches.batch_update([
                {
                    "range": f"{col_letter(col['confirmed_status'])}{found}",
                    "values": [["rejected"]]
                },
                {
                    "range": f"{col_letter(col['confirmed_by_id'])}{found}",
                    "values": [[uid]]
                },
                {
                    "range": f"{col_letter(col['updated_at'])}{found}",
                    "values": [[updated_at]]
                },
            ])
            cache_invalidate(ws_matches)
            invalidate_match_ram_index()
            invalidate_match_ac_index()

            try:
                for child in self.children:
                    child.disabled = True
                await interaction.message.edit(view=self)
            except Exception:
                pass

            try:
                await interaction.followup.send(
                    "⚠️ Resultado rejeitado. O match precisa ser reportado novamente.",
                    ephemeral=True
                )
            except Exception:
                pass

        except Exception as e:
            try:
                await interaction.followup.send(f"❌ Erro ao rejeitar: {e}", ephemeral=True)
            except Exception:
                pass


# =========================================================
# Posting do onboarding no canal (evento + comando admin)
# =========================================================
async def post_onboarding_message(channel: discord.abc.Messageable, member_mention: str | None = None):
    base = "Bem-vindo ao **Leme Holandês**! Para começar, clique no botão abaixo:"
    if member_mention:
        base = f"{member_mention}\n" + base
    try:
        await channel.send(base, view=OnboardingStartView())
    except Exception:
        pass


@client.event
async def on_member_join(member: discord.Member):
    # Ao entrar, postar onboarding no canal configurado (ou ignora se não tiver)
    try:
        guild = member.guild
        ch = None
        if WELCOME_CHANNEL_ID:
            ch = guild.get_channel(WELCOME_CHANNEL_ID)
            if not ch:
                try:
                    ch = await guild.fetch_channel(WELCOME_CHANNEL_ID)
                except Exception:
                    ch = None
        if ch:
            await post_onboarding_message(ch, member_mention=member.mention)
            await log_admin_guild(guild, f"🟢 Onboarding postado para {member} ({member.id}) em {ch.mention}")
    except Exception:
        pass


@client.tree.command(name="onboarding", description="Reposta o botão de onboarding no canal atual (ADM).")
async def onboarding(interaction: discord.Interaction):
    try:
        if not await is_admin_or_organizer(interaction):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
    except Exception:
        try:
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
        except Exception:
            pass
        return

    try:
        await interaction.response.send_message("✅ Onboarding repostado neste canal.", ephemeral=True)
    except Exception:
        pass

    try:
        await post_onboarding_message(interaction.channel, member_mention=None)
    except Exception:
        pass


# =========================================================
# [BLOCO 5/8 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO C/3
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 6/8
# SUB-BLOCO: A/2
# RESUMO: Cabeçalho do bloco, regras implementadas, helpers para verificar se o jogador
# está ativo na season ou no ciclo, helper para garantir linha na aba Decks, helpers
# de autocomplete para /inscrever e comando /inscrever com season, ciclo, deck e decklist.
# REVISÃO: integração com índices RAM de Seasons/Cycles/Players, redução de leituras
# diretas no Sheets, reaproveitamento de cache e autocomplete de ciclo focado apenas
# em ciclos abertos.
# =================================================

# =========================================================
# [BLOCO 6/8] — INSCRIÇÃO + DROP + DECK/DECKLIST (REVISADO)
# REGRAS IMPLEMENTADAS:
# - jogador só pode entrar em ciclo OPEN
# - ciclo LOCKED não aceita inscrições
# - jogador não pode estar ativo em dois ciclos da mesma season
# - Deck e Decklist: 1 vez POR CICLO (não trava ciclo seguinte)
# - Blindagem 429: cached_get_all_* + cache_invalidate após escrita
# =========================================================


# =========================================================
# Helper: verifica se jogador já está ativo em algum ciclo (na season)
# =========================================================
def player_active_in_season(ws_enr, season_id: int, player_id: str) -> bool:
    rows = cached_get_all_records(ws_enr, ttl_seconds=10)

    pid = str(player_id).strip()
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if str(r.get("player_id", "")).strip() != pid:
            continue
        if str(r.get("status", "")).strip().lower() == "active":
            return True

    return False


# =========================================================
# Helper: verifica se jogador está ACTIVE no ciclo específico
# =========================================================
def player_active_in_cycle(ws_enr, season_id: int, cycle: int, player_id: str) -> bool:
    rows = cached_get_all_records(ws_enr, ttl_seconds=10)

    pid = str(player_id).strip()
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        if str(r.get("player_id", "")).strip() != pid:
            continue
        if str(r.get("status", "")).strip().lower() == "active":
            return True

    return False


# =========================================================
# Helper: garante linha na aba Decks (1 vez por ciclo por jogador)
# (BUG FIX: esta função era usada no bloco e NÃO existia)
# =========================================================
def ensure_deck_row(ws_decks, season_id: int, cycle: int, player_id: str) -> int:
    """
    Retorna a linha (1-based) do registro (season, cycle, player).
    Se não existir, cria e retorna a nova linha.
    """
    pid = str(player_id).strip()

    existing = get_deck_row(ws_decks, season_id, cycle, pid)
    if existing is not None:
        return existing

    nowb = now_br_str()
    ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

    ws_decks.append_row(
        [
            str(season_id),
            str(cycle),
            pid,
            "",
            "",
            nowb,
            nowb,
        ],
        value_input_option="USER_ENTERED"
    )
    cache_invalidate(ws_decks)

    vals = cached_get_all_values(ws_decks, ttl_seconds=5)
    return len(vals)


# =========================================================
# Helpers autocomplete /inscrever
# =========================================================
def get_player_row_by_discord_id(ws_players, discord_id: str) -> int | None:
    rows = cached_get_all_values(ws_players, ttl_seconds=10)
    col = ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)

    did = str(discord_id).strip()
    for i in range(2, len(rows) + 1):
        r = rows[i - 1]
        val = r[col["discord_id"]] if col["discord_id"] < len(r) else ""
        if str(val).strip() == did:
            return i
    return None

def get_player_record_by_discord_id(ws_players, discord_id: str) -> dict | None:
    did = str(discord_id).strip()

    try:
        sh = ws_players.spreadsheet
        row = get_player_row_fast(sh, did)
        return row if row else None
    except Exception:
        rows = cached_get_all_records(ws_players, ttl_seconds=10)
        for r in rows:
            if str(r.get("discord_id", "")).strip() == did:
                return r
        return None

def get_deck_record_by_keys(ws_decks, season_id: int, cycle: int, player_id: str) -> dict | None:
    rows = cached_get_all_records(ws_decks, ttl_seconds=10)
    pid = str(player_id).strip()

    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        if str(r.get("player_id", "")).strip() != pid:
            continue
        return r

    return None

async def ac_inscrever_season(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_inscrever_season"):
            return []

        sh = open_sheet()
        q = str(current or "").strip().lower()

        items = get_season_choices_fast(sh, query=q, limit=25)

        out: list[app_commands.Choice[int]] = []
        for item in items:
            sid = safe_int(item.get("season_id", 0), 0)
            label = str(item.get("label", "")).strip()
            if sid <= 0 or not label:
                continue
            out.append(app_commands.Choice(name=label[:100], value=sid))

        return out[:25]
    except Exception:
        return []

async def ac_inscrever_cycle(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_inscrever_cycle"):
            return []

        sh = open_sheet()

        season_selected = safe_int(getattr(interaction.namespace, "season", 0), 0)
        q = str(current or "").strip().lower()

        if season_selected <= 0:
            return []

        items = get_cycle_choices_fast(
            sh,
            season_id=season_selected,
            query=q,
            only_open=True,
            limit=25
        )

        out: list[app_commands.Choice[int]] = []
        for item in items:
            cyc = safe_int(item.get("value", 0), 0)
            label = str(item.get("label", "")).strip()
            if cyc <= 0 or not label:
                continue
            out.append(app_commands.Choice(name=label[:100], value=cyc))

        return out[:25]
    except Exception:
        return []


# =========================================================
# /inscrever
# =========================================================
@client.tree.command(name="inscrever", description="Se inscreve no ciclo aberto informando deck e decklist.")
@app_commands.describe(
    season="Season",
    cycle="Número do ciclo",
    deck="Nome do deck",
    decklist="Link da decklist"
)
@app_commands.autocomplete(season=ac_inscrever_season, cycle=ac_inscrever_cycle)
async def inscrever(interaction: discord.Interaction, season: int, cycle: int, deck: str, decklist: str):
    await interaction.response.defer(ephemeral=True)

    ok, val = validate_decklist_url(decklist)
    if not ok:
        return await interaction.followup.send(val, ephemeral=True)

    try:
        sh = open_sheet()

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        pid = str(interaction.user.id).strip()

        player_row = get_player_row_by_discord_id(ws_players, pid)
        if player_row is None:
            return await interaction.followup.send(
                "❌ Seu cadastro não foi encontrado. Entre em contato com um ADM.",
                ephemeral=True
            )

        player_record = get_player_record_by_discord_id(ws_players, pid) or {}
        nick_atual = str(player_record.get("nick", "")).strip()
        if not nick_atual:
            return await interaction.followup.send(
                "❌ Seu nick não está cadastrado corretamente. Entre em contato com um ADM.",
                ephemeral=True
            )

        if not season_exists(sh, season):
            return await interaction.followup.send(f"❌ A season {season} não existe.", ephemeral=True)

        cf = get_cycle_fields(ws_cycles, season, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ O ciclo {cycle} não existe na season {season}.", ephemeral=True)

        status = str(cf.get("status", "")).strip().lower()
        if status != "open":
            if status == "locked":
                return await interaction.followup.send(
                    "❌ Este ciclo já teve os pods gerados e está fechado para novas inscrições.",
                    ephemeral=True
                )
            return await interaction.followup.send(
                f"❌ O ciclo não está aberto para inscrições (status: {status}).",
                ephemeral=True
            )

        if player_active_in_season(ws_enr, season, pid):
            return await interaction.followup.send(
                "❌ Você já possui inscrição ativa nesta season. Entre em contato com um ADM.",
                ephemeral=True
            )

        if player_active_in_cycle(ws_enr, season, cycle, pid):
            return await interaction.followup.send(
                "❌ Você já está inscrito neste ciclo. Entre em contato com um ADM.",
                ephemeral=True
            )

        existing_deck = get_deck_record_by_keys(ws_decks, season, cycle, pid)
        if existing_deck is not None:
            if str(existing_deck.get("deck", "")).strip() or str(existing_deck.get("decklist_url", "")).strip():
                return await interaction.followup.send(
                    "❌ Já existe informação gravada para sua inscrição neste ciclo. Entre em contato com um ADM.",
                    ephemeral=True
                )

        deck = str(deck).strip()
        if not deck or len(deck) > 80:
            return await interaction.followup.send("❌ Nome de deck inválido (1 a 80 caracteres).", ephemeral=True)

        nowb = now_br_str()

        ws_enr.append_row(
            [str(season), str(cycle), pid, "active", nowb, nowb],
            value_input_option="USER_ENTERED"
        )
        cache_invalidate(ws_enr)

        rown = ensure_deck_row(ws_decks, season, cycle, pid)
        col_decks = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        ws_decks.batch_update([
            {
                "range": f"{col_letter(col_decks['deck'])}{rown}",
                "values": [[deck]]
            },
            {
                "range": f"{col_letter(col_decks['decklist_url'])}{rown}",
                "values": [[val]]
            },
            {
                "range": f"{col_letter(col_decks['updated_at'])}{rown}",
                "values": [[nowb]]
            },
        ])
        cache_invalidate(ws_decks)

        await interaction.followup.send(
            f"✅ Inscrição confirmada na **Season {season} / Ciclo {cycle}**.\n"
            f"- Nick: **{nick_atual}**\n"
            f"- Deck: **{deck}**\n"
            f"- Decklist: salva com sucesso.",
            ephemeral=True
        )
        await log_admin(interaction, f"inscrição completa: {interaction.user} S{season} C{cycle}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscrever: {e}", ephemeral=True)

# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 6/8
# SUB-BLOCO: B/2
# RESUMO: Comandos /drop, /deck e /decklist, incluindo validações de inscrição
# ativa no ciclo, controle administrativo de alteração de deck e decklist,
# seleção de jogador por nickname do banco Players, validação de URL de
# decklist, atualização das abas no Google Sheets e invalidação de cache
# após escrita.
# REVISÃO: integração com índice RAM de Players para autocomplete e resolução
# rápida de jogador, redução de updates isolados, debounce leve no autocomplete
# e alinhamento com a estratégia geral de cache/performance.
# =================================================

# =========================================================
# /drop
# =========================================================
@client.tree.command(name="drop", description="Sai do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def drop(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = get_current_season_id(sh)

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        rows = cached_get_all_values(ws_enr, ttl_seconds=10)
        pid = str(interaction.user.id).strip()

        for idx in range(1, len(rows)):
            r = rows[idx]

            def getc(name: str) -> str:
                ci = col[name]
                return r[ci] if ci < len(r) else ""

            if (
                safe_int(getc("season_id"), 0) == season_id
                and safe_int(getc("cycle"), 0) == cycle
                and str(getc("player_id")).strip() == pid
            ):
                rown = idx + 1

                ws_enr.batch_update([
                    {
                        "range": f"{col_letter(col['status'])}{rown}",
                        "values": [["dropped"]]
                    },
                    {
                        "range": f"{col_letter(col['updated_at'])}{rown}",
                        "values": [[now_br_str()]]
                    },
                ])
                cache_invalidate(ws_enr)

                return await interaction.followup.send("✅ Você saiu do ciclo.", ephemeral=True)

        await interaction.followup.send("❌ Você não está inscrito neste ciclo.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)


# =========================================================
# Helpers autocomplete /deck e /decklist
# =========================================================
async def ac_player_nick(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_player_nick"):
            return []

        sh = open_sheet()
        q = str(current or "").strip().lower()

        items = get_player_choices_fast(sh, query=q, limit=25)

        out: list[app_commands.Choice[str]] = []
        for item in items:
            label = str(item.get("label", "")).strip()
            value = str(item.get("value", "")).strip()
            if not label or not value:
                continue
            out.append(app_commands.Choice(name=label[:100], value=value))

        return out[:25]
    except Exception:
        return []


def resolve_player_nick(ws_players, player_id: str) -> str:
    try:
        sh = ws_players.spreadsheet
        nick_map = get_player_nick_map_fast(sh)
        pid = str(player_id).strip()
        return nick_map.get(pid, pid)
    except Exception:
        rows = cached_get_all_records(ws_players, ttl_seconds=10)
        pid = str(player_id).strip()
        for r in rows:
            if str(r.get("discord_id", "")).strip() == pid:
                return str(r.get("nick", "")).strip() or pid
        return pid


def resolve_player_id_from_value(ws_players, jogador: str) -> str:
    raw = str(jogador or "").strip()
    if not raw:
        return ""

    try:
        sh = ws_players.spreadsheet
        return resolve_player_id_fast(sh, raw)
    except Exception:
        raw_norm = raw.lower()
        rows = cached_get_all_records(ws_players, ttl_seconds=10)

        for r in rows:
            pid = str(r.get("discord_id", "")).strip()
            if pid == raw:
                return pid

            nick = str(r.get("nick", "")).strip()
            if nick and nick.lower() == raw_norm:
                return pid

        return ""


# =========================================================
# /deck
# =========================================================
@client.tree.command(name="deck", description="(ADM/Organizador/Owner) Define ou altera deck do jogador no ciclo.")
@app_commands.describe(cycle="Ciclo", nome="Nome do deck", jogador="Jogador")
@app_commands.autocomplete(cycle=ac_cycle_open, jogador=ac_player_nick)
async def deck(interaction: discord.Interaction, cycle: int, nome: str, jogador: str):
    await interaction.response.defer(ephemeral=True)

    try:
        if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
            return await interaction.followup.send(
                "❌ Apenas ADM, Organizador ou Owner podem usar este comando.",
                ephemeral=True
            )

        sh = open_sheet()
        season_id = get_current_season_id(sh)

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)

        pid = resolve_player_id_from_value(ws_players, jogador)
        if not pid:
            return await interaction.followup.send(
                "❌ Jogador não encontrado no cadastro.",
                ephemeral=True
            )

        if not player_active_in_cycle(ws_enr, season_id, cycle, pid):
            return await interaction.followup.send(
                "❌ O jogador precisa estar inscrito (ativo) neste ciclo para cadastrar deck.",
                ephemeral=True
            )

        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER)
        rown = ensure_deck_row(ws_decks, season_id, cycle, pid)

        col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        nome = str(nome).strip()
        if not nome or len(nome) > 80:
            return await interaction.followup.send("❌ Nome de deck inválido (1 a 80 caracteres).", ephemeral=True)

        ws_decks.batch_update([
            {
                "range": f"{col_letter(col['deck'])}{rown}",
                "values": [[nome]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{rown}",
                "values": [[now_br_str()]]
            },
        ])
        cache_invalidate(ws_decks)

        nick = resolve_player_nick(ws_players, pid)
        await interaction.followup.send(f"✅ Deck salvo para **{nick}**: **{nome}**", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /decklist
# =========================================================
@client.tree.command(name="decklist", description="(ADM/Organizador/Owner) Define ou altera decklist do jogador no ciclo.")
@app_commands.describe(cycle="Ciclo", url="Link da decklist", jogador="Jogador")
@app_commands.autocomplete(cycle=ac_cycle_open, jogador=ac_player_nick)
async def decklist(interaction: discord.Interaction, cycle: int, url: str, jogador: str):
    await interaction.response.defer(ephemeral=True)

    ok, val = validate_decklist_url(url)
    if not ok:
        return await interaction.followup.send(val, ephemeral=True)

    try:
        if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
            return await interaction.followup.send(
                "❌ Apenas ADM, Organizador ou Owner podem usar este comando.",
                ephemeral=True
            )

        sh = open_sheet()
        season_id = get_current_season_id(sh)

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)

        pid = resolve_player_id_from_value(ws_players, jogador)
        if not pid:
            return await interaction.followup.send(
                "❌ Jogador não encontrado no cadastro.",
                ephemeral=True
            )

        if not player_active_in_cycle(ws_enr, season_id, cycle, pid):
            return await interaction.followup.send(
                "❌ O jogador precisa estar inscrito (ativo) neste ciclo para cadastrar decklist.",
                ephemeral=True
            )

        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER)
        rown = ensure_deck_row(ws_decks, season_id, cycle, pid)

        col = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        ws_decks.batch_update([
            {
                "range": f"{col_letter(col['decklist_url'])}{rown}",
                "values": [[val]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{rown}",
                "values": [[now_br_str()]]
            },
        ])
        cache_invalidate(ws_decks)

        nick = resolve_player_nick(ws_players, pid)
        await interaction.followup.send(f"✅ Decklist salva para **{nick}**.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# [BLOCO 6/8] — Termina aqui
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 7/8
# SUB-BLOCO: A/2
# RESUMO: Cabeçalho do bloco, constante de auto-confirmação, helpers de visualização,
# parser de placar V-D-E, autocomplete de /pods_ver e comandos /pods_ver e /meus_matches
# para visualização de PODs e matches do jogador.
# REVISÃO: integração com índices RAM de Seasons/Cycles/Players/Matches, redução de leituras
# indiretas no Sheets e melhor aproveitamento dos dados já carregados em memória
# para Players/Decks/Matches.
# =================================================

# =========================================================
# [BLOCO 7/8] — RESULTADOS + PODS/MATCHES DO JOGADOR
# =========================================================

AUTO_CONFIRM_HOURS = 48


# =========================================================
# Helpers de visualização
# =========================================================
def _player_display_name(nick_map: dict[str, str], pid: str) -> str:
    return nick_map.get(str(pid).strip(), str(pid).strip())

def _match_status_label(status: str) -> str:
    st = str(status or "").strip().lower()
    if st == "confirmed":
        return "confirmado"
    if st == "pending":
        return "pendente"
    if st == "rejected":
        return "rejeitado"
    return "aberto"

def _build_nick_map_from_records(rows: list[dict]) -> dict[str, str]:
    m: dict[str, str] = {}
    for r in rows:
        pid = str(r.get("discord_id", "")).strip()
        nick = str(r.get("nick", "")).strip()
        if pid:
            m[pid] = nick or pid
    return m


# =========================================================
# Parser de placar V-D-E
# =========================================================
def parse_vde(score: str):
    try:
        s = str(score or "").replace(" ", "")
        parts = s.split("-")
        if len(parts) != 3:
            return None

        v = safe_int(parts[0], -1)
        d = safe_int(parts[1], -1)
        e = safe_int(parts[2], -1)

        if v < 0 or d < 0 or e < 0:
            return None

        ok, _ = validate_3parts_rules(v, d, e)
        if not ok:
            return None

        return v, d, e

    except Exception:
        return None


# =========================================================
# AUTOCOMPLETE — /pods_ver
# =========================================================
async def ac_pods_ver_season(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        q = str(current or "").strip().lower()

        items = get_season_choices_fast(sh, query=q, limit=25)

        out: list[app_commands.Choice[int]] = []
        for item in items:
            sid = safe_int(item.get("season_id", 0), 0)
            label = str(item.get("label", "")).strip()
            if sid <= 0 or not label:
                continue
            out.append(app_commands.Choice(name=label[:100], value=sid))

        return out[:25]
    except Exception:
        return []


async def ac_pods_ver_cycle(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()

        season_selected = safe_int(getattr(interaction.namespace, "season", 0), 0)
        q = str(current or "").strip().lower()

        if season_selected <= 0:
            return []

        items = get_cycle_choices_fast(
            sh,
            season_id=season_selected,
            query=q,
            only_open=False,
            limit=25
        )

        out: list[app_commands.Choice[int]] = []
        for item in items:
            cyc = safe_int(item.get("value", 0), 0)
            label = str(item.get("label", "")).strip()
            if cyc <= 0 or not label:
                continue
            out.append(app_commands.Choice(name=label[:100], value=cyc))

        return out[:25]
    except Exception:
        return []


# =========================================================
# /pods_ver
# =========================================================
@client.tree.command(name="pods_ver", description="Mostra todos os PODs de uma season/ciclo.")
@app_commands.describe(season="Season", cycle="Número do ciclo")
@app_commands.autocomplete(season=ac_pods_ver_season, cycle=ac_pods_ver_cycle)
async def pods_ver(interaction: discord.Interaction, season: int, cycle: int):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)

        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        rows = cached_get_all_records(ws_pods, ttl_seconds=10)
        deck_rows = cached_get_all_records(ws_decks, ttl_seconds=10)
        nick_map = get_player_nick_map_fast(sh)

        deck_map: dict[tuple[int, int, str], dict[str, str]] = {}
        for r in deck_rows:
            sid = safe_int(r.get("season_id", 0), 0)
            cyc = safe_int(r.get("cycle", 0), 0)
            pid = str(r.get("player_id", "")).strip()
            if not pid:
                continue
            deck_map[(sid, cyc, pid)] = {
                "deck": str(r.get("deck", "")).strip(),
                "decklist_url": str(r.get("decklist_url", "")).strip(),
            }

        pods: dict[str, list[str]] = {}
        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue

            pod = str(r.get("pod", "")).strip()
            pid = str(r.get("player_id", "")).strip()
            if not pod or not pid:
                continue

            pods.setdefault(pod, []).append(pid)

        if not pods:
            return await interaction.followup.send(
                f"❌ Não há PODs gerados na **Season {season} / Ciclo {cycle}**.",
                ephemeral=True
            )

        def pod_sort_key(x: str):
            return safe_int(x, 999999)

        chunks = []
        current_chunk = f"📦 **PODs da Season {season} / Ciclo {cycle}**"
        limit = 1900

        for pod in sorted(pods.keys(), key=pod_sort_key):
            players = list(dict.fromkeys(pods[pod]))

            pod_block_lines = [f"\n**POD {pod}**"]
            for i, pid in enumerate(players, start=1):
                deck_info = deck_map.get((season, cycle, pid), {})
                deck_name = deck_info.get("deck", "") or "PENDENTE"
                decklist_url = deck_info.get("decklist_url", "") or "PENDENTE"

                player_block = [
                    f"{i}. **{_player_display_name(nick_map, pid)}**",
                    f"   Deck: {deck_name}",
                    f"   Decklist: <{decklist_url}>",
                ]
                pod_block_lines.extend(player_block)

            pod_block = "\n".join(pod_block_lines)

            if len(current_chunk) + len(pod_block) + 2 > limit:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                current_chunk = f"📦 **PODs da Season {season} / Ciclo {cycle}**\n{pod_block}".strip()
            else:
                current_chunk = f"{current_chunk}\n{pod_block}".strip()

        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        if not chunks:
            return await interaction.followup.send(
                f"❌ Não consegui montar a visualização dos PODs da **Season {season} / Ciclo {cycle}**.",
                ephemeral=True
            )

        await interaction.followup.send(chunks[0], ephemeral=True)
        for chunk in chunks[1:]:
            await interaction.followup.send(chunk, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_ver: {e}", ephemeral=True)


# =========================================================
# /meus_matches
# =========================================================
@client.tree.command(name="meus_matches", description="Lista seus matches da season/ciclo.")
@app_commands.describe(season="Season", cycle="Número do ciclo")
@app_commands.autocomplete(season=ac_pods_ver_season, cycle=ac_pods_ver_cycle)
async def meus_matches(interaction: discord.Interaction, season: int, cycle: int):
    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        nick_map = get_player_nick_map_fast(sh)
        uid = str(interaction.user.id).strip()

        rows = get_matches_for_player_fast(
            sh,
            player_id=uid,
            season_id=season,
            cycle=cycle,
            only_active=True
        )

        items = []
        for r in rows:
            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if uid not in (a, b):
                continue

            opp = b if uid == a else a
            a_w = safe_int(r.get("a_games_won", 0), 0)
            b_w = safe_int(r.get("b_games_won", 0), 0)
            d_g = safe_int(r.get("draw_games", 0), 0)

            if uid == a:
                my_score = f"{a_w}-{b_w}-{d_g}"
            else:
                my_score = f"{b_w}-{a_w}-{d_g}"

            items.append(
                f"• `{r.get('match_id', '')}` | POD {r.get('pod', '')} | "
                f"vs **{_player_display_name(nick_map, opp)}** | "
                f"status: **{_match_status_label(r.get('confirmed_status', ''))}** | "
                f"placar: **{my_score}**"
            )

        if not items:
            return await interaction.followup.send(
                f"❌ Você não possui matches na **Season {season} / Ciclo {cycle}**.",
                ephemeral=True
            )

        msg = f"🎮 **Seus matches na Season {season} / Ciclo {cycle}**\n" + "\n".join(items)
        await send_followup_chunks(interaction, msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /meus_matches: {e}", ephemeral=True)

# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 7/8
# SUB-BLOCO: B/2
# RESUMO: Comandos de interação com resultados de partidas: /resultado para reportar
# placar V-D-E e /rejeitar para contestar resultados pendentes dentro da janela de 48h.
# Inclui atualização de dados no Sheets, auto-confirm timer, envio de DM ao oponente
# com botões de confirmar/rejeitar e invalidação de cache.
# REVISÃO: integração com MATCH RAM INDEX, mantendo UX com "oponente" no comando,
# batch_update, invalidação correta do índice RAM e mesma lógica funcional.
# =================================================

# =========================================================
# /resultado
# =========================================================
@client.tree.command(name="resultado", description="Reporta resultado de um match (V-D-E).")
@app_commands.describe(oponente="Selecione seu oponente", placar="Formato V-D-E (ex: 2-1-0)")
@app_commands.autocomplete(oponente=ac_match_id_user_pending, placar=ac_score_vde)
async def resultado(interaction: discord.Interaction, oponente: str, placar: str):

    await interaction.response.defer(ephemeral=True)

    match_id = str(oponente).strip()
    parsed = parse_vde(placar)

    if not parsed:
        return await interaction.followup.send(
            "❌ Placar inválido. Use o formato **V-D-E** (ex: 2-1-0).",
            ephemeral=True
        )

    v, d, e = parsed

    try:
        sh = open_sheet()
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # usa índice RAM primeiro
        match_row = get_match_by_id_fast(sh, match_id)

        if not match_row:
            rows = cached_get_all_values(ws_matches, ttl_seconds=10)
        else:
            rows = None

        pid = str(interaction.user.id).strip()

        found_row = None

        if rows is not None:
            for idx in range(1, len(rows)):
                r = rows[idx]

                def getc(name):
                    ci = col[name]
                    return r[ci] if ci < len(r) else ""

                if str(getc("match_id")).strip() == match_id:
                    found_row = idx + 1
                    player_a = str(getc("player_a_id")).strip()
                    player_b = str(getc("player_b_id")).strip()
                    status = str(getc("confirmed_status")).strip().lower()
                    break
        else:
            player_a = match_row["player_a_id"]
            player_b = match_row["player_b_id"]
            status = match_row["confirmed_status"]

            # precisa achar linha real no sheet
            rows = cached_get_all_values(ws_matches, ttl_seconds=10)
            for idx in range(1, len(rows)):
                r = rows[idx]
                if str(r[col["match_id"]]).strip() == match_id:
                    found_row = idx + 1
                    break

        if not found_row:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        if pid not in (player_a, player_b):
            return await interaction.followup.send("❌ Você não participa deste match.", ephemeral=True)

        if status == "confirmed":
            return await interaction.followup.send("❌ Este match já está confirmado.", ephemeral=True)

        rown = found_row

        if pid == player_a:
            a_val = str(v)
            b_val = str(d)
            opponent_id = player_b
            my_wins = v
            opp_wins = d
        else:
            a_val = str(d)
            b_val = str(v)
            opponent_id = player_a
            my_wins = v
            opp_wins = d

        auto_confirm = auto_confirm_deadline_iso(utc_now_dt())
        updated_at = now_iso_utc()

        ws_matches.batch_update([
            {"range": f"{col_letter(col['a_games_won'])}{rown}", "values": [[a_val]]},
            {"range": f"{col_letter(col['b_games_won'])}{rown}", "values": [[b_val]]},
            {"range": f"{col_letter(col['draw_games'])}{rown}", "values": [[str(e)]]},
            {"range": f"{col_letter(col['result_type'])}{rown}", "values": [["normal"]]},
            {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["pending"]]},
            {"range": f"{col_letter(col['reported_by_id'])}{rown}", "values": [[pid]]},
            {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [[""]]},
            {"range": f"{col_letter(col['auto_confirm_at'])}{rown}", "values": [[auto_confirm]]},
            {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_matches)
        invalidate_match_ram_index()

        dm_status = ""

        try:
            user_level = await get_access_level(interaction)
        except Exception:
            user_level = "jogador"

        if user_level == "jogador":
            try:
                opponent_user = client.get_user(int(opponent_id))
                if opponent_user is None:
                    opponent_user = await client.fetch_user(int(opponent_id))

                reporter_name = interaction.user.display_name
                embed = discord.Embed(
                    title="Resultado reportado",
                    description=(
                        f"**{reporter_name}** registrou o resultado "
                        f"**{my_wins}-{opp_wins}-{e}**.\n"
                        f"O prazo para confirmar ou rejeitar é de **48h**."
                    )
                )

                embed.add_field(name="Match", value=f"`{match_id}`", inline=False)
                embed.set_footer(text=f"match_id:{match_id}")

                await opponent_user.send(embed=embed, view=ResultConfirmView())
                dm_status = "\n📩 O oponente foi notificado por DM."

            except Exception:
                dm_status = "\n⚠️ Não consegui enviar DM, mas o prazo segue."

        await interaction.followup.send(
            f"✅ Resultado enviado: **{placar}**\n"
            f"O oponente tem **48h** para confirmar ou rejeitar.{dm_status}",
            ephemeral=True
        )

        await log_admin(interaction, f"resultado reportado {match_id} {placar}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /resultado: {e}", ephemeral=True)


# =========================================================
# /rejeitar
# =========================================================
@client.tree.command(name="rejeitar", description="Rejeita um resultado pendente.")
@app_commands.describe(oponente="Selecione seu oponente")
@app_commands.autocomplete(oponente=ac_match_id_user_pending)
async def rejeitar(interaction: discord.Interaction, oponente: str):

    await interaction.response.defer(ephemeral=True)

    match_id = str(oponente).strip()

    try:
        sh = open_sheet()
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        match_row = get_match_by_id_fast(sh, match_id)

        if not match_row:
            rows = cached_get_all_values(ws_matches, ttl_seconds=10)
        else:
            rows = None

        pid = str(interaction.user.id).strip()
        found_row = None

        if rows is not None:
            for idx in range(1, len(rows)):
                r = rows[idx]
                if str(r[col["match_id"]]).strip() == match_id:
                    found_row = idx + 1
                    player_a = str(r[col["player_a_id"]]).strip()
                    player_b = str(r[col["player_b_id"]]).strip()
                    status = str(r[col["confirmed_status"]]).strip().lower()
                    reported_by = str(r[col["reported_by_id"]]).strip()
                    break
        else:
            player_a = match_row["player_a_id"]
            player_b = match_row["player_b_id"]
            status = match_row["confirmed_status"]
            reported_by = match_row["reported_by_id"]

            rows = cached_get_all_values(ws_matches, ttl_seconds=10)
            for idx in range(1, len(rows)):
                if str(rows[idx][col["match_id"]]).strip() == match_id:
                    found_row = idx + 1
                    break

        if not found_row:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        if pid not in (player_a, player_b):
            return await interaction.followup.send("❌ Você não participa deste match.", ephemeral=True)

        if reported_by == pid:
            return await interaction.followup.send(
                "❌ Quem reportou não pode rejeitar.",
                ephemeral=True
            )

        if status != "pending":
            return await interaction.followup.send(
                "❌ Este match não está pendente.",
                ephemeral=True
            )

        rown = found_row
        updated_at = now_iso_utc()

        ws_matches.batch_update([
            {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["rejected"]]},
            {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [[pid]]},
            {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_matches)
        invalidate_match_ram_index()

        await interaction.followup.send(
            "⚠️ Resultado rejeitado. Reporte novamente.",
            ephemeral=True
        )

        await log_admin(interaction, f"resultado rejeitado {match_id}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================================================
# [BLOCO 7/8 termina aqui]
# =================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: A/7
# RESUMO: Cabeçalho do bloco, helper para exigir season ativa, helpers de standings/ranking
# e comandos administrativos iniciais: /forcesync, /ciclo_abrir, /ciclo_fechar e /ciclo_encerrar.
# REVISÃO: remoção de invalidação redundante de cache em Cycles e ajuste leve de TTL
# para reduzir pressão no Google Sheets, mantendo a mesma lógica funcional.
# =================================================

# =========================================================
# [BLOCO 8/8] — ADMIN FINAL + PRAZO + RANKINGS + EXPORT + START
# =========================================================

def require_current_season(sh) -> int:
    sid = get_current_season_id(sh)
    if sid <= 0:
        raise RuntimeError("Não existe season ativa.")
    return sid


# =========================================================
# Helpers de standings/ranking
# =========================================================
def _read_cycle_standings(ws_standings, season_id: int, cycle: int) -> list[dict]:
    rows = cached_get_all_records(ws_standings, ttl_seconds=10)
    out = []
    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        out.append(r)
    out.sort(key=lambda x: safe_int(x.get("rank_position", 999999), 999999))
    return out

def _format_standings_text(rows: list[dict], nick_map: dict[str, str], season_id: int, cycle: int, top: int = 30) -> str:
    top = max(1, min(top, 100))
    lines = [f"🏆 **Ranking do Ciclo {cycle}** | Season {season_id}"]
    lines.append("pos | jogador | pts | OMW | GW | OGW | J")
    lines.append("--- | ------ | --- | --- | --- | --- | ---")

    for r in rows[:top]:
        pid = str(r.get("player_id", "")).strip()
        lines.append(
            f"{safe_int(r.get('rank_position', 0), 0)} | "
            f"{nick_map.get(pid, pid)} | "
            f"{safe_int(r.get('match_points', 0), 0)} | "
            f"{r.get('omw_percent', 0)} | "
            f"{r.get('gw_percent', 0)} | "
            f"{r.get('ogw_percent', 0)} | "
            f"{safe_int(r.get('matches_played', 0), 0)}"
        )
    return "\n".join(lines)

def _cycle_has_generated_data(ws_pods, ws_matches, season_id: int, cycle: int) -> bool:
    for r in cached_get_all_records(ws_pods, ttl_seconds=10):
        if safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle:
            return True
    for r in cached_get_all_records(ws_matches, ttl_seconds=10):
        if safe_int(r.get("season_id", 0), 0) == season_id and safe_int(r.get("cycle", 0), 0) == cycle:
            return True
    return False


# =========================================================
# /forcesync
# =========================================================
@client.tree.command(name="forcesync", description="(ADM) Sincroniza os comandos do bot no servidor.")
async def forcesync(interaction: discord.Interaction):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            synced = await client.tree.sync(guild=guild)
            return await interaction.followup.send(
                f"✅ Sync concluído no servidor.\nComandos sincronizados: **{len(synced)}**",
                ephemeral=True
            )

        synced = await client.tree.sync()
        await interaction.followup.send(
            f"✅ Sync global concluído.\nComandos sincronizados: **{len(synced)}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /forcesync: {e}", ephemeral=True)


# =========================================================
# /ciclo_abrir
# =========================================================
@client.tree.command(name="ciclo_abrir", description="(ADM) Abre um ciclo para inscrições.")
@app_commands.describe(cycle="Número do ciclo")
async def ciclo_abrir(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        ensure_all_sheets(sh)

        ws_cycles = sh.worksheet("Cycles")
        set_cycle_status(ws_cycles, season_id, cycle, "open")

        await interaction.followup.send(f"✅ Ciclo **{cycle}** aberto para inscrições.", ephemeral=True)
        await log_admin(interaction, f"ciclo_abrir S{season_id} C{cycle}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ciclo_abrir: {e}", ephemeral=True)


# =========================================================
# /ciclo_fechar
# =========================================================
@client.tree.command(name="ciclo_fechar", description="(ADM) Fecha inscrições do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
async def ciclo_fechar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        ensure_all_sheets(sh)

        ws_cycles = sh.worksheet("Cycles")
        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ O ciclo {cycle} não existe.", ephemeral=True)

        set_cycle_status(ws_cycles, season_id, cycle, "locked")

        await interaction.followup.send(f"✅ Inscrições do ciclo **{cycle}** fechadas.", ephemeral=True)
        await log_admin(interaction, f"ciclo_fechar S{season_id} C{cycle}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ciclo_fechar: {e}", ephemeral=True)


# =========================================================
# /ciclo_encerrar
# =========================================================
@client.tree.command(name="ciclo_encerrar", description="(ADM) Encerra o ciclo (completed).")
@app_commands.describe(cycle="Número do ciclo")
async def ciclo_encerrar(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)

        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ O ciclo {cycle} não existe.", ephemeral=True)

        set_cycle_status(ws_cycles, season_id, cycle, "completed")

        await interaction.followup.send(f"✅ Ciclo **{cycle}** encerrado.", ephemeral=True)
        await log_admin(interaction, f"ciclo_encerrar S{season_id} C{cycle}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ciclo_encerrar: {e}", ephemeral=True)

# =================================================
# FIM DO SUB-BLOCO A/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: B/7
# RESUMO: Comandos relacionados a prazo, pendências e ranking do ciclo, incluindo
# /prazo, /deadline, /recalcular, /ranking e /standings_publicar.
# REVISÃO: melhor aproveitamento de cache, redução de releitura indireta de Players
# e pequeno ajuste de TTL para diminuir pressão no Google Sheets.
# =================================================

# =========================================================
# /prazo
# =========================================================
@client.tree.command(name="prazo", description="Mostra o prazo oficial do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def prazo(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)

        start_br, end_br, max_pod, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)

        if not start_br or not end_br:
            return await interaction.followup.send(
                "⚠️ Não consegui determinar o prazo deste ciclo ainda.\n"
                "Isso costuma acontecer quando o ciclo não tem PodsHistory ou quando o ciclo ainda não foi travado.",
                ephemeral=False
            )

        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        if (not (cf.get("start_at_br") or "").strip()) or (not (cf.get("deadline_at_br") or "").strip()):
            set_cycle_times(ws_cycles, season_id, cycle, start_br, end_br)
            cache_invalidate(ws_cycles)

        msg = (
            f"⏳ **Prazo do Ciclo {cycle}** (Season {season_id})\n"
            f"- Início: **{start_br} (BR)**\n"
            f"- Fim: **{end_br} (BR)**\n"
            f"- Regra aplicada: **{days} dias** (maior pod = **{max_pod}** jogador(es))\n"
            f"- Lembrete: resultados até **13:59 (BR)** do último dia."
        )
        await interaction.followup.send(msg, ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /prazo: {e}", ephemeral=False)


# =========================================================
# /deadline
# =========================================================
@client.tree.command(name="deadline", description="(ADM) Lista matches pending próximos de expirar.")
@app_commands.describe(cycle="Número do ciclo", horas="Janela em horas (1..48)")
@app_commands.autocomplete(cycle=ac_cycle_open)
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

        rows = cached_get_all_records(ws, ttl_seconds=10)
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

        msg = "⏰ Pendências próximas de expirar:\n" + "\n".join(items[:200])
        await send_followup_chunks(interaction, msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /recalcular
# =========================================================
@client.tree.command(name="recalcular", description="(ADM) Auto-confirma pendências vencidas e recalcula standings do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def recalcular(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        auto = sweep_auto_confirm(sh, season_id, cycle)
        rows = recalculate_cycle(sh, season_id, cycle)

        await interaction.followup.send(
            f"✅ Recalculo concluído.\n"
            f"- Auto-confirmados: **{auto}**\n"
            f"- Linhas standings geradas: **{len(rows)}**",
            ephemeral=True
        )

        await log_admin(interaction, f"recalcular: S{season_id} C{cycle} | auto={auto} | standings={len(rows)}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /ranking
# =========================================================
@client.tree.command(name="ranking", description="Mostra o ranking do ciclo.")
@app_commands.describe(cycle="Número do ciclo", top="Quantidade de jogadores")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def ranking(interaction: discord.Interaction, cycle: int, top: int = 30):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_standings = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)

        rows = _read_cycle_standings(ws_standings, season_id, cycle)
        if not rows:
            return await interaction.followup.send(
                "⚠️ Não há standings para este ciclo ainda. Use `/recalcular` primeiro.",
                ephemeral=False
            )

        player_rows = cached_get_all_records(ws_players, ttl_seconds=10)
        nick_map = _build_nick_map_from_records(player_rows)

        text = _format_standings_text(rows, nick_map, season_id, cycle, top=top)
        await send_followup_chunks(interaction, text, ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /ranking: {e}", ephemeral=False)


# =========================================================
# /standings_publicar
# =========================================================
@client.tree.command(name="standings_publicar", description="(ADM) Publica standings no canal configurado.")
@app_commands.describe(cycle="Número do ciclo", top="Quantidade de jogadores")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def standings_publicar(interaction: discord.Interaction, cycle: int, top: int = 30):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_standings = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)

        rows = _read_cycle_standings(ws_standings, season_id, cycle)
        if not rows:
            return await interaction.followup.send("⚠️ Não há standings para este ciclo ainda.", ephemeral=True)

        player_rows = cached_get_all_records(ws_players, ttl_seconds=10)
        nick_map = _build_nick_map_from_records(player_rows)

        text = _format_standings_text(rows, nick_map, season_id, cycle, top=top)

        target_channel = None
        if RANKING_CHANNEL_ID and interaction.guild:
            target_channel = interaction.guild.get_channel(RANKING_CHANNEL_ID)
            if not target_channel:
                try:
                    target_channel = await interaction.guild.fetch_channel(RANKING_CHANNEL_ID)
                except Exception:
                    target_channel = None

        if not target_channel:
            target_channel = interaction.channel

        chunks = split_text_lines(text, limit=1900)
        for c in chunks:
            await target_channel.send(c)

        await interaction.followup.send("✅ Standings publicados.", ephemeral=True)
        await log_admin(interaction, f"standings_publicar S{season_id} C{cycle}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /standings_publicar: {e}", ephemeral=True)

# =================================================
# FIM DO SUB-BLOCO B/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: C
# RESUMO: Comandos administrativos de encerramento de resultados e gestão de matches:
# /final, /admin_resultado_editar, /admin_resultado_cancelar, /status_ciclo e /ranking_geral.
# REVISÃO: integração com os índices RAM de matches/autocomplete, redução de escritas
# isoladas no Sheets, melhor aproveitamento dos dados já carregados e eliminação
# de releituras desnecessárias da aba Cycles/Players.
# =================================================

# =========================================================
# /final
# =========================================================
@client.tree.command(name="final", description="(ADM) Aplica 0-0-3 após deadline do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
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

        _, end_br, _, _ = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)

        if not end_br:
            return await interaction.followup.send("❌ Ciclo sem prazo definido.", ephemeral=True)

        deadline_dt = parse_br_dt(end_br)
        if deadline_dt and now_br_dt() < deadline_dt:
            return await interaction.followup.send("❌ Deadline ainda não chegou.", ephemeral=True)

        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = cached_get_all_values(ws_matches, ttl_seconds=10)

        changed = 0
        updates = []
        updated_at = now_iso_utc()

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

            updates.extend([
                {
                    "range": f"{col_letter(col['a_games_won'])}{rown}",
                    "values": [["0"]]
                },
                {
                    "range": f"{col_letter(col['b_games_won'])}{rown}",
                    "values": [["0"]]
                },
                {
                    "range": f"{col_letter(col['draw_games'])}{rown}",
                    "values": [["3"]]
                },
                {
                    "range": f"{col_letter(col['result_type'])}{rown}",
                    "values": [["intentional_draw"]]
                },
                {
                    "range": f"{col_letter(col['confirmed_status'])}{rown}",
                    "values": [["confirmed"]]
                },
                {
                    "range": f"{col_letter(col['confirmed_by_id'])}{rown}",
                    "values": [["AUTO_FINAL"]]
                },
                {
                    "range": f"{col_letter(col['updated_at'])}{rown}",
                    "values": [[updated_at]]
                },
            ])
            changed += 1

        if changed:
            ws_matches.batch_update(updates)
            cache_invalidate(ws_matches)
            invalidate_match_ram_index()
            invalidate_match_ac_index()

        await interaction.followup.send(f"✅ FINAL aplicado. {changed} matches ajustados.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# /admin_resultado_editar
# =========================================================
@client.tree.command(name="admin_resultado_editar", description="(ADM) Edita e confirma um resultado.")
@app_commands.describe(match_id="ID do match", placar="Formato V-D-E")
@app_commands.autocomplete(placar=ac_score_vde)
async def admin_resultado_editar(interaction: discord.Interaction, match_id: str, placar: str):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    parsed = parse_score_3parts(placar)
    if not parsed:
        return await interaction.followup.send("❌ Placar inválido. Use V-D-E, ex: 2-1-0.", ephemeral=True)

    v, d, e = parsed
    ok, msg = validate_3parts_rules(v, d, e)
    if not ok:
        return await interaction.followup.send(f"❌ {msg}", ephemeral=True)

    try:
        sh = open_sheet()
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = cached_get_all_values(ws_matches, ttl_seconds=10)

        found = None
        for idx in range(1, len(rows)):
            r = rows[idx]
            val = r[col["match_id"]] if col["match_id"] < len(r) else ""
            if str(val).strip() == match_id:
                found = idx + 1
                break

        if not found:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        ws_matches.batch_update([
            {
                "range": f"{col_letter(col['a_games_won'])}{found}",
                "values": [[str(v)]]
            },
            {
                "range": f"{col_letter(col['b_games_won'])}{found}",
                "values": [[str(d)]]
            },
            {
                "range": f"{col_letter(col['draw_games'])}{found}",
                "values": [[str(e)]]
            },
            {
                "range": f"{col_letter(col['result_type'])}{found}",
                "values": [["normal"]]
            },
            {
                "range": f"{col_letter(col['confirmed_status'])}{found}",
                "values": [["confirmed"]]
            },
            {
                "range": f"{col_letter(col['confirmed_by_id'])}{found}",
                "values": [[str(interaction.user.id)]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{found}",
                "values": [[now_iso_utc()]]
            },
        ])
        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

        await interaction.followup.send(f"✅ Resultado editado e confirmado: **{placar}**", ephemeral=True)
        await log_admin(interaction, f"admin_resultado_editar {match_id} {placar}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /admin_resultado_editar: {e}", ephemeral=True)


# =========================================================
# /admin_resultado_cancelar
# =========================================================
@client.tree.command(name="admin_resultado_cancelar", description="(ADM) Cancela um resultado e reabre o match.")
@app_commands.describe(match_id="ID do match")
async def admin_resultado_cancelar(interaction: discord.Interaction, match_id: str):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = cached_get_all_values(ws_matches, ttl_seconds=10)

        found = None
        for idx in range(1, len(rows)):
            r = rows[idx]
            val = r[col["match_id"]] if col["match_id"] < len(r) else ""
            if str(val).strip() == match_id:
                found = idx + 1
                break

        if not found:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        ws_matches.batch_update([
            {
                "range": f"{col_letter(col['a_games_won'])}{found}",
                "values": [["0"]]
            },
            {
                "range": f"{col_letter(col['b_games_won'])}{found}",
                "values": [["0"]]
            },
            {
                "range": f"{col_letter(col['draw_games'])}{found}",
                "values": [["0"]]
            },
            {
                "range": f"{col_letter(col['result_type'])}{found}",
                "values": [["normal"]]
            },
            {
                "range": f"{col_letter(col['confirmed_status'])}{found}",
                "values": [["open"]]
            },
            {
                "range": f"{col_letter(col['reported_by_id'])}{found}",
                "values": [[""]]
            },
            {
                "range": f"{col_letter(col['confirmed_by_id'])}{found}",
                "values": [[""]]
            },
            {
                "range": f"{col_letter(col['auto_confirm_at'])}{found}",
                "values": [[""]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{found}",
                "values": [[now_iso_utc()]]
            },
        ])
        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

        await interaction.followup.send("✅ Resultado cancelado. Match reaberto.", ephemeral=True)
        await log_admin(interaction, f"admin_resultado_cancelar {match_id}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /admin_resultado_cancelar: {e}", ephemeral=True)


# =========================================================
# /status_ciclo
# =========================================================
@client.tree.command(name="status_ciclo", description="(ADM) Mostra status dos ciclos da season atual.")
async def status_ciclo(interaction: discord.Interaction):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)

        items = list_cycles(ws_cycles, season_id)
        if not items:
            return await interaction.followup.send("⚠️ Não há ciclos cadastrados nesta season.", ephemeral=True)

        cycle_rows = cached_get_all_records(ws_cycles, ttl_seconds=10)
        cycle_fields_map: dict[int, dict] = {}
        for r in cycle_rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            cyc = safe_int(r.get("cycle", 0), 0)
            if cyc <= 0:
                continue
            cycle_fields_map[cyc] = {
                "start_at_br": str(r.get("start_at_br", "")).strip(),
                "deadline_at_br": str(r.get("deadline_at_br", "")).strip(),
            }

        lines = [f"📘 **Status dos ciclos** | Season {season_id}"]
        for c, st in items:
            cf = cycle_fields_map.get(c, {})
            lines.append(
                f"• Ciclo {c} | status: **{st}** | "
                f"início: `{cf.get('start_at_br', '') or '-'}` | "
                f"prazo: `{cf.get('deadline_at_br', '') or '-'}`"
            )

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /status_ciclo: {e}", ephemeral=True)


# =========================================================
# /ranking_geral
# =========================================================
@client.tree.command(name="ranking_geral", description="Mostra ranking geral (agregado) da season atual.")
@app_commands.describe(top="Quantidade de jogadores (10..60)")
async def ranking_geral(interaction: discord.Interaction, top: int = 30):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        rows = get_matches_for_cycle_fast(sh, season_id=season_id, cycle=None, only_active=False) if False else None
        # ranking geral agrega toda a season, então mantém leitura da season inteira
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = cached_get_all_records(ws_matches, ttl_seconds=10)

        stats: dict[str, dict] = {}
        opps_map: dict[str, list[str]] = {}

        def ensure_player(pid: str):
            if pid not in stats:
                stats[pid] = {"pts": 0, "matches": 0, "gw": 0, "gl": 0, "gd": 0}
            if pid not in opps_map:
                opps_map[pid] = []

        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if not as_bool(r.get("active", "TRUE")):
                continue
            if str(r.get("result_type", "")).strip().lower() == "bye":
                continue
            if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if not a or not b:
                continue

            ensure_player(a)
            ensure_player(b)

            a_w = safe_int(r.get("a_games_won", 0), 0)
            b_w = safe_int(r.get("b_games_won", 0), 0)
            d_g = safe_int(r.get("draw_games", 0), 0)

            stats[a]["gw"] += a_w
            stats[a]["gl"] += b_w
            stats[a]["gd"] += d_g

            stats[b]["gw"] += b_w
            stats[b]["gl"] += a_w
            stats[b]["gd"] += d_g

            stats[a]["matches"] += 1
            stats[b]["matches"] += 1

            if a_w > b_w:
                stats[a]["pts"] += 3
            elif b_w > a_w:
                stats[b]["pts"] += 3
            else:
                stats[a]["pts"] += 1
                stats[b]["pts"] += 1

            opps_map[a].append(b)
            opps_map[b].append(a)

        if not stats:
            return await interaction.followup.send("Sem matches confirmados.", ephemeral=False)

        mwp = {}
        gwp = {}
        for pid, s in stats.items():
            m = s["matches"]
            mwp[pid] = 1/3 if m <= 0 else floor_333(s["pts"] / (3 * m))

            games = s["gw"] + s["gl"] + s["gd"]
            gwp[pid] = 1/3 if games <= 0 else floor_333((s["gw"] + 0.5 * s["gd"]) / games)

        omw = {}
        ogw = {}
        for pid in stats.keys():
            opps = opps_map.get(pid, [])
            if not opps:
                omw[pid] = 1/3
                ogw[pid] = 1/3
            else:
                omw[pid] = sum(mwp.get(o, 1/3) for o in opps) / len(opps)
                ogw[pid] = sum(gwp.get(o, 1/3) for o in opps) / len(opps)

        table = []
        for pid, s in stats.items():
            table.append({
                "pid": pid,
                "pts": s["pts"],
                "mwp": pct1(mwp[pid]),
                "omw": pct1(omw[pid]),
                "gw": pct1(gwp[pid]),
                "ogw": pct1(ogw[pid]),
                "j": s["matches"],
            })

        table.sort(key=lambda r: (r["mwp"], r["omw"], r["gw"], r["ogw"], r["pts"]), reverse=True)

        top = max(10, min(top, 60))
        out = [f"🏆 Ranking Geral — Season {season_id} (Top {top})"]
        out.append("pos | jogador | MWP | pts | OMW | GW | OGW | J")
        out.append("--- | ------ | --- | --- | --- | --- | --- | ---")

        nick_map = get_player_nick_map_fast(sh)

        for i, r in enumerate(table[:top], 1):
            out.append(
                f"{i} | {nick_map.get(r['pid'], r['pid'])} | {r['mwp']} | {r['pts']} | {r['omw']} | {r['gw']} | {r['ogw']} | {r['j']}"
            )

        await send_followup_chunks(interaction, "\n".join(out), ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=False)


# =================================================
# FIM DO SUB-BLOCO C/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: D/7
# RESUMO: Comandos administrativos de temporada e cadastro manual de jogadores.
# Ajustado para permitir uso de /cadastrar_player por ADM, Organizador e Owner.
# Nenhuma outra lógica foi alterada.
# REVISÃO: adoção de cache em leitura de Seasons, integração com índices RAM de
# Seasons/Cycles/Players, debounce leve nos autocompletes e redução de updates
# isolados no bloco Players e Decks do /cadastrar_player.
# =================================================

# =========================================================
# OWNER — START/CLOSE SEASON + CADASTRAR PLAYER + START_CYCLE
# =========================================================
def _next_season_id(sh) -> int:
    ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
    rows = cached_get_all_records(ws, ttl_seconds=10)
    mx = 0
    for r in rows:
        mx = max(mx, safe_int(r.get("season_id", 0), 0))
    return mx + 1 if mx > 0 else 1


@client.tree.command(name="startseason", description="(OWNER) Abre uma nova season e define como ativa.")
@app_commands.describe(nome="Nome opcional da season (ex: Season 3)")
async def startseason(interaction: discord.Interaction, nome: str = ""):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ensure_all_sheets(sh)

        new_id = _next_season_id(sh)
        season_name = (nome or f"Temporada {new_id}").strip()

        set_season_status(sh, new_id, "open", name=season_name)
        close_all_other_seasons(sh, keep_open_id=new_id)
        set_current_season_id(sh, new_id)
        invalidate_season_ram_index()

        await interaction.followup.send(
            f"✅ Season aberta e ativa: **{season_name}** (ID {new_id}).",
            ephemeral=True
        )
        await log_admin(interaction, f"OWNER startseason: opened S{new_id} name='{season_name}'")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /startseason: {e}", ephemeral=True)


@client.tree.command(name="closeseason", description="(OWNER) Fecha a season atual (desativa).")
async def closeseason(interaction: discord.Interaction):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        sid = get_current_season_id(sh)
        if sid <= 0:
            return await interaction.followup.send("⚠️ Não existe season ativa.", ephemeral=True)

        set_season_status(sh, sid, "closed")
        set_current_season_id(sh, 0)
        invalidate_season_ram_index()

        await interaction.followup.send(f"✅ Season **{sid}** fechada.", ephemeral=True)
        await log_admin(interaction, f"OWNER closeseason: closed S{sid}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /closeseason: {e}", ephemeral=True)


# =========================================================
# HELPERS AUTOCOMPLETE — OWNER /cadastrar_player
# =========================================================
async def ac_owner_season(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_owner_season"):
            return []

        sh = open_sheet()
        q = str(current or "").strip().lower()

        items = get_season_choices_fast(sh, query=q, limit=25)

        out: list[app_commands.Choice[int]] = []
        for item in items:
            sid = safe_int(item.get("season_id", 0), 0)
            label = str(item.get("label", "")).strip()
            if sid <= 0 or not label:
                continue
            out.append(app_commands.Choice(name=label[:100], value=sid))

        return out[:25]
    except Exception:
        return []


async def ac_owner_cycle_for_season(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_owner_cycle_for_season"):
            return []

        sh = open_sheet()

        season_selected = safe_int(getattr(interaction.namespace, "season", 0), 0)
        q = str(current or "").strip().lower()

        if season_selected <= 0:
            return []

        items = get_cycle_choices_fast(
            sh,
            season_id=season_selected,
            query=q,
            only_open=False,
            limit=25
        )

        out: list[app_commands.Choice[int]] = []
        for item in items:
            cyc = safe_int(item.get("value", 0), 0)
            label = str(item.get("label", "")).strip()
            if cyc <= 0 or not label:
                continue
            out.append(app_commands.Choice(name=label[:100], value=cyc))

        return out[:25]
    except Exception:
        return []


@client.tree.command(name="cadastrar_player", description="(ADM/Organizador/Owner) Cadastra player manualmente com season, ciclo, inscrição, deck e decklist.")
@app_commands.describe(
    membro="Selecione o usuário no Discord",
    nick="Nome e Sobrenome (sem abreviações)",
    season="Season para cadastrar",
    ciclo="Ciclo para cadastrar",
    deck="Nome do deck",
    decklist="Link (moxfield/ligamagic)"
)
@app_commands.autocomplete(season=ac_owner_season, ciclo=ac_owner_cycle_for_season)
async def cadastrar_player(
    interaction: discord.Interaction,
    membro: discord.Member,
    nick: str,
    season: int,
    ciclo: int,
    deck: str,
    decklist: str
):
    if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
        return await interaction.response.send_message(
            "❌ Apenas ADM, Organizador ou Owner podem usar este comando.",
            ephemeral=True
        )

    await interaction.response.defer(ephemeral=True)

    try:
        raw = " ".join(str(nick or "").strip().split())
        if len(raw.split()) < 2:
            return await interaction.followup.send("❌ Informe Nome e Sobrenome (ex: João Silva).", ephemeral=True)

        sh = open_sheet()
        ensure_all_sheets(sh)

        if not season_exists(sh, season):
            return await interaction.followup.send(f"❌ A season {season} não existe.", ephemeral=True)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        cf = get_cycle_fields(ws_cycles, season, ciclo)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ O ciclo {ciclo} não existe na season {season}.", ephemeral=True)

        did = str(membro.id)
        nowc = now_br_str()

        # =========================
        # PLAYERS
        # =========================
        ws_players = sh.worksheet("Players")
        col_players = ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        rows_players = cached_get_all_values(ws_players, ttl_seconds=10)

        found_row = None
        for i in range(2, len(rows_players) + 1):
            r = rows_players[i - 1]
            val = r[col_players["discord_id"]] if col_players["discord_id"] < len(r) else ""
            if str(val).strip() == did:
                found_row = i
                break

        if found_row:
            ws_players.batch_update([
                {
                    "range": f"{col_letter(col_players['nick'])}{found_row}",
                    "values": [[raw]]
                },
                {
                    "range": f"{col_letter(col_players['name'])}{found_row}",
                    "values": [[raw]]
                },
                {
                    "range": f"{col_letter(col_players['status'])}{found_row}",
                    "values": [["active"]]
                },
                {
                    "range": f"{col_letter(col_players['updated_at'])}{found_row}",
                    "values": [[nowc]]
                },
            ])
        else:
            row = [""] * len(PLAYERS_HEADER)
            row[col_players["discord_id"]] = did
            row[col_players["nick"]] = raw
            row[col_players["name"]] = raw
            row[col_players["status"]] = "active"
            row[col_players["created_at"]] = nowc
            row[col_players["updated_at"]] = nowc
            ws_players.append_row(row, value_input_option="USER_ENTERED")

        cache_invalidate(ws_players)
        invalidate_player_ram_index()

        msg_parts = [f"✅ Player cadastrado/atualizado: **{raw}** ({membro.id})"]

        # =========================
        # ENROLLMENTS
        # =========================
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        col_enr = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        rows_enr = cached_get_all_values(ws_enr, ttl_seconds=10)

        enr_row = None
        for i in range(2, len(rows_enr) + 1):
            r = rows_enr[i - 1]
            s = safe_int(r[col_enr["season_id"]] if col_enr["season_id"] < len(r) else 0, 0)
            c = safe_int(r[col_enr["cycle"]] if col_enr["cycle"] < len(r) else 0, 0)
            p = str(r[col_enr["player_id"]] if col_enr["player_id"] < len(r) else "").strip()
            if s == season and c == ciclo and p == did:
                enr_row = i
                break

        if enr_row is None:
            ws_enr.append_row(
                [str(season), str(ciclo), did, "active", nowc, nowc],
                value_input_option="USER_ENTERED"
            )
            msg_parts.append(f"- Inscrição criada na **Season {season} / Ciclo {ciclo}**.")
        else:
            ws_enr.batch_update([
                {
                    "range": f"{col_letter(col_enr['status'])}{enr_row}",
                    "values": [["active"]]
                },
                {
                    "range": f"{col_letter(col_enr['updated_at'])}{enr_row}",
                    "values": [[nowc]]
                },
            ])
            msg_parts.append(f"- Inscrição reativada/confirmada na **Season {season} / Ciclo {ciclo}**.")

        cache_invalidate(ws_enr)

        # =========================
        # DECKS
        # =========================
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)
        rown = ensure_deck_row(ws_decks, season, ciclo, did)
        col_deck = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        nm = str(deck or "").strip()
        if not nm:
            return await interaction.followup.send("❌ Informe o deck.", ephemeral=True)
        if len(nm) > 80:
            return await interaction.followup.send("❌ Nome de deck inválido (1 a 80 caracteres).", ephemeral=True)

        ok, val = validate_decklist_url(decklist)
        if not ok:
            return await interaction.followup.send(f"❌ Decklist inválida: {val}", ephemeral=True)

        ws_decks.batch_update([
            {
                "range": f"{col_letter(col_deck['deck'])}{rown}",
                "values": [[nm]]
            },
            {
                "range": f"{col_letter(col_deck['decklist_url'])}{rown}",
                "values": [[val]]
            },
            {
                "range": f"{col_letter(col_deck['updated_at'])}{rown}",
                "values": [[nowc]]
            },
        ])
        cache_invalidate(ws_decks)

        msg_parts.append(f"- Deck setado: **{nm}**")
        msg_parts.append("- Decklist setada.")
        msg_parts.append(f"- Referência final: **Season {season} / Ciclo {ciclo}**")

        await interaction.followup.send("\n".join(msg_parts), ephemeral=True)
        await log_admin(interaction, f"cadastrar_player: {raw} ({membro.id}) season={season} ciclo={ciclo}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /cadastrar_player: {e}", ephemeral=True)


# =================================================
# FIM DO SUB-BLOCO D/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: E/7
# RESUMO: Finalização do comando /cadastrar_player (Enrollments e Decks)
# e início das regras do sistema de geração de pods e layout de pods
# utilizadas posteriormente pelo comando /start_cycle.
# REVISÃO: redução de updates isolados no trecho final de Decks do
# /cadastrar_player, mantendo a mesma lógica funcional.
# =================================================

        # =========================
        # ENROLLMENTS
        # =========================
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        col_enr = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        rows_enr = cached_get_all_values(ws_enr, ttl_seconds=10)

        enr_row = None
        for i in range(2, len(rows_enr) + 1):
            r = rows_enr[i - 1]
            s = safe_int(r[col_enr["season_id"]] if col_enr["season_id"] < len(r) else 0, 0)
            c = safe_int(r[col_enr["cycle"]] if col_enr["cycle"] < len(r) else 0, 0)
            p = str(r[col_enr["player_id"]] if col_enr["player_id"] < len(r) else "").strip()
            if s == season and c == ciclo and p == did:
                enr_row = i
                break

        if enr_row is None:
            ws_enr.append_row(
                [str(season), str(ciclo), did, "active", nowc, nowc],
                value_input_option="USER_ENTERED"
            )
            msg_parts.append(f"- Inscrição criada na **Season {season} / Ciclo {ciclo}**.")
        else:
            ws_enr.update([["active"]], range_name=f"{col_letter(col_enr['status'])}{enr_row}")
            ws_enr.update([[nowc]], range_name=f"{col_letter(col_enr['updated_at'])}{enr_row}")
            msg_parts.append(f"- Inscrição reativada/confirmada na **Season {season} / Ciclo {ciclo}**.")

        cache_invalidate(ws_enr)

        # =========================
        # DECKS
        # =========================
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)
        rown = ensure_deck_row(ws_decks, season, ciclo, did)
        col_deck = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        nm = str(deck or "").strip()
        if not nm:
            return await interaction.followup.send("❌ Informe o deck.", ephemeral=True)
        if len(nm) > 80:
            return await interaction.followup.send("❌ Nome de deck inválido (1 a 80 caracteres).", ephemeral=True)

        ok, val = validate_decklist_url(decklist)
        if not ok:
            return await interaction.followup.send(f"❌ Decklist inválida: {val}", ephemeral=True)

        ws_decks.batch_update([
            {
                "range": f"{col_letter(col_deck['deck'])}{rown}",
                "values": [[nm]]
            },
            {
                "range": f"{col_letter(col_deck['decklist_url'])}{rown}",
                "values": [[val]]
            },
            {
                "range": f"{col_letter(col_deck['updated_at'])}{rown}",
                "values": [[nowc]]
            },
        ])
        cache_invalidate(ws_decks)

        msg_parts.append(f"- Deck setado: **{nm}**")
        msg_parts.append("- Decklist setada.")
        msg_parts.append(f"- Referência final: **Season {season} / Ciclo {ciclo}**")

        await interaction.followup.send("\n".join(msg_parts), ephemeral=True)
        await log_admin(interaction, f"OWNER cadastrar_player: {raw} ({membro.id}) season={season} ciclo={ciclo}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /cadastrar_player: {e}", ephemeral=True)


# =========================================================
# /start_cycle
# - mínimo 4 e máximo 6 por pod
# - exceção apenas quando matematicamente impossível
# - mantém somente este comando (sem alias /pods_gerar)
# =========================================================
def _find_valid_pod_layouts(total_players: int) -> list[list[int]]:
    """
    Retorna combinações exatas usando somente pods 4..6.
    Ex:
    14 -> [5,5,4] e [6,4,4]
    """
    results: list[list[int]] = []

    def dfs(remaining: int, current: list[int]):
        if remaining == 0:
            results.append(sorted(current, reverse=True))
            return
        for s in (4, 5, 6):
            if remaining - s < 0:
                continue
            dfs(remaining - s, current + [s])

    dfs(total_players, [])

    uniq = []
    seen = set()
    for layout in results:
        key = tuple(layout)
        if key not in seen:
            seen.add(key)
            uniq.append(layout)
    return uniq


def _choose_pod_layout(total_players: int, preferred_size: int = 0) -> list[int]:
    """
    Regra:
    - Preferir layouts exatos com pods entre 4 e 6.
    - Se for matematicamente impossível, abrir exceção.
    Casos impossíveis clássicos:
    - 3 jogadores -> [3]
    - 7 jogadores -> [4,3]
    """
    candidates = _find_valid_pod_layouts(total_players)

    if candidates:
        pref = preferred_size if preferred_size in (4, 5, 6) else 0

        def score(layout: list[int]):
            spread = max(layout) - min(layout)
            pref_count = sum(1 for x in layout if x == pref) if pref else 0
            return (
                spread,
                -pref_count,
                len(layout),
                -max(layout),
            )

        candidates.sort(key=score)
        return candidates[0]

    if total_players == 3:
        return [3]
    if total_players == 7:
        return [4, 3]

    return [total_players]


def _build_pods_from_layout(players: list[str], layout: list[int]) -> list[list[str]]:
    pods = []
    idx = 0
    for size in layout:
        pods.append(players[idx:idx + size])
        idx += size
    return pods


# =================================================
# FIM DO SUB-BLOCO E/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: F/7
# RESUMO: Algoritmo anti-repetição de pods, gravação em lote para reduzir 429,
# normalização automática de ciclo parcialmente gerado e comando /start_cycle
# com match_id curto por pod no formato Sx-Cy-Pz-01.
# REVISÃO: integração com o índice RAM de autocomplete de matches via
# invalidate_match_ac_index(), mantendo o melhor aproveitamento de cache em Matches,
# eliminação de releitura desnecessária do histórico de confrontos e consolidação
# de invalidação de cache.
# =================================================

def _best_layout_shuffle_min_repeats(players: list[str], layout: list[int], past_pairs: set[frozenset], tries: int = 250):
    best = None
    best_score = None

    for _ in range(max(1, tries)):
        cand = players[:]
        random.shuffle(cand)
        pods = _build_pods_from_layout(cand, layout)
        s = score_pods_repeats(pods, past_pairs)

        if best is None or s < best_score:
            best = pods
            best_score = s
            if best_score == 0:
                break

    return best, best_score


def _chunked_append_rows(ws, rows: list[list], chunk_size: int = 200):
    if not rows:
        return
    for i in range(0, len(rows), chunk_size):
        ws.append_rows(rows[i:i + chunk_size], value_input_option="USER_ENTERED")


def _past_confirmed_pairs_from_records(rows: list[dict]) -> set[frozenset]:
    """
    Cria um conjunto de pares (A,B) já enfrentados em matches CONFIRMED (qualquer season/cycle),
    exceto BYE e matches inativos, usando registros já carregados em memória.
    """
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


@client.tree.command(name="start_cycle", description="(ADM) Gera pods + matches e trava o ciclo (locked).")
@app_commands.describe(
    cycle="Número do ciclo",
    pod_size="Opcional: preferência de tamanho (4..6). 0 = automático",
    tries="Tentativas anti-repetição (50..500)"
)
async def start_cycle(interaction: discord.Interaction, cycle: int, pod_size: int = 0, tries: int = 250):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        ensure_all_sheets(sh)

        ws_cycles = sh.worksheet("Cycles")
        ws_enr = sh.worksheet("Enrollments")
        ws_pods = sh.worksheet("PodsHistory")
        ws_matches = sh.worksheet("Matches")

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        cf = get_cycle_fields(ws_cycles, season_id, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(f"❌ O ciclo {cycle} não existe.", ephemeral=True)

        st = (cf.get("status") or "").strip().lower()
        if st not in ("open", "locked"):
            return await interaction.followup.send(f"❌ O ciclo {cycle} não pode iniciar (status: {st}).", ephemeral=True)

        if _cycle_has_generated_data(ws_pods, ws_matches, season_id, cycle):
            start_br, end_br, max_pod, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)

            set_cycle_status(ws_cycles, season_id, cycle, "locked")

            if start_br and end_br:
                set_cycle_times(ws_cycles, season_id, cycle, start_br, end_br)

            cache_invalidate(ws_cycles)
            invalidate_match_ac_index()

            return await interaction.followup.send(
                "⚠️ Este ciclo já possui PODs ou matches gerados.\n"
                "Não vou gerar novamente.\n"
                f"- Status normalizado para: **locked**\n"
                f"- Prazo: **{start_br or '-'}** → **{end_br or '-'}** (BR)\n"
                f"- Regra aplicada: **{days} dias** (maior pod = **{max_pod}** jogador(es))",
                ephemeral=True
            )

        enr_rows = cached_get_all_records(ws_enr, ttl_seconds=10)
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
            return await interaction.followup.send("❌ Precisa de pelo menos 3 jogadores ativos para gerar pods.", ephemeral=True)

        if pod_size and pod_size not in (4, 5, 6):
            return await interaction.followup.send("❌ pod_size inválido (use 4, 5, 6 ou 0).", ephemeral=True)

        layout = _choose_pod_layout(len(players), preferred_size=pod_size)

        matches_rows = cached_get_all_records(ws_matches, ttl_seconds=10)
        past_pairs = _past_confirmed_pairs_from_records(matches_rows)

        pods, score = _best_layout_shuffle_min_repeats(
            players,
            layout,
            past_pairs,
            tries=max(50, min(tries, 500))
        )

        nowb = now_br_str()
        nowu = utc_now_dt()

        pod_labels = []
        pods_rows = []

        pod_num = 1
        for pod in pods:
            label = f"{pod_num}"
            pod_labels.append((label, pod))
            for pid in pod:
                pods_rows.append([str(season_id), str(cycle), label, str(pid), nowb])
            pod_num += 1

        _chunked_append_rows(ws_pods, pods_rows, chunk_size=200)
        cache_invalidate(ws_pods)

        matches_rows_to_append = []
        created = 0

        for label, pod in pod_labels:
            pairs = round_robin_pairs(pod)
            width = max(2, len(str(len(pairs))))

            for seq, (a, b) in enumerate(pairs, start=1):
                mid = f"S{season_id}-C{cycle}-P{label}-{str(seq).zfill(width)}"
                matches_rows_to_append.append([
                    mid, str(season_id), str(cycle), str(label),
                    str(a), str(b),
                    "0", "0", "0",
                    "normal", "open",
                    "", "", "",
                    "TRUE",
                    now_iso_utc(), now_iso_utc(),
                    auto_confirm_deadline_iso(nowu)
                ])
                created += 1

        _chunked_append_rows(ws_matches, matches_rows_to_append, chunk_size=200)
        cache_invalidate(ws_matches)
        invalidate_match_ac_index()

        set_cycle_status(ws_cycles, season_id, cycle, "locked")

        start_br, end_br, max_pod, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)
        if start_br and end_br:
            set_cycle_times(ws_cycles, season_id, cycle, start_br, end_br)

        cache_invalidate(ws_cycles)

        layout_txt = " + ".join(str(x) for x in layout)

        await interaction.followup.send(
            "✅ Ciclo travado e pods gerados.\n"
            f"- Jogadores: **{len(players)}**\n"
            f"- Layout dos pods: **{layout_txt}**\n"
            f"- Penalidade anti-repetição: **{score}**\n"
            f"- Matches criados: **{created}**\n"
            f"- Prazo: **{start_br}** → **{end_br}** (BR)",
            ephemeral=True
        )
        await log_admin(interaction, f"start_cycle S{season_id} C{cycle} layout={layout_txt} matches={created} score={score}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /start_cycle: {e}", ephemeral=True)

# =================================================
# FIM DO SUB-BLOCO F/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/8
# SUB-BLOCO: G/7
# RESUMO: Comandos administrativos finais do sistema incluindo exportação,
# fechamento automático de resultados, substituição de jogadores, histórico
# de confrontos, estatísticas da liga e novo comando /inscritos para consulta
# administrativa de inscritos por season/ciclo.
# REVISÃO: otimização de loops pesados, redução de updates isolados no Sheets
# e melhor aproveitamento dos dados já carregados em memória.
# =================================================


# =========================================================
# /exportar_ciclo
# =========================================================
@client.tree.command(name="exportar_ciclo", description="(ADM) Exporta CSV do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def exportar_ciclo(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        rows = cached_get_all_records(ws_matches, ttl_seconds=10)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(MATCHES_HEADER)

        count = 0
        for r in rows:
            if safe_int(r.get("season_id", 0), 0) != season_id:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            writer.writerow([r.get(h, "") for h in MATCHES_HEADER])
            count += 1

        data = output.getvalue().encode("utf-8")
        file = discord.File(io.BytesIO(data), filename=f"season_{season_id}_ciclo_{cycle}_matches.csv")

        await interaction.followup.send(
            content=f"✅ Exportado. Linhas: **{count}**",
            file=file,
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /exportar_ciclo: {e}", ephemeral=True)


# =========================================================
# /fechar_resultados_atrasados
# =========================================================
@client.tree.command(name="fechar_resultados_atrasados", description="(ADM) Auto-confirma pendências vencidas do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def fechar_resultados_atrasados(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        changed = sweep_auto_confirm(sh, season_id, cycle)
        await interaction.followup.send(f"✅ Pendências vencidas auto-confirmadas: **{changed}**", ephemeral=True)
        await log_admin(interaction, f"fechar_resultados_atrasados S{season_id} C{cycle} auto={changed}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /fechar_resultados_atrasados: {e}", ephemeral=True)


# =========================================================
# /inscritos
# =========================================================
@client.tree.command(name="inscritos", description="(ADM/Organizador/Owner) Lista inscritos e pendências de deck/decklist.")
@app_commands.describe(season="Season", cycle="Ciclo")
@app_commands.autocomplete(season=ac_owner_season, cycle=ac_owner_cycle_for_season)
async def inscritos(interaction: discord.Interaction, season: int, cycle: int):

    if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:

        sh = open_sheet()

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)

        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        players_rows = cached_get_all_records(ws_players, ttl_seconds=10)
        enr_rows = cached_get_all_records(ws_enr, ttl_seconds=10)
        decks_rows = cached_get_all_records(ws_decks, ttl_seconds=10)

        nick_map = _build_nick_map_from_records(players_rows)

        deck_status_map: dict[str, dict[str, bool]] = {}
        for d in decks_rows:
            if safe_int(d.get("season_id", 0), 0) != season:
                continue
            if safe_int(d.get("cycle", 0), 0) != cycle:
                continue

            pid = str(d.get("player_id", "")).strip()
            if not pid:
                continue

            entry = deck_status_map.setdefault(pid, {"deck_ok": False, "decklist_ok": False})
            if str(d.get("deck", "")).strip():
                entry["deck_ok"] = True
            if str(d.get("decklist_url", "")).strip():
                entry["decklist_ok"] = True

        inscritos = []
        inscritos_ids = set()

        for r in enr_rows:

            if safe_int(r.get("season_id", 0), 0) != season:
                continue

            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue

            if str(r.get("status", "")).strip().lower() != "active":
                continue

            pid = str(r.get("player_id", "")).strip()
            inscritos_ids.add(pid)

            st = deck_status_map.get(pid, {"deck_ok": False, "decklist_ok": False})

            inscritos.append(
                f"{nick_map.get(pid, pid)} | Deck: {'OK' if st['deck_ok'] else 'PENDENTE'} | Decklist: {'OK' if st['decklist_ok'] else 'PENDENTE'}"
            )

        nao_inscritos = []

        for p in players_rows:

            pid = str(p.get("discord_id", "")).strip()

            if not pid:
                continue

            if pid in inscritos_ids:
                continue

            nao_inscritos.append(nick_map.get(pid, pid))

        lines = [
            f"📋 **Inscritos Season {season} / Ciclo {cycle}**",
            ""
        ]

        if inscritos:
            lines.append("**Jogadores inscritos:**")
            lines.extend(inscritos)
        else:
            lines.append("Nenhum inscrito.")

        lines.append("")
        lines.append("**Jogadores cadastrados que ainda NÃO se inscreveram:**")

        if nao_inscritos:
            lines.extend(nao_inscritos[:100])
        else:
            lines.append("Todos já inscritos.")

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscritos: {e}", ephemeral=True)


# =========================================================
# /substituir_jogador
# =========================================================
@client.tree.command(name="substituir_jogador", description="(ADM) Substitui um jogador por outro no ciclo.")
@app_commands.describe(cycle="Número do ciclo", antigo="Jogador atual", novo="Novo jogador")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def substituir_jogador(interaction: discord.Interaction, cycle: int, antigo: discord.Member, novo: discord.Member):

    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:

        sh = open_sheet()
        season_id = require_current_season(sh)
        ensure_all_sheets(sh)

        old_id = str(antigo.id)
        new_id = str(novo.id)

        if old_id == new_id:
            return await interaction.followup.send("❌ Os jogadores são iguais.", ephemeral=True)

        ws_players = sh.worksheet("Players")
        upsert_player(ws_players, new_id, novo.display_name)

        ws_enr = sh.worksheet("Enrollments")
        ws_pods = sh.worksheet("PodsHistory")
        ws_matches = sh.worksheet("Matches")
        ws_decks = sh.worksheet("Decks")

        changed = 0
        nowb = now_br_str()

        col_enr = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        vals_enr = cached_get_all_values(ws_enr, ttl_seconds=10)
        enr_updates = []

        for rown in range(2, len(vals_enr) + 1):

            r = vals_enr[rown - 1]

            s = safe_int(r[col_enr["season_id"]] if col_enr["season_id"] < len(r) else 0, 0)
            c = safe_int(r[col_enr["cycle"]] if col_enr["cycle"] < len(r) else 0, 0)
            p = str(r[col_enr["player_id"]] if col_enr["player_id"] < len(r) else "").strip()

            if s == season_id and c == cycle and p == old_id:

                enr_updates.append({
                    "range": f"{col_letter(col_enr['player_id'])}{rown}",
                    "values": [[new_id]]
                })
                enr_updates.append({
                    "range": f"{col_letter(col_enr['updated_at'])}{rown}",
                    "values": [[nowb]]
                })
                changed += 1

        if enr_updates:
            ws_enr.batch_update(enr_updates)

        col_pods = ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        vals_pods = cached_get_all_values(ws_pods, ttl_seconds=10)
        pods_updates = []

        for rown in range(2, len(vals_pods) + 1):

            r = vals_pods[rown - 1]

            s = safe_int(r[col_pods["season_id"]] if col_pods["season_id"] < len(r) else 0, 0)
            c = safe_int(r[col_pods["cycle"]] if col_pods["cycle"] < len(r) else 0, 0)
            p = str(r[col_pods["player_id"]] if col_pods["player_id"] < len(r) else "").strip()

            if s == season_id and c == cycle and p == old_id:

                pods_updates.append({
                    "range": f"{col_letter(col_pods['player_id'])}{rown}",
                    "values": [[new_id]]
                })
                changed += 1

        if pods_updates:
            ws_pods.batch_update(pods_updates)

        col_m = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        vals_m = cached_get_all_values(ws_matches, ttl_seconds=10)
        matches_updates = []

        for rown in range(2, len(vals_m) + 1):

            r = vals_m[rown - 1]

            s = safe_int(r[col_m["season_id"]] if col_m["season_id"] < len(r) else 0, 0)
            c = safe_int(r[col_m["cycle"]] if col_m["cycle"] < len(r) else 0, 0)

            if s != season_id or c != cycle:
                continue

            a = str(r[col_m["player_a_id"]] if col_m["player_a_id"] < len(r) else "").strip()
            b = str(r[col_m["player_b_id"]] if col_m["player_b_id"] < len(r) else "").strip()

            if a == old_id:

                matches_updates.append({
                    "range": f"{col_letter(col_m['player_a_id'])}{rown}",
                    "values": [[new_id]]
                })
                changed += 1

            if b == old_id:

                matches_updates.append({
                    "range": f"{col_letter(col_m['player_b_id'])}{rown}",
                    "values": [[new_id]]
                })
                changed += 1

        if matches_updates:
            ws_matches.batch_update(matches_updates)

        col_d = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)
        vals_d = cached_get_all_values(ws_decks, ttl_seconds=10)
        deck_updates = []

        for rown in range(2, len(vals_d) + 1):

            r = vals_d[rown - 1]

            s = safe_int(r[col_d["season_id"]] if col_d["season_id"] < len(r) else 0, 0)
            c = safe_int(r[col_d["cycle"]] if col_d["cycle"] < len(r) else 0, 0)
            p = str(r[col_d["player_id"]] if col_d["player_id"] < len(r) else "").strip()

            if s == season_id and c == cycle and p == old_id:

                deck_updates.append({
                    "range": f"{col_letter(col_d['player_id'])}{rown}",
                    "values": [[new_id]]
                })
                deck_updates.append({
                    "range": f"{col_letter(col_d['updated_at'])}{rown}",
                    "values": [[nowb]]
                })
                changed += 1

        if deck_updates:
            ws_decks.batch_update(deck_updates)

        cache_invalidate(ws_enr)
        cache_invalidate(ws_pods)
        cache_invalidate(ws_matches)
        cache_invalidate(ws_decks)

        await interaction.followup.send(
            f"✅ Substituição concluída.\n"
            f"- Antigo: **{antigo.display_name}**\n"
            f"- Novo: **{novo.display_name}**\n"
            f"- Ajustes aplicados: **{changed}**",
            ephemeral=True
        )

        await log_admin(interaction, f"substituir_jogador S{season_id} C{cycle} old={old_id} new={new_id}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /substituir_jogador: {e}", ephemeral=True)


# =========================================================
# /historico_confronto
# =========================================================
@client.tree.command(name="historico_confronto", description="(ADM) Mostra histórico entre dois jogadores.")
@app_commands.describe(jogador_a="Primeiro jogador", jogador_b="Segundo jogador")
async def historico_confronto(interaction: discord.Interaction, jogador_a: discord.Member, jogador_b: discord.Member):

    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:

        sh = open_sheet()

        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)

        rows = cached_get_all_records(ws_matches, ttl_seconds=10)

        a_id = str(jogador_a.id)
        b_id = str(jogador_b.id)

        total = 0
        a_wins = 0
        b_wins = 0
        draws = 0
        details = []

        for r in rows:

            if not as_bool(r.get("active", "TRUE")):
                continue

            if str(r.get("confirmed_status", "")).strip().lower() != "confirmed":
                continue

            if str(r.get("result_type", "")).strip().lower() == "bye":
                continue

            pa = str(r.get("player_a_id", "")).strip()
            pb = str(r.get("player_b_id", "")).strip()

            pair = {pa, pb}

            if pair != {a_id, b_id}:
                continue

            total += 1

            a_gw = safe_int(r.get("a_games_won", 0), 0)
            b_gw = safe_int(r.get("b_games_won", 0), 0)
            d_g = safe_int(r.get("draw_games", 0), 0)

            if pa == a_id:

                if a_gw > b_gw:
                    a_wins += 1
                elif b_gw > a_gw:
                    b_wins += 1
                else:
                    draws += 1

                score = f"{a_gw}-{b_gw}-{d_g}"

            else:

                if b_gw > a_gw:
                    a_wins += 1
                elif a_gw > b_gw:
                    b_wins += 1
                else:
                    draws += 1

                score = f"{b_gw}-{a_gw}-{d_g}"

            details.append(
                f"• S{r.get('season_id')} C{r.get('cycle')} | `{r.get('match_id')}` | placar {score}"
            )

        if total == 0:
            return await interaction.followup.send(
                "⚠️ Não há histórico confirmado entre esses jogadores.",
                ephemeral=True
            )

        lines = [
            f"⚔️ **Histórico de confronto**",
            f"**{jogador_a.display_name}** x **{jogador_b.display_name}**",
            f"Matches: **{total}**",
            f"Vitórias {jogador_a.display_name}: **{a_wins}**",
            f"Vitórias {jogador_b.display_name}: **{b_wins}**",
            f"Empates: **{draws}**",
            "",
            "Detalhes:"
        ]

        lines.extend(details[:100])

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /historico_confronto: {e}", ephemeral=True)


# =========================================================
# /estatisticas
# =========================================================
@client.tree.command(name="estatisticas", description="(ADM) Mostra estatísticas gerais da liga.")
async def estatisticas(interaction: discord.Interaction):

    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:

        sh = open_sheet()

        season_id = require_current_season(sh)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)

        players_all = cached_get_all_records(ws_players, ttl_seconds=10)
        cycles_all = cached_get_all_records(ws_cycles, ttl_seconds=10)
        enr_all = cached_get_all_records(ws_enr, ttl_seconds=10)
        matches_all = cached_get_all_records(ws_matches, ttl_seconds=10)
        pods_all = cached_get_all_records(ws_pods, ttl_seconds=10)

        season_cycles = [r for r in cycles_all if safe_int(r.get("season_id", 0), 0) == season_id]
        season_enr = [r for r in enr_all if safe_int(r.get("season_id", 0), 0) == season_id]
        season_matches = [r for r in matches_all if safe_int(r.get("season_id", 0), 0) == season_id]
        season_pods = [r for r in pods_all if safe_int(r.get("season_id", 0), 0) == season_id]

        confirmed = sum(1 for r in season_matches if str(r.get("confirmed_status", "")).strip().lower() == "confirmed")
        pending = sum(1 for r in season_matches if str(r.get("confirmed_status", "")).strip().lower() == "pending")
        open_m = sum(1 for r in season_matches if str(r.get("confirmed_status", "")).strip().lower() in ("", "open"))
        active_enr = sum(1 for r in season_enr if str(r.get("status", "")).strip().lower() == "active")

        lines = [
            f"📊 **Estatísticas da liga** | Season {season_id}",
            f"Players cadastrados: **{len(players_all)}**",
            f"Ciclos na season: **{len(season_cycles)}**",
            f"Inscrições na season: **{len(season_enr)}**",
            f"Inscrições ativas: **{active_enr}**",
            f"Registros em PodsHistory: **{len(season_pods)}**",
            f"Matches na season: **{len(season_matches)}**",
            f"Matches confirmados: **{confirmed}**",
            f"Matches pendentes: **{pending}**",
            f"Matches abertos: **{open_m}**",
        ]

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /estatisticas: {e}", ephemeral=True)


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


# =================================================
# FIM DO SUB-BLOCO G/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 9/9
# SUB-BLOCO: ÚNICO
# RESUMO: Índice em memória para autocomplete de matches do /resultado e /rejeitar,
# com rebuild automático por tempo, invalidação manual por evento e leitura rápida
# por usuário sem consultar Google Sheets a cada digitação.
# =================================================

# =========================================================
# CACHE DE ÍNDICE — AUTOCOMPLETE DE MATCHES
# =========================================================
_MATCH_AC_INDEX = {
    "ts": 0.0,
    "season_id": 0,
    "locked_cycles": set(),
    "by_user": {},   # user_id -> list[{"label","value","search"}]
}
_MATCH_AC_INDEX_LOCK = threading.Lock()
_MATCH_AC_INDEX_TTL_SECONDS = 60


def invalidate_match_ac_index():
    """
    Invalida completamente o índice de autocomplete de matches.
    Chamar após eventos que alterem matches/ciclos, por exemplo:
    - /start_cycle
    - /resultado
    - /rejeitar
    - confirmação/rejeição por DM
    - auto-confirm
    - admin_resultado_editar
    - admin_resultado_cancelar
    """
    with _MATCH_AC_INDEX_LOCK:
        _MATCH_AC_INDEX["ts"] = 0.0
        _MATCH_AC_INDEX["season_id"] = 0
        _MATCH_AC_INDEX["locked_cycles"] = set()
        _MATCH_AC_INDEX["by_user"] = {}


def _match_visual_status_from_row(r: dict) -> str:
    status = str(r.get("confirmed_status", "")).strip().lower()
    reported_by = str(r.get("reported_by_id", "")).strip()
    return "registrado" if (status == "pending" and reported_by) or status == "confirmed" else "pendente"


def _build_match_ac_index(sh):
    """
    Reconstrói o índice completo do autocomplete de matches para a season ativa,
    considerando apenas ciclos LOCKED e matches ACTIVE.
    """
    season_id = get_current_season_id(sh)
    if season_id <= 0:
        return {
            "ts": _cache_now(),
            "season_id": 0,
            "locked_cycles": set(),
            "by_user": {},
        }

    ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
    ws_matches = ensure_worksheet(sh, "Matches", MATCHES_REQUIRED_COLS, rows=50000, cols=30)

    ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
    ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

    cycle_rows = cached_get_all_records(ws_cycles, ttl_seconds=10)
    player_rows = cached_get_all_records(ws_players, ttl_seconds=10)
    match_rows = cached_get_all_records(ws_matches, ttl_seconds=10)

    locked_cycles = set()
    for r in cycle_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        cyc = safe_int(r.get("cycle", 0), 0)
        st = str(r.get("status", "")).strip().lower()
        if cyc > 0 and st == "locked":
            locked_cycles.add(cyc)

    if not locked_cycles:
        return {
            "ts": _cache_now(),
            "season_id": season_id,
            "locked_cycles": set(),
            "by_user": {},
        }

    nick_map: dict[str, str] = {}
    for r in player_rows:
        pid = str(r.get("discord_id", "")).strip()
        nick = str(r.get("nick", "")).strip()
        if pid:
            nick_map[pid] = nick or pid

    by_user: dict[str, list[dict]] = {}

    for r in match_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        cyc = safe_int(r.get("cycle", 0), 0)
        if cyc not in locked_cycles:
            continue

        if not as_bool(r.get("active", "TRUE")):
            continue

        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()
        mid = str(r.get("match_id", "")).strip()
        pod = str(r.get("pod", "")).strip()

        if not a or not b or not mid:
            continue

        visual_status = _match_visual_status_from_row(r)

        a_opp = nick_map.get(b, b)
        b_opp = nick_map.get(a, a)

        if pod:
            label_a = f"{a_opp} | POD {pod} | {visual_status}"
            label_b = f"{b_opp} | POD {pod} | {visual_status}"
        else:
            label_a = f"{a_opp} | {visual_status}"
            label_b = f"{b_opp} | {visual_status}"

        search_a = f"{mid} {a_opp} {visual_status} {pod}".lower()
        search_b = f"{mid} {b_opp} {visual_status} {pod}".lower()

        by_user.setdefault(a, []).append({
            "label": label_a[:100],
            "value": mid,
            "search": search_a,
            "cycle": cyc,
            "status": visual_status,
        })

        by_user.setdefault(b, []).append({
            "label": label_b[:100],
            "value": mid,
            "search": search_b,
            "cycle": cyc,
            "status": visual_status,
        })

    # ordenação: pendente primeiro, ciclo mais alto primeiro, label por último
    for uid, items in by_user.items():
        items.sort(
            key=lambda x: (
                0 if x.get("status") == "pendente" else 1,
                -safe_int(x.get("cycle", 0), 0),
                str(x.get("label", "")).lower(),
            )
        )

    return {
        "ts": _cache_now(),
        "season_id": season_id,
        "locked_cycles": locked_cycles,
        "by_user": by_user,
    }


def ensure_match_ac_index(sh, max_age_seconds: int = _MATCH_AC_INDEX_TTL_SECONDS):
    """
    Garante que o índice esteja pronto e recente.
    Rebuild automático por tempo.
    """
    now = _cache_now()

    with _MATCH_AC_INDEX_LOCK:
        ts = float(_MATCH_AC_INDEX.get("ts", 0.0) or 0.0)
        if ts > 0 and (now - ts) <= max_age_seconds:
            return

    built = _build_match_ac_index(sh)

    with _MATCH_AC_INDEX_LOCK:
        _MATCH_AC_INDEX["ts"] = built["ts"]
        _MATCH_AC_INDEX["season_id"] = built["season_id"]
        _MATCH_AC_INDEX["locked_cycles"] = built["locked_cycles"]
        _MATCH_AC_INDEX["by_user"] = built["by_user"]


def get_match_ac_choices_for_user(sh, user_id: str, query: str = "", limit: int = 25) -> list[dict]:
    """
    Retorna as opções de autocomplete já prontas para um usuário.
    Cada item retorna:
    - label
    - value
    - search
    """
    ensure_match_ac_index(sh)

    uid = str(user_id).strip()
    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    with _MATCH_AC_INDEX_LOCK:
        items = list(_MATCH_AC_INDEX.get("by_user", {}).get(uid, []))

    if not q:
        return items[:limit]

    out = []
    for item in items:
        if q in str(item.get("search", "")):
            out.append(item)
            if len(out) >= limit:
                break
    return out


def get_match_ac_index_snapshot() -> dict:
    """
    Helper opcional de diagnóstico.
    """
    with _MATCH_AC_INDEX_LOCK:
        return {
            "ts": _MATCH_AC_INDEX.get("ts", 0.0),
            "season_id": _MATCH_AC_INDEX.get("season_id", 0),
            "locked_cycles": sorted(_MATCH_AC_INDEX.get("locked_cycles", set())),
            "users_indexed": len(_MATCH_AC_INDEX.get("by_user", {})),
        }


# =================================================
# FIM DO SUB-BLOCO
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 10/10
# SUB-BLOCO: ÚNICO
# RESUMO: Índice RAM de MATCHES para acelerar autocomplete, leitura por jogador,
# leitura por match_id e consultas por season/cycle, com rebuild automático por
# tempo e invalidação manual por evento.
# =================================================

# =========================================================
# CACHE DE ÍNDICE — MATCHES
# =========================================================
_MATCH_RAM_INDEX = {
    "ts": 0.0,
    "by_match_id": {},          # match_id -> row dict
    "by_player": {},            # player_id -> list[row dict]
    "by_cycle": {},             # (season_id, cycle) -> list[row dict]
    "season_id_active": 0,
}
_MATCH_RAM_INDEX_LOCK = threading.Lock()
_MATCH_RAM_INDEX_TTL_SECONDS = 60


def invalidate_match_ram_index():
    with _MATCH_RAM_INDEX_LOCK:
        _MATCH_RAM_INDEX["ts"] = 0.0
        _MATCH_RAM_INDEX["by_match_id"] = {}
        _MATCH_RAM_INDEX["by_player"] = {}
        _MATCH_RAM_INDEX["by_cycle"] = {}
        _MATCH_RAM_INDEX["season_id_active"] = 0


def _copy_match_row_dict(r: dict) -> dict:
    return {
        "match_id": str(r.get("match_id", "")).strip(),
        "season_id": safe_int(r.get("season_id", 0), 0),
        "cycle": safe_int(r.get("cycle", 0), 0),
        "pod": str(r.get("pod", "")).strip(),
        "player_a_id": str(r.get("player_a_id", "")).strip(),
        "player_b_id": str(r.get("player_b_id", "")).strip(),
        "a_games_won": safe_int(r.get("a_games_won", 0), 0),
        "b_games_won": safe_int(r.get("b_games_won", 0), 0),
        "draw_games": safe_int(r.get("draw_games", 0), 0),
        "result_type": str(r.get("result_type", "")).strip().lower(),
        "confirmed_status": str(r.get("confirmed_status", "")).strip().lower(),
        "reported_by_id": str(r.get("reported_by_id", "")).strip(),
        "confirmed_by_id": str(r.get("confirmed_by_id", "")).strip(),
        "message_id": str(r.get("message_id", "")).strip(),
        "active": as_bool(r.get("active", "TRUE")),
        "created_at": str(r.get("created_at", "")).strip(),
        "updated_at": str(r.get("updated_at", "")).strip(),
        "auto_confirm_at": str(r.get("auto_confirm_at", "")).strip(),
    }


def _match_sort_key(r: dict):
    return (
        safe_int(r.get("season_id", 0), 0),
        safe_int(r.get("cycle", 0), 0),
        safe_int(r.get("pod", 0), 999999),
        str(r.get("match_id", "")).lower(),
    )


def _build_match_ram_index(sh):
    ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
    ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

    rows = cached_get_all_records(ws_matches, ttl_seconds=10)

    by_match_id: dict[str, dict] = {}
    by_player: dict[str, list[dict]] = {}
    by_cycle: dict[tuple[int, int], list[dict]] = {}

    for raw in rows:
        r = _copy_match_row_dict(raw)
        mid = r["match_id"]
        if not mid:
            continue

        by_match_id[mid] = r

        season_id = r["season_id"]
        cycle = r["cycle"]
        if season_id > 0 and cycle > 0:
            by_cycle.setdefault((season_id, cycle), []).append(r)

        a = r["player_a_id"]
        b = r["player_b_id"]
        if a:
            by_player.setdefault(a, []).append(r)
        if b:
            by_player.setdefault(b, []).append(r)

    for key in by_cycle:
        by_cycle[key].sort(key=_match_sort_key)

    for pid in by_player:
        by_player[pid].sort(key=_match_sort_key)

    season_id_active = get_current_season_id(sh)

    return {
        "ts": _cache_now(),
        "by_match_id": by_match_id,
        "by_player": by_player,
        "by_cycle": by_cycle,
        "season_id_active": season_id_active,
    }


def ensure_match_ram_index(sh, max_age_seconds: int = _MATCH_RAM_INDEX_TTL_SECONDS):
    now = _cache_now()

    with _MATCH_RAM_INDEX_LOCK:
        ts = float(_MATCH_RAM_INDEX.get("ts", 0.0) or 0.0)
        if ts > 0 and (now - ts) <= max_age_seconds:
            return

    built = _build_match_ram_index(sh)

    with _MATCH_RAM_INDEX_LOCK:
        _MATCH_RAM_INDEX["ts"] = built["ts"]
        _MATCH_RAM_INDEX["by_match_id"] = built["by_match_id"]
        _MATCH_RAM_INDEX["by_player"] = built["by_player"]
        _MATCH_RAM_INDEX["by_cycle"] = built["by_cycle"]
        _MATCH_RAM_INDEX["season_id_active"] = built["season_id_active"]


def get_match_by_id_fast(sh, match_id: str) -> dict | None:
    ensure_match_ram_index(sh)
    mid = str(match_id or "").strip()
    if not mid:
        return None

    with _MATCH_RAM_INDEX_LOCK:
        r = _MATCH_RAM_INDEX.get("by_match_id", {}).get(mid)
        return dict(r) if r else None


def get_matches_for_player_fast(
    sh,
    player_id: str,
    season_id: int | None = None,
    cycle: int | None = None,
    only_active: bool = False
) -> list[dict]:
    ensure_match_ram_index(sh)
    pid = str(player_id or "").strip()
    if not pid:
        return []

    with _MATCH_RAM_INDEX_LOCK:
        rows = list(_MATCH_RAM_INDEX.get("by_player", {}).get(pid, []))

    out = []
    for r in rows:
        if season_id is not None and safe_int(r.get("season_id", 0), 0) != safe_int(season_id, 0):
            continue
        if cycle is not None and safe_int(r.get("cycle", 0), 0) != safe_int(cycle, 0):
            continue
        if only_active and not as_bool(r.get("active", True)):
            continue
        out.append(dict(r))
    return out


def get_matches_for_cycle_fast(sh, season_id: int, cycle: int, only_active: bool = False) -> list[dict]:
    ensure_match_ram_index(sh)
    key = (safe_int(season_id, 0), safe_int(cycle, 0))

    with _MATCH_RAM_INDEX_LOCK:
        rows = list(_MATCH_RAM_INDEX.get("by_cycle", {}).get(key, []))

    if not only_active:
        return [dict(r) for r in rows]

    out = []
    for r in rows:
        if as_bool(r.get("active", True)):
            out.append(dict(r))
    return out


def get_match_ram_index_snapshot() -> dict:
    with _MATCH_RAM_INDEX_LOCK:
        return {
            "ts": _MATCH_RAM_INDEX.get("ts", 0.0),
            "season_id_active": _MATCH_RAM_INDEX.get("season_id_active", 0),
            "matches_indexed": len(_MATCH_RAM_INDEX.get("by_match_id", {})),
            "players_indexed": len(_MATCH_RAM_INDEX.get("by_player", {})),
            "cycles_indexed": len(_MATCH_RAM_INDEX.get("by_cycle", {})),
        }


# =================================================
# FIM DO SUB-BLOCO
# =================================================
