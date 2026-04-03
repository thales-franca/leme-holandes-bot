# =================================================
# BLOCO ORIGINAL: BLOCO 1/22
# SUB-BLOCO: A/2
# REVISÃO FINAL — cache otimizado + RAM index + locks globais mais leves
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
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials


# =========================================================
# LEME HOLANDÊS BOT
# =========================================================


# =========================
# HTTP keep-alive (Render)
# =========================

app = Flask(__name__)


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


def now_br_dt():
    return datetime.now(BR_TZ)


def now_br_str():
    return now_br_dt().strftime("%Y-%m-%d %H:%M:%S")


def now_iso_utc():
    return datetime.now(timezone.utc).isoformat()


def utc_now_dt():
    return datetime.now(timezone.utc)


def parse_iso_dt(s):

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


def parse_br_dt(s):

    try:
        return datetime.strptime(
            str(s).strip(),
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=BR_TZ)

    except Exception:
        return None


def fmt_br_dt(dt):
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


def floor_333(x):
    return max(x, 1/3)


def pct1(x):
    return round(x * 100.0, 1)

def fmt_compact_num(v) -> str:
    try:
        n = float(v)

        if n.is_integer():
            return str(int(n))

        return f"{n:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)

def col_letter(ci_0):

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

_SHEETS_CACHE = {}
_SHEETS_CACHE_LOCK = threading.Lock()

_CACHE_MAX_ITEMS = 500


def _cache_now():
    return time.monotonic()


def _ws_cache_prefix(ws):

    try:
        sid = ws.spreadsheet.id
    except Exception:
        sid = SHEET_ID or "sheet"

    try:
        wid = ws.id
    except Exception:
        wid = None

    try:
        title = ws.title
    except Exception:
        title = "ws"

    if wid:
        return f"{sid}:{wid}:{title}:"

    return f"{sid}:{title}:"


def _cache_key(ws, kind):
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

    with _SHEETS_CACHE_LOCK:

        _SHEETS_CACHE[key] = {
            "ts": now,
            "data": data,
        }

        # limpeza leve
        if len(_SHEETS_CACHE) > _CACHE_MAX_ITEMS:

            cutoff = now - 120

            remove = [
                k for k, v in _SHEETS_CACHE.items()
                if v.get("ts", 0) < cutoff
            ]

            for k in remove:
                _SHEETS_CACHE.pop(k, None)


def cache_invalidate(ws, kind=None):

    prefix = _ws_cache_prefix(ws)

    with _SHEETS_CACHE_LOCK:

        if kind is None:

            keys = [
                k for k in list(_SHEETS_CACHE.keys())
                if k.startswith(prefix)
            ]

            for k in keys:
                _SHEETS_CACHE.pop(k, None)

        else:

            _SHEETS_CACHE.pop(
                prefix + kind,
                None
            )

# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 1/22
# SUB-BLOCO: B/2
# REVISÃO: cache leve para cliente/planilha do Google Sheets, melhor
# reaproveitamento de cabeçalho e manutenção integral da lógica funcional.
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
_GS_CLIENT_CACHE = {
    "client": None,
    "service_json": None,
}
_GS_CLIENT_LOCK = threading.Lock()

_OPEN_SHEET_CACHE = {
    "sheet": None,
    "sheet_id": None,
}
_OPEN_SHEET_LOCK = threading.Lock()


def get_sheets_client():
    if not SERVICE_JSON:
        return None

    with _GS_CLIENT_LOCK:
        cached_client = _GS_CLIENT_CACHE.get("client")
        cached_json = _GS_CLIENT_CACHE.get("service_json")

        if cached_client is not None and cached_json == SERVICE_JSON:
            return cached_client

        data = json.loads(SERVICE_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(data, scopes=scopes)
        client = gspread.authorize(creds)

        _GS_CLIENT_CACHE["client"] = client
        _GS_CLIENT_CACHE["service_json"] = SERVICE_JSON

        return client


def open_sheet():
    gc = get_sheets_client()
    if not gc or not SHEET_ID:
        raise RuntimeError("Google Sheets não configurado (SHEET_ID ou GOOGLE_SERVICE_ACCOUNT_JSON).")

    with _OPEN_SHEET_LOCK:
        cached_sheet = _OPEN_SHEET_CACHE.get("sheet")
        cached_sheet_id = _OPEN_SHEET_CACHE.get("sheet_id")

        if cached_sheet is not None and cached_sheet_id == SHEET_ID:
            return cached_sheet

        sh = gc.open_by_key(SHEET_ID)
        _OPEN_SHEET_CACHE["sheet"] = sh
        _OPEN_SHEET_CACHE["sheet_id"] = SHEET_ID

        return sh


def ensure_sheet_columns(ws, required_cols: list[str]):
    vals = cached_get_all_values(ws, ttl_seconds=10)
    header = vals[0] if vals else []

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
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)

    current = cached_get_all_values(ws, ttl_seconds=10)
    if not current:
        ws.append_row(header)
        cache_invalidate(ws)

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
    if not interaction.guild or not interaction.user:
        return False
    return interaction.guild.owner_id == interaction.user.id


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


async def get_access_level(interaction: discord.Interaction) -> str:
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
from urllib.parse import urlparse, parse_qs

def validate_decklist_url(url: str) -> tuple[bool, str]:
    raw = str(url).strip()

    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = "https://" + raw

    if " " in raw or len(raw) < 10 or len(raw) > 400:
        return False, "Link inválido. Envie uma URL completa (sem espaços)."

    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()

    # Remove www. se existir
    if host.startswith("www."):
        host = host[4:]

#    allowed_hosts = {"lemeholandes.com.br"}
#    if host not in allowed_hosts:
#        return False, "Link não permitido. Use apenas lemeholandes.com.br"
    # LemeHolandes
#    if "lemeholandes.com.br" in host:
#        if "/Decklist/" not in (parsed.path or ""):
#            return False, "Link inválido do Melee. Exemplo: https://lemeholandes.com.br/Decklist/..."
#    return True, raw
    
    allowed_hosts = {"moxfield.com", "ligamagic.com", "ligamagic.com.br", "mtgdecks.net", "mtggoldfish.com", "melee.gg", "mtgtop8.com"}
    if host not in allowed_hosts:
        return False, "Link não permitido. Use apenas moxfield.com, ligamagic.com(.br), mtggoldfish.com, mtgdecks.net, melee.gg ou mtgtop8.com"

    # Melee
    if "melee.gg" in host:
        if "/Decklist/View/" not in (parsed.path or ""):
            return False, "Link inválido do Melee. Exemplo: https://melee.gg/Decklist/View/..."
            
    # Mtgtop8
    if "mtgtop8.com" in host:
        if "/event?e=" not in (parsed.path or ""):
            return False, "Link inválido do Mtgtop8. Exemplo: https://mtgtop8.com/event?e=..."
            
    # Mtgdecks
    if "mtgdecks.net" in host:
        if "/Modern/" not in (parsed.path or ""):
            return False, "Link inválido do Mtgdecks. Exemplo: https://mtgdecks.net/Modern/..."

    # Mtggoldfish
    if "mtggoldfish.com" in host:
        if "/archetype/modern" not in (parsed.path or ""):
            return False, "Link inválido do Mtggoldfish. Exemplo: https://www.mtggoldfish.com/archetype/modern..."
            
    # Moxfield
    if "moxfield.com" in host:
        if "/decks/" not in (parsed.path or ""):
            return False, "Link inválido do Moxfield. Exemplo: https://www.moxfield.com/decks/SEU_ID"

    # LigaMagic
    if "ligamagic.com" in host:
        qs = parse_qs(parsed.query or "")
        deck_id = (qs.get("id", [""])[0] or "").strip()
        if not deck_id.isdigit():
            return False, "Link inválido da LigaMagic. Exemplo: https://www.ligamagic.com(.br)/?view=dks/deck&id=123456"

    return True, raw


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
# [BLOCO 1/22 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 2/22
# SUB-BLOCO: A/2
# REVISÃO: redução de leituras repetidas, uso consistente de ensure_worksheet,
# menos chamadas diretas ao Sheets e melhor aproveitamento do cache.
# =================================================

# =========================
# Sheets schema (headers)
# =========================
SEASONSTATE_HEADER = ["key", "value", "updated_at"]

SEASONS_HEADER = ["season_id", "status", "name", "created_at", "updated_at"]
SEASONS_REQUIRED = SEASONS_HEADER[:]

PLAYERS_HEADER = ["discord_id", "nick", "name", "notes", "status", "rating", "created_at", "updated_at"]
PLAYERS_REQUIRED = PLAYERS_HEADER[:]

DECKS_HEADER = ["season_id", "cycle", "player_id", "deck", "decklist_url", "created_at", "updated_at"]
DECKS_REQUIRED = DECKS_HEADER[:]

ENROLLMENTS_HEADER = ["season_id", "cycle", "player_id", "status", "created_at", "updated_at"]
ENROLLMENTS_REQUIRED = ENROLLMENTS_HEADER[:]

CYCLE_BONUSES_HEADER = ["season_id", "cycle", "bonus_percent", "updated_at"]
CYCLE_BONUSES_REQUIRED = CYCLE_BONUSES_HEADER[:]

CYCLES_HEADER = ["season_id", "cycle", "status", "start_at_br", "deadline_at_br", "created_at", "updated_at"]
CYCLES_REQUIRED = CYCLES_HEADER[:]

PODSHISTORY_HEADER = ["season_id", "cycle", "pod", "player_id", "created_at"]
PODSHISTORY_REQUIRED = PODSHISTORY_HEADER[:]

MATCHES_REQUIRED_COLS = [
    "match_id","season_id","cycle","pod",
    "player_a_id","player_b_id",
    "a_games_won","b_games_won","draw_games",
    "result_type","confirmed_status",
    "reported_by_id","confirmed_by_id","message_id",
    "active","created_at","updated_at","auto_confirm_at",
]

MATCHES_HEADER = MATCHES_REQUIRED_COLS[:]

STANDINGS_HEADER = [
    "season_id", "cycle", "player_id",
    "matches_played", "match_points",
    "matches", "points",
    "mwp", "omw", "gw", "ogw",
    "mwp_percent",
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
    ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)
    ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
    ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    ensure_worksheet(sh, "CycleBonuses", CYCLE_BONUSES_HEADER, rows=2000, cols=10)
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
    aceita worksheet Seasons OU spreadsheet sh
    """

    try:
        if hasattr(arg, "title") and str(arg.title).lower() == "seasons":
            ensure_sheet_columns(arg, SEASONS_REQUIRED)
            return _infer_open_season_id_from_seasons_ws(arg)
    except Exception:
        pass

    sh = arg

    try:
        ws_state = ensure_worksheet(sh, "SeasonState", SEASONSTATE_HEADER)
        rows = cached_get_all_records(ws_state, ttl_seconds=10)

        for r in rows:
            if str(r.get("key", "")).strip() == "current_season_id":
                sid = safe_int(r.get("value", 0), 0)
                if sid > 0:
                    return sid

    except Exception:
        pass

    try:
        ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER)
        return _infer_open_season_id_from_seasons_ws(ws_seasons)
    except Exception:
        return 0


def set_current_season_id(sh, season_id: int):

    ws = ensure_worksheet(sh, "SeasonState", SEASONSTATE_HEADER)

    vals = cached_get_all_values(ws, ttl_seconds=10)

    nowb = now_br_str()

    found = None

    for i in range(2, len(vals) + 1):

        row = vals[i - 1]

        if len(row) >= 1 and str(row[0]).strip() == "current_season_id":
            found = i
            break

    if found is None:

        ws.append_row(
            ["current_season_id", str(season_id), nowb],
            value_input_option="USER_ENTERED"
        )

    else:

        ws.batch_update([
            {"range": f"B{found}", "values": [[str(season_id)]]},
            {"range": f"C{found}", "values": [[nowb]]},
        ])

    cache_invalidate(ws)


def season_exists(sh, season_id: int) -> bool:

    ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER)

    rows = cached_get_all_records(ws, ttl_seconds=10)

    for r in rows:

        if safe_int(r.get("season_id", 0), 0) == season_id:
            return True

    return False


def get_season_status(sh, season_id: int) -> str:

    ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER)

    rows = cached_get_all_records(ws, ttl_seconds=10)

    for r in rows:

        if safe_int(r.get("season_id", 0), 0) == season_id:
            return str(r.get("status", "")).strip().lower()

    return ""


def set_season_status(sh, season_id: int, status: str, name: str | None = None):

    status = str(status).strip().lower()

    ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER)

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

        ws.append_row(
            [str(season_id), status, nm, nowb, nowb],
            value_input_option="USER_ENTERED"
        )

    else:

        updates = [
            {"range": f"B{found}", "values": [[status]]},
            {"range": f"E{found}", "values": [[nowb]]},
        ]

        if name is not None:
            updates.append({"range": f"C{found}", "values": [[name]]})

        ws.batch_update(updates)

    cache_invalidate(ws)


def close_all_other_seasons(sh, keep_open_id: int):

    ws = ensure_worksheet(sh, "Seasons", SEASONS_HEADER)

    data = cached_get_all_values(ws, ttl_seconds=10)

    if len(data) <= 1:
        return 0

    nowb = now_br_str()

    updates = []

    changed = 0

    for i in range(2, len(data) + 1):

        row = data[i - 1]

        sid = safe_int(row[0] if len(row) > 0 else 0, 0)

        if sid <= 0 or sid == keep_open_id:
            continue

        st = (row[1] if len(row) > 1 else "").strip().lower()

        if st != "closed":

            updates.append({"range": f"B{i}", "values": [["closed"]]})
            updates.append({"range": f"E{i}", "values": [[nowb]]})

            changed += 1

    if updates:

        ws.batch_update(updates)
        cache_invalidate(ws)

    return changed


# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 2/22
# SUB-BLOCO: B/2
# REVISÃO: redução de leituras repetidas, melhor uso de cache e menor custo
# em busca de ciclos, players e decks sem alterar comportamento.
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

    out = {
        "season_id": season_id,
        "cycle": cycle,
        "status": None,
        "start_at_br": "",
        "deadline_at_br": "",
    }

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

        ws_cycles.append_row(
            [str(season_id), str(cycle), status, "", "", nowb, nowb],
            value_input_option="USER_ENTERED"
        )

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

        ws_cycles.append_row(
            [str(season_id), str(cycle), "open", start_at_br, deadline_at_br, nowb, nowb],
            value_input_option="USER_ENTERED"
        )

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


def get_auto_inscription_target(sh) -> tuple[int, int]:
    season_id = get_current_season_id(sh)
    if season_id <= 0:
        raise RuntimeError("Não existe season ativa.")

    ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    rows = cached_get_all_records(ws_cycles, ttl_seconds=10)

    open_cycles = []

    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        cyc = safe_int(r.get("cycle", 0), 0)
        st = str(r.get("status", "")).strip().lower()

        if cyc > 0 and st == "open":
            open_cycles.append(cyc)

    if not open_cycles:
        raise RuntimeError("Não existe ciclo aberto para inscrição na season atual.")

    cycle = max(open_cycles)
    return season_id, cycle


# =========================
# Cycle bonus helpers
# =========================
def get_cycle_bonus_percent(ws_bonus, season_id: int, cycle: int) -> float:
    """
    Retorna o bônus percentual vigente do ciclo.
    Se não existir registro, retorna 0.0
    """
    rows = cached_get_all_values(ws_bonus, ttl_seconds=10)
    if len(rows) <= 1:
        return 0.0

    col = ensure_sheet_columns(ws_bonus, CYCLE_BONUSES_REQUIRED)

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]

        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        c = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)

        if s != season_id or c != cycle:
            continue

        raw = r[col["bonus_percent"]] if col["bonus_percent"] < len(r) else 0
        return sheet_float(raw, 0.0)

    return 0.0


def set_cycle_bonus_percent(ws_bonus, season_id: int, cycle: int, bonus_percent: float):
    """
    Faz upsert do bônus percentual do ciclo.
    """
    rows = cached_get_all_values(ws_bonus, ttl_seconds=10)
    col = ensure_sheet_columns(ws_bonus, CYCLE_BONUSES_REQUIRED)

    nowb = now_br_str()
    found = None

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]

        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        c = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)

        if s == season_id and c == cycle:
            found = i
            break

    bonus_text = str(bonus_percent)

    if found is None:
        ws_bonus.append_row(
            [str(season_id), str(cycle), bonus_text, nowb],
            value_input_option="USER_ENTERED"
        )
    else:
        ws_bonus.batch_update([
            {
                "range": f"{col_letter(col['bonus_percent'])}{found}",
                "values": [[bonus_text]]
            },
            {
                "range": f"{col_letter(col['updated_at'])}{found}",
                "values": [[nowb]]
            },
        ])

    cache_invalidate(ws_bonus)


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

def get_deck_fields(ws_decks, row: int) -> dict:

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
# [BLOCO 2/22 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 3/22
# SUB-BLOCO: A/2
# REVISÃO: melhora de consistência em auto-confirmação, com melhor aproveitamento
# do índice RAM de matches sem perder segurança na localização da linha real no
# Sheets, além de manter as mesmas regras de prazo e anti-repetição.
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
    ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
    col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

    # Mantém leitura do Sheets para localizar a linha real, mas aproveita
    # o índice RAM para reduzir parte da filtragem quando possível.
    try:
        cycle_matches = get_matches_for_cycle_fast(
            sh,
            season_id=season_id,
            cycle=cycle,
            only_active=True
        )
    except Exception:
        cycle_matches = None

    rows = cached_get_all_values(ws_matches, ttl_seconds=10)
    if len(rows) <= 1:
        return 0

    allowed_match_ids = None
    if cycle_matches is not None:
        allowed_match_ids = {
            str(r.get("match_id", "")).strip()
            for r in cycle_matches
            if str(r.get("confirmed_status", "")).strip().lower() == "pending"
            and str(r.get("reported_by_id", "")).strip()
        }

    nowu = utc_now_dt()
    updated_at = now_iso_utc()
    changed = 0
    updates = []

    for rown in range(2, len(rows) + 1):
        r = rows[rown - 1]

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        mid = str(getc("match_id")).strip()
        if allowed_match_ids is not None and mid not in allowed_match_ids:
            continue

        r_season = safe_int(getc("season_id"), 0)
        r_cycle = safe_int(getc("cycle"), 0)
        if r_season != season_id or r_cycle != cycle:
            continue

        if not as_bool(getc("active") or "TRUE"):
            continue

        status = str(getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            continue

        reported_by = str(getc("reported_by_id") or "").strip()
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
# BLOCO ORIGINAL: BLOCO 3/22
# SUB-BLOCO: B/2
# REVISÃO: autocomplete ultra-rápido com prioridade para snapshots RAM,
# fallback leve para ensure_*_index e menor chance de Unknown interaction.
# =================================================

# =========================
# DEBOUNCE LEVE — AUTOCOMPLETE
# =========================
_AC_DEBOUNCE_STATE: dict[str, float] = {}
_AC_DEBOUNCE_LOCK = threading.Lock()
_AC_DEBOUNCE_WINDOW_SECONDS = 0.08

def _ac_should_skip(interaction: discord.Interaction, ac_name: str, window_seconds: float = _AC_DEBOUNCE_WINDOW_SECONDS) -> bool:
    try:
        uid = str(interaction.user.id)
    except Exception:
        uid = "0"

    key = f"{ac_name}:{uid}"
    now = _cache_now()

    with _AC_DEBOUNCE_LOCK:
        last = _AC_DEBOUNCE_STATE.get(key, 0.0)
        _AC_DEBOUNCE_STATE[key] = now

        expired = [k for k, ts in _AC_DEBOUNCE_STATE.items() if now - ts > 30]
        for k in expired:
            _AC_DEBOUNCE_STATE.pop(k, None)

    return (now - last) < window_seconds


# =========================
# Helpers rápidos — snapshot do autocomplete index
# =========================
def _get_match_ac_choices_snapshot_for_user(user_id: str, query: str = "", limit: int = 25) -> list[dict]:
    uid = str(user_id or "").strip()
    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    if not uid:
        return []

    try:
        with _MATCH_AC_INDEX_LOCK:
            items = list(_MATCH_AC_INDEX.get("by_user", {}).get(uid, []))
    except Exception:
        return []

    if not items:
        return []

    if not q:
        return [dict(item) for item in items[:limit]]

    out = []
    for item in items:
        if q in str(item.get("search", "")):
            out.append(dict(item))
            if len(out) >= limit:
                break
    return out


def _get_season_choices_snapshot(query: str = "", limit: int = 25) -> list[dict]:
    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    try:
        with _SEASON_RAM_INDEX_LOCK:
            items = list(_SEASON_RAM_INDEX.get("choices", []))
    except Exception:
        return []

    if not items:
        return []

    if not q:
        return [dict(item) for item in items[:limit]]

    out = []
    for item in items:
        if q in str(item.get("search", "")):
            out.append(dict(item))
            if len(out) >= limit:
                break
    return out


def _get_cycle_choices_snapshot(
    season_id: int,
    query: str = "",
    only_open: bool = False,
    limit: int = 25
) -> list[dict]:
    sid = safe_int(season_id, 0)
    if sid <= 0:
        return []

    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    try:
        with _CYCLE_RAM_LOCK:
            items = list(_CYCLE_RAM_INDEX.get("by_season", {}).get(sid, {}).get("choices", []))
    except Exception:
        return []

    if not items:
        return []

    out = []
    for item in items:
        status = str(item.get("status", "")).strip().lower()

        if only_open and status != "open":
            continue

        if q and q not in str(item.get("search", "")):
            continue

        out.append(dict(item))
        if len(out) >= limit:
            break

    return out


# =========================
# AUTOCOMPLETE FUNCTIONS
# =========================
async def ac_cycle_open(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_cycle_open"):
            return []

        q = str(current or "").strip().lower()

        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return []

        # 1) snapshot imediato
        items = _get_cycle_choices_snapshot(
            season_id=season_id,
            query=q,
            only_open=False,
            limit=25
        )

        # 2) fallback leve
        if not items:
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
    try:
        if _ac_should_skip(interaction, "ac_cycle_only_open"):
            return []

        q = str(current or "").strip().lower()

        sh = open_sheet()
        season_id = get_current_season_id(sh)
        if season_id <= 0:
            return []

        # 1) snapshot imediato
        items = _get_cycle_choices_snapshot(
            season_id=season_id,
            query=q,
            only_open=True,
            limit=25
        )

        # 2) fallback leve
        if not items:
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


# ✅ CORRIGIDO (mesmo padrão dos outros autocompletes)
async def ac_season_open(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_season_open"):
            return []

        q = str(current or "").strip().lower()

        # 1) snapshot imediato
        items = _get_season_choices_snapshot(query=q, limit=25)

        # 2) fallback leve
        if not items:
            sh = open_sheet()
            items = get_season_choices_fast(sh, query=q, limit=25)

        out: list[app_commands.Choice[int]] = []

        for item in items:
            sid = safe_int(item.get("season_id", 0), 0)
            status = str(item.get("status", "")).strip().lower()

            if sid <= 0 or status != "open":
                continue

            out.append(
                app_commands.Choice(
                    name=f"Season {sid}",
                    value=sid
                )
            )

            if len(out) >= 25:
                break

        return out[:25]

    except Exception:
        return []


async def ac_match_id_user_pending(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_match_id_user_pending"):
            return []

        uid = str(interaction.user.id).strip()
        q = str(current or "").strip().lower()

        # 1) snapshot imediato
        items = _get_match_ac_choices_snapshot_for_user(
            user_id=uid,
            query=q,
            limit=25
        )

        # 2) fallback leve
        if not items:
            sh = open_sheet()
            items = get_match_ac_choices_for_user(
                sh,
                user_id=uid,
                query=q,
                limit=25
            )

        out: list[app_commands.Choice[str]] = []
        seen = set()

        for item in items:
            label = str(item.get("label", "")).strip()
            value = str(item.get("value", "")).strip()

            if not label or not value or value in seen:
                continue

            out.append(app_commands.Choice(name=label[:100], value=value))
            seen.add(value)

            if len(out) >= 25:
                break

        return out[:25]

    except Exception:
        return []


async def ac_score_vde(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_score_vde"):
            return []

        q = str(current or "").strip().replace(" ", "")

        options = [
            ("2-0-0", "WIN"),
            ("2-1-0", "WIN"),
            ("1-0-0", "WIN"),

            ("0-2-0", "LOSS"),
            ("1-2-0", "LOSS"),
            ("0-1-0", "LOSS"),

            ("0-0-1", "DRAW"),
            ("1-1-0", "DRAW"),
            ("1-1-1", "DRAW"),
            ("0-0-3", "DRAW"),
        ]

        out = []

        for score, label in options:
            if q and q not in score:
                continue

            out.append(
                app_commands.Choice(
                    name=f"{score} ({label})",
                    value=score
                )
            )

        return out[:25]

    except Exception:
        return []


async def ac_owner_season(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_owner_season"):
            return []

        q = str(current or "").strip().lower()

        # 1) snapshot imediato
        items = _get_season_choices_snapshot(query=q, limit=25)

        # 2) fallback leve
        if not items:
            sh = open_sheet()
            items = get_season_choices_fast(sh, query=q, limit=25)

        out: list[app_commands.Choice[int]] = []
        for item in items:
            sid = safe_int(item.get("season_id", 0), 0)
            label = str(item.get("label", "")).strip()
            if sid <= 0 or not label:
                continue

            out.append(app_commands.Choice(name=label[:100], value=sid))

            if len(out) >= 25:
                break

        return out[:25]

    except Exception:
        return []


async def ac_owner_cycle_for_season(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_owner_cycle_for_season"):
            return []

        season_selected = safe_int(getattr(interaction.namespace, "season", 0), 0)
        q = str(current or "").strip().lower()

        if season_selected <= 0:
            return []

        # 1) snapshot imediato
        items = _get_cycle_choices_snapshot(
            season_id=season_selected,
            query=q,
            only_open=False,
            limit=25
        )

        # 2) fallback leve
        if not items:
            sh = open_sheet()
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

            if len(out) >= 25:
                break

        return out[:25]

    except Exception:
        return []


# =========================================================
# [BLOCO 3/22 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 4/22
# SUB-BLOCO: ÚNICA
# REVISÃO V10: ordenação persistida alinhada à regra oficial da liga
# =================================================

def recalculate_cycle(sh, season_id: int, cycle: int, bonus_percent: float | None = None):
    """
    Recalcula o ranking do ciclo (SEMPRE do zero), persistindo o Standing
    no padrão oficial atual da liga.

    Critério oficial de ordenação:
    1. score
    2. ppm
    3. mwp
    4. omw
    5. gw
    6. ogw

    Regras base:
    - Match points reais: Win=3, Draw=1, Loss=0 (por match)
    - MWP com piso de 33,3%
    - GWP com piso de 33,3%
    - OMW = média do MWP dos oponentes enfrentados
    - OGW = média do GWP dos oponentes enfrentados
    - Considera apenas matches:
      active=TRUE e confirmed_status=confirmed e result_type != bye

    Bônus:
    - aplicado sobre os pontos reais recalculados do zero
    - nunca acumula sobre bônus anterior
    """
    ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
    ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
    ws_standings = ensure_worksheet(sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30)
    ws_bonus = ensure_worksheet(sh, "CycleBonuses", CYCLE_BONUSES_HEADER, rows=2000, cols=10)

    try:
        sweep_auto_confirm(sh, season_id, cycle)
    except Exception:
        pass

    if bonus_percent is None:
        bonus_percent = get_cycle_bonus_percent(ws_bonus, season_id, cycle)

    bonus_percent = sheet_float(bonus_percent, 0.0)

    ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
    enr_rows = cached_get_all_records(ws_enr, ttl_seconds=10)

    all_player_ids = set()
    for r in enr_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue
        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue
        if str(r.get("status", "")).strip().lower() != "active":
            continue

        pid = str(r.get("player_id", "")).strip()
        if pid:
            all_player_ids.add(pid)

    stats = {}
    opponents = {}

    def ensure(pid: str):
        if pid not in stats:
            stats[pid] = {
                "real_match_points": 0.0,
                "matches_played": 0,
                "game_wins": 0,
                "game_losses": 0,
                "game_draws": 0,
                "games_played": 0,
            }
            opponents[pid] = []

    for pid in all_player_ids:
        ensure(pid)

    try:
        matches_rows = get_matches_for_cycle_fast(
            sh,
            season_id=season_id,
            cycle=cycle,
            only_active=True
        )
    except Exception:
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
            stats[a]["real_match_points"] += 3.0
        elif b_gw > a_gw:
            stats[b]["real_match_points"] += 3.0
        else:
            stats[a]["real_match_points"] += 1.0
            stats[b]["real_match_points"] += 1.0

        opponents[a].append(b)
        opponents[b].append(a)

    mwp = {}
    gwp = {}

    for pid, s in stats.items():
        mp_real = sheet_float(s["real_match_points"], 0.0)
        mplayed = s["matches_played"]

        mwp[pid] = (1 / 3) if mplayed == 0 else floor_333(mp_real / (3.0 * mplayed))

        gplayed = s["games_played"]
        if gplayed == 0:
            gwp[pid] = 1 / 3
        else:
            gwp_raw = (s["game_wins"] + 0.5 * s["game_draws"]) / float(gplayed)
            gwp[pid] = floor_333(gwp_raw)

    omw = {}
    ogw = {}

    for pid in stats.keys():
        opps = opponents.get(pid, [])
        if not opps:
            omw[pid] = 1 / 3
            ogw[pid] = 1 / 3
        else:
            omw_vals = [mwp.get(oid, 1 / 3) for oid in opps]
            ogw_vals = [gwp.get(oid, 1 / 3) for oid in opps]
            omw[pid] = sum(omw_vals) / len(omw_vals)
            ogw[pid] = sum(ogw_vals) / len(ogw_vals)

    K = 3
    out_rows = []

    for pid, s in stats.items():
        matches_played = s["matches_played"]
        real_points = round(sheet_float(s["real_match_points"], 0.0), 2)

        final_points = round(real_points * (1.0 + (bonus_percent / 100.0)), 2)

        ppm = (final_points + K) / (matches_played + K)
        peso_pts = matches_played / (matches_played + K)
        peso_ppm = K / (matches_played + K)
        score = (final_points * peso_pts) + (ppm * peso_ppm)

        out_rows.append({
            "season_id": season_id,
            "cycle": cycle,
            "player_id": pid,

            "matches_played": matches_played,
            "match_points": final_points,

            "matches": matches_played,
            "points": final_points,

            "real_points": real_points,
            "bonus_percent": bonus_percent,

            "ppm": round(ppm, 6),
            "score": round(score, 6),

            "mwp": round(mwp[pid], 6),
            "omw": round(omw[pid], 6),
            "gw": round(gwp[pid], 6),
            "ogw": round(ogw[pid], 6),

            "mwp_percent": pct1(mwp[pid]),

            "game_wins": s["game_wins"],
            "game_losses": s["game_losses"],
            "game_draws": s["game_draws"],
            "games_played": s["games_played"],

            "gw_percent": pct1(gwp[pid]),
            "omw_percent": pct1(omw[pid]),
            "ogw_percent": pct1(ogw[pid]),
        })

    out_rows.sort(
        key=lambda r: (
            r["score"],
            r["ppm"],
            r["mwp"],
            r["omw"],
            r["gw"],
            r["ogw"],
        ),
        reverse=True
    )

    ts = now_iso_utc()
    for i, r in enumerate(out_rows, start=1):
        r["rank_position"] = i
        r["last_recalc_at"] = ts

    header = [
        "season_id", "cycle", "player_id",
        "matches_played", "match_points",
        "matches", "points",
        "mwp", "omw", "gw", "ogw",
        "mwp_percent",
        "game_wins", "game_losses", "game_draws", "games_played", "gw_percent",
        "omw_percent", "ogw_percent",
        "rank_position", "last_recalc_at"
    ]

    existing = cached_get_all_values(ws_standings, ttl_seconds=10)
    kept = []

    if existing and len(existing) > 1:
        existing_header = existing[0]
        if existing_header == header:
            for r in existing[1:]:
                while len(r) < len(header):
                    r.append("")
                r_season = safe_int(r[0], 0)
                r_cycle = safe_int(r[1], 0)
                if r_season == season_id and r_cycle == cycle:
                    continue
                kept.append(r)

    values = []
    for r in out_rows:
        values.append([
            r["season_id"], r["cycle"], r["player_id"],
            r["matches_played"], r["match_points"],
            r["matches"], r["points"],
            r["mwp"], r["omw"], r["gw"], r["ogw"],
            r["mwp_percent"],
            r["game_wins"], r["game_losses"], r["game_draws"], r["games_played"], r["gw_percent"],
            r["omw_percent"], r["ogw_percent"],
            r["rank_position"], r["last_recalc_at"]
        ])

    ws_standings.clear()
    ws_standings.append_row(header)

    rows_to_write = []
    if kept:
        rows_to_write.extend(kept)
    if values:
        rows_to_write.extend(values)

    if rows_to_write:
        for i in range(0, len(rows_to_write), 500):
            ws_standings.append_rows(
                rows_to_write[i:i + 500],
                value_input_option="RAW"
            )

    cache_invalidate(ws_standings)
    return out_rows
    
# =========================================================
# [BLOCO 4/22 termina aqui]
# =================================================

# =================================================
# FIM DO SUB-BLOCO ÚNICA
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 5/22
# SUB-BLOCO: A/3
# REVISÃO: warm cache mais robusto e compatível com os índices RAM criados,
# incluindo MATCH AC INDEX, sem alterar a lógica do bot, setup, limpeza
# automática e utilitários já existentes.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - warm cache com yield para não travar setup_hook
# - sync protegido contra duplicação
# - setup_hook com medição de tempo
# - intents mantidas sem alterar regra funcional
# - estrutura pronta para reduzir risco de "application did not respond"
# =================================================

import asyncio
import time
from discord.ext import tasks

# =========================================================
# Warm cache dos índices RAM
# =========================================================
def _get_global_callable(name: str):
    fn = globals().get(name)
    return fn if callable(fn) else None


async def warm_ram_indexes():
    """
    Pré-carrega apenas os índices RAM leves no boot do bot para evitar
    custo alto de inicialização no Render.

    Observação:
    - faz best effort
    - não quebra o boot se algum índice ainda não existir
    - mantém compatibilidade com a organização atual por blocos
    - índices pesados de matches ficam em lazy load no primeiro uso
    """
    start_total = time.perf_counter()

    try:
        sh = open_sheet()
    except Exception:
        return

    warmers = [
        "ensure_player_ram_index",
        "ensure_cycle_ram_index",
        "ensure_season_ram_index",
    ]

    for fn_name in warmers:
        step_start = time.perf_counter()

        try:
            fn = _get_global_callable(fn_name)
            if fn is None:
                continue

            fn(sh)

            try:
                print(f"SETUP: {fn_name} ok em {round((time.perf_counter() - step_start) * 1000, 2)} ms")
            except Exception:
                pass

        except Exception as e:
            try:
                print(f"SETUP: {fn_name} falhou: {e}")
            except Exception:
                pass

        # cede o loop para não monopolizar o setup_hook
        try:
            await asyncio.sleep(0)
        except Exception:
            pass

    try:
        print(f"SETUP: warm_ram_indexes total em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================
# Discord Bot (Client)
# =========================
class LemeBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # necessário para fetch_member / on_member_join
        intents.message_content = True

        super().__init__(
            intents=intents,
            heartbeat_timeout=120.0
        )
        self.tree = app_commands.CommandTree(self)
        self._setup_done = False
        self._setup_lock = asyncio.Lock()

    async def setup_hook(self):
        async with self._setup_lock:
            if self._setup_done:
                try:
                    print("SETUP: setup_hook já executado, ignorando nova chamada.")
                except Exception:
                    pass
                return

            setup_total_start = time.perf_counter()

            # Views persistentes (funcionam após restart quando o bot volta online)
            add_view_start = time.perf_counter()
            try:
                self.add_view(OnboardingStartView())
                self.add_view(ResultConfirmView())
                print(f"SETUP: views persistentes registradas com sucesso em {round((time.perf_counter() - add_view_start) * 1000, 2)} ms.")
            except Exception as e:
                print(f"ERRO SETUP add_view: {e}")
                raise

            # cede o loop antes do sync
            try:
                await asyncio.sleep(0)
            except Exception:
                pass

            # Sync commands (guild-scoped quando possível)
            sync_start = time.perf_counter()
            try:
                if GUILD_ID:
                    guild = discord.Object(id=GUILD_ID)
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)
                    print(
                        f"SETUP: sync guild ok. Comandos sincronizados: {len(synced)} "
                        f"em {round((time.perf_counter() - sync_start) * 1000, 2)} ms"
                    )
                else:
                    synced = await self.tree.sync()
                    print(
                        f"SETUP: sync global ok. Comandos sincronizados: {len(synced)} "
                        f"em {round((time.perf_counter() - sync_start) * 1000, 2)} ms"
                    )
            except Exception as e:
                print(f"ERRO SETUP sync: {e}")
                raise

            # cede o loop antes do warm cache
            try:
                await asyncio.sleep(0)
            except Exception:
                pass

            # Warm cache dos índices RAM leves
            warm_start = time.perf_counter()
            try:
                await warm_ram_indexes()
                print(f"SETUP: warm_ram_indexes ok em {round((time.perf_counter() - warm_start) * 1000, 2)} ms.")
            except Exception as e:
                print(f"ERRO SETUP warm_ram_indexes: {e}")
                raise

            self._setup_done = True

            try:
                print(f"SETUP: setup_hook total em {round((time.perf_counter() - setup_total_start) * 1000, 2)} ms.")
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
# BLOCO ORIGINAL: BLOCO 5/22
# SUB-BLOCO: B/3
# REVISÃO: catálogo em ordem alfabética, inclusão dos comandos novos
# da fase final e tutorial revisado com foco no básico do jogador.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - send_followup_chunks mais resiliente e com menos tentativas redundantes
# - upsert_player com saída rápida, batch_update consolidado e medição leve
# - /comando e /tutorial com defer seguro e montagem mais leve
# - sem alterar regras funcionais do projeto
# =================================================

async def send_followup_chunks(interaction: discord.Interaction, text: str, ephemeral: bool = True, limit: int = 1900):
    """
    Regra do projeto: quando resposta for grande, enviar em múltiplas mensagens
    para não estourar o limite do Discord.
    """
    raw = str(text or "").strip()
    if not raw:
        return

    chunks = split_text_lines(raw, limit=limit)
    if not chunks:
        return

    first_sent = False

    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(chunks[0], ephemeral=ephemeral)
        else:
            await interaction.followup.send(chunks[0], ephemeral=ephemeral)
        first_sent = True
    except Exception:
        try:
            await interaction.followup.send(chunks[0], ephemeral=ephemeral)
            first_sent = True
        except Exception:
            return

    if not first_sent:
        return

    for c in chunks[1:]:
        try:
            await interaction.followup.send(c, ephemeral=ephemeral)
        except Exception:
            break


# =========================================================
# Players upsert (Sheets) — mínimo necessário
# + BLINDAGEM 429: invalida cache e PLAYER RAM INDEX após escrita
# =========================================================
def upsert_player(ws_players, discord_id: str, nickname: str):
    start_total = time.perf_counter()

    did = str(discord_id).strip()
    nick = str(nickname).strip()

    if not did or not nick:
        return

    col = ensure_sheet_columns(ws_players, PLAYERS_HEADER)
    rows = cached_get_all_values(ws_players, ttl_seconds=10)

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
            updates = []

            if "nick" in col:
                current_nick = ""
                row = rows[found_row - 1] if 0 < found_row <= len(rows) else []
                current_nick = str(row[col["nick"]] if col["nick"] < len(row) else "").strip()

                if current_nick != nick:
                    updates.append({
                        "range": f"{col_letter(col['nick'])}{found_row}",
                        "values": [[nick]]
                    })

            if "updated_at" in col:
                updates.append({
                    "range": f"{col_letter(col['updated_at'])}{found_row}",
                    "values": [[nowc]]
                })

            if updates:
                ws_players.batch_update(updates)
                cache_invalidate(ws_players)
                invalidate_player_ram_index()

        except Exception:
            pass

        try:
            print(f"DEBUG: upsert_player(update) {did} em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
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
        cache_invalidate(ws_players)
        invalidate_player_ram_index()
    except Exception:
        pass

    try:
        print(f"DEBUG: upsert_player(insert) {did} em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# /comando (catálogo)
# =========================================================
# OBS: aqui é catálogo (listagem). A implementação real dos comandos fica
# distribuída nos demais blocos. A intenção é:
# /comando sempre refletir os comandos oficiais disponíveis na base atual.
COMMANDS_CATALOG = [
    # Jogador
    ("jogador", "/abdicar_final", "Abdica da fase final antes do início oficial, se estiver classificado."),
    ("jogador", "/chaveamento", "Mostra o chaveamento da fase final."),
    ("jogador", "/comando", "Mostra os comandos que você tem acesso."),
    ("jogador", "/drop", "Sai do ciclo escolhido."),
    ("jogador", "/historico_confronto", "Mostra histórico de confrontos entre dois jogadores."),
    ("jogador", "/inscrever", "Se inscreve automaticamente no ciclo aberto com deck e decklist."),
    ("jogador", "/inscrever_final", "Se inscreve automaticamente na Fase Final do Ciclo TOP4, TOP8 ou TOP16, apenas se estiver classifcado."),
    ("jogador", "/meuid", "Mostra seu ID do Discord."),
    ("jogador", "/meus_matches", "Lista seus matches da season/ciclo."),
    ("jogador", "/meta", "Mostra o meta field da season e ciclo indicado."),
    ("jogador", "/pods_ver", "Mostra todos os PODs do ciclo com deck e decklist."),
    ("jogador", "/prazo", "Mostra o prazo oficial do ciclo."),
    ("jogador", "/ranking", "Mostra o ranking do ciclo."),
    ("jogador", "/ranking_geral", "Mostra o ranking geral da season."),
    ("jogador", "/rejeitar", "Rejeita um resultado pendente."),
    ("jogador", "/resultado", "Reporta resultado de um match do ciclo."),
    ("jogador", "/resultado_final", "Reporta resultado da fase final, se você estiver na match."),
    ("jogador", "/status_ciclo", "Mostra status e datas dos ciclos da season atual."),
    ("jogador", "/tutorial", "Mostra um guia rápido de como usar o bot."),

    # Administrativo (ADM / Organizador)
    ("adm", "/abdicar_final_adm", "Remove classificado da fase final e sobe o próximo do ranking."),
    ("adm", "/admin_resultado_cancelar", "Cancela um resultado e reabre o match."),
    ("adm", "/admin_resultado_editar", "Edita e confirma um resultado de match."),
    ("adm", "/admin_resultado_final_cancelar", "Cancela um resultado da fase final e limpa a progressão derivada."),
    ("adm", "/admin_resultado_final_editar", "Edita um resultado da fase final e repropaga o chaveamento."),
    ("adm", "/cadastrar_final", "Atualiza deck e decklist de participante já classificado na fase final."),
    ("adm", "/cadastrar_player", "Cadastra player manualmente com season, ciclo, deck e decklist."),
    ("adm", "/ciclo_abrir", "Abre um ciclo para inscrições."),
    ("adm", "/ciclo_fechar", "Fecha inscrições do ciclo."),
    ("adm", "/deadline", "Lista matches pending próximos de expirar."),
    ("adm", "/drop_adm", "Remove jogador do ciclo escolhido e resolve matches pendentes."),
    ("adm", "/estatisticas", "Mostra estatísticas gerais da liga."),
    ("adm", "/fechar_resultados_atrasados", "Auto-confirma pendências vencidas do ciclo."),
    ("adm", "/inscritos", "Lista inscritos, deck/decklist e pendências do ciclo."),
    ("adm", "/matches_ciclo", "Lista todas as matches do ciclo, separando registradas e pendentes."),
    ("adm", "/start_cycle", "Gera pods + matches e trava o ciclo."),
    ("adm", "/status_final", "Mostra diagnóstico e consistência da fase final."),
    ("adm", "/substituir_jogador", "Substitui um jogador por outro no ciclo."),

    # Owner
    ("owner", "/closeseason", "Fecha a season atual."),
    ("owner", "/ciclo_encerrar", "Encerra o ciclo como completed."),
    ("owner", "/exportar_ciclo", "Exporta CSV do ciclo."),
    ("owner", "/fase_final", "Gera a fase final da season após todos os ciclos completed."),
    ("owner", "/final", "Aplica 0-0-3 após o deadline do ciclo."),
    ("owner", "/final_iniciar", "Inicia oficialmente a fase final e trava alterações."),
    ("owner", "/forcesync", "Sincroniza os comandos do bot no servidor."),
    ("owner", "/onboarding", "Reposta o botão de onboarding no canal atual."),
    ("owner", "/recalcular", "Auto-confirma pendências vencidas e recalcula standings do ciclo."),
    ("owner", "/startseason", "Abre uma nova season e define como ativa."),
]


def level_allows(user_level: str, cmd_level: str) -> bool:
    order = {"jogador": 1, "adm": 2, "organizador": 3, "owner": 4}
    return order.get(user_level, 1) >= order.get(cmd_level, 1)


@client.tree.command(name="comando", description="Mostra seus comandos disponíveis.")
async def comando(interaction: discord.Interaction):
    start_total = time.perf_counter()

    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        user_level = await get_access_level(interaction)

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

        visible_lines = []
        for lvl, cmd, desc in COMMANDS_CATALOG:
            if not level_allows(user_level, lvl):
                continue

            if real_cmds and cmd not in real_cmds:
                continue

            visible_lines.append(f"• **{cmd}** — {desc}")

        lines = [f"📌 **Seus comandos disponíveis ({titulo_nivel})**\n"]
        lines.extend(visible_lines)

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True, limit=1500)

    except Exception as e:
        try:
            await interaction.followup.send(f"❌ Erro no /comando: {e}", ephemeral=True)
        except Exception:
            pass

    try:
        print(f"DEBUG: /comando em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# /tutorial
# =========================================================
@client.tree.command(name="tutorial", description="Mostra um tutorial rápido de como usar o bot.")
async def tutorial(interaction: discord.Interaction):
    start_total = time.perf_counter()

    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
    except Exception:
        pass

    try:
        lines = [
            "📘 **Tutorial rápido do jogador**",
            "",
            "**1. Veja seus comandos**",
            "• Use **/comando** para ver tudo que está liberado para você.",
            "",
            "**2. Faça seu cadastro no servidor**",
            "• Use o botão de onboarding quando ele estiver disponível.",
            "• Isso ajuda o bot a reconhecer você corretamente.",
            "",
            "**3. Inscreva-se no ciclo**",
            "• Use **/inscrever** informando guilda, arquétipo e link da decklist.",
            "• O sistema inscreve você no ciclo aberto da season atual.",
            "",
            "**4. Consulte os PODs e suas partidas**",
            "• Use **/pods_ver** para ver os grupos do ciclo.",
            "• Use **/meus_matches** para ver suas partidas.",
            "",
            "**5. Envie seu resultado**",
            "• Use **/resultado** para reportar o placar da sua match.",
            "• O oponente pode confirmar por DM do bot ou rejeitar com **/rejeitar**.",
            "• Se não houver rejeição em até **48h**, o sistema pode auto-confirmar.",
            "",
            "**6. Acompanhe o campeonato**",
            "• Use **/ranking** para ver o ranking do ciclo.",
            "• Use **/ranking_geral** para ver o ranking geral da season.",
            "• Use **/prazo** para consultar a data limite do ciclo.",
            "• Use **/meta** para ver o meta field.",
            "",
            "**7. Se precisar sair do ciclo**",
            "• Use **/drop** para sair do ciclo escolhido.",
            "",
            "**8. Se você se classificar para a fase final**",
            "• Use **/inscrever_final** para se inscrever.",
            "• Use **/chaveamento** para acompanhar o mata-mata.",
            "• Use **/resultado_final** para reportar sua match da fase final.",
            "• Antes do início oficial, você pode usar **/abdicar_final** se precisar sair.",
            "",
            "**Comandos mais usados no dia a dia**",
            "• /comando",
            "• /inscrever",
            "• /pods_ver",
            "• /meus_matches",
            "• /resultado",
            "• /rejeitar",
            "• /ranking",
            "• /ranking_geral",
            "• /prazo",
            "",
            "**Se tiver dúvida, acione um ADM ou Organizador.**",
        ]

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True, limit=1500)

    except Exception as e:
        try:
            await interaction.followup.send(f"❌ Erro no /tutorial: {e}", ephemeral=True)
        except Exception:
            pass

    try:
        print(f"DEBUG: /tutorial em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# Comandos básicos do onboarding (BLOCO 5)
# =========================================================
@client.tree.command(name="meuid", description="Mostra seu ID do Discord.")
async def meuid(interaction: discord.Interaction):
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(f"Seu ID é: `{interaction.user.id}`", ephemeral=True)
        else:
            await interaction.followup.send(f"Seu ID é: `{interaction.user.id}`", ephemeral=True)
    except Exception:
        pass

# =================================================
# FIM DO SUB-BLOCO B/3
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 5/22
# SUB-BLOCO: C/3
# REVISÃO: redução de duplicação interna, blindagem extra contra timeout
# em interações por botão/DM e melhor aproveitamento de leitura de player,
# sem alterar a lógica funcional do onboarding e da confirmação de resultado.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - respostas mais seguras em modal/botões
# - redução de leituras redundantes
# - debug leve de tempo
# - proteção extra em followup e edição de view
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
        start_total = time.perf_counter()

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
            guild = interaction.guild
            nome_salvo = raw

            if guild:
                member = guild.get_member(interaction.user.id)
                if member is None:
                    try:
                        member = await guild.fetch_member(interaction.user.id)
                    except Exception:
                        member = None

                if member is not None:
                    try:
                        await member.edit(
                            nick=nome_salvo,
                            reason="Onboarding Leme Holandês"
                        )
                    except Exception:
                        try:
                            await interaction.followup.send(
                                "⚠️ Não consegui aplicar seu Nome e Sobrenome no servidor. Verifique se o bot tem permissão de gerenciar apelidos e tente novamente.",
                                ephemeral=True
                            )
                        except Exception:
                            pass
                        return

                    role = discord.utils.get(guild.roles, name=ROLE_JOGADOR)
                    if role:
                        try:
                            await member.add_roles(role, reason="Onboarding Leme Holandês")
                        except Exception:
                            pass

            await interaction.followup.send(
                "✅ Cadastro concluído com sucesso. Você está marcado como **Jogador**.",
                ephemeral=True
            )

        except Exception:
            try:
                await interaction.followup.send(
                    "✅ Cadastro salvo com sucesso.",
                    ephemeral=True
                )
            except Exception:
                pass

        try:
            print(f"DEBUG: NicknameModal.on_submit em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
        except Exception:
            pass


class OnboardingStartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Iniciar cadastro", style=discord.ButtonStyle.success, custom_id="lhb_onb_start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(NicknameModal())
        except Exception:
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(
                        "⚠️ Não consegui abrir o formulário agora. Tente novamente.",
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        "⚠️ Não consegui abrir o formulário agora. Tente novamente.",
                        ephemeral=True
                    )
            except Exception:
                pass


# =========================================================
# RESULTADO — Helpers internos da View de confirmação por DM
# =========================================================
def _extract_match_id_from_interaction_message(interaction: discord.Interaction) -> str:
    try:
        message = interaction.message
        if not message or not message.embeds:
            return ""

        embed = message.embeds[0]
        if embed.footer and embed.footer.text:
            footer = str(embed.footer.text).strip()
            if footer.lower().startswith("match_id:"):
                return footer.split(":", 1)[1].strip()
    except Exception:
        return ""

    return ""


def _find_match_sheet_row_by_id(ws_matches, col: dict, match_id: str):
    rows = cached_get_all_values(ws_matches, ttl_seconds=10)

    target = str(match_id).strip()

    for idx in range(1, len(rows)):
        r = rows[idx]
        val = r[col["match_id"]] if col["match_id"] < len(r) else ""
        if str(val).strip() == target:
            return idx + 1, r

    return None, None


async def _disable_result_view_message(view: discord.ui.View, interaction: discord.Interaction):
    try:
        for child in view.children:
            child.disabled = True
        if interaction.message:
            await interaction.message.edit(view=view)
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
        start_total = time.perf_counter()

        try:
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass

            match_id = _extract_match_id_from_interaction_message(interaction)
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

            if match_row is not None:
                found, row_data = _find_match_sheet_row_by_id(ws_matches, col, match_id)
            else:
                found, row_data = _find_match_sheet_row_by_id(ws_matches, col, match_id)

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

            if season_id > 0 and cycle > 0:
                try:
                    recalculate_cycle(sh, season_id, cycle)
                except Exception:
                    pass

            await _disable_result_view_message(self, interaction)

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

        try:
            print(f"DEBUG: ResultConfirmView.confirm em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
        except Exception:
            pass

    @discord.ui.button(label="Rejeitar", style=discord.ButtonStyle.danger, custom_id="lhb_result_reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        start_total = time.perf_counter()

        try:
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass

            match_id = _extract_match_id_from_interaction_message(interaction)
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

            if match_row is not None:
                found, row_data = _find_match_sheet_row_by_id(ws_matches, col, match_id)
            else:
                found, row_data = _find_match_sheet_row_by_id(ws_matches, col, match_id)

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

            await _disable_result_view_message(self, interaction)

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

        try:
            print(f"DEBUG: ResultConfirmView.reject em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
        except Exception:
            pass


# =========================================================
# Posting do onboarding no canal (comando admin)
# =========================================================
async def post_onboarding_message(channel: discord.abc.Messageable):
    base = "Bem-vindo ao **Leme Holandês**! Para começar, clique no botão abaixo:"
    try:
        await channel.send(base, view=OnboardingStartView())
    except Exception:
        pass


@client.tree.command(name="onboarding", description="Reposta o botão de onboarding no canal atual (OWNER).")
async def onboarding(interaction: discord.Interaction):
    start_total = time.perf_counter()

    try:
        if not await is_owner_only(interaction):
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)
            return
    except Exception:
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)
        except Exception:
            pass
        return

    try:
        if not interaction.response.is_done():
            await interaction.response.send_message("✅ Onboarding repostado neste canal.", ephemeral=True)
        else:
            await interaction.followup.send("✅ Onboarding repostado neste canal.", ephemeral=True)
    except Exception:
        pass

    try:
        if interaction.channel is not None:
            await post_onboarding_message(interaction.channel)
    except Exception:
        pass

    try:
        print(f"DEBUG: /onboarding em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# [BLOCO 5/22 termina aqui]
# =========================================================

# =================================================
# FIM DO SUB-BLOCO C/3
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 6/22
# SUB-BLOCO: A/2
# REVISÃO: melhor aproveitamento dos índices RAM de Players, redução de
# leituras repetidas no fluxo de inscrição e manutenção integral das regras.
# =================================================

# =========================================================
# [BLOCO 6/22] — INSCRIÇÃO + DROP + DECK/DECKLIST (REVISADO)
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
# LISTAS PRESETADAS — DECKS (ORDENADAS)
# =========================================================

DECK_GUILDAS = [
    "Sem Guilda",
    "Tribal",
    "5C",
    "4C",
    "Abzan",
    "Azorius",
    "Bant",
    "Boros",
    "Colorless",
    "Dimir",
    "Esper",
    "Golgari",
    "Grixis",
    "Gruul",
    "Izzet",
    "Jeskai",
    "Jund",
    "Mardu",
    "Mono Black",
    "Mono Blue",
    "Mono Green",
    "Mono Red",
    "Mono White",
    "Naya",
    "Orzhov",
    "Rakdos",
    "Selesnya",
    "Simic",
    "Sultai",
    "Temur",
]


DECK_ARQUETIPOS = [
    "Affinity",
    "Aggro",
    "Amulet Titan",
    "Asmo Food",
    "Belcher",
    "Blink",
    "Broodscale",
    "Burn",
    "Combo",
    "Control",
    "Creativity",
    "Death's Shadow",
    "Delirium",
    "Domain Zoo",
    "Dredge",
    "Eldrazi",
    "Eldrazi Aggro",
    "Eldrazi Breach",
    "Eldrazi Ramp",
    "Elementals",
    "Emry Station",
    "Energy",
    "Goblins",
    "Goryo's",
    "Hammer",
    "Hollow One",
    "Humans",
    "Land Destruction",
    "Living End",
    "Merfolk",
    "Metalcraft",
    "Midrange",
    "Mill",
    "Miracles",
    "Murktide",
    "Necro",
    "Neoform",
    "Omnath",
    "Prowess",
    "Reanimator",
    "Ritual",
    "Ruby Storm",
    "Samwise Gamgee Combo",
    "Scam",
    "Song of Creation",
    "Spirits",
    "Storm",
    "Tempo",
    "Through the Breach",
    "Tron",
    "Valakut",
    "Vampire",
    "Wizards",
    "Yawgmoth",
    "Zombies",
]


# =========================================================
# HELPERS DE NORMALIZAÇÃO / VALIDAÇÃO
# =========================================================
def _normalize_deck_token(s: str) -> str:
    return " ".join(str(s or "").strip().split()).lower()


def _resolve_case_insensitive_choice(raw_value: str, allowed_items: list[str]) -> str:
    norm = _normalize_deck_token(raw_value)
    for item in allowed_items:
        if _normalize_deck_token(item) == norm:
            return item
    return ""


# =========================================================
# AUTOCOMPLETE
# =========================================================

def _filter_preset_choices(items: list[str], current: str, limit: int = 25) -> list[str]:
    q = str(current or "").strip().lower()
    limit = max(1, min(limit, 25))

    if not q:
        return items[:limit]

    starts = [x for x in items if x.lower().startswith(q)]
    contains = [x for x in items if q in x.lower() and x not in starts]

    return (starts + contains)[:limit]


async def ac_deck_guilda(interaction: discord.Interaction, current: str):
    try:
        items = _filter_preset_choices(DECK_GUILDAS, current)
        return [app_commands.Choice(name=i, value=i) for i in items]
    except Exception:
        return []


async def ac_deck_arquetipo(interaction: discord.Interaction, current: str):
    try:
        items = _filter_preset_choices(DECK_ARQUETIPOS, current)
        return [app_commands.Choice(name=i, value=i) for i in items]
    except Exception:
        return []


# =========================================================
# MONTAGEM DO NOME DO DECK
# =========================================================

def _montar_nome_deck(guilda: str, arquetipo: str) -> str:
    g = " ".join(str(guilda or "").strip().split())
    a = " ".join(str(arquetipo or "").strip().split())

    if not a:
        return ""

    # regra especial
    if g.lower() == "sem guilda":
        return a

    return f"{g} {a}".strip()


# =========================================================
# /inscrever (AJUSTADO)
# =========================================================

@client.tree.command(name="inscrever", description="Se inscreve no ciclo informando guilda, arquétipo e decklist.")
@app_commands.describe(
    guilda="Base do deck",
    arquetipo="Arquétipo do deck",
    decklist="Link da decklist"
)
@app_commands.autocomplete(guilda=ac_deck_guilda, arquetipo=ac_deck_arquetipo)
async def inscrever(interaction: discord.Interaction, guilda: str, arquetipo: str, decklist: str):

    await interaction.response.defer(ephemeral=True)

    ok, val = validate_decklist_url(decklist)
    if not ok:
        return await interaction.followup.send(val, ephemeral=True)

    try:
        sh = open_sheet()
        season, cycle = get_auto_inscription_target(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER)

        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        pid = str(interaction.user.id).strip()

        player_record = get_player_record_by_discord_id(ws_players, pid)
        if not player_record:
            return await interaction.followup.send("❌ Cadastro não encontrado.", ephemeral=True)

        nick_atual = str(player_record.get("nick", "")).strip()

        if not season_exists(sh, season):
            return await interaction.followup.send(f"❌ Season {season} não existe.", ephemeral=True)

        cf = get_cycle_fields(ws_cycles, season, cycle)

        if str(cf.get("status", "")).lower() != "open":
            return await interaction.followup.send("❌ Ciclo não está aberto.", ephemeral=True)

        if player_active_in_cycle(ws_enr, season, cycle, pid):
            return await interaction.followup.send("❌ Você já está inscrito.", ephemeral=True)

        # validações (case insensitive + padronização)
        guilda_final = _resolve_case_insensitive_choice(guilda, DECK_GUILDAS)
        if not guilda_final:
            return await interaction.followup.send("❌ Guilda inválida.", ephemeral=True)

        arquetipo_final = _resolve_case_insensitive_choice(arquetipo, DECK_ARQUETIPOS)
        if not arquetipo_final:
            return await interaction.followup.send("❌ Arquétipo inválido.", ephemeral=True)

        nome_deck = _montar_nome_deck(guilda_final, arquetipo_final)

        if not nome_deck or len(nome_deck) > 80:
            return await interaction.followup.send("❌ Nome de deck inválido.", ephemeral=True)

        nowb = now_br_str()

        ws_enr.append_row(
            [str(season), str(cycle), pid, "active", nowb, nowb],
            value_input_option="USER_ENTERED"
        )
        cache_invalidate(ws_enr)

        rown = ensure_deck_row(ws_decks, season, cycle, pid)
        col_decks = ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        ws_decks.batch_update([
            {"range": f"{col_letter(col_decks['deck'])}{rown}", "values": [[nome_deck]]},
            {"range": f"{col_letter(col_decks['decklist_url'])}{rown}", "values": [[val]]},
            {"range": f"{col_letter(col_decks['updated_at'])}{rown}", "values": [[nowb]]},
        ])
        cache_invalidate(ws_decks)

        await interaction.followup.send(
            f"✅ Inscrição confirmada\n"
            f"Season {season} / Ciclo {cycle}\n"
            f"Deck: **{nome_deck}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 6/22
# SUB-BLOCO: B/2
# REVISÃO FINAL: FIX REAL do AUTO_FORFEIT + DROP ADM (CORRIGIDO)
# =================================================

# =========================================================
# Helper: resolve matches de player que dropou
# =========================================================
def resolve_drop_matches(sh, season_id: int, cycle: int, player_id: str) -> int:
    try:
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        rows = cached_get_all_values(ws_matches, ttl_seconds=10)
        updates = []
        resolved = 0
        updated_at = now_iso_utc()

        pid = str(player_id).strip()

        for idx in range(1, len(rows)):
            r = rows[idx]

            def getc(name: str) -> str:
                ci = col[name]
                return r[ci] if ci < len(r) else ""

            if safe_int(getc("season_id"), 0) != season_id:
                continue

            if safe_int(getc("cycle"), 0) != cycle:
                continue

            if not as_bool(getc("active") or "TRUE"):
                continue

            if str(getc("confirmed_status")).strip().lower() == "confirmed":
                continue

            pa = str(getc("player_a_id")).strip()
            pb = str(getc("player_b_id")).strip()

            if pid not in (pa, pb):
                continue

            rown = idx + 1

            if pa == pid:
                a_w, b_w = 0, 2
            else:
                a_w, b_w = 2, 0

            updates.extend([
                {"range": f"{col_letter(col['a_games_won'])}{rown}", "values": [[a_w]]},
                {"range": f"{col_letter(col['b_games_won'])}{rown}", "values": [[b_w]]},
                {"range": f"{col_letter(col['draw_games'])}{rown}", "values": [[0]]},
                {"range": f"{col_letter(col['result_type'])}{rown}", "values": [["auto_forfeit"]]},
                {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["confirmed"]]},
                {"range": f"{col_letter(col['reported_by_id'])}{rown}", "values": [["SYSTEM"]]},
                {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [["SYSTEM"]]},
                {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
            ])

            resolved += 1

        if updates:
            ws_matches.batch_update(updates)
            cache_invalidate(ws_matches)
            invalidate_match_ram_index()
            invalidate_match_ac_index()

        return resolved

    except Exception as e:
        print("ERRO resolve_drop_matches:", e)
        return 0

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
        nowb = now_br_str()

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

                if str(getc("status")).strip().lower() == "dropped":
                    return await interaction.followup.send("⚠️ Você já saiu deste ciclo.", ephemeral=True)

                ws_enr.batch_update([
                    {"range": f"{col_letter(col['status'])}{rown}", "values": [["dropped"]]},
                    {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[nowb]]},
                ])
                cache_invalidate(ws_enr)

                resolved = resolve_drop_matches(sh, season_id, cycle, pid)

                await log_admin(
                    interaction,
                    f"drop: {interaction.user.display_name} ({pid}) season={season_id} ciclo={cycle} resolved={resolved}"
                )

                return await interaction.followup.send(
                    f"✅ Viadinho, cagão. Você saiu do ciclo, desista da sua vida!.\n⚙️ {resolved} matches resolvidas como **2-0 AUTO_FORFEIT**.",
                    ephemeral=True
                )

        await interaction.followup.send("❌ Você não está inscrito neste ciclo.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /drop: {e}", ephemeral=True)


# =========================================================
# AUTOCOMPLETE — PLAYER POR SEASON + CICLO (COM NICK)
# =========================================================
async def ac_player_in_cycle(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()

        season = interaction.namespace.season
        cycle = interaction.namespace.cycle

        if not season or not cycle:
            return []

        ws = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        col = ensure_sheet_columns(ws, ENROLLMENTS_REQUIRED)
        rows = cached_get_all_values(ws, ttl_seconds=10)

        nick_map = get_player_nick_map_fast(sh)

        choices = []

        for r in rows[1:]:
            def getc(name: str) -> str:
                ci = col[name]
                return r[ci] if ci < len(r) else ""

            if (
                safe_int(getc("season_id"), 0) == season
                and safe_int(getc("cycle"), 0) == cycle
            ):
                pid = str(getc("player_id")).strip()
                if not pid:
                    continue

                nick = nick_map.get(pid, pid)

                # filtro pelo texto digitado (nick ou id)
                if current.lower() in nick.lower() or current in pid:
                    choices.append(
                        app_commands.Choice(
                            name=f"{nick}",
                            value=pid
                        )
                    )

        return choices[:25]

    except Exception as e:
        print("ERRO ac_player_in_cycle:", e)
        return []


# =========================================================
# /drop_adm
# =========================================================
@client.tree.command(name="drop_adm", description="(ADM) Remove jogador e resolve matches.")
@app_commands.describe(season="Season", cycle="Ciclo", jogador="Jogador")
@app_commands.autocomplete(season=ac_season_open, cycle=ac_cycle_open, jogador=ac_player_in_cycle)
async def drop_adm(interaction: discord.Interaction, season: int, cycle: int, jogador: str):
    await interaction.response.defer(ephemeral=True)

    try:
        if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
            return await interaction.followup.send("❌ Sem permissão.", ephemeral=True)

        sh = open_sheet()

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER)
        col = ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)

        rows = cached_get_all_values(ws_enr, ttl_seconds=10)
        nowb = now_br_str()

        for idx in range(1, len(rows)):
            r = rows[idx]

            def getc(name: str) -> str:
                ci = col[name]
                return r[ci] if ci < len(r) else ""

            if (
                safe_int(getc("season_id"), 0) == season
                and safe_int(getc("cycle"), 0) == cycle
                and str(getc("player_id")).strip() == str(jogador)
            ):
                rown = idx + 1

                if str(getc("status")).strip().lower() == "dropped":
                    return await interaction.followup.send("⚠️ Já está dropped.", ephemeral=True)

                ws_enr.batch_update([
                    {"range": f"{col_letter(col['status'])}{rown}", "values": [["dropped"]]},
                    {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[nowb]]},
                ])
                cache_invalidate(ws_enr)

                resolved = resolve_drop_matches(sh, season, cycle, str(jogador))

                nick = get_player_nick_map_fast(sh).get(str(jogador), str(jogador))

                await log_admin(
                    interaction,
                    f"drop_adm: executor={interaction.user.display_name} target={nick} ({jogador}) S{season} C{cycle} resolved={resolved}"
                )

                return await interaction.followup.send(
                    f"✅ {nick} removido.\n⚙️ {resolved} matches = AUTO_FORFEIT",
                    ephemeral=True
                )

        return await interaction.followup.send("❌ Jogador não encontrado.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================================================
# FIM DO BLOCO 6/B
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 7/22
# SUB-BLOCO: A/2
# REVISÃO: debounce nos autocompletes de /pods_ver, melhor montagem dos
# /meta - meta field
# chunks e menor custo de leitura/organização sem alterar a lógica funcional.
# =================================================

# =========================================================
# [BLOCO 7/22] — RESULTADOS + PODS/MATCHES DO JOGADOR
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
        if _ac_should_skip(interaction, "ac_pods_ver_season"):
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


async def ac_pods_ver_cycle(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_pods_ver_cycle"):
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

        lines = [f"📦 **PODs da Season {season} / Ciclo {cycle}**"]

        for pod in sorted(pods.keys(), key=pod_sort_key):
            players = list(dict.fromkeys(pods[pod]))
            lines.append("")
            lines.append(f"**POD {pod}**")

            for i, pid in enumerate(players, start=1):
                deck_info = deck_map.get((season, cycle, pid), {})
                deck_name = deck_info.get("deck", "") or "PENDENTE"
                decklist_url = deck_info.get("decklist_url", "") or "PENDENTE"

                lines.append(f"{i}. **{_player_display_name(nick_map, pid)}**")
                lines.append(f"   Deck: {deck_name}")
                lines.append(f"   Decklist: <{decklist_url}>")

        text = "\n".join(lines).strip()

        if not text:
            return await interaction.followup.send(
                f"❌ Não consegui montar a visualização dos PODs da **Season {season} / Ciclo {cycle}**.",
                ephemeral=True
            )

        await send_followup_chunks(interaction, text, ephemeral=True, limit=1900)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_ver: {e}", ephemeral=True)


# =========================================================
# /meta
# =========================================================
@client.tree.command(name="meta", description="Mostra o meta field do ciclo.")
@app_commands.describe(season="Season", cycle="Número do ciclo")
@app_commands.autocomplete(season=ac_pods_ver_season, cycle=ac_pods_ver_cycle)
async def meta(interaction: discord.Interaction, season: int, cycle: int):
    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send(
                f"❌ A season {season} não existe.",
                ephemeral=False
            )

        ws_cycles = ensure_worksheet(
            sh,
            "Cycles",
            CYCLES_HEADER,
            rows=2000,
            cols=25
        )

        cf = get_cycle_fields(ws_cycles, season, cycle)

        if cf.get("status") is None:
            return await interaction.followup.send(
                f"❌ O ciclo {cycle} não existe na season {season}.",
                ephemeral=False
            )

        ws_decks = ensure_worksheet(
            sh,
            "Decks",
            DECKS_HEADER,
            rows=10000,
            cols=25
        )

        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        meta_rows, total = _build_meta_rows(
            ws_decks,
            season,
            cycle
        )

        if total == 0 or not meta_rows:
            return await interaction.followup.send(
                f"⚠️ Não há decks cadastrados na **Season {season} / Ciclo {cycle}**.",
                ephemeral=False
            )

        lines = [
            f"📊 **Meta Field LEME HOLANDÊS ⚓🚢**",
            f" **Season {season} / Ciclo {cycle}**",
            f"Total de decks registrados: **{total}**",
            ""
        ]

        for i, (deck_name, qtd, pct_txt) in enumerate(meta_rows, start=1):
            lines.append(
                f"{i} - **{deck_name}**: {pct_txt}%"
            )

        text = "\n".join(lines).strip()

        await send_followup_chunks(
            interaction,
            text,
            ephemeral=False,
            limit=1900
        )

    except Exception as e:
        await interaction.followup.send(
            f"❌ Erro no /meta: {e}",
            ephemeral=False
        )

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
                f"• **{_player_display_name(nick_map, uid)}** "
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
# BLOCO ORIGINAL: BLOCO 7/22
# SUB-BLOCO: B/2
# REVISÃO CRÍTICA: proteção contra overwrite de AUTO_FORFEIT + match inativo
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - redução de leituras redundantes no fluxo de resultado/rejeitar
# - busca rápida pela linha quando possível
# - defer seguro + debug leve
# - sem alterar a lógica funcional do sistema
# =================================================

# =========================================================
# Helpers internos — resultado / rejeitar
# =========================================================
def _find_match_sheet_row_by_match_id(ws_matches, col: dict, match_id: str):
    rows = cached_get_all_values(ws_matches, ttl_seconds=10)
    target = str(match_id).strip()

    for idx in range(1, len(rows)):
        r = rows[idx]
        val = r[col["match_id"]] if col["match_id"] < len(r) else ""
        if str(val).strip() == target:
            return idx + 1, r

    return None, None


def _get_match_sheet_row_preferring_fast(
    sh,
    ws_matches,
    col: dict,
    match_id: str
):
    """
    Estratégia:
    1) tenta índice RAM primeiro
    2) localiza a linha real no Sheets apenas uma vez
    """
    match_row = None

    try:
        match_row = get_match_by_id_fast(sh, match_id)
    except Exception:
        match_row = None

    found_row, row_data = _find_match_sheet_row_by_match_id(ws_matches, col, match_id)

    return match_row, found_row, row_data


def _match_getc_factory(match_row: dict | None, row_data: list | None, col: dict):
    def getc(name: str) -> str:
        if match_row is not None and name in match_row:
            return str(match_row.get(name, ""))
        ci = col[name]
        return row_data[ci] if row_data is not None and ci < len(row_data) else ""
    return getc


async def _try_send_result_dm(
    interaction: discord.Interaction,
    opponent_id: str,
    match_id: str,
    player_a_name: str,
    player_b_name: str,
    placar: str
) -> bool:
    dm_sent = False

    try:
        guild = interaction.guild
        opponent_member = None

        if guild:
            opponent_member = guild.get_member(int(opponent_id))
            if opponent_member is None:
                try:
                    opponent_member = await guild.fetch_member(int(opponent_id))
                except Exception:
                    opponent_member = None

        if opponent_member is None:
            try:
                opponent_member = await client.fetch_user(int(opponent_id))
            except Exception:
                opponent_member = None

        if opponent_member is not None:
            embed = discord.Embed(
                title="Confirmação de resultado pendente",
                description=(
                    "Seu oponente lançou um resultado e sua confirmação é necessária.\n\n"
                    f"**Match:** `{match_id}`\n"
                    f"**Confronto:** {player_a_name} vs {player_b_name}\n"
                    f"**Placar informado:** **{placar}**\n\n"
                    f"Você pode **Confirmar** ou **Rejeitar** abaixo.\n"
                    f"Se não houver rejeição em até **{AUTO_CONFIRM_HOURS}h**, o sistema poderá auto-confirmar."
                ),
            )
            embed.set_footer(text=f"match_id:{match_id}")

            await opponent_member.send(embed=embed, view=ResultConfirmView())
            dm_sent = True

    except Exception:
        dm_sent = False

    return dm_sent


# =========================================================
# /resultado
# =========================================================
@client.tree.command(name="resultado", description="Reporta resultado de um match (V-D-E).")
@app_commands.describe(oponente="Selecione seu oponente", placar="Formato V-D-E (ex: 2-1-0)")
@app_commands.autocomplete(oponente=ac_match_id_user_pending, placar=ac_score_vde)
async def resultado(interaction: discord.Interaction, oponente: str, placar: str):
    start_total = time.perf_counter()

    await interaction.response.defer(ephemeral=True)

    match_id = str(oponente).strip()
    parsed = parse_vde(placar)

    if not parsed:
        return await interaction.followup.send(
            "❌ Placar inválido.",
            ephemeral=True
        )

    v, d, e = parsed

    try:
        sh = open_sheet()
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        pid = str(interaction.user.id).strip()

        match_row, found_row, row_data = _get_match_sheet_row_preferring_fast(
            sh=sh,
            ws_matches=ws_matches,
            col=col,
            match_id=match_id
        )

        if not found_row:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        getc = _match_getc_factory(match_row, row_data, col)

        # 🔥 NOVAS PROTEÇÕES
        if str(getc("active")).strip().lower() != "true":
            return await interaction.followup.send(
                "❌ Este match já foi encerrado.",
                ephemeral=True
            )

        if str(getc("result_type")).strip().upper() == "AUTO_FORFEIT":
            return await interaction.followup.send(
                "❌ Este match foi resolvido automaticamente (drop).",
                ephemeral=True
            )

        player_a = str(getc("player_a_id")).strip()
        player_b = str(getc("player_b_id")).strip()
        status = str(getc("confirmed_status")).strip().lower()

        if pid not in (player_a, player_b):
            return await interaction.followup.send("❌ Você não participa.", ephemeral=True)

        if status == "confirmed":
            return await interaction.followup.send("❌ Já confirmado.", ephemeral=True)

        rown = found_row

        if pid == player_a:
            a_val, b_val = v, d
            opponent_id = player_b
        else:
            a_val, b_val = d, v
            opponent_id = player_a

        nowu = utc_now_dt()
        updated_at = now_iso_utc()
        auto_confirm_at = auto_confirm_deadline_iso(nowu)

        ws_matches.batch_update([
            {"range": f"{col_letter(col['a_games_won'])}{rown}", "values": [[a_val]]},
            {"range": f"{col_letter(col['b_games_won'])}{rown}", "values": [[b_val]]},
            {"range": f"{col_letter(col['draw_games'])}{rown}", "values": [[e]]},
            {"range": f"{col_letter(col['result_type'])}{rown}", "values": [["normal"]]},
            {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["pending"]]},
            {"range": f"{col_letter(col['reported_by_id'])}{rown}", "values": [[pid]]},
            {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [[""]]},
            {"range": f"{col_letter(col['auto_confirm_at'])}{rown}", "values": [[auto_confirm_at]]},
            {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

        nick_map = get_player_nick_map_fast(sh)

        player_a_name = nick_map.get(player_a, player_a)
        player_b_name = nick_map.get(player_b, player_b)
        reporter_name = nick_map.get(pid, str(interaction.user))
        opponent_name = nick_map.get(opponent_id, opponent_id)

        dm_sent = await _try_send_result_dm(
            interaction=interaction,
            opponent_id=opponent_id,
            match_id=match_id,
            player_a_name=player_a_name,
            player_b_name=player_b_name,
            placar=placar
        )

        await log_admin(
            interaction,
            f"resultado lançado: {reporter_name} ({pid}) | "
            f"match={match_id} | "
            f"{player_a_name} ({player_a}) vs {player_b_name} ({player_b}) | "
            f"placar={placar} | "
            f"dm_oponente={'ok' if dm_sent else 'falhou'}"
        )

        msg = f"✅ Resultado enviado: **{placar}**"
        if dm_sent:
            msg += f"\n📨 Oponente notificado por DM: **{opponent_name}**."
        else:
            msg += (
                f"\n⚠️ Não consegui enviar DM para o oponente: **{opponent_name}**.\n"
                f"Ele ainda pode confirmar manualmente pelo sistema."
            )

        await interaction.followup.send(
            msg,
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

    try:
        print(f"DEBUG: /resultado em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# /rejeitar
# =========================================================
@client.tree.command(name="rejeitar", description="Rejeita um resultado pendente.")
@app_commands.describe(oponente="Selecione seu oponente")
@app_commands.autocomplete(oponente=ac_match_id_user_pending)
async def rejeitar(interaction: discord.Interaction, oponente: str):
    start_total = time.perf_counter()

    await interaction.response.defer(ephemeral=True)

    match_id = str(oponente).strip()

    try:
        sh = open_sheet()
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER)
        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        pid = str(interaction.user.id).strip()

        match_row, found_row, row_data = _get_match_sheet_row_preferring_fast(
            sh=sh,
            ws_matches=ws_matches,
            col=col,
            match_id=match_id
        )

        if not found_row:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        getc = _match_getc_factory(match_row, row_data, col)

        # 🔥 NOVAS PROTEÇÕES
        if str(getc("active")).strip().lower() != "true":
            return await interaction.followup.send(
                "❌ Este match já foi encerrado.",
                ephemeral=True
            )

        if str(getc("result_type")).strip().upper() == "AUTO_FORFEIT":
            return await interaction.followup.send(
                "❌ Match resolvido automaticamente.",
                ephemeral=True
            )

        status = str(getc("confirmed_status")).strip().lower()
        reported_by = str(getc("reported_by_id")).strip()

        if reported_by == pid:
            return await interaction.followup.send("❌ Quem reportou não pode rejeitar.", ephemeral=True)

        if status != "pending":
            return await interaction.followup.send("❌ Match não está pendente.", ephemeral=True)

        rown = found_row
        updated_at = now_iso_utc()

        ws_matches.batch_update([
            {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["rejected"]]},
            {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [[pid]]},
            {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

        await interaction.followup.send("⚠️ Resultado rejeitado.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

    try:
        print(f"DEBUG: /rejeitar em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =================================================
# FIM DO BLOCO 7/B
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: A/7
# RESUMO: Cabeçalho do bloco, helper para exigir season ativa, helpers de standings/ranking
# e comandos administrativos iniciais: /forcesync, /ciclo_abrir, /ciclo_fechar e /ciclo_encerrar.
# REVISÃO: remoção de invalidação redundante de cache em Cycles e ajuste leve de TTL
# para reduzir pressão no Google Sheets, mantendo a mesma lógica funcional.
# =================================================

# =========================================================
# [BLOCO 8/22] — ADMIN FINAL + PRAZO + RANKINGS + EXPORT + START
# =========================================================

def require_current_season(sh) -> int:
    sid = get_current_season_id(sh)
    if sid <= 0:
        raise RuntimeError("Não existe season ativa.")
    return sid


# =========================================================
# Helpers de standings/ranking
# =========================================================

def sheet_float(v, default=0.0):
    try:
        s = str(v).strip()
        if not s:
            return default

        s = s.replace(" ", "")

        # Formato BR: 1.234,56
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")

        return float(s)
    except Exception:
        return default


def normalize_text_key(s: str) -> str:
    import unicodedata

    s = str(s or "").strip().lower()

    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")

    s = " ".join(s.split())

    return s


def _build_meta_rows(ws_decks, season: int, cycle: int) -> tuple[list[tuple[str, int, str]], int]:
    rows = cached_get_all_records(ws_decks, ttl_seconds=10)

    counts: dict[str, int] = {}
    display_name: dict[str, str] = {}

    total = 0

    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season:
            continue

        if safe_int(r.get("cycle", 0), 0) != cycle:
            continue

        deck_raw = str(r.get("deck", "")).strip()
        if not deck_raw:
            continue

        key = normalize_text_key(deck_raw)

        counts[key] = counts.get(key, 0) + 1

        if key not in display_name:
            display_name[key] = deck_raw

        total += 1

    if total == 0:
        return [], 0

    ordered = sorted(
        counts.items(),
        key=lambda x: (-x[1], display_name.get(x[0], "").lower())
    )

    result: list[tuple[str, int, str]] = []

    for key, qtd in ordered:
        name = display_name.get(key, key)
        pct = round((qtd / total) * 100.0, 2)

        if float(pct).is_integer():
            pct_txt = str(int(pct))
        else:
            pct_txt = f"{pct:.2f}".rstrip("0").rstrip(".").replace(".", ",")

        result.append((name, qtd, pct_txt))

    return result, total


def _read_cycle_standings(ws_standings, season_id: int, cycle: int) -> list[dict]:
    vals = ws_standings.get_all_values()

    if len(vals) <= 1:
        return []

    header = vals[0]
    idx = {name: i for i, name in enumerate(header)}

    out = []

    for row in vals[1:]:
        def getv(name: str, default=""):
            i = idx.get(name, -1)
            if i < 0 or i >= len(row):
                return default
            return row[i]

        r_season = safe_int(getv("season_id", 0), 0)
        r_cycle = safe_int(getv("cycle", 0), 0)

        if r_season != season_id:
            continue
        if r_cycle != cycle:
            continue

        matches_played = safe_int(getv("matches_played", 0), 0)
        match_points = sheet_float(getv("match_points", 0), 0.0)

        item = {
            "season_id": r_season,
            "cycle": r_cycle,
            "player_id": str(getv("player_id", "")).strip(),

            "matches_played": matches_played,
            "match_points": match_points,

            # aliases para compatibilidade
            "matches": matches_played,
            "points": match_points,

            # frações normalizadas
            "mwp": sheet_float(getv("mwp", 0), 0.0),
            "omw": sheet_float(getv("omw", 0), 0.0),
            "gw": sheet_float(getv("gw", 0), 0.0),
            "ogw": sheet_float(getv("ogw", 0), 0.0),

            # percentuais prontos da planilha
            "mwp_percent": sheet_float(getv("mwp_percent", 0), 0.0),
            "gw_percent": sheet_float(getv("gw_percent", 0), 0.0),
            "omw_percent": sheet_float(getv("omw_percent", 0), 0.0),
            "ogw_percent": sheet_float(getv("ogw_percent", 0), 0.0),

            "game_wins": safe_int(getv("game_wins", 0), 0),
            "game_losses": safe_int(getv("game_losses", 0), 0),
            "game_draws": safe_int(getv("game_draws", 0), 0),
            "games_played": safe_int(getv("games_played", 0), 0),

            "rank_position": safe_int(getv("rank_position", 999999), 999999),
            "last_recalc_at": str(getv("last_recalc_at", "")).strip(),
        }

        out.append(item)

    out.sort(key=lambda x: safe_int(x.get("rank_position", 999999), 999999))
    return out


def _format_standings_text_legacy(rows: list[dict], nick_map: dict[str, str], season_id: int, cycle: int, top: int = 30) -> str:
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
@client.tree.command(name="forcesync", description="(OWNER) Sincroniza os comandos do bot no servidor.")
async def forcesync(interaction: discord.Interaction):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message(
            "❌ Apenas o OWNER do servidor pode usar.",
            ephemeral=True
        )

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
        await interaction.followup.send(
            f"❌ Erro no /forcesync: {e}",
            ephemeral=True
        )


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
@client.tree.command(name="ciclo_encerrar", description="(OWNER) Encerra o ciclo (completed).")
@app_commands.describe(cycle="Número do ciclo")
async def ciclo_encerrar(interaction: discord.Interaction, cycle: int):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)

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
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: B/7
# REVISÃO: uso de índices RAM para Players e Matches, redução de releituras
# em /ranking e /deadline, mantendo a mesma lógica
# funcional dos comandos de prazo, pendências e ranking do ciclo.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - early return e clamp de parâmetros
# - menos variáveis recalculadas dentro de loops
# - envio paginado mantido com menor custo de montagem
# - debug leve de tempo sem alterar regras
# =================================================

# =========================================================
# /prazo
# =========================================================
@client.tree.command(name="prazo", description="Mostra o prazo oficial do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def prazo(interaction: discord.Interaction, cycle: int):
    start_total = time.perf_counter()

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

    try:
        print(f"DEBUG: /prazo em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# /deadline
# =========================================================
@client.tree.command(name="deadline", description="(ADM) Lista matches pending próximos de expirar.")
@app_commands.describe(cycle="Número do ciclo", horas="Janela em horas (1..48)")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def deadline(interaction: discord.Interaction, cycle: int, horas: int = 12):
    start_total = time.perf_counter()

    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        horas_clamped = max(1, min(safe_int(horas, 12), 48))
        nowu = utc_now_dt()
        limit = nowu + timedelta(hours=horas_clamped)

        rows = get_matches_for_cycle_fast(
            sh,
            season_id=season_id,
            cycle=cycle,
            only_active=True
        )

        items = []

        for r in rows:
            if str(r.get("confirmed_status", "")).strip().lower() != "pending":
                continue

            ac_raw = r.get("auto_confirm_at", "") or ""
            ac = parse_iso_dt(ac_raw)
            if not ac:
                continue

            if ac <= limit:
                items.append(f"`{r.get('match_id')}` expira {ac.isoformat()} UTC")
                if len(items) >= 200:
                    break

        if not items:
            return await interaction.followup.send("✅ Nenhuma pendência na janela.", ephemeral=True)

        msg = "⏰ Pendências próximas de expirar:\n" + "\n".join(items)
        await send_followup_chunks(interaction, msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

    try:
        print(f"DEBUG: /deadline em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# /recalcular
# =========================================================
@client.tree.command(name="recalcular", description="(OWNER) Auto-confirma pendências vencidas e recalcula standings do ciclo.")
@app_commands.describe(
    season="Season",
    cycle="Número do ciclo",
    bonus_percentual="Opcional. Se vazio, mantém o último bônus do ciclo."
)
@app_commands.autocomplete(season=ac_owner_season, cycle=ac_owner_cycle_for_season)
async def recalcular(
    interaction: discord.Interaction,
    season: int,
    cycle: int,
    bonus_percentual: str = ""
):
    start_total = time.perf_counter()

    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send(
                f"❌ A season {season} não existe.",
                ephemeral=True
            )

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        cf = get_cycle_fields(ws_cycles, season, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(
                f"❌ O ciclo {cycle} não existe na season {season}.",
                ephemeral=True
            )

        ws_bonus = ensure_worksheet(sh, "CycleBonuses", CYCLE_BONUSES_HEADER, rows=2000, cols=10)
        ensure_sheet_columns(ws_bonus, CYCLE_BONUSES_REQUIRED)

        bonus_raw = str(bonus_percentual or "").strip()

        # Se o owner informou novo bônus, salva.
        # Se deixou vazio, reaproveita o último do ciclo.
        if bonus_raw:
            bonus = sheet_float(bonus_raw, None)
            if bonus is None:
                return await interaction.followup.send(
                    "❌ bônus_percentual inválido. Use apenas número. Ex: 0, 50, 100, 25.5",
                    ephemeral=True
                )

            set_cycle_bonus_percent(ws_bonus, season, cycle, bonus)
        else:
            bonus = get_cycle_bonus_percent(ws_bonus, season, cycle)

        auto = sweep_auto_confirm(sh, season, cycle)
        rows = recalculate_cycle(sh, season, cycle, bonus_percent=bonus)

        await interaction.followup.send(
            f"✅ Recalculo concluído.\n"
            f"- Season: **{season}**\n"
            f"- Ciclo: **{cycle}**\n"
            f"- Auto-confirmados: **{auto}**\n"
            f"- Bônus aplicado: **{fmt_compact_num(bonus)}%**\n"
            f"- Linhas standings geradas: **{len(rows)}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

    try:
        print(f"DEBUG: /recalcular em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# FORMATADOR NUMÉRICO (ATÉ 2 CASAS, SÓ SE NECESSÁRIO)
# =========================================================
def fmt_num2(x) -> str:
    try:
        v = float(x)
        if v.is_integer():
            return str(int(v))
        return f"{v:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return "0"


# =========================================================
# HELPERS — RANKING
# =========================================================
def _build_cycle_ranking_table(rows: list[dict]) -> list[dict]:
    K = 3
    table = []

    for r in rows:
        try:
            m = safe_int(r.get("matches_played", 0), 0)
            pts = sheet_float(r.get("match_points", 0), 0.0)

            mwp = sheet_float(r.get("mwp", 0), 0.0)
            omw = sheet_float(r.get("omw", 0), 0.0)
            gw = sheet_float(r.get("gw", 0), 0.0)
            ogw = sheet_float(r.get("ogw", 0), 0.0)

            ppm = (pts + K) / (m + K) if m else 0.0
            peso_pts = m / (m + K) if m else 0.0
            peso_ppm = K / (m + K) if m else 0.0
            score = pts * peso_pts + ppm * peso_ppm

            table.append({
                "p": str(r.get("player_id", "")).strip(),
                "score": score,
                "pts": pts,
                "mwp": mwp,
                "ppm": ppm,
                "omw": omw,
                "gw": gw,
                "ogw": ogw,
                "j": m,
                "mwp_percent": sheet_float(r.get("mwp_percent", 0), 0.0),
                "omw_percent": sheet_float(r.get("omw_percent", 0), 0.0),
                "gw_percent": sheet_float(r.get("gw_percent", 0), 0.0),
                "ogw_percent": sheet_float(r.get("ogw_percent", 0), 0.0),
            })
        except Exception:
            continue

    table.sort(
        key=lambda x: (
            x["score"],
            x["ppm"],
            x["mwp"],
            x["omw"],
            x["gw"],
            x["ogw"]
        ),
        reverse=True
    )

    return table


# =========================================================
# /ranking (PADRÃO IDÊNTICO AO /ranking_geral, FILTRADO POR CICLO)
# =========================================================
@client.tree.command(name="ranking", description="Mostra o ranking do ciclo.")
@app_commands.describe(season="Season", cycle="Número do ciclo", top="Quantidade de jogadores")
@app_commands.autocomplete(season=ac_pods_ver_season, cycle=ac_pods_ver_cycle)
async def ranking(interaction: discord.Interaction, season: int, cycle: int, top: int = 30):
    start_total = time.perf_counter()

    await interaction.response.defer(ephemeral=False)

    try:
        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send(
                f"❌ A season {season} não existe.",
                ephemeral=False
            )

        ws_standings = ensure_worksheet(
            sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30
        )

        rows = _read_cycle_standings(ws_standings, season, cycle)

        if not rows:
            return await interaction.followup.send(
                "⚠️ Não há standings para este ciclo ainda. Use `/recalcular` primeiro.",
                ephemeral=False
            )

        table = _build_cycle_ranking_table(rows)
        nick_map = get_player_nick_map_fast(sh)

        top = max(8, min(top, 60))

        header_lines = []
        header_lines.append(f"🏆 Ranking — Season {season} | Ciclo {cycle} (Top {top})")
        header_lines.append(
            f"{'pos':>3} | {'jogador':<22} | {'J':>2} | {'SCORE':>6} | {'PTS':>6} | {'PPM':>6} | {'MWP':>6} | {'OMW':>6} | {'GW':>6} | {'OGW':>6}"
        )
        header_lines.append("-" * 110)

        row_lines = []
        for i, r in enumerate(table[:top], 1):
            nome = nick_map.get(str(r["p"]), str(r["p"]))

            score_txt = fmt_num2(r["score"])
            pts_txt = fmt_num2(r["pts"])
            ppm_txt = fmt_num2(r["ppm"])
            mwp_txt = fmt_num2(r["mwp_percent"])
            omw_txt = fmt_num2(r["omw_percent"])
            gw_txt = fmt_num2(r["gw_percent"])
            ogw_txt = fmt_num2(r["ogw_percent"])

            row_lines.append(
                f"{i:>3} | "
                f"{nome[:20]:<22} | "
                f"{r['j']:>2} | "
                f"{score_txt:>6} | "
                f"{pts_txt:>6} | "
                f"{ppm_txt:>6} | "
                f"{mwp_txt:>6} | "
                f"{omw_txt:>6} | "
                f"{gw_txt:>6} | "
                f"{ogw_txt:>6}"
            )

        chunk_size = 12
        total_rows = len(row_lines)

        for start in range(0, total_rows, chunk_size):
            part_lines = header_lines + row_lines[start:start + chunk_size]
            part_msg = "```txt\n" + "\n".join(part_lines) + "\n```"
            await interaction.followup.send(part_msg, ephemeral=False)

        legend_lines = []
        legend_lines.append("Legenda:")
        legend_lines.append("J = Número de jogos realizados")
        legend_lines.append("SCORE = {PTS×[J÷(J+3)]} + {PPM×[3÷(J+3)]}")
        legend_lines.append("PTS = Pontos totais acumulados")
        legend_lines.append("PPM = Points Per Match")
        legend_lines.append("MWP = Match Win Percentage")
        legend_lines.append("OMW = Opponent's Match Win Percentage")
        legend_lines.append("GW = Game Win Percentage")
        legend_lines.append("OGW = Opponent's Game Win Percentage")

        legend_msg = "```txt\n" + "\n".join(legend_lines) + "\n```"
        await interaction.followup.send(legend_msg, ephemeral=False)

    except Exception as e:
        await interaction.followup.send(
            f"❌ Erro no /ranking: {e}",
            ephemeral=False
        )

    try:
        print(f"DEBUG: /ranking em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass


# =========================================================
# FORMATADOR DE STANDINGS
# =========================================================
def _format_standings_text(rows, nick_map, season_id, cycle, top=30):
    """
    Mantido apenas por compatibilidade.
    O /ranking atualizado não depende mais deste formatador.
    """
    top = max(8, min(top, 60))

    out = []
    out.append(f"🏆 Ranking — Season {season_id} | Ciclo {cycle} (Top {top})")
    out.append(
        f"{'pos':>3} | {'jogador':<22} | {'J':>2} | {'SCORE':>6} | {'PTS':>6} | {'PPM':>6} | {'MWP':>6} | {'OMW':>6} | {'GW':>6} | {'OGW':>6}"
    )
    out.append("-" * 110)

    for i, r in enumerate(rows[:top], 1):
        p = r.get("player_id", "")
        nome = nick_map.get(p, p)

        score_txt = fmt_num2(r.get('score', 0))
        pts_txt = fmt_num2(r.get('pts', r.get('match_points', 0)))
        ppm_txt = fmt_num2(r.get('ppm', 0))
        mwp_txt = fmt_num2(sheet_float(r.get('mwp_percent', 0), 0.0))
        omw_txt = fmt_num2(sheet_float(r.get('omw_percent', 0), 0.0))
        gw_txt = fmt_num2(sheet_float(r.get('gw_percent', 0), 0.0))
        ogw_txt = fmt_num2(sheet_float(r.get('ogw_percent', 0), 0.0))

        out.append(
            f"{i:>3} | "
            f"{nome[:20]:<22} | "
            f"{safe_int(r.get('j', r.get('matches_played', 0)), 0):>2} | "
            f"{score_txt:>6} | "
            f"{pts_txt:>6} | "
            f"{ppm_txt:>6} | "
            f"{mwp_txt:>6} | "
            f"{omw_txt:>6} | "
            f"{gw_txt:>6} | "
            f"{ogw_txt:>6}"
        )

    return "```txt\n" + "\n".join(out) + "\n```"


# =================================================
# FIM DO SUB-BLOCO B/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: C/7
# REVISÃO: integração mais consistente com índices RAM de matches e cycles,
# redução de releituras desnecessárias e manutenção da mesma lógica funcional
# em /final, /admin_resultado_editar, /admin_resultado_cancelar, /status_ciclo
# e /ranking_geral.
# =================================================

# =========================================================
# /final
# =========================================================
@client.tree.command(name="final", description="(OWNER) Aplica 0-0-3 após deadline do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def final(interaction: discord.Interaction, cycle: int):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25)
        ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)

        _, end_br, _, _ = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)

        if not end_br:
            return await interaction.followup.send("❌ Ciclo sem prazo definido.", ephemeral=True)

        deadline_dt = parse_br_dt(end_br)
        if deadline_dt and now_br_dt() < deadline_dt:
            return await interaction.followup.send("❌ Deadline ainda não chegou.", ephemeral=True)

        col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
        rows = cached_get_all_values(ws_matches, ttl_seconds=10)

        pending_confirmed = 0
        id_applied = 0
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
            if not as_bool(getc("active") or "TRUE"):
                continue

            status = str(getc("confirmed_status") or "").strip().lower()
            reported_by = str(getc("reported_by_id") or "").strip()

            # 1) confirma tudo que estiver pending
            if status == "pending":
                updates.extend([
                    {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["confirmed"]]},
                    {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [["AUTO_FINAL"]]},
                    {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
                ])
                pending_confirmed += 1
                continue

            # 2) aplica 0-0-3 apenas nas matches ainda sem resultado reportado
            if reported_by:
                continue

            updates.extend([
                {"range": f"{col_letter(col['a_games_won'])}{rown}", "values": [["0"]]},
                {"range": f"{col_letter(col['b_games_won'])}{rown}", "values": [["0"]]},
                {"range": f"{col_letter(col['draw_games'])}{rown}", "values": [["3"]]},
                {"range": f"{col_letter(col['result_type'])}{rown}", "values": [["intentional_draw"]]},
                {"range": f"{col_letter(col['confirmed_status'])}{rown}", "values": [["confirmed"]]},
                {"range": f"{col_letter(col['confirmed_by_id'])}{rown}", "values": [["AUTO_FINAL"]]},
                {"range": f"{col_letter(col['updated_at'])}{rown}", "values": [[updated_at]]},
            ])
            id_applied += 1

        if updates:
            ws_matches.batch_update(updates)
            cache_invalidate(ws_matches)
            invalidate_match_ram_index()
            invalidate_match_ac_index()

        # recálculo obrigatório após alterações
        rows_recalc = recalculate_cycle(sh, season_id, cycle)

        await interaction.followup.send(
            f"✅ FINAL aplicado.\n"
            f"- Pending confirmadas: **{pending_confirmed}**\n"
            f"- Matches ajustadas com 0-0-3: **{id_applied}**\n"
            f"- Linhas standings recalculadas: **{len(rows_recalc)}**",
            ephemeral=True
        )

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
        return await interaction.followup.send("❌ Placar inválido.", ephemeral=True)

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
        season_id = 0
        cycle = 0

        for idx in range(1, len(rows)):
            r = rows[idx]
            val = r[col["match_id"]] if col["match_id"] < len(r) else ""
            if str(val).strip() == match_id:
                found = idx + 1
                season_id = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
                cycle = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)
                break

        if not found:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        updated_at = now_iso_utc()

        ws_matches.batch_update([
            {"range": f"{col_letter(col['a_games_won'])}{found}", "values": [[str(v)]]},
            {"range": f"{col_letter(col['b_games_won'])}{found}", "values": [[str(d)]]},
            {"range": f"{col_letter(col['draw_games'])}{found}", "values": [[str(e)]]},
            {"range": f"{col_letter(col['result_type'])}{found}", "values": [["normal"]]},
            {"range": f"{col_letter(col['confirmed_status'])}{found}", "values": [["confirmed"]]},
            {"range": f"{col_letter(col['confirmed_by_id'])}{found}", "values": [[str(interaction.user.id)]]},
            {"range": f"{col_letter(col['updated_at'])}{found}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

        rows_recalc = []
        if season_id > 0 and cycle > 0:
            rows_recalc = recalculate_cycle(sh, season_id, cycle)

        await interaction.followup.send(
            f"✅ Resultado editado: **{placar}**\n"
            f"- Linhas standings recalculadas: **{len(rows_recalc)}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

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

        found = None
        season_id = 0
        cycle = 0

        rows = cached_get_all_values(ws_matches, ttl_seconds=10)
        for idx in range(1, len(rows)):
            r = rows[idx]
            val = r[col["match_id"]] if col["match_id"] < len(r) else ""
            if str(val).strip() == match_id:
                found = idx + 1
                season_id = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
                cycle = safe_int(r[col["cycle"]] if col["cycle"] < len(r) else 0, 0)
                break

        if not found:
            return await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)

        updated_at = now_iso_utc()

        ws_matches.batch_update([
            {"range": f"{col_letter(col['a_games_won'])}{found}", "values": [["0"]]},
            {"range": f"{col_letter(col['b_games_won'])}{found}", "values": [["0"]]},
            {"range": f"{col_letter(col['draw_games'])}{found}", "values": [["0"]]},
            {"range": f"{col_letter(col['result_type'])}{found}", "values": [["normal"]]},
            {"range": f"{col_letter(col['confirmed_status'])}{found}", "values": [["open"]]},
            {"range": f"{col_letter(col['reported_by_id'])}{found}", "values": [[""]]},
            {"range": f"{col_letter(col['confirmed_by_id'])}{found}", "values": [[""]]},
            {"range": f"{col_letter(col['auto_confirm_at'])}{found}", "values": [[""]]},
            {"range": f"{col_letter(col['updated_at'])}{found}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
        invalidate_match_ac_index()

        rows_recalc = []
        if season_id > 0 and cycle > 0:
            rows_recalc = recalculate_cycle(sh, season_id, cycle)

        await interaction.followup.send(
            f"✅ Resultado cancelado. Match reaberto.\n"
            f"- Linhas standings recalculadas: **{len(rows_recalc)}**",
            ephemeral=True
        )
        await log_admin(interaction, f"admin_resultado_cancelar {match_id}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /admin_resultado_cancelar: {e}", ephemeral=True)

# =========================================================
# /status_ciclo
# =========================================================
@client.tree.command(name="status_ciclo", description="Mostra status dos ciclos da season atual.")
async def status_ciclo(interaction: discord.Interaction):

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        items = get_cycle_choices_fast(
            sh,
            season_id=season_id,
            query="",
            only_open=False,
            limit=25
        )

        if not items:
            return await interaction.followup.send("⚠️ Não há ciclos cadastrados nesta season.", ephemeral=True)

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
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
                "status": str(r.get("status", "")).strip().lower(),
            }

        ordered_cycles = sorted(cycle_fields_map.keys())

        lines = [f"📘 **Status dos ciclos** | Season {season_id}"]
        for c in ordered_cycles:
            cf = cycle_fields_map.get(c, {})
            lines.append(
                f"• Ciclo {c} | status: **{cf.get('status', '')}** | "
                f"início: `{cf.get('start_at_br', '') or '-'}` | "
                f"prazo: `{cf.get('deadline_at_br', '') or '-'}`"
            )

        await send_followup_chunks(interaction, "\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /status_ciclo: {e}", ephemeral=True)

# =========================================================
# /ranking_geral (REVISADO + PADRÃO VISUAL DO /ranking)
# REVISÃO V10: parsing robusto + percentuais corrigidos
# =========================================================
@client.tree.command(name="ranking_geral", description="Mostra ranking geral da season.")
@app_commands.describe(season="Season", top="Quantidade de jogadores (8..60)")
@app_commands.autocomplete(season=ac_pods_ver_season)
async def ranking_geral(interaction: discord.Interaction, season: int, top: int = 30):
    await interaction.response.defer()

    try:
        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send(f"❌ A season {season} não existe.")

        ws_standings = ensure_worksheet(
            sh, "Standings", STANDINGS_HEADER, rows=50000, cols=30
        )
        ensure_sheet_columns(ws_standings, STANDINGS_REQUIRED)

        vals = cached_get_all_values(ws_standings, ttl_seconds=10)

        if len(vals) <= 1:
            return await interaction.followup.send("Sem standings para esta season.")

        header = vals[0]
        idx = {name: i for i, name in enumerate(header)}

        stats = {}

        # =========================================================
        # COLETA DE DADOS DA ABA STANDINGS (TODOS OS CICLOS DA SEASON)
        # =========================================================

        for row in vals[1:]:
            def getv(name: str, default=""):
                i = idx.get(name, -1)
                if i < 0 or i >= len(row):
                    return default
                return row[i]

            if safe_int(getv("season_id", 0), 0) != season:
                continue

            p = str(getv("player_id", "")).strip()
            if not p:
                continue

            if p not in stats:
                stats[p] = {
                    "pts": 0,
                    "m": 0,
                    "gwins": 0,
                    "glosses": 0,
                    "gdraws": 0,
                    "gplayed": 0,
                    "omw_weighted_sum": 0.0,
                    "ogw_weighted_sum": 0.0,
                    "omw_weight": 0,
                    "ogw_weight": 0,
                }

            matches_played = safe_int(getv("matches_played", 0), 0)
            match_points = sheet_float(getv("match_points", 0), 0.0)
            game_wins = safe_int(getv("game_wins", 0), 0)
            game_losses = safe_int(getv("game_losses", 0), 0)
            game_draws = safe_int(getv("game_draws", 0), 0)
            games_played = safe_int(getv("games_played", 0), 0)

            omw_raw = sheet_float(getv("omw", 0), 0.0)
            ogw_raw = sheet_float(getv("ogw", 0), 0.0)

            stats[p]["pts"] += match_points
            stats[p]["m"] += matches_played
            stats[p]["gwins"] += game_wins
            stats[p]["glosses"] += game_losses
            stats[p]["gdraws"] += game_draws
            stats[p]["gplayed"] += games_played

            # média ponderada pelos matches do ciclo
            if matches_played > 0:
                stats[p]["omw_weighted_sum"] += omw_raw * matches_played
                stats[p]["ogw_weighted_sum"] += ogw_raw * matches_played
                stats[p]["omw_weight"] += matches_played
                stats[p]["ogw_weight"] += matches_played

        if not stats:
            return await interaction.followup.send("Sem standings para esta season.")

        # =========================================================
        # CÁLCULOS
        # =========================================================
        K = 3
        table = []

        for p, s in stats.items():
            m = s["m"]
            pts = s["pts"]

            raw_mwp = (pts / (3 * m)) if m else 0
            mwp = max(raw_mwp, 0.333)

            ppm = (pts + K) / (m + K) if m else 0

            games = s["gplayed"]
            raw_gw = ((s["gwins"] + 0.5 * s["gdraws"]) / games) if games else 0
            gw = max(raw_gw, 0.333)

            if s["omw_weight"] > 0:
                omw = max(s["omw_weighted_sum"] / s["omw_weight"], 0.333)
            else:
                omw = 0.333

            if s["ogw_weight"] > 0:
                ogw = max(s["ogw_weighted_sum"] / s["ogw_weight"], 0.333)
            else:
                ogw = 0.333

            peso_pts = m / (m + K) if m > 0 else 0
            peso_ppm = K / (m + K) if m > 0 else 0

            score = pts * peso_pts + ppm * peso_ppm

            table.append({
                "p": p,
                "score": score,
                "pts": pts,
                "mwp": mwp,
                "ppm": ppm,
                "omw": omw,
                "gw": gw,
                "ogw": ogw,
                "j": m
            })

        # =========================================================
        # ORDENAÇÃO OFICIAL
        # =========================================================
        table.sort(
            key=lambda x: (
                x["score"],
                x["ppm"],
                x["mwp"],
                x["omw"],
                x["gw"],
                x["ogw"],
            ),
            reverse=True
        )

        nick_map = get_player_nick_map_fast(sh)

        top = max(8, min(top, 30))

        # =========================================================
        # FORMATAÇÃO
        # =========================================================
        header_lines = []
        header_lines.append(f"🏆 Ranking Geral — Season {season} (Top {top})")
        header_lines.append(
            f"{'pos':>3} | {'jogador':<22} | {'J':>2} | {'SCORE':>6} | {'PTS':>6} | {'PPM':>6} | {'MWP':>6} | {'OMW':>6} | {'GW':>6} | {'OGW':>6}"
        )
        header_lines.append("-" * 110)

        row_lines = []
        for i, r in enumerate(table[:top], 1):
            nome = nick_map.get(str(r["p"]), str(r["p"]))

            score_txt = fmt_num2(r["score"])
            pts_txt = fmt_num2(r["pts"])
            ppm_txt = fmt_num2(r["ppm"])
            mwp_txt = fmt_num2(r["mwp"] * 100)
            omw_txt = fmt_num2(r["omw"] * 100)
            gw_txt = fmt_num2(r["gw"] * 100)
            ogw_txt = fmt_num2(r["ogw"] * 100)

            row_lines.append(
                f"{i:>3} | "
                f"{nome[:20]:<22} | "
                f"{r['j']:>2} | "
                f"{score_txt:>6} | "
                f"{pts_txt:>6} | "
                f"{ppm_txt:>6} | "
                f"{mwp_txt:>6} | "
                f"{omw_txt:>6} | "
                f"{gw_txt:>6} | "
                f"{ogw_txt:>6}"
            )

        chunk_size = 12
        total_rows = len(row_lines)

        for start in range(0, total_rows, chunk_size):
            part_lines = []
            part_lines.extend(header_lines)
            part_lines.extend(row_lines[start:start + chunk_size])

            part_msg = "```txt\n" + "\n".join(part_lines) + "\n```"
            await interaction.followup.send(part_msg, ephemeral=False)

        legend_lines = []
        legend_lines.append("Legenda:")
        legend_lines.append("J = Número de jogos realizados")
        legend_lines.append("SCORE = {PTS×[J÷(J+3)]} + {PPM×[3÷(J+3)]}")
        legend_lines.append("PTS = Pontos totais acumulados")
        legend_lines.append("PPM = Points Per Match")
        legend_lines.append("MWP = Match Win Percentage")
        legend_lines.append("OMW = Opponent's Match Win Percentage")
        legend_lines.append("GW = Game Win Percentage")
        legend_lines.append("OGW = Opponent's Game Win Percentage")

        legend_msg = "```txt\n" + "\n".join(legend_lines) + "\n```"
        await interaction.followup.send(legend_msg, ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}")
        
# =================================================
# FIM DO SUB-BLOCO C/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: D/7
# REVISÃO: padronização do /cadastrar_player para o mesmo formato de deck
# do /inscrever (guilda + arquétipo), com autocomplete correspondente,
# mantendo integralmente as regras administrativas e a estrutura existente.
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


@client.tree.command(name="startseason", description="(OWNER) Abre uma nova season e define como season atual.")
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
        invalidate_cycle_ram_index()
        invalidate_match_ac_index()

        await interaction.followup.send(
            f"✅ Season aberta e ativa: **{season_name}** (ID {new_id}).",
            ephemeral=True
        )
        await log_admin(interaction, f"OWNER startseason: opened S{new_id} name='{season_name}'")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /startseason: {e}", ephemeral=True)


@client.tree.command(name="closeseason", description="(OWNER) Fecha a season atual.")
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
        invalidate_cycle_ram_index()
        invalidate_match_ac_index()

        await interaction.followup.send(f"✅ Season **{sid}** fechada.", ephemeral=True)
        await log_admin(interaction, f"OWNER closeseason: closed S{sid}")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /closeseason: {e}", ephemeral=True)


@client.tree.command(name="cadastrar_player", description="(ADM/Organizador/Owner) Cadastra player manualmente com season, ciclo, inscrição, deck e decklist.")
@app_commands.describe(
    membro="Selecione o usuário no Discord",
    season="Season para cadastrar",
    ciclo="Ciclo para cadastrar",
    guilda="Base do deck",
    arquetipo="Arquétipo do deck",
    decklist="Link (moxfield/ligamagic/mtgdecks/mtggoldfish/melee/mtgtop8)"
#    decklist="Link (lemeholandes)"
)
@app_commands.autocomplete(
    season=ac_owner_season,
    ciclo=ac_owner_cycle_for_season,
    guilda=ac_deck_guilda,
    arquetipo=ac_deck_arquetipo
)
async def cadastrar_player(
    interaction: discord.Interaction,
    membro: discord.Member,
    season: int,
    ciclo: int,
    guilda: str,
    arquetipo: str,
    decklist: str
):
    if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
        return await interaction.response.send_message(
            "❌ Apenas ADM, Organizador ou Owner podem usar este comando.",
            ephemeral=True
        )

    await interaction.response.defer(ephemeral=True)

    try:
        raw = str(membro.display_name or membro.name).strip()
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

        guilda_final = _resolve_case_insensitive_choice(guilda, DECK_GUILDAS)
        if not guilda_final:
            return await interaction.followup.send("❌ Guilda inválida.", ephemeral=True)

        arquetipo_final = _resolve_case_insensitive_choice(arquetipo, DECK_ARQUETIPOS)
        if not arquetipo_final:
            return await interaction.followup.send("❌ Arquétipo inválido.", ephemeral=True)

        nm = _montar_nome_deck(guilda_final, arquetipo_final)
        if not nm or len(nm) > 80:
            return await interaction.followup.send("❌ Nome de deck inválido (1 a 80 caracteres).", ephemeral=True)

        ok, val = validate_decklist_url(decklist)
        if not ok:
            return await interaction.followup.send(f"❌ Decklist inválida: {val}", ephemeral=True)

        did = str(membro.id).strip()
        nowc = now_br_str()

        # =========================
        # PLAYERS
        # =========================
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
        col_players = ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)
        rows_players = cached_get_all_values(ws_players, ttl_seconds=10)

        found_row = None
        for i in range(2, len(rows_players) + 1):
            r = rows_players[i - 1]
            val_player = r[col_players["discord_id"]] if col_players["discord_id"] < len(r) else ""
            if str(val_player).strip() == did:
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
        await log_admin(interaction, f"cadastrar_player: {raw} ({membro.id}) season={season} ciclo={ciclo} deck='{nm}'")

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /cadastrar_player: {e}", ephemeral=True)


# =================================================
# FIM DO SUB-BLOCO D/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: E/7
# REVISÃO: helpers de layout dos pods para o /start_cycle,
# mantendo a lógica oficial de distribuição.
# =================================================

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
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: F/7
# REVISÃO: melhor aproveitamento do índice RAM de matches, consolidação
# de timestamps na criação em lote, invalidação consistente dos índices
# RAM após geração e manutenção da mesma lógica funcional do /start_cycle.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - menos leituras redundantes
# - early return e clamp antes de processamento pesado
# - criação em lote mantida
# - debug leve de tempo sem alterar regra funcional
# =================================================

def _best_layout_shuffle_min_repeats(players: list[str], layout: list[int], past_pairs: set[frozenset], tries: int = 250):
    best = None
    best_score = None

    tries = max(1, tries)

    for _ in range(tries):
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

    chunk_size = max(1, safe_int(chunk_size, 200))

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


def _get_active_cycle_players(ws_enr, season_id: int, cycle: int) -> list[str]:
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

    return sorted(set(players))


@client.tree.command(name="start_cycle", description="(ADM) Gera pods + matches e trava o ciclo (locked).")
@app_commands.describe(
    cycle="Número do ciclo",
    pod_size="Opcional: preferência de tamanho (4..6). 0 = automático",
    tries="Tentativas anti-repetição (50..500)"
)
async def start_cycle(interaction: discord.Interaction, cycle: int, pod_size: int = 0, tries: int = 250):
    start_total = time.perf_counter()

    if not await is_admin_or_organizer(interaction):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)
        ensure_all_sheets(sh)

        tries = max(50, min(safe_int(tries, 250), 500))
        pod_size = safe_int(pod_size, 0)

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
            invalidate_match_ram_index()
            invalidate_match_ac_index()

            return await interaction.followup.send(
                "⚠️ Este ciclo já possui PODs ou matches gerados.\n"
                "Não vou gerar novamente.\n"
                f"- Status normalizado para: **locked**\n"
                f"- Prazo: **{start_br or '-'}** → **{end_br or '-'}** (BR)\n"
                f"- Regra aplicada: **{days} dias** (maior pod = **{max_pod}** jogador(es))",
                ephemeral=True
            )

        players = _get_active_cycle_players(ws_enr, season_id, cycle)

        if len(players) < 3:
            return await interaction.followup.send("❌ Precisa de pelo menos 3 jogadores ativos para gerar pods.", ephemeral=True)

        if pod_size and pod_size not in (4, 5, 6):
            return await interaction.followup.send("❌ pod_size inválido (use 4, 5, 6 ou 0).", ephemeral=True)

        layout = _choose_pod_layout(len(players), preferred_size=pod_size)

        matches_rows = get_matches_for_cycle_fast(sh, season_id=season_id, cycle=cycle, only_active=False)
        if matches_rows:
            # segurança extra: se por qualquer motivo já existem matches deste ciclo,
            # normaliza o status e evita geração duplicada
            start_br, end_br, max_pod, days = compute_cycle_start_deadline_br(season_id, cycle, ws_pods, ws_cycles)

            set_cycle_status(ws_cycles, season_id, cycle, "locked")

            if start_br and end_br:
                set_cycle_times(ws_cycles, season_id, cycle, start_br, end_br)

            cache_invalidate(ws_cycles)
            invalidate_match_ram_index()
            invalidate_match_ac_index()

            return await interaction.followup.send(
                "⚠️ Este ciclo já possui matches gerados.\n"
                "Não vou gerar novamente.\n"
                f"- Status normalizado para: **locked**\n"
                f"- Prazo: **{start_br or '-'}** → **{end_br or '-'}** (BR)\n"
                f"- Regra aplicada: **{days} dias** (maior pod = **{max_pod}** jogador(es))",
                ephemeral=True
            )

        all_match_rows = cached_get_all_records(ws_matches, ttl_seconds=10)
        past_pairs = _past_confirmed_pairs_from_records(all_match_rows)

        pods, score = _best_layout_shuffle_min_repeats(
            players,
            layout,
            past_pairs,
            tries=tries
        )

        if not pods:
            return await interaction.followup.send(
                "❌ Não foi possível gerar os pods deste ciclo.",
                ephemeral=True
            )

        nowb = now_br_str()
        nowu = utc_now_dt()
        created_iso = now_iso_utc()
        auto_confirm_iso = auto_confirm_deadline_iso(nowu)

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
                    created_iso, created_iso,
                    auto_confirm_iso
                ])
                created += 1

        _chunked_append_rows(ws_matches, matches_rows_to_append, chunk_size=200)
        cache_invalidate(ws_matches)
        invalidate_match_ram_index()
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

    try:
        print(f"DEBUG: /start_cycle em {round((time.perf_counter() - start_total) * 1000, 2)} ms")
    except Exception:
        pass

# =================================================
# FIM DO SUB-BLOCO F/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 8/22
# SUB-BLOCO: G/7
# REVISÃO: otimização de loops pesados com melhor aproveitamento dos índices RAM
# de Players e Matches, invalidação consistente após substituição e remoção do
# START deste sub-bloco, que deve permanecer apenas no último bloco.
# =================================================


# =========================================================
# /exportar_ciclo
# =========================================================
@client.tree.command(name="exportar_ciclo", description="(OWNER) Exporta CSV do ciclo.")
@app_commands.describe(cycle="Número do ciclo")
@app_commands.autocomplete(cycle=ac_cycle_open)
async def exportar_ciclo(interaction: discord.Interaction, cycle: int):
    if not await is_owner_only(interaction):
        return await interaction.response.send_message("❌ Apenas o OWNER do servidor pode usar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        season_id = require_current_season(sh)

        rows = get_matches_for_cycle_fast(
            sh,
            season_id=season_id,
            cycle=cycle,
            only_active=False
        )

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(MATCHES_HEADER)

        count = 0
        for r in rows:
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

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25)
        ws_decks = ensure_worksheet(sh, "Decks", DECKS_HEADER, rows=10000, cols=25)

        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_decks, DECKS_REQUIRED)

        enr_rows = cached_get_all_records(ws_enr, ttl_seconds=10)
        decks_rows = cached_get_all_records(ws_decks, ttl_seconds=10)

        nick_map = get_player_nick_map_fast(sh)

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

        inscritos_linhas = []
        inscritos_ids = set()

        for r in enr_rows:
            if safe_int(r.get("season_id", 0), 0) != season:
                continue
            if safe_int(r.get("cycle", 0), 0) != cycle:
                continue
            if str(r.get("status", "")).strip().lower() != "active":
                continue

            pid = str(r.get("player_id", "")).strip()
            if not pid:
                continue

            inscritos_ids.add(pid)
            st = deck_status_map.get(pid, {"deck_ok": False, "decklist_ok": False})

            inscritos_linhas.append(
                f"{nick_map.get(pid, pid)} | Deck: {'OK' if st['deck_ok'] else 'PENDENTE'} | Decklist: {'OK' if st['decklist_ok'] else 'PENDENTE'}"
            )

        all_player_ids = sorted(nick_map.keys(), key=lambda x: nick_map.get(x, x).lower())
        nao_inscritos = [nick_map.get(pid, pid) for pid in all_player_ids if pid not in inscritos_ids]

        lines = [
            f"📋 **Inscritos Season {season} / Ciclo {cycle}**",
            ""
        ]

        if inscritos_linhas:
            lines.append(f"**Jogadores inscritos:** {len(inscritos_linhas)}")
            lines.extend(inscritos_linhas)
        else:
            lines.append("**Jogadores inscritos:** 0")
            lines.append("Nenhum inscrito.")

        lines.append("")
        lines.append(f"**Jogadores cadastrados que ainda NÃO se inscreveram:** {len(nao_inscritos)}")

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

        invalidate_player_ram_index()
        invalidate_match_ram_index()
        invalidate_match_ac_index()

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
@client.tree.command(name="historico_confronto", description="Mostra histórico entre dois jogadores.")
@app_commands.describe(jogador_a="Primeiro jogador", jogador_b="Segundo jogador")
async def historico_confronto(interaction: discord.Interaction, jogador_a: discord.Member, jogador_b: discord.Member):

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()

        rows = cached_get_all_records(
            ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30),
            ttl_seconds=10
        )

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

            if {pa, pb} != {a_id, b_id}:
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

        players_all = cached_get_all_records(
            ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25),
            ttl_seconds=10
        )
        cycles_all = cached_get_all_records(
            ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25),
            ttl_seconds=10
        )
        enr_all = cached_get_all_records(
            ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=20000, cols=25),
            ttl_seconds=10
        )
        matches_all = cached_get_all_records(
            ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30),
            ttl_seconds=10
        )
        pods_all = cached_get_all_records(
            ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=50000, cols=25),
            ttl_seconds=10
        )

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
# [BLOCO 8/22 termina aqui]
# =========================================================


# =================================================
# FIM DO SUB-BLOCO G/7
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 9/22
# SUB-BLOCO: ÚNICO
# REVISÃO: comportamento stale-while-revalidate para o índice de autocomplete,
# reduzindo timeout do Discord sem alterar a lógica funcional.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - filtros mais baratos antes de montar payload
# - rebuild não bloqueante mantido
# - early return mais agressivo
# - menor pressão de cópia/loop no autocomplete
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
_MATCH_AC_INDEX_BUILD_LOCK = threading.Lock()
_MATCH_AC_INDEX_TTL_SECONDS = 300


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
    t0 = time.perf_counter()

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

    ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)

    cycle_rows = cached_get_all_records(ws_cycles, ttl_seconds=10)
    player_rows = cached_get_all_records(ws_players, ttl_seconds=10)

    locked_cycles = set()
    for r in cycle_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        cyc = safe_int(r.get("cycle", 0), 0)
        if cyc <= 0:
            continue

        st = str(r.get("status", "")).strip().lower()
        if st == "locked":
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
        if not pid:
            continue

        nick = str(r.get("nick", "")).strip()
        nick_map[pid] = nick or pid

    by_user: dict[str, list[dict]] = {}

    ws_matches = ensure_worksheet(sh, "Matches", MATCHES_HEADER, rows=50000, cols=30)
    ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    match_rows = cached_get_all_records(ws_matches, ttl_seconds=10)

    locked_cycles_local = locked_cycles
    nick_map_local = nick_map
    by_user_local = by_user

    for r in match_rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        cyc = safe_int(r.get("cycle", 0), 0)
        if cyc not in locked_cycles_local:
            continue

        if not as_bool(r.get("active", "TRUE")):
            continue

        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()
        mid = str(r.get("match_id", "")).strip()

        if not a or not b or not mid:
            continue

        pod = str(r.get("pod", "")).strip()
        visual_status = _match_visual_status_from_row(r)

        a_opp = nick_map_local.get(b, b)
        b_opp = nick_map_local.get(a, a)

        if pod:
            label_a = f"{a_opp} | POD {pod} | {visual_status}"
            label_b = f"{b_opp} | POD {pod} | {visual_status}"
        else:
            label_a = f"{a_opp} | {visual_status}"
            label_b = f"{b_opp} | {visual_status}"

        search_a = f"{mid} {a_opp} {visual_status} {pod}".lower()
        search_b = f"{mid} {b_opp} {visual_status} {pod}".lower()

        by_user_local.setdefault(a, []).append({
            "label": label_a[:100],
            "value": mid,
            "search": search_a,
            "cycle": cyc,
            "status": visual_status,
        })

        by_user_local.setdefault(b, []).append({
            "label": label_b[:100],
            "value": mid,
            "search": search_b,
            "cycle": cyc,
            "status": visual_status,
        })

    for uid, items in by_user_local.items():
        items.sort(
            key=lambda x: (
                0 if x.get("status") == "pendente" else 1,
                -safe_int(x.get("cycle", 0), 0),
                str(x.get("label", "")).lower(),
            )
        )

    try:
        print(f"DEBUG: _build_match_ac_index em {round((time.perf_counter() - t0) * 1000, 2)} ms | users={len(by_user_local)}")
    except Exception:
        pass

    return {
        "ts": _cache_now(),
        "season_id": season_id,
        "locked_cycles": locked_cycles_local,
        "by_user": by_user_local,
    }


def ensure_match_ac_index(sh, max_age_seconds: int = _MATCH_AC_INDEX_TTL_SECONDS):
    """
    Garante que o índice esteja pronto e recente.
    Rebuild automático por tempo, mas sem bloquear múltiplos rebuilds concorrentes.
    """
    now = _cache_now()

    with _MATCH_AC_INDEX_LOCK:
        ts = float(_MATCH_AC_INDEX.get("ts", 0.0) or 0.0)
        has_data = bool(_MATCH_AC_INDEX.get("by_user", {}))

        if ts > 0 and (now - ts) <= max_age_seconds:
            return

        if has_data and _MATCH_AC_INDEX_BUILD_LOCK.locked():
            return

    acquired = _MATCH_AC_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
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
    finally:
        _MATCH_AC_INDEX_BUILD_LOCK.release()


def get_match_ac_choices_for_user(sh, user_id: str, query: str = "", limit: int = 25) -> list[dict]:
    """
    Retorna as opções de autocomplete já prontas para um usuário.
    Cada item retorna:
    - label
    - value
    - search

    Estratégia:
    - tenta usar snapshot atual imediatamente
    - se estiver vazio, tenta garantir rebuild
    - se rebuild ainda não estiver pronto, devolve vazio rapidamente
    """
    uid = str(user_id).strip()
    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    if not uid:
        return []

    with _MATCH_AC_INDEX_LOCK:
        items = _MATCH_AC_INDEX.get("by_user", {}).get(uid, [])

    if not items:
        ensure_match_ac_index(sh)

        with _MATCH_AC_INDEX_LOCK:
            items = _MATCH_AC_INDEX.get("by_user", {}).get(uid, [])

    if not items:
        return []

    if not q:
        return [dict(item) for item in items[:limit]]

    out = []
    for item in items:
        if q in str(item.get("search", "")):
            out.append(dict(item))
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
# FIM DO SUB-BLOCO 9/22
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 10/22
# SUB-BLOCO: ÚNICO
# REVISÃO: blindagem contra rebuild concorrente do índice RAM de matches,
# mantendo a mesma interface e a mesma lógica funcional.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - stale snapshot antes de rebuild bloqueante
# - cópias mais leves
# - filtros mais baratos
# - debug leve do tempo de rebuild
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
_MATCH_RAM_INDEX_BUILD_LOCK = threading.Lock()
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
    t0 = time.perf_counter()

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

    for key, items in by_cycle.items():
        items.sort(key=_match_sort_key)

    for pid, items in by_player.items():
        items.sort(key=_match_sort_key)

    season_id_active = get_current_season_id(sh)

    try:
        print(
            f"DEBUG: _build_match_ram_index em "
            f"{round((time.perf_counter() - t0) * 1000, 2)} ms | "
            f"matches={len(by_match_id)} players={len(by_player)} cycles={len(by_cycle)}"
        )
    except Exception:
        pass

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
        has_data = bool(_MATCH_RAM_INDEX.get("by_match_id", {}))

        if ts > 0 and (now - ts) <= max_age_seconds:
            return

        # stale snapshot aceitável: não bloqueia interação se já há dados
        if has_data and _MATCH_RAM_INDEX_BUILD_LOCK.locked():
            return

    acquired = _MATCH_RAM_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
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
    finally:
        _MATCH_RAM_INDEX_BUILD_LOCK.release()


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

    target_season = safe_int(season_id, 0) if season_id is not None else None
    target_cycle = safe_int(cycle, 0) if cycle is not None else None

    with _MATCH_RAM_INDEX_LOCK:
        rows = _MATCH_RAM_INDEX.get("by_player", {}).get(pid, [])

    out = []

    for r in rows:
        if target_season is not None and safe_int(r.get("season_id", 0), 0) != target_season:
            continue
        if target_cycle is not None and safe_int(r.get("cycle", 0), 0) != target_cycle:
            continue
        if only_active and not as_bool(r.get("active", True)):
            continue

        out.append(dict(r))

    return out


def get_matches_for_cycle_fast(sh, season_id: int, cycle: int, only_active: bool = False) -> list[dict]:
    ensure_match_ram_index(sh)

    key = (safe_int(season_id, 0), safe_int(cycle, 0))

    with _MATCH_RAM_INDEX_LOCK:
        rows = _MATCH_RAM_INDEX.get("by_cycle", {}).get(key, [])

    if not rows:
        return []

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
# FIM DO SUB-BLOCO 10/22
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 11/22
# SUB-BLOCO: A/2
# RESUMO: Índices RAM de PLAYERS, CYCLES e SEASONS, com helpers *_fast
# para autocomplete, resolução rápida de jogador, leitura rápida de nicknames,
# seasons e ciclos sem consultar Google Sheets a cada uso.
# NOVO BLOCO: criado para suprir dependências já usadas pelos BLOCOS 5, 6, 7 e 8.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - stale snapshot antes de rebuild
# - rebuild leve com debug de tempo
# - menos cópias desnecessárias
# - filtros mais baratos para autocomplete
# =================================================

# =========================================================
# LOCKS GLOBAIS — RAM INDEX
# =========================================================
_PLAYER_RAM_INDEX = None
_PLAYER_RAM_LOCK = threading.Lock()
_PLAYER_RAM_BUILD_LOCK = threading.Lock()

_CYCLE_RAM_INDEX = None
_CYCLE_RAM_LOCK = threading.Lock()
_CYCLE_RAM_BUILD_LOCK = threading.Lock()

# =========================================================
# CACHE DE ÍNDICE — PLAYERS
# =========================================================
_PLAYER_RAM_INDEX_TTL_SECONDS = 60

# Estrutura reaproveita os globais declarados no BLOCO 1:
# _PLAYER_RAM_INDEX
# _PLAYER_RAM_LOCK
#
# Formato:
# {
#   "ts": float,
#   "by_id": {player_id: row_dict},
#   "nick_map": {player_id: nick},
#   "choices": [{"label","value","search"}],
# }

def invalidate_player_ram_index():
    global _PLAYER_RAM_INDEX

    with _PLAYER_RAM_LOCK:
        _PLAYER_RAM_INDEX = {
            "ts": 0.0,
            "by_id": {},
            "nick_map": {},
            "choices": [],
        }


def _normalize_player_row(raw: dict) -> dict:
    pid = str(raw.get("discord_id", "")).strip()
    nick = str(raw.get("nick", "")).strip()
    name = str(raw.get("name", "")).strip()
    notes = str(raw.get("notes", "")).strip()
    status = str(raw.get("status", "")).strip().lower()
    rating = str(raw.get("rating", "")).strip()
    created_at = str(raw.get("created_at", "")).strip()
    updated_at = str(raw.get("updated_at", "")).strip()

    return {
        "discord_id": pid,
        "nick": nick,
        "name": name,
        "notes": notes,
        "status": status,
        "rating": rating,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _build_player_ram_index(sh) -> dict:
    t0 = time.perf_counter()

    ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=5000, cols=25)
    ensure_sheet_columns(ws_players, PLAYERS_REQUIRED)

    rows = cached_get_all_records(ws_players, ttl_seconds=10)

    by_id: dict[str, dict] = {}
    nick_map: dict[str, str] = {}
    choices: list[dict] = []

    for raw in rows:
        r = _normalize_player_row(raw)
        pid = r["discord_id"]
        if not pid:
            continue

        by_id[pid] = r

        nick = r["nick"] or pid
        nick_map[pid] = nick

        choices.append({
            "label": nick[:100],
            "value": pid,
            "search": f"{pid} {nick} {r['name']} {r['status']}".lower(),
        })

    choices.sort(key=lambda x: str(x.get("label", "")).lower())

    try:
        print(
            f"DEBUG: _build_player_ram_index em "
            f"{round((time.perf_counter() - t0) * 1000, 2)} ms | "
            f"players={len(by_id)}"
        )
    except Exception:
        pass

    return {
        "ts": _cache_now(),
        "by_id": by_id,
        "nick_map": nick_map,
        "choices": choices,
    }


def ensure_player_ram_index(sh, max_age_seconds: int = _PLAYER_RAM_INDEX_TTL_SECONDS):
    global _PLAYER_RAM_INDEX

    now = _cache_now()

    with _PLAYER_RAM_LOCK:
        data = _PLAYER_RAM_INDEX
        if isinstance(data, dict):
            ts = float(data.get("ts", 0.0) or 0.0)
            has_data = bool(data.get("by_id", {}))

            if ts > 0 and (now - ts) <= max_age_seconds:
                return

            if has_data and _PLAYER_RAM_BUILD_LOCK.locked():
                return

    acquired = _PLAYER_RAM_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _PLAYER_RAM_LOCK:
            data = _PLAYER_RAM_INDEX
            if isinstance(data, dict):
                ts = float(data.get("ts", 0.0) or 0.0)
                if ts > 0 and (now - ts) <= max_age_seconds:
                    return

        built = _build_player_ram_index(sh)

        with _PLAYER_RAM_LOCK:
            _PLAYER_RAM_INDEX = built
    finally:
        _PLAYER_RAM_BUILD_LOCK.release()


def get_player_ram_index_snapshot() -> dict:
    with _PLAYER_RAM_LOCK:
        data = _PLAYER_RAM_INDEX if isinstance(_PLAYER_RAM_INDEX, dict) else {}
        return {
            "ts": data.get("ts", 0.0),
            "players_indexed": len(data.get("by_id", {})),
            "choices_indexed": len(data.get("choices", [])),
        }


def get_player_nick_map_fast(sh) -> dict[str, str]:
    ensure_player_ram_index(sh)

    with _PLAYER_RAM_LOCK:
        data = _PLAYER_RAM_INDEX if isinstance(_PLAYER_RAM_INDEX, dict) else {}
        return dict(data.get("nick_map", {}))


def get_player_row_fast(sh, discord_id: str) -> dict | None:
    ensure_player_ram_index(sh)

    pid = str(discord_id or "").strip()
    if not pid:
        return None

    with _PLAYER_RAM_LOCK:
        data = _PLAYER_RAM_INDEX if isinstance(_PLAYER_RAM_INDEX, dict) else {}
        row = data.get("by_id", {}).get(pid)
        return dict(row) if row else None


def get_player_record_by_discord_id(ws_players, discord_id: str) -> dict | None:
    did = str(discord_id or "").strip()
    if not did:
        return None

    try:
        sh = ws_players.spreadsheet
        return get_player_row_fast(sh, did)
    except Exception:
        rows = cached_get_all_records(ws_players, ttl_seconds=10)
        for r in rows:
            if str(r.get("discord_id", "")).strip() == did:
                return dict(r)
        return None


def resolve_player_id_fast(sh, raw_value: str) -> str:
    ensure_player_ram_index(sh)

    raw = str(raw_value or "").strip()
    if not raw:
        return ""

    if raw.startswith("<@") and raw.endswith(">"):
        raw = "".join(ch for ch in raw if ch.isdigit())

    raw_lower = raw.lower()

    with _PLAYER_RAM_LOCK:
        data = _PLAYER_RAM_INDEX if isinstance(_PLAYER_RAM_INDEX, dict) else {}
        by_id = data.get("by_id", {})

        if raw in by_id:
            return raw

        for pid, r in by_id.items():
            nick = str(r.get("nick", "")).strip()
            if nick and nick.lower() == raw_lower:
                return pid

        for pid, r in by_id.items():
            name = str(r.get("name", "")).strip()
            if name and name.lower() == raw_lower:
                return pid

    return ""


def get_player_choices_fast(sh, query: str = "", limit: int = 25) -> list[dict]:
    ensure_player_ram_index(sh)

    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    with _PLAYER_RAM_LOCK:
        data = _PLAYER_RAM_INDEX if isinstance(_PLAYER_RAM_INDEX, dict) else {}
        items = data.get("choices", [])

    if not items:
        return []

    if not q:
        return [dict(item) for item in items[:limit]]

    out = []
    for item in items:
        if q in str(item.get("search", "")):
            out.append(dict(item))
            if len(out) >= limit:
                break
    return out


# =========================================================
# CACHE DE ÍNDICE — CYCLES
# =========================================================
_CYCLE_RAM_INDEX_TTL_SECONDS = 60

# Estrutura reaproveita os globais declarados no BLOCO 1:
# _CYCLE_RAM_INDEX
# _CYCLE_RAM_LOCK
#
# Formato:
# {
#   "ts": float,
#   "by_season": {
#       season_id: {
#           "rows": [row_dict],
#           "choices": [choice_dict]
#       }
#   }
# }

def invalidate_cycle_ram_index():
    global _CYCLE_RAM_INDEX

    with _CYCLE_RAM_LOCK:
        _CYCLE_RAM_INDEX = {
            "ts": 0.0,
            "by_season": {},
        }


def _cycle_status_label_pt(status: str) -> str:
    st = str(status or "").strip().lower()
    if st == "open":
        return "aberto"
    if st == "locked":
        return "fechado"
    if st == "completed":
        return "encerrado"
    return st or "desconhecido"


def _normalize_cycle_row(raw: dict) -> dict:
    season_id = safe_int(raw.get("season_id", 0), 0)
    cycle = safe_int(raw.get("cycle", 0), 0)
    status = str(raw.get("status", "")).strip().lower()
    start_at_br = str(raw.get("start_at_br", "")).strip()
    deadline_at_br = str(raw.get("deadline_at_br", "")).strip()
    created_at = str(raw.get("created_at", "")).strip()
    updated_at = str(raw.get("updated_at", "")).strip()

    return {
        "season_id": season_id,
        "cycle": cycle,
        "status": status,
        "start_at_br": start_at_br,
        "deadline_at_br": deadline_at_br,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _build_cycle_ram_index(sh) -> dict:
    t0 = time.perf_counter()

    ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
    ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

    rows = cached_get_all_records(ws_cycles, ttl_seconds=10)

    by_season: dict[int, dict] = {}

    for raw in rows:
        r = _normalize_cycle_row(raw)
        sid = r["season_id"]
        cyc = r["cycle"]

        if sid <= 0 or cyc <= 0:
            continue

        bucket = by_season.setdefault(sid, {
            "rows": [],
            "choices": [],
        })
        bucket["rows"].append(r)

    for sid, bucket in by_season.items():
        bucket["rows"].sort(key=lambda x: safe_int(x.get("cycle", 0), 0))

        choices = []
        for r in bucket["rows"]:
            cyc = safe_int(r.get("cycle", 0), 0)
            status = str(r.get("status", "")).strip().lower()
            pt = _cycle_status_label_pt(status)

            search = f"ciclo {cyc} {status} {pt} season {sid}".lower()
            if r.get("start_at_br"):
                search += f" {str(r['start_at_br']).lower()}"
            if r.get("deadline_at_br"):
                search += f" {str(r['deadline_at_br']).lower()}"

            choices.append({
                "label": f"Ciclo {cyc} | {pt}"[:100],
                "value": cyc,
                "search": search,
                "status": status,
                "season_id": sid,
            })

        bucket["choices"] = choices

    try:
        total_cycles = sum(len(v.get("rows", [])) for v in by_season.values())
        print(
            f"DEBUG: _build_cycle_ram_index em "
            f"{round((time.perf_counter() - t0) * 1000, 2)} ms | "
            f"seasons={len(by_season)} cycles={total_cycles}"
        )
    except Exception:
        pass

    return {
        "ts": _cache_now(),
        "by_season": by_season,
    }


def ensure_cycle_ram_index(sh, max_age_seconds: int = _CYCLE_RAM_INDEX_TTL_SECONDS):
    global _CYCLE_RAM_INDEX

    now = _cache_now()

    with _CYCLE_RAM_LOCK:
        data = _CYCLE_RAM_INDEX
        if isinstance(data, dict):
            ts = float(data.get("ts", 0.0) or 0.0)
            has_data = bool(data.get("by_season", {}))

            if ts > 0 and (now - ts) <= max_age_seconds:
                return

            if has_data and _CYCLE_RAM_BUILD_LOCK.locked():
                return

    acquired = _CYCLE_RAM_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _CYCLE_RAM_LOCK:
            data = _CYCLE_RAM_INDEX
            if isinstance(data, dict):
                ts = float(data.get("ts", 0.0) or 0.0)
                if ts > 0 and (now - ts) <= max_age_seconds:
                    return

        built = _build_cycle_ram_index(sh)

        with _CYCLE_RAM_LOCK:
            _CYCLE_RAM_INDEX = built
    finally:
        _CYCLE_RAM_BUILD_LOCK.release()


def get_cycle_ram_index_snapshot() -> dict:
    with _CYCLE_RAM_LOCK:
        data = _CYCLE_RAM_INDEX if isinstance(_CYCLE_RAM_INDEX, dict) else {}
        by_season = data.get("by_season", {})
        total_rows = sum(len(v.get("rows", [])) for v in by_season.values())
        return {
            "ts": data.get("ts", 0.0),
            "seasons_indexed": len(by_season),
            "cycles_indexed": total_rows,
        }


def get_cycle_choices_fast(
    sh,
    season_id: int,
    query: str = "",
    only_open: bool = False,
    limit: int = 25
) -> list[dict]:
    ensure_cycle_ram_index(sh)

    sid = safe_int(season_id, 0)
    if sid <= 0:
        return []

    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    with _CYCLE_RAM_LOCK:
        data = _CYCLE_RAM_INDEX if isinstance(_CYCLE_RAM_INDEX, dict) else {}
        items = data.get("by_season", {}).get(sid, {}).get("choices", [])

    if not items:
        return []

    out = []
    for item in items:
        status = str(item.get("status", "")).strip().lower()

        if only_open and status != "open":
            continue
        if q and q not in str(item.get("search", "")):
            continue

        out.append(dict(item))
        if len(out) >= limit:
            break

    return out


# =========================================================
# CACHE DE ÍNDICE — SEASONS
# =========================================================
_SEASON_RAM_INDEX = {
    "ts": 0.0,
    "current_season_id": 0,
    "rows": [],
    "choices": [],
}
_SEASON_RAM_INDEX_LOCK = threading.Lock()
_SEASON_RAM_INDEX_BUILD_LOCK = threading.Lock()
_SEASON_RAM_INDEX_TTL_SECONDS = 60


def invalidate_season_ram_index():
    with _SEASON_RAM_INDEX_LOCK:
        _SEASON_RAM_INDEX["ts"] = 0.0
        _SEASON_RAM_INDEX["current_season_id"] = 0
        _SEASON_RAM_INDEX["rows"] = []
        _SEASON_RAM_INDEX["choices"] = []


def _season_status_label_pt(status: str) -> str:
    st = str(status or "").strip().lower()
    if st == "open":
        return "aberta"
    if st == "closed":
        return "fechada"
    return st or "desconhecida"


def _normalize_season_row(raw: dict) -> dict:
    sid = safe_int(raw.get("season_id", 0), 0)
    status = str(raw.get("status", "")).strip().lower()
    name = str(raw.get("name", "")).strip()
    created_at = str(raw.get("created_at", "")).strip()
    updated_at = str(raw.get("updated_at", "")).strip()

    return {
        "season_id": sid,
        "status": status,
        "name": name,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _build_season_ram_index(sh) -> dict:
    t0 = time.perf_counter()

    ws_seasons = ensure_worksheet(sh, "Seasons", SEASONS_HEADER, rows=200, cols=20)
    ensure_sheet_columns(ws_seasons, SEASONS_REQUIRED)

    rows = cached_get_all_records(ws_seasons, ttl_seconds=10)

    out_rows = []
    choices = []

    for raw in rows:
        r = _normalize_season_row(raw)
        sid = r["season_id"]
        if sid <= 0:
            continue
        out_rows.append(r)

    out_rows.sort(key=lambda x: safe_int(x.get("season_id", 0), 0), reverse=True)

    for r in out_rows:
        sid = r["season_id"]
        status = str(r.get("status", "")).strip().lower()
        pt = _season_status_label_pt(status)
        name = str(r.get("name", "")).strip() or f"Temporada {sid}"

        choices.append({
            "season_id": sid,
            "label": f"Season {sid} | {name} | {pt}"[:100],
            "value": sid,
            "search": f"season {sid} {name} {status} {pt}".lower(),
            "status": status,
            "name_text": name,
        })

    current_sid = get_current_season_id(sh)

    try:
        print(
            f"DEBUG: _build_season_ram_index em "
            f"{round((time.perf_counter() - t0) * 1000, 2)} ms | "
            f"seasons={len(out_rows)} current={current_sid}"
        )
    except Exception:
        pass

    return {
        "ts": _cache_now(),
        "current_season_id": current_sid,
        "rows": out_rows,
        "choices": choices,
    }


def ensure_season_ram_index(sh, max_age_seconds: int = _SEASON_RAM_INDEX_TTL_SECONDS):
    now = _cache_now()

    with _SEASON_RAM_INDEX_LOCK:
        ts = float(_SEASON_RAM_INDEX.get("ts", 0.0) or 0.0)
        has_data = bool(_SEASON_RAM_INDEX.get("rows", []))

        if ts > 0 and (now - ts) <= max_age_seconds:
            return

        if has_data and _SEASON_RAM_INDEX_BUILD_LOCK.locked():
            return

    acquired = _SEASON_RAM_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _SEASON_RAM_INDEX_LOCK:
            ts = float(_SEASON_RAM_INDEX.get("ts", 0.0) or 0.0)
            if ts > 0 and (now - ts) <= max_age_seconds:
                return

        built = _build_season_ram_index(sh)

        with _SEASON_RAM_INDEX_LOCK:
            _SEASON_RAM_INDEX["ts"] = built["ts"]
            _SEASON_RAM_INDEX["current_season_id"] = built["current_season_id"]
            _SEASON_RAM_INDEX["rows"] = built["rows"]
            _SEASON_RAM_INDEX["choices"] = built["choices"]
    finally:
        _SEASON_RAM_INDEX_BUILD_LOCK.release()


def get_season_ram_index_snapshot() -> dict:
    with _SEASON_RAM_INDEX_LOCK:
        return {
            "ts": _SEASON_RAM_INDEX.get("ts", 0.0),
            "current_season_id": _SEASON_RAM_INDEX.get("current_season_id", 0),
            "seasons_indexed": len(_SEASON_RAM_INDEX.get("rows", [])),
        }


def get_season_choices_fast(sh, query: str = "", limit: int = 25) -> list[dict]:
    ensure_season_ram_index(sh)

    q = str(query or "").strip().lower()
    limit = max(1, min(limit, 25))

    with _SEASON_RAM_INDEX_LOCK:
        items = _SEASON_RAM_INDEX.get("choices", [])

    if not items:
        return []

    if not q:
        return [dict(item) for item in items[:limit]]

    out = []
    for item in items:
        if q in str(item.get("search", "")):
            out.append(dict(item))
            if len(out) >= limit:
                break

    return out


# =================================================
# FIM DO SUB-BLOCO A/2
# =================================================


# =================================================
# BLOCO ORIGINAL: 11/22
# SUB-BLOCO: B/2
# REVISÃO: comando administrativo para listar matches por season/ciclo,
# com separação entre registradas e pendentes, placar mais claro e
# compatibilidade integral com os índices RAM e helpers atuais.
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - early return mais rápido
# - menos processamento por linha
# - redução de concatenações repetidas
# - prioriza índice RAM sem rebuild pesado em fluxo
# =================================================

# =========================================================
# HELPERS — CONSULTA ADMIN DE MATCHES
# =========================================================
def _admin_match_status_label(status: str) -> str:
    st = str(status or "").strip().lower()
    if st == "confirmed":
        return "confirmado"
    if st == "pending":
        return "pendente_confirmação"
    if st == "rejected":
        return "rejeitado"
    return "aberto"


def _admin_match_score_text(r: dict) -> str:
    a_w = safe_int(r.get("a_games_won", 0), 0)
    b_w = safe_int(r.get("b_games_won", 0), 0)
    d_g = safe_int(r.get("draw_games", 0), 0)

    if a_w == 0 and b_w == 0 and d_g == 0:
        return "—"

    return f"{a_w}-{b_w}-{d_g}"


def _admin_match_is_registered(r: dict) -> bool:
    """
    Registrada:
    - tem reported_by_id preenchido
    - ou status diferente de open/vazio
    """
    reported_by = str(r.get("reported_by_id", "")).strip()
    if reported_by:
        return True

    status = str(r.get("confirmed_status", "")).strip().lower()
    return status not in ("", "open")


def _admin_match_sort_key(r: dict):
    return (
        safe_int(r.get("pod", 0), 999999),
        str(r.get("match_id", "")).strip().lower(),
    )


# =========================================================
# /matches_ciclo
# =========================================================
@client.tree.command(name="matches_ciclo", description="(ADM) Lista todas as matches do ciclo, separando registradas e pendentes.")
@app_commands.describe(season="Season", cycle="Ciclo")
@app_commands.autocomplete(season=ac_owner_season, cycle=ac_owner_cycle_for_season)
async def matches_ciclo(interaction: discord.Interaction, season: int, cycle: int):
    if not (await is_admin_or_organizer(interaction) or await is_owner_only(interaction)):
        return await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send(
                f"❌ A season {season} não existe.",
                ephemeral=True
            )

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=25)
        cf = get_cycle_fields(ws_cycles, season, cycle)
        if cf.get("status") is None:
            return await interaction.followup.send(
                f"❌ O ciclo {cycle} não existe na season {season}.",
                ephemeral=True
            )

        rows = get_matches_for_cycle_fast(
            sh,
            season_id=season,
            cycle=cycle,
            only_active=False
        )

        if not rows:
            return await interaction.followup.send(
                f"⚠️ Não há matches na **Season {season} / Ciclo {cycle}**.",
                ephemeral=True
            )

        nick_map = get_player_nick_map_fast(sh)
        rows.sort(key=_admin_match_sort_key)

        registradas: list[str] = []
        pendentes: list[str] = []

        for r in rows:
            match_id = str(r.get("match_id", "")).strip()
            if not match_id:
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()
            if not a or not b:
                continue

            pod = str(r.get("pod", "")).strip()
            a_name = nick_map.get(a, a)
            b_name = nick_map.get(b, b)
            score = _admin_match_score_text(r)
            status = _admin_match_status_label(r.get("confirmed_status", ""))

            line = (
                f"`{match_id}` | POD {pod or '-'} | "
                f"**{a_name}** vs **{b_name}** | "
                f"status: **{status}** | "
                f"placar: **{score}**"
            )

            if _admin_match_is_registered(r):
                registradas.append(line)
            else:
                pendentes.append(line)

        lines = [f"📋 **Matches da Season {season} / Ciclo {cycle}**", ""]

        lines.append(f"**Registradas:** {len(registradas)}")
        if registradas:
            lines.extend(registradas)
        else:
            lines.append("Nenhuma match registrada.")

        lines.append("")
        lines.append(f"**Pendentes:** {len(pendentes)}")
        if pendentes:
            lines.extend(pendentes)
        else:
            lines.append("Nenhuma match pendente.")

        await send_followup_chunks(
            interaction,
            "\n".join(lines),
            ephemeral=True,
            limit=1800
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /matches_ciclo: {e}", ephemeral=True)


# =================================================
# FIM DO SUB-BLOCO B/2
# =================================================

# =================================================
# FIM DO BLOCO 11/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 12/22
# SUB-BLOCO: ÚNICO
# REVISÃO: Estrutura base da Fase Final (FinalStage, FinalParticipants, FinalMatches),
# validações da season, leitura do ranking geral e definição do TOP (2/4/8/16).
# CORREÇÃO CRÍTICA:
# - invalidação correta do índice RAM de FinalParticipants
# - rebuild da fase final passa a refletir o estado real do Sheets
# CORREÇÃO DE PERFORMANCE/ESTABILIDADE:
# - menos recomputação de header/idx
# - menos lookups repetidos por linha
# - early return mais leve
# - preserva integralmente a lógica atual
# =========================================================

# =========================================================
# HEADERS — FASE FINAL
# =========================================================

FINAL_STAGE_HEADER = [
    "season_id",
    "status",
    "top_size",
    "format",
    "created_at",
    "updated_at",
]

FINAL_STAGE_REQUIRED = [
    "season_id",
    "status",
    "top_size",
]

FINAL_PARTICIPANTS_HEADER = [
    "season_id",
    "seed",
    "player_id",
    "ranking_position",
    "status",
    "created_at",
    "updated_at",
]

FINAL_PARTICIPANTS_REQUIRED = [
    "season_id",
    "seed",
    "player_id",
]

FINAL_MATCHES_HEADER = [
    "final_match_id",
    "season_id",
    "bracket",
    "round",
    "match_order",
    "player_a_id",
    "player_b_id",
    "a_games_won",
    "b_games_won",
    "result_type",
    "status",
    "winner_id",
    "loser_id",
    "next_win_match_id",
    "next_win_slot",
    "next_lose_match_id",
    "next_lose_slot",
    "is_reset_match",
    "created_at",
    "updated_at",
]

FINAL_MATCHES_REQUIRED = [
    "final_match_id",
    "season_id",
    "bracket",
    "round",
    "match_order",
    "player_a_id",
    "player_b_id",
    "status",
]


# =========================================================
# ENSURE — FASE FINAL
# =========================================================

def ensure_final_sheets(sh):
    ws_stage = ensure_worksheet(
        sh,
        "FinalStage",
        FINAL_STAGE_HEADER,
        rows=50,
        cols=20
    )
    ws_participants = ensure_worksheet(
        sh,
        "FinalParticipants",
        FINAL_PARTICIPANTS_HEADER,
        rows=200,
        cols=20
    )
    ws_matches = ensure_worksheet(
        sh,
        "FinalMatches",
        FINAL_MATCHES_HEADER,
        rows=2000,
        cols=25
    )

    ensure_sheet_columns(ws_stage, FINAL_STAGE_REQUIRED)
    ensure_sheet_columns(ws_participants, FINAL_PARTICIPANTS_REQUIRED)
    ensure_sheet_columns(ws_matches, FINAL_MATCHES_REQUIRED)

    return ws_stage, ws_participants, ws_matches


# =========================================================
# HELPERS — FASE FINAL / STAGE
# =========================================================

def get_final_stage_row(ws_stage, season_id: int) -> int | None:
    rows = cached_get_all_values(ws_stage, ttl_seconds=10)
    if len(rows) <= 1:
        return None

    col = ensure_sheet_columns(ws_stage, FINAL_STAGE_REQUIRED)

    season_col = col["season_id"]

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]
        sid = safe_int(r[season_col] if season_col < len(r) else 0, 0)

        if sid == season_id:
            return i

    return None


def get_final_stage_fields(ws_stage, season_id: int) -> dict:
    rows = cached_get_all_values(ws_stage, ttl_seconds=10)

    out = {
        "season_id": season_id,
        "status": None,
        "top_size": 0,
        "format": "",
        "created_at": "",
        "updated_at": "",
    }

    if len(rows) <= 1:
        return out

    header = rows[0]
    idx = {name: j for j, name in enumerate(header)}

    sid_idx = idx.get("season_id", -1)
    status_idx = idx.get("status", -1)
    top_idx = idx.get("top_size", -1)
    fmt_idx = idx.get("format", -1)
    created_idx = idx.get("created_at", -1)
    updated_idx = idx.get("updated_at", -1)

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]
        sid = safe_int(r[sid_idx] if 0 <= sid_idx < len(r) else 0, 0)

        if sid != season_id:
            continue

        out["status"] = str(r[status_idx] if 0 <= status_idx < len(r) else "").strip().lower()
        out["top_size"] = safe_int(r[top_idx] if 0 <= top_idx < len(r) else 0, 0)
        out["format"] = str(r[fmt_idx] if 0 <= fmt_idx < len(r) else "").strip()
        out["created_at"] = str(r[created_idx] if 0 <= created_idx < len(r) else "").strip()
        out["updated_at"] = str(r[updated_idx] if 0 <= updated_idx < len(r) else "").strip()
        return out

    return out


def final_stage_exists(ws_stage, season_id: int) -> bool:
    return get_final_stage_row(ws_stage, season_id) is not None


def set_final_stage(
    ws_stage,
    season_id: int,
    status: str,
    top_size: int,
    fmt: str = "single_elimination"
):
    status = str(status or "").strip().lower()
    top_size = safe_int(top_size, 0)
    nowb = now_br_str()

    rown = get_final_stage_row(ws_stage, season_id)
    header = cached_get_all_values(ws_stage, ttl_seconds=10)
    idx = {name: i for i, name in enumerate(header[0] if header else FINAL_STAGE_HEADER)}

    if rown is None:
        ws_stage.append_row(
            [
                str(season_id),
                status,
                str(top_size),
                str(fmt),
                nowb,
                nowb,
            ],
            value_input_option="USER_ENTERED"
        )
        cache_invalidate(ws_stage)
        try:
            invalidate_final_stage_ram_index()
        except Exception:
            pass
        return

    updates = []

    if "status" in idx:
        updates.append({
            "range": f"{col_letter(idx['status'])}{rown}",
            "values": [[status]]
        })

    if "top_size" in idx:
        updates.append({
            "range": f"{col_letter(idx['top_size'])}{rown}",
            "values": [[str(top_size)]]
        })

    if "format" in idx:
        updates.append({
            "range": f"{col_letter(idx['format'])}{rown}",
            "values": [[str(fmt)]]
        })

    if "updated_at" in idx:
        updates.append({
            "range": f"{col_letter(idx['updated_at'])}{rown}",
            "values": [[nowb]]
        })

    if updates:
        ws_stage.batch_update(updates)
        cache_invalidate(ws_stage)
        try:
            invalidate_final_stage_ram_index()
        except Exception:
            pass


# =========================================================
# VALIDAÇÕES — FASE FINAL
# =========================================================

def final_all_cycles_completed(sh, season_id: int) -> bool:
    """
    Regra:
    - a season precisa ter pelo menos 1 ciclo cadastrado
    - todos os ciclos da season precisam estar completed
    """
    ws_cycles = ensure_worksheet(
        sh,
        "Cycles",
        CYCLES_HEADER,
        rows=2000,
        cols=25
    )

    rows = cached_get_all_records(ws_cycles, ttl_seconds=10)
    found_any = False

    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        found_any = True

        st = str(r.get("status", "")).strip().lower()
        if st != "completed":
            return False

    return found_any


# =========================================================
# LEITURA — RANKING GERAL FINAL DA SEASON
# BASE OFICIAL PARA CLASSIFICAÇÃO DA FASE FINAL
# =========================================================

def _final_read_ranking_geral_rows(sh, season_id: int) -> list[dict]:
    """
    Retorna a classificação final da season usando a mesma lógica oficial
    do /ranking_geral, mas em formato estruturado.
    """
    ws_standings = ensure_worksheet(
        sh,
        "Standings",
        STANDINGS_HEADER,
        rows=50000,
        cols=30
    )
    ensure_sheet_columns(ws_standings, STANDINGS_REQUIRED)

    vals = cached_get_all_values(ws_standings, ttl_seconds=10)

    if len(vals) <= 1:
        return []

    header = vals[0]
    idx = {name: i for i, name in enumerate(header)}

    season_idx = idx.get("season_id", -1)
    player_idx = idx.get("player_id", -1)
    matches_idx = idx.get("matches_played", -1)
    match_points_idx = idx.get("match_points", -1)
    game_wins_idx = idx.get("game_wins", -1)
    game_losses_idx = idx.get("game_losses", -1)
    game_draws_idx = idx.get("game_draws", -1)
    games_played_idx = idx.get("games_played", -1)
    omw_idx = idx.get("omw", -1)
    ogw_idx = idx.get("ogw", -1)

    stats = {}

    for row in vals[1:]:
        row_season = safe_int(row[season_idx] if 0 <= season_idx < len(row) else 0, 0)
        if row_season != season_id:
            continue

        pid = str(row[player_idx] if 0 <= player_idx < len(row) else "").strip()
        if not pid:
            continue

        if pid not in stats:
            stats[pid] = {
                "pts": 0.0,
                "m": 0,
                "gwins": 0,
                "glosses": 0,
                "gdraws": 0,
                "gplayed": 0,
                "omw_weighted_sum": 0.0,
                "ogw_weighted_sum": 0.0,
                "omw_weight": 0,
                "ogw_weight": 0,
            }

        matches_played = safe_int(row[matches_idx] if 0 <= matches_idx < len(row) else 0, 0)
        match_points = sheet_float(row[match_points_idx] if 0 <= match_points_idx < len(row) else 0, 0.0)
        game_wins = safe_int(row[game_wins_idx] if 0 <= game_wins_idx < len(row) else 0, 0)
        game_losses = safe_int(row[game_losses_idx] if 0 <= game_losses_idx < len(row) else 0, 0)
        game_draws = safe_int(row[game_draws_idx] if 0 <= game_draws_idx < len(row) else 0, 0)
        games_played = safe_int(row[games_played_idx] if 0 <= games_played_idx < len(row) else 0, 0)

        omw_raw = sheet_float(row[omw_idx] if 0 <= omw_idx < len(row) else 0, 0.0)
        ogw_raw = sheet_float(row[ogw_idx] if 0 <= ogw_idx < len(row) else 0, 0.0)

        s = stats[pid]
        s["pts"] += match_points
        s["m"] += matches_played
        s["gwins"] += game_wins
        s["glosses"] += game_losses
        s["gdraws"] += game_draws
        s["gplayed"] += games_played

        if matches_played > 0:
            s["omw_weighted_sum"] += omw_raw * matches_played
            s["ogw_weighted_sum"] += ogw_raw * matches_played
            s["omw_weight"] += matches_played
            s["ogw_weight"] += matches_played

    if not stats:
        return []

    K = 3
    table = []

    for pid, s in stats.items():
        m = s["m"]
        pts = sheet_float(s["pts"], 0.0)

        raw_mwp = (pts / (3 * m)) if m else 0.0
        mwp = max(raw_mwp, 0.333)

        ppm = (pts + K) / (m + K) if m else 0.0

        games = s["gplayed"]
        raw_gw = ((s["gwins"] + 0.5 * s["gdraws"]) / games) if games else 0.0
        gw = max(raw_gw, 0.333)

        if s["omw_weight"] > 0:
            omw = max(s["omw_weighted_sum"] / s["omw_weight"], 0.333)
        else:
            omw = 0.333

        if s["ogw_weight"] > 0:
            ogw = max(s["ogw_weighted_sum"] / s["ogw_weight"], 0.333)
        else:
            ogw = 0.333

        peso_pts = m / (m + K) if m > 0 else 0.0
        peso_ppm = K / (m + K) if m > 0 else 0.0
        score = pts * peso_pts + ppm * peso_ppm

        table.append({
            "player_id": pid,
            "score": round(score, 6),
            "pts": round(pts, 6),
            "ppm": round(ppm, 6),
            "mwp": round(mwp, 6),
            "omw": round(omw, 6),
            "gw": round(gw, 6),
            "ogw": round(ogw, 6),
            "matches": m,
        })

    table.sort(
        key=lambda x: (
            x["score"],
            x["ppm"],
            x["mwp"],
            x["omw"],
            x["gw"],
            x["ogw"],
        ),
        reverse=True
    )

    out = []
    for pos, item in enumerate(table, start=1):
        row = dict(item)
        row["ranking_position"] = pos
        out.append(row)

    return out


# =========================================================
# DEFINIÇÃO DO TOP DA FASE FINAL
# =========================================================

def define_final_top_size(total_players: int) -> int:
    total = safe_int(total_players, 0)

    if total <= 0:
        return 0
    if total <= 8:
        return 2
    if total <= 15:
        return 4
    if total <= 31:
        return 8
    return 16


def get_final_qualified_players(sh, season_id: int) -> tuple[list[dict], int]:
    ranking_rows = _final_read_ranking_geral_rows(sh, season_id)

    if not ranking_rows:
        return [], 0

    total_players = len(ranking_rows)
    top_size = define_final_top_size(total_players)

    if top_size <= 0:
        return [], 0

    qualified = ranking_rows[:top_size]

    out = []
    for seed, r in enumerate(qualified, start=1):
        out.append({
            "season_id": season_id,
            "seed": seed,
            "player_id": str(r.get("player_id", "")).strip(),
            "ranking_position": safe_int(r.get("ranking_position", seed), seed),
            "score": sheet_float(r.get("score", 0), 0.0),
            "pts": sheet_float(r.get("pts", 0), 0.0),
            "ppm": sheet_float(r.get("ppm", 0), 0.0),
            "mwp": sheet_float(r.get("mwp", 0), 0.0),
            "omw": sheet_float(r.get("omw", 0), 0.0),
            "gw": sheet_float(r.get("gw", 0), 0.0),
            "ogw": sheet_float(r.get("ogw", 0), 0.0),
            "matches": safe_int(r.get("matches", 0), 0),
        })

    return out, top_size


# =========================================================
# HELPERS — PARTICIPANTES DA FASE FINAL
# =========================================================

def clear_final_participants_for_season(ws_participants, season_id: int):
    """
    Remove todos os participantes da season e preserva outras seasons.
    """
    vals = cached_get_all_values(ws_participants, ttl_seconds=10)

    if not vals:
        ws_participants.append_row(FINAL_PARTICIPANTS_HEADER)
        cache_invalidate(ws_participants)
        try:
            invalidate_final_participants_ram_index()
        except Exception:
            pass
        return

    header = vals[0]
    kept = [header]

    idx = {name: i for i, name in enumerate(header)}
    sid_idx = idx.get("season_id", 0)

    for row in vals[1:]:
        sid = safe_int(row[sid_idx] if sid_idx < len(row) else 0, 0)
        if sid == season_id:
            continue
        kept.append(row)

    ws_participants.clear()
    ws_participants.append_rows(kept, value_input_option="RAW")
    cache_invalidate(ws_participants)
    try:
        invalidate_final_participants_ram_index()
    except Exception:
        pass


def save_final_participants(ws_participants, season_id: int, qualified_rows: list[dict]):
    """
    Salva os classificados da fase final da season.
    Sempre regrava a season por completo para garantir consistência.
    """
    clear_final_participants_for_season(ws_participants, season_id)

    if not qualified_rows:
        try:
            invalidate_final_participants_ram_index()
        except Exception:
            pass
        return

    nowb = now_br_str()
    rows_to_add = []

    for r in qualified_rows:
        rows_to_add.append([
            str(season_id),
            str(safe_int(r.get("seed", 0), 0)),
            str(r.get("player_id", "")).strip(),
            str(safe_int(r.get("ranking_position", 0), 0)),
            "active",
            nowb,
            nowb,
        ])

    if rows_to_add:
        ws_participants.append_rows(
            rows_to_add,
            value_input_option="USER_ENTERED"
        )

    cache_invalidate(ws_participants)
    try:
        invalidate_final_participants_ram_index()
    except Exception:
        pass


def get_final_participants_rows(ws_participants, season_id: int) -> list[dict]:
    rows = cached_get_all_records(ws_participants, ttl_seconds=10)

    out = []

    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        out.append({
            "season_id": season_id,
            "seed": safe_int(r.get("seed", 0), 0),
            "player_id": str(r.get("player_id", "")).strip(),
            "ranking_position": safe_int(r.get("ranking_position", 0), 0),
            "status": str(r.get("status", "")).strip().lower(),
            "created_at": str(r.get("created_at", "")).strip(),
            "updated_at": str(r.get("updated_at", "")).strip(),
        })

    out.sort(key=lambda x: safe_int(x.get("seed", 0), 999999))
    return out


# =========================================================
# HELPERS — MATCHES DA FASE FINAL (LEITURA BASE)
# =========================================================

def get_final_matches_rows(ws_matches, season_id: int) -> list[dict]:
    rows = cached_get_all_records(ws_matches, ttl_seconds=10)

    out = []

    for r in rows:
        if safe_int(r.get("season_id", 0), 0) != season_id:
            continue

        out.append({
            "final_match_id": str(r.get("final_match_id", "")).strip(),
            "season_id": safe_int(r.get("season_id", 0), 0),
            "bracket": str(r.get("bracket", "")).strip().lower(),
            "round": safe_int(r.get("round", 0), 0),
            "match_order": safe_int(r.get("match_order", 0), 0),
            "player_a_id": str(r.get("player_a_id", "")).strip(),
            "player_b_id": str(r.get("player_b_id", "")).strip(),
            "a_games_won": safe_int(r.get("a_games_won", 0), 0),
            "b_games_won": safe_int(r.get("b_games_won", 0), 0),
            "result_type": str(r.get("result_type", "")).strip().lower(),
            "status": str(r.get("status", "")).strip().lower(),
            "winner_id": str(r.get("winner_id", "")).strip(),
            "loser_id": str(r.get("loser_id", "")).strip(),
            "next_win_match_id": str(r.get("next_win_match_id", "")).strip(),
            "next_win_slot": str(r.get("next_win_slot", "")).strip(),
            "next_lose_match_id": str(r.get("next_lose_match_id", "")).strip(),
            "next_lose_slot": str(r.get("next_lose_slot", "")).strip(),
            "is_reset_match": as_bool(r.get("is_reset_match", "FALSE")),
            "created_at": str(r.get("created_at", "")).strip(),
            "updated_at": str(r.get("updated_at", "")).strip(),
        })

    out.sort(
        key=lambda x: (
            str(x.get("bracket", "")).lower(),
            safe_int(x.get("round", 0), 0),
            safe_int(x.get("match_order", 0), 0),
            str(x.get("final_match_id", "")).lower(),
        )
    )

    return out


# =================================================
# FIM DO BLOCO 12/22
# =================================================


# =================================================
# BLOCO ORIGINAL: BLOCO 13/22
# SUB-BLOCO: ÚNICO
# RESUMO: Índices RAM da Fase Final (FinalStage, FinalParticipants,
# FinalMatches e FinalDecks), com helpers *_fast, invalidação consistente
# e leitura otimizada para os próximos comandos e geração do chaveamento.
# REVISÃO DE PERFORMANCE:
# - early return mais agressivo
# - leitura e sort mais leves
# - rebuild concorrente protegido sem bloquear interação além do necessário
# - autocomplete com filtro curto e saída rápida
# - sem alterar a lógica funcional
# =================================================

# =========================================================
# HEADERS — FINAL DECKS
# =========================================================

FINAL_DECKS_HEADER = [
    "season_id",
    "player_id",
    "deck",
    "decklist_url",
    "created_at",
    "updated_at",
]

FINAL_DECKS_REQUIRED = [
    "season_id",
    "player_id",
    "deck",
    "decklist_url",
]


# =========================================================
# ENSURE — FINAL DECKS
# =========================================================

def ensure_final_decks_sheet(sh):
    ws = ensure_worksheet(
        sh,
        "FinalDecks",
        FINAL_DECKS_HEADER,
        rows=500,
        cols=20
    )
    ensure_sheet_columns(ws, FINAL_DECKS_REQUIRED)
    return ws


# =========================================================
# LOCKS GLOBAIS — FASE FINAL
# =========================================================

_FINAL_STAGE_RAM_INDEX = {
    "ts": 0.0,
    "by_season": {},
}
_FINAL_STAGE_RAM_INDEX_LOCK = threading.Lock()
_FINAL_STAGE_RAM_INDEX_BUILD_LOCK = threading.Lock()
_FINAL_STAGE_RAM_INDEX_TTL_SECONDS = 60

_FINAL_PARTICIPANTS_RAM_INDEX = {
    "ts": 0.0,
    "by_season": {},
    "by_player": {},
}
_FINAL_PARTICIPANTS_RAM_INDEX_LOCK = threading.Lock()
_FINAL_PARTICIPANTS_RAM_INDEX_BUILD_LOCK = threading.Lock()
_FINAL_PARTICIPANTS_RAM_INDEX_TTL_SECONDS = 60

_FINAL_MATCHES_RAM_INDEX = {
    "ts": 0.0,
    "by_season": {},
    "by_match_id": {},
    "by_player": {},
}
_FINAL_MATCHES_RAM_INDEX_LOCK = threading.Lock()
_FINAL_MATCHES_RAM_INDEX_BUILD_LOCK = threading.Lock()
_FINAL_MATCHES_RAM_INDEX_TTL_SECONDS = 60

_FINAL_DECKS_RAM_INDEX = {
    "ts": 0.0,
    "by_season": {},
    "by_player": {},
}
_FINAL_DECKS_RAM_INDEX_LOCK = threading.Lock()
_FINAL_DECKS_RAM_INDEX_BUILD_LOCK = threading.Lock()
_FINAL_DECKS_RAM_INDEX_TTL_SECONDS = 60


# =========================================================
# INVALIDATE — FASE FINAL
# =========================================================

def invalidate_final_stage_ram_index():
    with _FINAL_STAGE_RAM_INDEX_LOCK:
        _FINAL_STAGE_RAM_INDEX["ts"] = 0.0
        _FINAL_STAGE_RAM_INDEX["by_season"] = {}


def invalidate_final_participants_ram_index():
    with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
        _FINAL_PARTICIPANTS_RAM_INDEX["ts"] = 0.0
        _FINAL_PARTICIPANTS_RAM_INDEX["by_season"] = {}
        _FINAL_PARTICIPANTS_RAM_INDEX["by_player"] = {}


def invalidate_final_matches_ram_index():
    with _FINAL_MATCHES_RAM_INDEX_LOCK:
        _FINAL_MATCHES_RAM_INDEX["ts"] = 0.0
        _FINAL_MATCHES_RAM_INDEX["by_season"] = {}
        _FINAL_MATCHES_RAM_INDEX["by_match_id"] = {}
        _FINAL_MATCHES_RAM_INDEX["by_player"] = {}


def invalidate_final_decks_ram_index():
    with _FINAL_DECKS_RAM_INDEX_LOCK:
        _FINAL_DECKS_RAM_INDEX["ts"] = 0.0
        _FINAL_DECKS_RAM_INDEX["by_season"] = {}
        _FINAL_DECKS_RAM_INDEX["by_player"] = {}


def invalidate_all_final_ram_indexes():
    invalidate_final_stage_ram_index()
    invalidate_final_participants_ram_index()
    invalidate_final_matches_ram_index()
    invalidate_final_decks_ram_index()


# =========================================================
# NORMALIZADORES — FASE FINAL
# =========================================================

def _normalize_final_stage_row(raw: dict) -> dict:
    return {
        "season_id": safe_int(raw.get("season_id", 0), 0),
        "status": str(raw.get("status", "")).strip().lower(),
        "top_size": safe_int(raw.get("top_size", 0), 0),
        "format": str(raw.get("format", "")).strip(),
        "created_at": str(raw.get("created_at", "")).strip(),
        "updated_at": str(raw.get("updated_at", "")).strip(),
    }


def _normalize_final_participant_row(raw: dict) -> dict:
    return {
        "season_id": safe_int(raw.get("season_id", 0), 0),
        "seed": safe_int(raw.get("seed", 0), 0),
        "player_id": str(raw.get("player_id", "")).strip(),
        "ranking_position": safe_int(raw.get("ranking_position", 0), 0),
        "status": str(raw.get("status", "")).strip().lower(),
        "created_at": str(raw.get("created_at", "")).strip(),
        "updated_at": str(raw.get("updated_at", "")).strip(),
    }


def _normalize_final_match_row(raw: dict) -> dict:
    return {
        "final_match_id": str(raw.get("final_match_id", "")).strip(),
        "season_id": safe_int(raw.get("season_id", 0), 0),
        "bracket": str(raw.get("bracket", "")).strip().lower(),
        "round": safe_int(raw.get("round", 0), 0),
        "match_order": safe_int(raw.get("match_order", 0), 0),
        "player_a_id": str(raw.get("player_a_id", "")).strip(),
        "player_b_id": str(raw.get("player_b_id", "")).strip(),
        "a_games_won": safe_int(raw.get("a_games_won", 0), 0),
        "b_games_won": safe_int(raw.get("b_games_won", 0), 0),
        "result_type": str(raw.get("result_type", "")).strip().lower(),
        "status": str(raw.get("status", "")).strip().lower(),
        "winner_id": str(raw.get("winner_id", "")).strip(),
        "loser_id": str(raw.get("loser_id", "")).strip(),
        "next_win_match_id": str(raw.get("next_win_match_id", "")).strip(),
        "next_win_slot": str(raw.get("next_win_slot", "")).strip(),
        "next_lose_match_id": str(raw.get("next_lose_match_id", "")).strip(),
        "next_lose_slot": str(raw.get("next_lose_slot", "")).strip(),
        "is_reset_match": as_bool(raw.get("is_reset_match", "FALSE")),
        "created_at": str(raw.get("created_at", "")).strip(),
        "updated_at": str(raw.get("updated_at", "")).strip(),
    }


def _normalize_final_deck_row(raw: dict) -> dict:
    return {
        "season_id": safe_int(raw.get("season_id", 0), 0),
        "player_id": str(raw.get("player_id", "")).strip(),
        "deck": str(raw.get("deck", "")).strip(),
        "decklist_url": str(raw.get("decklist_url", "")).strip(),
        "created_at": str(raw.get("created_at", "")).strip(),
        "updated_at": str(raw.get("updated_at", "")).strip(),
    }


def _final_match_sort_key(r: dict):
    bracket_order = {
        "winners": 1,
        "losers": 2,
        "grand_final": 3,
    }
    return (
        bracket_order.get(str(r.get("bracket", "")).strip().lower(), 999),
        safe_int(r.get("round", 0), 0),
        safe_int(r.get("match_order", 0), 0),
        str(r.get("final_match_id", "")).lower(),
    )


def _final_participant_sort_key(r: dict):
    return (
        safe_int(r.get("seed", 0), 999999),
        safe_int(r.get("ranking_position", 0), 999999),
        str(r.get("player_id", "")).lower(),
    )


# =========================================================
# BUILD — FINAL STAGE RAM INDEX
# =========================================================

def _build_final_stage_ram_index(sh) -> dict:
    ws_stage, _, _ = ensure_final_sheets(sh)
    rows = cached_get_all_records(ws_stage, ttl_seconds=10)

    by_season = {}

    for raw in rows:
        r = _normalize_final_stage_row(raw)
        sid = r["season_id"]
        if sid <= 0:
            continue
        by_season[sid] = r

    return {
        "ts": _cache_now(),
        "by_season": by_season,
    }


def ensure_final_stage_ram_index(sh, max_age_seconds: int = _FINAL_STAGE_RAM_INDEX_TTL_SECONDS):
    now = _cache_now()

    with _FINAL_STAGE_RAM_INDEX_LOCK:
        ts = float(_FINAL_STAGE_RAM_INDEX.get("ts", 0.0) or 0.0)
        if ts > 0 and (now - ts) <= max_age_seconds:
            return

    acquired = _FINAL_STAGE_RAM_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _FINAL_STAGE_RAM_INDEX_LOCK:
            ts = float(_FINAL_STAGE_RAM_INDEX.get("ts", 0.0) or 0.0)
            if ts > 0 and (now - ts) <= max_age_seconds:
                return

        built = _build_final_stage_ram_index(sh)

        with _FINAL_STAGE_RAM_INDEX_LOCK:
            _FINAL_STAGE_RAM_INDEX["ts"] = built["ts"]
            _FINAL_STAGE_RAM_INDEX["by_season"] = built["by_season"]
    finally:
        _FINAL_STAGE_RAM_INDEX_BUILD_LOCK.release()


def get_final_stage_fast(sh, season_id: int) -> dict | None:
    sid = safe_int(season_id, 0)
    if sid <= 0:
        return None

    ensure_final_stage_ram_index(sh)

    with _FINAL_STAGE_RAM_INDEX_LOCK:
        row = _FINAL_STAGE_RAM_INDEX.get("by_season", {}).get(sid)
        return dict(row) if row else None


def get_final_stage_ram_index_snapshot() -> dict:
    with _FINAL_STAGE_RAM_INDEX_LOCK:
        return {
            "ts": _FINAL_STAGE_RAM_INDEX.get("ts", 0.0),
            "seasons_indexed": len(_FINAL_STAGE_RAM_INDEX.get("by_season", {})),
        }


# =========================================================
# BUILD — FINAL PARTICIPANTS RAM INDEX
# =========================================================

def _build_final_participants_ram_index(sh) -> dict:
    _, ws_participants, _ = ensure_final_sheets(sh)
    rows = cached_get_all_records(ws_participants, ttl_seconds=10)

    by_season = {}
    by_player = {}

    for raw in rows:
        r = _normalize_final_participant_row(raw)

        sid = r["season_id"]
        pid = r["player_id"]

        if sid <= 0 or not pid:
            continue

        by_season.setdefault(sid, []).append(r)
        by_player[(sid, pid)] = r

    for sid, items in by_season.items():
        items.sort(key=_final_participant_sort_key)

    return {
        "ts": _cache_now(),
        "by_season": by_season,
        "by_player": by_player,
    }


def ensure_final_participants_ram_index(sh, max_age_seconds: int = _FINAL_PARTICIPANTS_RAM_INDEX_TTL_SECONDS):
    now = _cache_now()

    with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
        ts = float(_FINAL_PARTICIPANTS_RAM_INDEX.get("ts", 0.0) or 0.0)
        if ts > 0 and (now - ts) <= max_age_seconds:
            return

    acquired = _FINAL_PARTICIPANTS_RAM_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
            ts = float(_FINAL_PARTICIPANTS_RAM_INDEX.get("ts", 0.0) or 0.0)
            if ts > 0 and (now - ts) <= max_age_seconds:
                return

        built = _build_final_participants_ram_index(sh)

        with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
            _FINAL_PARTICIPANTS_RAM_INDEX["ts"] = built["ts"]
            _FINAL_PARTICIPANTS_RAM_INDEX["by_season"] = built["by_season"]
            _FINAL_PARTICIPANTS_RAM_INDEX["by_player"] = built["by_player"]
    finally:
        _FINAL_PARTICIPANTS_RAM_INDEX_BUILD_LOCK.release()


def get_final_participants_fast(sh, season_id: int) -> list[dict]:
    sid = safe_int(season_id, 0)
    if sid <= 0:
        return []

    ensure_final_participants_ram_index(sh)

    with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
        rows = list(_FINAL_PARTICIPANTS_RAM_INDEX.get("by_season", {}).get(sid, []))

    return [dict(r) for r in rows]


def get_final_participant_by_player_fast(sh, season_id: int, player_id: str) -> dict | None:
    sid = safe_int(season_id, 0)
    pid = str(player_id or "").strip()

    if sid <= 0 or not pid:
        return None

    ensure_final_participants_ram_index(sh)

    with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
        row = _FINAL_PARTICIPANTS_RAM_INDEX.get("by_player", {}).get((sid, pid))
        return dict(row) if row else None


def get_final_participants_ram_index_snapshot() -> dict:
    with _FINAL_PARTICIPANTS_RAM_INDEX_LOCK:
        by_season = _FINAL_PARTICIPANTS_RAM_INDEX.get("by_season", {})
        total = sum(len(v) for v in by_season.values())
        return {
            "ts": _FINAL_PARTICIPANTS_RAM_INDEX.get("ts", 0.0),
            "seasons_indexed": len(by_season),
            "participants_indexed": total,
        }


# =========================================================
# BUILD — FINAL MATCHES RAM INDEX
# =========================================================

def _build_final_matches_ram_index(sh) -> dict:
    _, _, ws_matches = ensure_final_sheets(sh)
    rows = cached_get_all_records(ws_matches, ttl_seconds=10)

    by_season = {}
    by_match_id = {}
    by_player = {}

    for raw in rows:
        r = _normalize_final_match_row(raw)

        sid = r["season_id"]
        mid = r["final_match_id"]

        if sid <= 0 or not mid:
            continue

        by_season.setdefault(sid, []).append(r)
        by_match_id[mid] = r

        a = r["player_a_id"]
        b = r["player_b_id"]

        if a:
            by_player.setdefault((sid, a), []).append(r)
        if b:
            by_player.setdefault((sid, b), []).append(r)

    for sid, items in by_season.items():
        items.sort(key=_final_match_sort_key)

    for key, items in by_player.items():
        items.sort(key=_final_match_sort_key)

    return {
        "ts": _cache_now(),
        "by_season": by_season,
        "by_match_id": by_match_id,
        "by_player": by_player,
    }


def ensure_final_matches_ram_index(sh, max_age_seconds: int = _FINAL_MATCHES_RAM_INDEX_TTL_SECONDS):
    now = _cache_now()

    with _FINAL_MATCHES_RAM_INDEX_LOCK:
        ts = float(_FINAL_MATCHES_RAM_INDEX.get("ts", 0.0) or 0.0)
        if ts > 0 and (now - ts) <= max_age_seconds:
            return

    acquired = _FINAL_MATCHES_RAM_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _FINAL_MATCHES_RAM_INDEX_LOCK:
            ts = float(_FINAL_MATCHES_RAM_INDEX.get("ts", 0.0) or 0.0)
            if ts > 0 and (now - ts) <= max_age_seconds:
                return

        built = _build_final_matches_ram_index(sh)

        with _FINAL_MATCHES_RAM_INDEX_LOCK:
            _FINAL_MATCHES_RAM_INDEX["ts"] = built["ts"]
            _FINAL_MATCHES_RAM_INDEX["by_season"] = built["by_season"]
            _FINAL_MATCHES_RAM_INDEX["by_match_id"] = built["by_match_id"]
            _FINAL_MATCHES_RAM_INDEX["by_player"] = built["by_player"]
    finally:
        _FINAL_MATCHES_RAM_INDEX_BUILD_LOCK.release()


def get_final_matches_fast(sh, season_id: int) -> list[dict]:
    sid = safe_int(season_id, 0)
    if sid <= 0:
        return []

    ensure_final_matches_ram_index(sh)

    with _FINAL_MATCHES_RAM_INDEX_LOCK:
        rows = list(_FINAL_MATCHES_RAM_INDEX.get("by_season", {}).get(sid, []))

    return [dict(r) for r in rows]


def get_final_match_by_id_fast(sh, final_match_id: str) -> dict | None:
    mid = str(final_match_id or "").strip()
    if not mid:
        return None

    ensure_final_matches_ram_index(sh)

    with _FINAL_MATCHES_RAM_INDEX_LOCK:
        row = _FINAL_MATCHES_RAM_INDEX.get("by_match_id", {}).get(mid)
        return dict(row) if row else None


def get_final_matches_for_player_fast(sh, season_id: int, player_id: str) -> list[dict]:
    sid = safe_int(season_id, 0)
    pid = str(player_id or "").strip()

    if sid <= 0 or not pid:
        return []

    ensure_final_matches_ram_index(sh)

    with _FINAL_MATCHES_RAM_INDEX_LOCK:
        rows = list(_FINAL_MATCHES_RAM_INDEX.get("by_player", {}).get((sid, pid), []))

    return [dict(r) for r in rows]


def get_final_matches_ram_index_snapshot() -> dict:
    with _FINAL_MATCHES_RAM_INDEX_LOCK:
        by_season = _FINAL_MATCHES_RAM_INDEX.get("by_season", {})
        total = sum(len(v) for v in by_season.values())
        return {
            "ts": _FINAL_MATCHES_RAM_INDEX.get("ts", 0.0),
            "seasons_indexed": len(by_season),
            "matches_indexed": total,
            "match_ids_indexed": len(_FINAL_MATCHES_RAM_INDEX.get("by_match_id", {})),
        }


# =========================================================
# BUILD — FINAL DECKS RAM INDEX
# =========================================================

def _build_final_decks_ram_index(sh) -> dict:
    ws_decks = ensure_final_decks_sheet(sh)
    rows = cached_get_all_records(ws_decks, ttl_seconds=10)

    by_season = {}
    by_player = {}

    for raw in rows:
        r = _normalize_final_deck_row(raw)

        sid = r["season_id"]
        pid = r["player_id"]

        if sid <= 0 or not pid:
            continue

        by_season.setdefault(sid, []).append(r)
        by_player[(sid, pid)] = r

    for sid, items in by_season.items():
        items.sort(
            key=lambda x: (
                str(x.get("deck", "")).lower(),
                str(x.get("player_id", "")).lower(),
            )
        )

    return {
        "ts": _cache_now(),
        "by_season": by_season,
        "by_player": by_player,
    }


def ensure_final_decks_ram_index(sh, max_age_seconds: int = _FINAL_DECKS_RAM_INDEX_TTL_SECONDS):
    now = _cache_now()

    with _FINAL_DECKS_RAM_INDEX_LOCK:
        ts = float(_FINAL_DECKS_RAM_INDEX.get("ts", 0.0) or 0.0)
        if ts > 0 and (now - ts) <= max_age_seconds:
            return

    acquired = _FINAL_DECKS_RAM_INDEX_BUILD_LOCK.acquire(blocking=False)
    if not acquired:
        return

    try:
        now = _cache_now()

        with _FINAL_DECKS_RAM_INDEX_LOCK:
            ts = float(_FINAL_DECKS_RAM_INDEX.get("ts", 0.0) or 0.0)
            if ts > 0 and (now - ts) <= max_age_seconds:
                return

        built = _build_final_decks_ram_index(sh)

        with _FINAL_DECKS_RAM_INDEX_LOCK:
            _FINAL_DECKS_RAM_INDEX["ts"] = built["ts"]
            _FINAL_DECKS_RAM_INDEX["by_season"] = built["by_season"]
            _FINAL_DECKS_RAM_INDEX["by_player"] = built["by_player"]
    finally:
        _FINAL_DECKS_RAM_INDEX_BUILD_LOCK.release()


def get_final_decks_fast(sh, season_id: int) -> list[dict]:
    sid = safe_int(season_id, 0)
    if sid <= 0:
        return []

    ensure_final_decks_ram_index(sh)

    with _FINAL_DECKS_RAM_INDEX_LOCK:
        rows = list(_FINAL_DECKS_RAM_INDEX.get("by_season", {}).get(sid, []))

    return [dict(r) for r in rows]


def get_final_deck_by_player_fast(sh, season_id: int, player_id: str) -> dict | None:
    sid = safe_int(season_id, 0)
    pid = str(player_id or "").strip()

    if sid <= 0 or not pid:
        return None

    ensure_final_decks_ram_index(sh)

    with _FINAL_DECKS_RAM_INDEX_LOCK:
        row = _FINAL_DECKS_RAM_INDEX.get("by_player", {}).get((sid, pid))
        return dict(row) if row else None


def get_final_decks_ram_index_snapshot() -> dict:
    with _FINAL_DECKS_RAM_INDEX_LOCK:
        by_season = _FINAL_DECKS_RAM_INDEX.get("by_season", {})
        total = sum(len(v) for v in by_season.values())
        return {
            "ts": _FINAL_DECKS_RAM_INDEX.get("ts", 0.0),
            "seasons_indexed": len(by_season),
            "decks_indexed": total,
        }


# =========================================================
# HELPERS RÁPIDOS — ELEGIBILIDADE / DISPONIBILIDADE
# =========================================================

def get_final_eligible_players_fast(sh, season_id: int) -> list[dict]:
    """
    Reaproveita o helper ANTERIOR e retorna os elegíveis pelo ranking geral.
    """
    qualified, _top_size = get_final_qualified_players(sh, season_id)
    return [dict(r) for r in qualified]


def get_next_final_eligible_players_fast(sh, season_id: int, already_selected_ids: list[str] | set[str]) -> list[dict]:
    """
    Retorna a fila completa do ranking geral removendo os já selecionados.
    Útil para substituir classificados que não poderão participar.
    """
    ranking_rows = _final_read_ranking_geral_rows(sh, season_id)
    selected = {str(x).strip() for x in list(already_selected_ids or [])}

    out = []
    for row in ranking_rows:
        pid = str(row.get("player_id", "")).strip()
        if not pid or pid in selected:
            continue

        out.append({
            "player_id": pid,
            "ranking_position": safe_int(row.get("ranking_position", 0), 0),
            "score": sheet_float(row.get("score", 0), 0.0),
            "pts": sheet_float(row.get("pts", 0), 0.0),
            "ppm": sheet_float(row.get("ppm", 0), 0.0),
            "mwp": sheet_float(row.get("mwp", 0), 0.0),
            "omw": sheet_float(row.get("omw", 0), 0.0),
            "gw": sheet_float(row.get("gw", 0), 0.0),
            "ogw": sheet_float(row.get("ogw", 0), 0.0),
            "matches": safe_int(row.get("matches", 0), 0),
        })

    return out


# =========================================================
# AUTOCOMPLETE — JOGADORES DA FASE FINAL
# =========================================================

async def ac_final_player(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_final_player"):
            return []

        season_selected = safe_int(getattr(interaction.namespace, "season", 0), 0)
        if season_selected <= 0:
            return []

        q = str(current or "").strip().lower()
        sh = open_sheet()
        nick_map = get_player_nick_map_fast(sh)
        items = get_final_participants_fast(sh, season_selected)

        if not items:
            return []

        out: list[app_commands.Choice[str]] = []

        for item in items:
            pid = str(item.get("player_id", "")).strip()
            if not pid:
                continue

            nick = nick_map.get(pid, pid)
            seed = safe_int(item.get("seed", 0), 0)
            label = f"Seed {seed} | {nick}"

            if q:
                search = f"{pid} {nick} {label}".lower()
                if q not in search:
                    continue

            out.append(app_commands.Choice(name=label[:100], value=pid))
            if len(out) >= 25:
                break

        return out[:25]

    except Exception:
        return []


async def ac_final_player_any(interaction: discord.Interaction, current: str):
    """
    Autocomplete amplo de jogador para /cadastrar_final.
    Usa o índice RAM geral de Players.
    """
    try:
        if _ac_should_skip(interaction, "ac_final_player_any"):
            return []

        sh = open_sheet()
        q = str(current or "").strip().lower()
        items = get_player_choices_fast(sh, query=q, limit=25)

        if not items:
            return []

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


# =================================================
# FIM DO BLOCO 13/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 14/22
# SUB-BLOCO: ÚNICO
# REVISÃO: Fase Final convertida para MD5 mata-mata simples.
# Regras:
# - perdeu, está eliminado
# - sem losers bracket
# - sem grand final reset
# - progressão apenas do vencedor
# Compatibilidade:
# - mantém o header atual de FinalMatches
# - campos next_lose_match_id, next_lose_slot e is_reset_match
#   permanecem na planilha, mas não são utilizados nesta revisão
# REVISÃO DE PERFORMANCE:
# - redução de trabalho redundante na geração do bracket
# - menos cópias/loops desnecessários
# - append em lote preservado
# - sem alterar regra funcional
# =========================================================


# =========================================================
# HELPERS — IDS E MATCHES DA FASE FINAL
# =========================================================

def _final_match_id(season_id: int, bracket: str, round_num: int, order: int) -> str:
    return f"FS{season_id}-{str(bracket).upper()}-R{round_num}-M{order}"


def _build_final_match_row_dict(
    season_id: int,
    bracket: str,
    round_num: int,
    match_order: int,
    player_a_id: str = "",
    player_b_id: str = "",
    next_win_match_id: str = "",
    next_win_slot: str = "",
    next_lose_match_id: str = "",
    next_lose_slot: str = "",
    is_reset_match: bool = False,
) -> dict:
    nowu = now_iso_utc()

    return {
        "final_match_id": _final_match_id(season_id, bracket, round_num, match_order),
        "season_id": season_id,
        "bracket": str(bracket).strip().lower(),
        "round": round_num,
        "match_order": match_order,
        "player_a_id": str(player_a_id or "").strip(),
        "player_b_id": str(player_b_id or "").strip(),
        "a_games_won": 0,
        "b_games_won": 0,
        "result_type": "final",
        "status": "open",
        "winner_id": "",
        "loser_id": "",
        "next_win_match_id": str(next_win_match_id or "").strip(),
        "next_win_slot": str(next_win_slot or "").strip(),
        "next_lose_match_id": str(next_lose_match_id or "").strip(),
        "next_lose_slot": str(next_lose_slot or "").strip(),
        "is_reset_match": bool(is_reset_match),
        "created_at": nowu,
        "updated_at": nowu,
    }


def _final_match_row_dict_to_sheet_row(r: dict) -> list:
    return [
        str(r.get("final_match_id", "")).strip(),
        str(safe_int(r.get("season_id", 0), 0)),
        str(r.get("bracket", "")).strip().lower(),
        str(safe_int(r.get("round", 0), 0)),
        str(safe_int(r.get("match_order", 0), 0)),
        str(r.get("player_a_id", "")).strip(),
        str(r.get("player_b_id", "")).strip(),
        str(safe_int(r.get("a_games_won", 0), 0)),
        str(safe_int(r.get("b_games_won", 0), 0)),
        str(r.get("result_type", "")).strip().lower(),
        str(r.get("status", "")).strip().lower(),
        str(r.get("winner_id", "")).strip(),
        str(r.get("loser_id", "")).strip(),
        str(r.get("next_win_match_id", "")).strip(),
        str(r.get("next_win_slot", "")).strip(),
        str(r.get("next_lose_match_id", "")).strip(),
        str(r.get("next_lose_slot", "")).strip(),
        "TRUE" if as_bool(r.get("is_reset_match", False)) else "FALSE",
        str(r.get("created_at", "")).strip(),
        str(r.get("updated_at", "")).strip(),
    ]


# =========================================================
# HELPERS — SEEDING
# =========================================================

def _build_seed_pairings(top_size: int) -> list[tuple[int, int]]:
    """
    Pareamento inicial oficial:
    2  -> 1x2
    4  -> 1x4 / 2x3
    8  -> 1x8 / 4x5 / 2x7 / 3x6
    16 -> 1x16 / 8x9 / 4x13 / 5x12 / 2x15 / 7x10 / 3x14 / 6x11
    """
    if top_size == 2:
        return [(1, 2)]
    if top_size == 4:
        return [(1, 4), (2, 3)]
    if top_size == 8:
        return [(1, 8), (4, 5), (2, 7), (3, 6)]
    if top_size == 16:
        return [
            (1, 16), (8, 9), (4, 13), (5, 12),
            (2, 15), (7, 10), (3, 14), (6, 11),
        ]
    return []


# =========================================================
# HELPERS — GERAÇÃO DE BRACKET SIMPLES
# =========================================================

def _build_single_elimination_round(
    season_id: int,
    round_num: int,
    round_pairs: list[tuple[str, str]],
    next_round_match_ids: list[str] | None = None,
    bracket_name: str = "winners",
) -> list[dict]:
    """
    Gera uma rodada do mata-mata simples.
    Cada match aponta apenas para a próxima rodada via next_win_match_id/slot.
    """
    rows: list[dict] = []
    next_ids = next_round_match_ids or []

    for i, pair in enumerate(round_pairs, start=1):
        player_a_id = str(pair[0] or "").strip()
        player_b_id = str(pair[1] or "").strip()

        next_win_match_id = ""
        next_win_slot = ""

        if next_ids:
            next_idx = (i - 1) // 2
            if next_idx < len(next_ids):
                next_win_match_id = next_ids[next_idx]
                next_win_slot = "A" if (i % 2 == 1) else "B"

        rows.append(
            _build_final_match_row_dict(
                season_id=season_id,
                bracket=bracket_name,
                round_num=round_num,
                match_order=i,
                player_a_id=player_a_id,
                player_b_id=player_b_id,
                next_win_match_id=next_win_match_id,
                next_win_slot=next_win_slot,
                next_lose_match_id="",
                next_lose_slot="",
                is_reset_match=False,
            )
        )

    return rows


def _build_empty_match_ids_for_round(
    season_id: int,
    round_num: int,
    match_count: int,
    bracket_name: str = "winners"
) -> list[str]:
    if match_count <= 0:
        return []
    return [
        _final_match_id(season_id, bracket_name, round_num, i)
        for i in range(1, match_count + 1)
    ]


def _generate_single_elimination_bracket(season_id: int, participants: list[dict]) -> list[dict]:
    """
    Gera o bracket completo de mata-mata simples.

    TOP 2:
    - R1 final

    TOP 4:
    - R1 semifinal
    - R2 final

    TOP 8:
    - R1 quartas
    - R2 semifinal
    - R3 final

    TOP 16:
    - R1 oitavas
    - R2 quartas
    - R3 semifinal
    - R4 final
    """
    if not participants:
        raise RuntimeError("Participantes inválidos para geração da fase final.")

    items = sorted(
        participants,
        key=lambda x: safe_int(x.get("seed", 0), 999999)
    )

    top_size = len(items)
    if top_size not in (2, 4, 8, 16):
        raise RuntimeError("Top size inválido para geração da fase final.")

    seed_map = {}
    for p in items:
        seed = safe_int(p.get("seed", 0), 0)
        pid = str(p.get("player_id", "")).strip()
        if seed > 0:
            seed_map[seed] = pid

    initial_seed_pairs = _build_seed_pairings(top_size)
    if not initial_seed_pairs:
        raise RuntimeError("Não foi possível montar os pareamentos iniciais da fase final.")

    round_1_pairs = [
        (seed_map.get(s1, ""), seed_map.get(s2, ""))
        for s1, s2 in initial_seed_pairs
    ]

    round_match_counts: list[int] = []
    current = top_size // 2
    while current >= 1:
        round_match_counts.append(current)
        if current == 1:
            break
        current //= 2

    round_to_ids: dict[int, list[str]] = {
        round_num: _build_empty_match_ids_for_round(
            season_id=season_id,
            round_num=round_num,
            match_count=match_count,
            bracket_name="winners"
        )
        for round_num, match_count in enumerate(round_match_counts, start=1)
    }

    rows: list[dict] = []

    rows.extend(
        _build_single_elimination_round(
            season_id=season_id,
            round_num=1,
            round_pairs=round_1_pairs,
            next_round_match_ids=round_to_ids.get(2, []),
            bracket_name="winners"
        )
    )

    for round_num in range(2, len(round_match_counts) + 1):
        current_match_count = round_match_counts[round_num - 1]
        if current_match_count <= 0:
            continue

        rows.extend(
            _build_single_elimination_round(
                season_id=season_id,
                round_num=round_num,
                round_pairs=[("", "")] * current_match_count,
                next_round_match_ids=round_to_ids.get(round_num + 1, []),
                bracket_name="winners"
            )
        )

    return rows


# =========================================================
# MASTER — GERAÇÃO COMPLETA
# =========================================================

def build_final_bracket_rows(season_id: int, participants: list[dict]) -> list[dict]:
    if not participants:
        raise RuntimeError("Participantes inválidos para geração da fase final.")

    items = sorted(
        participants,
        key=lambda x: safe_int(x.get("seed", 0), 999999)
    )

    top_size = len(items)
    if top_size not in (2, 4, 8, 16):
        raise RuntimeError("Top size inválido para geração da fase final.")

    return _generate_single_elimination_bracket(season_id, items)


def generate_final_bracket(sh, season_id: int):
    participants = get_final_participants_fast(sh, season_id)

    if not participants:
        raise RuntimeError("Nenhum participante na fase final.")

    top_size = len(participants)
    if top_size not in (2, 4, 8, 16):
        raise RuntimeError("Tamanho inválido da fase final.")

    ws_matches = ensure_worksheet(
        sh,
        "FinalMatches",
        FINAL_MATCHES_HEADER,
        rows=5000,
        cols=30
    )
    ensure_sheet_columns(ws_matches, FINAL_MATCHES_REQUIRED)

    rows_dict = build_final_bracket_rows(season_id, participants)
    if not rows_dict:
        return 0

    rows_sheet = [_final_match_row_dict_to_sheet_row(r) for r in rows_dict]

    ws_matches.append_rows(rows_sheet, value_input_option="USER_ENTERED")
    cache_invalidate(ws_matches)
    invalidate_final_matches_ram_index()

    return len(rows_sheet)


def get_final_bracket_summary(sh, season_id: int) -> dict:
    rows = get_final_matches_fast(sh, season_id)

    winners = 0
    losers = 0
    grand_final = 0

    for r in rows:
        bracket = str(r.get("bracket", "")).strip().lower()
        if bracket == "winners":
            winners += 1
        elif bracket == "losers":
            losers += 1
        elif bracket == "grand_final":
            grand_final += 1

    return {
        "total_matches": len(rows),
        "winners": winners,
        "losers": losers,
        "grand_final": grand_final,
    }


# =================================================
# FIM DO BLOCO 14/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 15/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================


# =========================================================
# HELPERS — FINAL DECKS
# =========================================================

def get_final_deck_row(ws_final_decks, season_id: int, player_id: str) -> int | None:
    start = time.perf_counter()

    rows = cached_get_all_values(ws_final_decks, ttl_seconds=10)

    if len(rows) <= 1:
        return None

    col = ensure_sheet_columns(ws_final_decks, FINAL_DECKS_REQUIRED)
    pid = str(player_id or "").strip()

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]

        s = safe_int(r[col["season_id"]] if col["season_id"] < len(r) else 0, 0)
        p = str(r[col["player_id"]] if col["player_id"] < len(r) else "").strip()

        if s == season_id and p == pid:
            debug_log("get_final_deck_row", start)
            return i

    debug_log("get_final_deck_row", start)
    return None


def ensure_final_deck_row(ws_final_decks, season_id: int, player_id: str) -> int:
    rown = get_final_deck_row(ws_final_decks, season_id, player_id)
    if rown is not None:
        return rown

    nowb = now_br_str()

    ws_final_decks.append_row(
        [
            str(season_id),
            str(player_id).strip(),
            "",
            "",
            nowb,
            nowb,
        ],
        value_input_option="USER_ENTERED"
    )

    cache_invalidate(ws_final_decks)
    invalidate_final_decks_ram_index()

    vals = cached_get_all_values(ws_final_decks, ttl_seconds=5)
    return len(vals)


def upsert_final_deck(ws_final_decks, season_id: int, player_id: str, deck_name: str, decklist_url: str):
    start = time.perf_counter()

    rown = ensure_final_deck_row(ws_final_decks, season_id, player_id)
    col = ensure_sheet_columns(ws_final_decks, FINAL_DECKS_REQUIRED)
    nowb = now_br_str()

    updates = [
        {
            "range": f"{col_letter(col['deck'])}{rown}",
            "values": [[str(deck_name or "").strip()]]
        },
        {
            "range": f"{col_letter(col['decklist_url'])}{rown}",
            "values": [[str(decklist_url or "").strip()]]
        }
    ]

    header_vals = cached_get_all_values(ws_final_decks, ttl_seconds=10)
    idx = {name: i for i, name in enumerate(header_vals[0] if header_vals else FINAL_DECKS_HEADER)}

    if "updated_at" in idx:
        updates.append({
            "range": f"{col_letter(idx['updated_at'])}{rown}",
            "values": [[nowb]]
        })

    ws_final_decks.batch_update(updates)

    cache_invalidate(ws_final_decks)
    invalidate_final_decks_ram_index()

    debug_log("upsert_final_deck", start)


# =========================================================
# /fase_final (ANTI TIMEOUT)
# =========================================================

@client.tree.command(
    name="fase_final",
    description="(OWNER) Gera a fase final"
)
async def fase_final(interaction: discord.Interaction, season: int):
    async def _run():
        sh = open_sheet()
        ensure_all_sheets(sh)
        ensure_final_sheets(sh)

        if not season_exists(sh, season):
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        if not final_all_cycles_completed(sh, season):
            return await interaction.followup.send("❌ Ciclos não finalizados.", ephemeral=True)

        qualified_rows, top_size = build_final_player_pool(sh, season)
        if not qualified_rows:
            return await interaction.followup.send("❌ Sem classificados.", ephemeral=True)

        ws_stage, ws_participants, ws_matches = ensure_final_sheets(sh)
        ws_final_decks = ensure_final_decks_sheet(sh)

        clear_final_stage_for_season(ws_stage, season)
        clear_final_participants_for_season(ws_participants, season)
        clear_final_matches_for_season(ws_matches, season)
        clear_final_decks_for_season(ws_final_decks, season)

        set_final_stage(ws_stage, season, "generated", top_size, "single_elimination")

        save_final_participants(ws_participants, season, qualified_rows)

        total_matches = generate_final_bracket(sh, season)

        await interaction.followup.send(
            f"✅ Fase final gerada | TOP {top_size} | matches: {total_matches}",
            ephemeral=True
        )

    await safe_interaction_execute(interaction, _run)


# =========================================================
# /cadastrar_final (ANTI TIMEOUT)
# =========================================================

@client.tree.command(name="cadastrar_final", description="(ADM) Define deck final")
async def cadastrar_final(interaction: discord.Interaction, season: int, jogador: str, guilda: str, arquetipo: str, decklist: str):
    async def _run():
        sh = open_sheet()
        ensure_final_sheets(sh)
        ws_final_decks = ensure_final_decks_sheet(sh)

        pid = str(jogador).strip()

        participant = get_final_participant_by_player_fast(sh, season, pid)
        if not participant:
            return await interaction.followup.send("❌ Jogador não classificado.", ephemeral=True)

        ok, decklist_val = validate_decklist_url(decklist)
        if not ok:
            return await interaction.followup.send("❌ Decklist inválida.", ephemeral=True)

        nome_deck = _montar_nome_deck(guilda, arquetipo)

        upsert_final_deck(ws_final_decks, season, pid, nome_deck, decklist_val)

        await interaction.followup.send("✅ Deck atualizado.", ephemeral=True)

    await safe_interaction_execute(interaction, _run)


# =========================================================
# /inscrever_final (ANTI TIMEOUT)
# =========================================================

@client.tree.command(name="inscrever_final", description="Define seu deck final")
async def inscrever_final(interaction: discord.Interaction, guilda: str, arquetipo: str, decklist: str):
    async def _run():
        sh = open_sheet()
        ensure_final_sheets(sh)
        ws_final_decks = ensure_final_decks_sheet(sh)

        stage = get_latest_valid_final_stage(sh)
        if not stage:
            return await interaction.followup.send("❌ Sem fase final ativa.", ephemeral=True)

        season = safe_int(stage.get("season_id", 0), 0)
        pid = str(interaction.user.id)

        participant = get_final_participant_by_player_fast(sh, season, pid)
        if not participant:
            return await interaction.followup.send("❌ Você não está classificado.", ephemeral=True)

        ok, decklist_val = validate_decklist_url(decklist)
        if not ok:
            return await interaction.followup.send("❌ Decklist inválida.", ephemeral=True)

        nome_deck = _montar_nome_deck(guilda, arquetipo)

        upsert_final_deck(ws_final_decks, season, pid, nome_deck, decklist_val)

        await interaction.followup.send("✅ Deck registrado.", ephemeral=True)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 15/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 16/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================

FINAL_MD5_SCORE_OPTIONS = [
    ("3-0-0", "WIN"),
    ("3-1-0", "WIN"),
    ("3-2-0", "WIN"),
    ("0-3-0", "LOSS"),
    ("1-3-0", "LOSS"),
    ("2-3-0", "LOSS"),
]


# =========================================================
# AUTOCOMPLETE OTIMIZADO (ANTI TIMEOUT)
# =========================================================

async def ac_final_match_user_open(interaction: discord.Interaction, current: str):
    start = time.perf_counter()

    try:
        if _ac_should_skip(interaction, "ac_final_match_user_open"):
            return []

        season = safe_int(getattr(interaction.namespace, "season", 0), 0)
        if season <= 0:
            return []

        sh = open_sheet()
        pid = str(interaction.user.id).strip()
        q = str(current or "").strip().lower()

        rows = get_final_matches_for_player_fast(sh, season, pid)
        if not rows:
            return []

        nick_map = get_player_nick_map_fast(sh)

        out = []
        seen = set()

        for r in rows:
            if len(out) >= 25:
                break

            if str(r.get("status", "")).strip().lower() != "open":
                continue

            a = str(r.get("player_a_id", "")).strip()
            b = str(r.get("player_b_id", "")).strip()

            if not a or not b:
                continue

            mid = str(r.get("final_match_id", "")).strip()

            label = _final_match_visual_label(r, nick_map, pid)
            search = f"{mid} {label}".lower()

            if q and q not in search:
                continue

            if mid in seen:
                continue

            out.append(app_commands.Choice(name=label[:100], value=mid))
            seen.add(mid)

        debug_log("ac_final_match_user_open", start, f"| items={len(out)}")
        return out

    except Exception as e:
        print(f"❌ ERRO AUTOCOMPLETE MATCH: {e}")
        return []


async def ac_score_final_md5(interaction: discord.Interaction, current: str):
    try:
        if _ac_should_skip(interaction, "ac_score_final_md5"):
            return []

        q = str(current or "").strip().replace(" ", "")

        out = [
            app_commands.Choice(name=f"{s} ({lbl})", value=s)
            for s, lbl in FINAL_MD5_SCORE_OPTIONS
            if not q or q in s
        ]

        return out[:25]

    except Exception:
        return []


# =========================================================
# /resultado_final (ANTI TIMEOUT + DEBUG)
# =========================================================

@client.tree.command(
    name="resultado_final",
    description="Reporta resultado da fase final"
)
async def resultado_final(interaction: discord.Interaction, season: int, match_id: str, placar: str):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()

        # ===== VALIDAÇÕES RÁPIDAS =====
        if not season_exists(sh, season):
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        stage = get_final_stage_fast(sh, season)
        if not stage:
            return await interaction.followup.send("❌ Fase final não gerada.", ephemeral=True)

        status = str(stage.get("status", "")).lower()

        if status == "completed":
            return await interaction.followup.send("❌ Fase final já encerrada.", ephemeral=True)

        if status != "in_progress":
            return await interaction.followup.send("❌ Fase final não iniciada.", ephemeral=True)

        parsed = parse_final_md5_score(placar)
        if not parsed:
            return await interaction.followup.send("❌ Placar inválido.", ephemeral=True)

        my_v, my_d, _ = parsed
        uid = str(interaction.user.id).strip()

        # ===== MATCH =====
        final_match = get_final_match_by_id_fast(sh, match_id)
        if not final_match:
            return await interaction.followup.send("❌ Match não encontrada.", ephemeral=True)

        if safe_int(final_match.get("season_id", 0), 0) != season:
            return await interaction.followup.send("❌ Match inválida.", ephemeral=True)

        if str(final_match.get("status", "")).lower() != "open":
            return await interaction.followup.send("❌ Match já concluída.", ephemeral=True)

        a = str(final_match.get("player_a_id", "")).strip()
        b = str(final_match.get("player_b_id", "")).strip()

        if uid not in (a, b):
            return await interaction.followup.send("❌ Você não participa.", ephemeral=True)

        if not a or not b:
            return await interaction.followup.send("❌ Match incompleta.", ephemeral=True)

        # ===== SCORE =====
        if uid == a:
            a_w, b_w = my_v, my_d
        else:
            a_w, b_w = my_d, my_v

        if a_w == b_w:
            return await interaction.followup.send("❌ Empate não permitido.", ephemeral=True)

        winner_id = a if a_w > b_w else b
        loser_id = b if a_w > b_w else a

        # ===== WRITE =====
        ws_stage, _, ws_final_matches = ensure_final_sheets(sh)

        rown, row = find_final_match_sheet_row(ws_final_matches, match_id)
        if rown is None:
            return await interaction.followup.send("❌ Linha não encontrada.", ephemeral=True)

        header = cached_get_all_values(ws_final_matches, ttl_seconds=10)[0]
        idx = {name: i for i, name in enumerate(header)}

        updated_at = now_iso_utc()

        ws_final_matches.batch_update([
            {"range": f"{col_letter(idx['a_games_won'])}{rown}", "values": [[str(a_w)]]},
            {"range": f"{col_letter(idx['b_games_won'])}{rown}", "values": [[str(b_w)]]},
            {"range": f"{col_letter(idx['status'])}{rown}", "values": [["completed"]]},
            {"range": f"{col_letter(idx['winner_id'])}{rown}", "values": [[winner_id]]},
            {"range": f"{col_letter(idx['loser_id'])}{rown}", "values": [[loser_id]]},
            {"range": f"{col_letter(idx['updated_at'])}{rown}", "values": [[updated_at]]},
        ])

        cache_invalidate(ws_final_matches)
        invalidate_final_matches_ram_index()

        # ===== PROPAGAÇÃO =====
        final_match["winner_id"] = winner_id
        final_match["loser_id"] = loser_id
        final_match["status"] = "completed"

        _propagate_final_match_result(
            sh=sh,
            final_match=final_match,
            winner_id=winner_id,
            loser_id=loser_id
        )

        # ===== OUTPUT =====
        nick_map = get_player_nick_map_fast(sh)

        await interaction.followup.send(
            f"✅ Resultado registrado\n"
            f"- Match: {match_id}\n"
            f"- Placar: {placar}\n"
            f"- Vencedor: {nick_map.get(winner_id, winner_id)}",
            ephemeral=True
        )

        debug_log("resultado_final_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 16/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 17/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================


# =========================================================
# HELPERS — ROLLBACK OTIMIZADO
# =========================================================

def _final_clear_match_and_descendants(
    ws_final_matches,
    season_id: int,
    root_match_id: str,
    preserve_root_players: bool = False
):
    start = time.perf_counter()

    rows = get_final_matches_fast_by_season(ws_final_matches, season_id)
    by_id = {str(r.get("final_match_id", "")).strip(): r for r in rows}

    if root_match_id not in by_id:
        raise RuntimeError("Match raiz não encontrada.")

    visited = set()
    stack = [root_match_id]
    ordered = []

    while stack:
        mid = stack.pop()
        if mid in visited:
            continue

        visited.add(mid)
        ordered.append(mid)

        r = by_id.get(mid)
        if not r:
            continue

        next_mid = str(r.get("next_win_match_id", "")).strip()
        if next_mid and next_mid not in visited:
            stack.append(next_mid)

    ordered.reverse()

    vals = cached_get_all_values(ws_final_matches, ttl_seconds=10)
    header = vals[0]
    idx = {name: i for i, name in enumerate(header)}

    row_map = {}
    for rown in range(2, len(vals) + 1):
        row = vals[rown - 1]
        mid = str(row[idx["final_match_id"]] if idx["final_match_id"] < len(row) else "").strip()
        if mid:
            row_map[mid] = rown

    updated_at = now_iso_utc()
    updates = []

    for mid in ordered:
        rown = row_map.get(mid)
        if not rown:
            continue

        keep_players = preserve_root_players and (mid == root_match_id)

        if not keep_players:
            updates.append({"range": f"{col_letter(idx['player_a_id'])}{rown}", "values": [[""]]})
            updates.append({"range": f"{col_letter(idx['player_b_id'])}{rown}", "values": [[""]]})

        updates.extend([
            {"range": f"{col_letter(idx['a_games_won'])}{rown}", "values": [["0"]]},
            {"range": f"{col_letter(idx['b_games_won'])}{rown}", "values": [["0"]]},
            {"range": f"{col_letter(idx['status'])}{rown}", "values": [["open"]]},
            {"range": f"{col_letter(idx['winner_id'])}{rown}", "values": [[""]]},
            {"range": f"{col_letter(idx['loser_id'])}{rown}", "values": [[""]]},
            {"range": f"{col_letter(idx['updated_at'])}{rown}", "values": [[updated_at]]},
        ])

    if updates:
        ws_final_matches.batch_update(updates)
        cache_invalidate(ws_final_matches)
        invalidate_final_matches_ram_index()

    debug_log("rollback_chain", start, f"| affected={len(ordered)}")
    return len(ordered)


# =========================================================
# /admin_resultado_final_editar
# =========================================================

@client.tree.command(
    name="admin_resultado_final_editar",
    description="(ADM) Edita resultado da fase final"
)
async def admin_resultado_final_editar(
    interaction: discord.Interaction,
    season: int,
    match_id: str,
    placar: str
):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        parsed = parse_final_md5_score(placar)
        if not parsed:
            return await interaction.followup.send("❌ Placar inválido.", ephemeral=True)

        a_w, b_w, _ = parsed

        ws_stage, _, ws_final_matches = ensure_final_sheets(sh)

        current_match = get_final_match_by_id_fast(sh, match_id)
        if not current_match:
            return await interaction.followup.send("❌ Match não encontrada.", ephemeral=True)

        player_a = str(current_match.get("player_a_id", "")).strip()
        player_b = str(current_match.get("player_b_id", "")).strip()

        if not player_a or not player_b:
            return await interaction.followup.send("❌ Match incompleta.", ephemeral=True)

        # ===== ROLLBACK =====
        affected = _final_clear_match_and_descendants(
            ws_final_matches,
            season,
            match_id,
            preserve_root_players=True
        )

        _final_reopen_stage_if_completed(ws_stage, season)

        # ===== APLICA RESULTADO =====
        result = _final_apply_match_result_direct(
            sh,
            ws_final_matches,
            season,
            match_id,
            a_w,
            b_w
        )

        nick_map = get_player_nick_map_fast(sh)

        await interaction.followup.send(
            f"✅ Editado\n"
            f"- Match: {match_id}\n"
            f"- Vencedor: {nick_map.get(result['winner_id'], result['winner_id'])}\n"
            f"- Afetados: {affected}",
            ephemeral=True
        )

        debug_log("admin_edit_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =========================================================
# /admin_resultado_final_cancelar
# =========================================================

@client.tree.command(
    name="admin_resultado_final_cancelar",
    description="(ADM) Cancela resultado da fase final"
)
async def admin_resultado_final_cancelar(
    interaction: discord.Interaction,
    season: int,
    match_id: str
):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        ws_stage, _, ws_final_matches = ensure_final_sheets(sh)

        current_match = get_final_match_by_id_fast(sh, match_id)
        if not current_match:
            return await interaction.followup.send("❌ Match não encontrada.", ephemeral=True)

        player_a = str(current_match.get("player_a_id", "")).strip()
        player_b = str(current_match.get("player_b_id", "")).strip()

        affected = _final_clear_match_and_descendants(
            ws_final_matches,
            season,
            match_id,
            preserve_root_players=True
        )

        _final_reopen_stage_if_completed(ws_stage, season)

        nick_map = get_player_nick_map_fast(sh)

        await interaction.followup.send(
            f"✅ Cancelado\n"
            f"- Match: {match_id}\n"
            f"- Confronto: {nick_map.get(player_a, player_a)} vs {nick_map.get(player_b, player_b)}\n"
            f"- Afetados: {affected}",
            ephemeral=True
        )

        debug_log("admin_cancel_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 17/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 18/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================


# =========================================================
# HELPERS — STATUS OTIMIZADO
# =========================================================

def _final_stage_status_pt(status: str) -> str:
    st = str(status or "").strip().lower()
    return {
        "generated": "gerada",
        "waiting_confirmation": "aguardando_início_oficial",
        "in_progress": "em_andamento",
        "completed": "concluída",
    }.get(st, st or "não_gerada")


def _final_group_matches_by_round(rows: list[dict]) -> dict[int, list[dict]]:
    grouped = {}

    for r in rows:
        round_num = safe_int(r.get("round", 0), 0)
        if round_num <= 0:
            continue
        grouped.setdefault(round_num, []).append(r)

    return grouped


# =========================================================
# SNAPSHOT OTIMIZADO (LEITURA ÚNICA)
# =========================================================

def get_final_status_snapshot_fast(stage, participants, matches, final_decks) -> dict:
    if not stage:
        return {
            "exists": False,
            "status": "não_gerada",
            "top_size": 0,
            "participants": 0,
            "matches_total": 0,
            "matches_open_ready": 0,
            "matches_open_waiting": 0,
            "matches_completed": 0,
            "final_decks": 0,
            "rounds_count": 0,
        }

    open_ready = 0
    open_waiting = 0
    completed = 0

    for r in matches:
        status = str(r.get("status", "")).lower()
        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()

        if status == "completed":
            completed += 1
        elif status == "open":
            if a and b:
                open_ready += 1
            else:
                open_waiting += 1

    return {
        "exists": True,
        "status": str(stage.get("status", "")).lower(),
        "top_size": safe_int(stage.get("top_size", 0), 0),
        "participants": len(participants),
        "matches_total": len(matches),
        "matches_open_ready": open_ready,
        "matches_open_waiting": open_waiting,
        "matches_completed": completed,
        "final_decks": len(final_decks),
        "rounds_count": len(_final_group_matches_by_round(matches)),
    }


# =========================================================
# /status_final (ANTI TIMEOUT)
# =========================================================

@client.tree.command(
    name="status_final",
    description="(ADM) Diagnóstico da fase final"
)
async def status_final(interaction: discord.Interaction, season: int):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send("❌ Season inválida.", ephemeral=True)

        # ===== LEITURA ÚNICA =====
        stage = get_final_stage_fast(sh, season)
        participants = get_final_participants_fast(sh, season)
        matches = get_final_matches_fast(sh, season)
        final_decks = get_final_decks_fast(sh, season)

        snapshot = get_final_status_snapshot_fast(stage, participants, matches, final_decks)

        lines = [
            f"📘 Status Final | Season {season}",
            f"Status: {_final_stage_status_pt(snapshot['status'])}",
            f"Top: {snapshot['top_size']}",
            f"Players: {snapshot['participants']}",
            f"Decks: {snapshot['final_decks']}",
            "",
            f"Matches: {snapshot['matches_total']}",
            f"Rounds: {snapshot['rounds_count']}",
            f"Open prontas: {snapshot['matches_open_ready']}",
            f"Open aguardando: {snapshot['matches_open_waiting']}",
            f"Concluídas: {snapshot['matches_completed']}",
        ]

        await interaction.followup.send("\n".join(lines), ephemeral=True)

        debug_log("status_final_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 18/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 19/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================


# =========================================================
# HELPERS — REGRAS
# =========================================================

def final_stage_allows_roster_change(status: str) -> bool:
    st = str(status or "").strip().lower()
    return st in ("generated", "waiting_confirmation")


# =========================================================
# HELPERS — PRUNE OTIMIZADO
# =========================================================

def prune_final_decks_for_players(ws_final_decks, season_id: int, allowed_player_ids):
    start = time.perf_counter()

    allowed = {str(x).strip() for x in allowed_player_ids if str(x).strip()}

    vals = cached_get_all_values(ws_final_decks, ttl_seconds=10)

    if not vals:
        return

    header = vals[0]
    idx = {name: i for i, name in enumerate(header)}

    sid_idx = idx.get("season_id", 0)
    pid_idx = idx.get("player_id", 1)

    kept = [header]

    for row in vals[1:]:
        sid = safe_int(row[sid_idx] if sid_idx < len(row) else 0, 0)
        pid = str(row[pid_idx] if pid_idx < len(row) else "").strip()

        if sid != season_id or pid in allowed:
            kept.append(row)

    ws_final_decks.clear()
    ws_final_decks.append_rows(kept, value_input_option="RAW")

    cache_invalidate(ws_final_decks)
    invalidate_final_decks_ram_index()

    debug_log("prune_final_decks", start, f"| kept={len(kept)}")


# =========================================================
# HELPERS — REBUILD OTIMIZADO
# =========================================================

def build_reseeded_final_participants_after_removal(sh, season_id: int, removed_player_id: str):
    start = time.perf_counter()

    removed_pid = str(removed_player_id).strip()

    stage = get_final_stage_fast(sh, season_id)
    top_size = safe_int(stage.get("top_size", 0), 0)

    current = get_final_participants_fast(sh, season_id)
    ranking = _final_read_ranking_geral_rows(sh, season_id)

    selected = []
    selected_ids = set()

    for r in current:
        pid = str(r.get("player_id", "")).strip()
        if not pid or pid == removed_pid:
            continue
        selected.append(r)
        selected_ids.add(pid)

    next_added = ""

    if len(selected) < top_size:
        for r in ranking:
            pid = str(r.get("player_id", "")).strip()
            if not pid or pid == removed_pid or pid in selected_ids:
                continue

            selected.append({
                "player_id": pid,
                "ranking_position": safe_int(r.get("ranking_position", 999999), 999999),
            })
            selected_ids.add(pid)

            if not next_added:
                next_added = pid

            if len(selected) >= top_size:
                break

    selected.sort(key=lambda x: safe_int(x.get("ranking_position", 999999), 999999))

    out = []
    for i, item in enumerate(selected[:top_size], start=1):
        out.append({
            "season_id": season_id,
            "seed": i,
            "player_id": str(item.get("player_id", "")).strip(),
            "ranking_position": safe_int(item.get("ranking_position", 999999), 999999),
        })

    debug_log("reseed_participants", start)
    return out, next_added


def rebuild_final_bracket_after_roster_change(sh, season_id: int, new_rows):
    start = time.perf_counter()

    ws_stage, ws_participants, ws_matches = ensure_final_sheets(sh)
    ws_final_decks = ensure_final_decks_sheet(sh)

    clear_final_participants_for_season(ws_participants, season_id)
    save_final_participants(ws_participants, season_id, new_rows)

    clear_final_matches_for_season(ws_matches, season_id)
    generate_final_bracket(sh, season_id)

    allowed_ids = [str(r.get("player_id", "")).strip() for r in new_rows]
    prune_final_decks_for_players(ws_final_decks, season_id, allowed_ids)

    set_final_stage(
        ws_stage,
        season_id,
        "waiting_confirmation",
        len(new_rows),
        "single_elimination"
    )

    invalidate_final_stage_ram_index()
    invalidate_final_participants_ram_index()
    invalidate_final_matches_ram_index()
    invalidate_final_decks_ram_index()

    debug_log("rebuild_bracket", start)


# =========================================================
# CORE — ABDICAÇÃO
# =========================================================

def execute_final_abdication(sh, season_id: int, target_player_id: str):
    start = time.perf_counter()

    stage = get_final_stage_fast(sh, season_id)

    if not final_stage_allows_roster_change(stage.get("status", "")):
        raise RuntimeError("Fase final já iniciada.")

    new_rows, added = build_reseeded_final_participants_after_removal(
        sh,
        season_id,
        target_player_id
    )

    rebuild_final_bracket_after_roster_change(sh, season_id, new_rows)

    debug_log("execute_abdication", start)

    return {
        "removed_player_id": target_player_id,
        "added_player_id": added,
        "participants_count": len(new_rows),
        "top_size": len(new_rows),
    }


# =========================================================
# /abdicar_final
# =========================================================

@client.tree.command(name="abdicar_final")
async def abdicar_final(interaction: discord.Interaction, season: int):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()
        uid = str(interaction.user.id)

        result = execute_final_abdication(sh, season, uid)

        await interaction.followup.send(
            f"✅ Abdicação concluída\n"
            f"- Removido: {result['removed_player_id']}\n"
            f"- Novo: {result['added_player_id'] or 'Nenhum'}",
            ephemeral=True
        )

        debug_log("abdicar_final_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =========================================================
# /abdicar_final_adm
# =========================================================

@client.tree.command(name="abdicar_final_adm")
async def abdicar_final_adm(interaction: discord.Interaction, season: int, jogador: str):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()
        pid = str(jogador).strip()

        result = execute_final_abdication(sh, season, pid)

        await interaction.followup.send(
            f"✅ Abdicação ADM concluída\n"
            f"- Removido: {result['removed_player_id']}\n"
            f"- Novo: {result['added_player_id'] or 'Nenhum'}",
            ephemeral=True
        )

        debug_log("abdicar_final_adm_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 19/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 20/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================


# =========================================================
# HELPER — VALIDAÇÃO OTIMIZADA
# =========================================================

def validate_final_ready_to_start(sh, season_id: int) -> tuple[bool, str]:
    start = time.perf_counter()

    stage = get_final_stage_fast(sh, season_id)
    if not stage:
        return False, "Fase final não encontrada."

    status = str(stage.get("status", "")).strip().lower()
    if status not in ("generated", "waiting_confirmation"):
        return False, "Fase final já iniciada ou inválida."

    participants = get_final_participants_fast(sh, season_id)
    if not participants:
        return False, "Nenhum participante encontrado."

    top_size = safe_int(stage.get("top_size", 0), 0)

    if len(participants) != top_size:
        return False, f"Participantes inválidos ({len(participants)}/{top_size})."

    # valida seeds sem criar estruturas pesadas
    seen = set()
    for p in participants:
        seed = safe_int(p.get("seed", 0), 0)
        if seed <= 0 or seed > top_size or seed in seen:
            return False, "Seeds inconsistentes."
        seen.add(seed)

    debug_log("validate_final_ready", start)
    return True, ""


# =========================================================
# /final_iniciar
# =========================================================

@client.tree.command(
    name="final_iniciar",
    description="(OWNER) Inicia fase final"
)
async def final_iniciar(interaction: discord.Interaction, season: int):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()

        # ===== VALIDAÇÃO RÁPIDA =====
        if not season_exists(sh, season):
            return await interaction.followup.send(
                "❌ Season inválida.",
                ephemeral=True
            )

        ok, msg = validate_final_ready_to_start(sh, season)
        if not ok:
            return await interaction.followup.send(
                f"❌ Não pode iniciar: {msg}",
                ephemeral=True
            )

        # ===== EXECUÇÃO =====
        ws_stage, _, _ = ensure_final_sheets(sh)

        stage = get_final_stage_fast(sh, season)
        top_size = safe_int(stage.get("top_size", 0), 0)

        set_final_stage(
            ws_stage=ws_stage,
            season_id=season,
            status="in_progress",
            top_size=top_size,
            fmt="single_elimination"
        )

        invalidate_final_stage_ram_index()

        await interaction.followup.send(
            f"🏆 Fase final iniciada\n"
            f"- Season: {season}\n"
            f"- TOP {top_size}\n"
            f"- Status: em andamento",
            ephemeral=True
        )

        await log_admin(
            interaction,
            f"final_iniciar: season={season} top={top_size}"
        )

        debug_log("final_iniciar_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 20/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 21/22
# SUB-BLOCO: ÚNICO
# REVISÃO: PERFORMANCE + ANTI-TIMEOUT + DEBUG
# =========================================================


# =========================================================
# HELPERS — VISUALIZAÇÃO OTIMIZADA
# =========================================================

def _final_status_label_pt(status: str) -> str:
    st = str(status or "").strip().lower()
    if st == "completed":
        return "concluída"
    if st == "open":
        return "aberta"
    return st or "-"


def _final_player_name(nick_map: dict[str, str], pid: str) -> str:
    p = str(pid or "").strip()
    return nick_map.get(p, p) if p else "Aguardando"


def _final_match_score_compact(r: dict) -> str:
    return f"{safe_int(r.get('a_games_won', 0))}-{safe_int(r.get('b_games_won', 0))}"


def _final_group_rows(rows: list[dict]) -> dict[int, list[dict]]:
    grouped = {}

    for r in rows:
        rd = safe_int(r.get("round", 0), 0)
        if rd <= 0:
            continue
        grouped.setdefault(rd, []).append(r)

    for rd in grouped:
        grouped[rd].sort(key=lambda x: safe_int(x.get("match_order", 0), 0))

    return grouped


def _final_round_display_name(top_size: int, round_num: int) -> str:
    rounds = 0
    tmp = max(1, top_size)

    while tmp > 1:
        rounds += 1
        tmp //= 2

    pos = rounds - round_num + 1

    return {
        1: "Final",
        2: "Semifinal",
        3: "Quartas de final",
        4: "Oitavas de final",
    }.get(pos, f"Round {round_num}")


def _build_dynamic_chaveamento_text_fast(stage, rows, nick_map, season: int) -> str:
    if not stage:
        return f"⚠️ Fase final não gerada (Season {season})"

    if not rows:
        return f"⚠️ Sem matches (Season {season})"

    top_size = safe_int(stage.get("top_size", 0), 0)
    grouped = _final_group_rows(rows)

    lines = [
        f"🏆 Chaveamento — Season {season}",
        f"Status: {_final_status_label_pt(stage.get('status', ''))} | TOP {top_size}",
        ""
    ]

    for rd in sorted(grouped.keys()):
        lines.append(f"**{_final_round_display_name(top_size, rd)}**")

        for r in grouped[rd]:
            a = _final_player_name(nick_map, r.get("player_a_id", ""))
            b = _final_player_name(nick_map, r.get("player_b_id", ""))

            if str(r.get("status", "")).lower() == "completed":
                score = _final_match_score_compact(r)
                winner = _final_player_name(nick_map, r.get("winner_id", ""))
                lines.append(f"{a} vs {b} | {score} | {winner}")
            else:
                lines.append(f"{a} vs {b}")

        lines.append("")

    return "\n".join(lines)


# =========================================================
# /chaveamento (ANTI TIMEOUT)
# =========================================================

@client.tree.command(
    name="chaveamento",
    description="Mostra chaveamento da fase final"
)
async def chaveamento(interaction: discord.Interaction, season: int):

    async def _run():
        start_total = time.perf_counter()

        sh = open_sheet()

        if not season_exists(sh, season):
            return await interaction.followup.send("❌ Season inválida.", ephemeral=False)

        # ===== LEITURA ÚNICA =====
        stage = get_final_stage_fast(sh, season)
        rows = get_final_matches_fast(sh, season)
        nick_map = get_player_nick_map_fast(sh)

        text = _build_dynamic_chaveamento_text_fast(stage, rows, nick_map, season)

        await send_followup_chunks(
            interaction,
            text,
            ephemeral=False,
            limit=1800
        )

        debug_log("chaveamento_total", start_total)

    await safe_interaction_execute(interaction, _run)


# =================================================
# FIM DO BLOCO 21/22
# =================================================


# =========================================================
# BLOCO ORIGINAL: BLOCO 22/22
# SUB-BLOCO: ÚNICO
# REVISÃO FINAL: DEBUG PROFISSIONAL + BLINDAGEM ANTI-TIMEOUT
# =========================================================

from flask import jsonify
import time
import traceback

START_TIME = datetime.now(timezone.utc)

# =========================================================
# DEBUG PROFISSIONAL — METRICS
# =========================================================
DEBUG_ENABLED = True

def debug_log(label: str, start_time: float, extra: str = ""):
    if not DEBUG_ENABLED:
        return
    elapsed = round((time.perf_counter() - start_time) * 1000, 2)
    print(f"[DEBUG] {label} | {elapsed} ms {extra}")


# =========================================================
# WRAPPER GLOBAL DE INTERAÇÃO (ANTI TIMEOUT)
# =========================================================
async def safe_interaction_execute(interaction: discord.Interaction, func, *args, **kwargs):
    start_total = time.perf_counter()

    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        result = await func(*args, **kwargs)

        debug_log("COMMAND_TOTAL", start_total)
        return result

    except Exception as e:
        print("========================================")
        print("❌ ERRO EM COMANDO")
        print(f"Comando: {getattr(interaction.command, 'name', 'unknown')}")
        print(f"Erro: {e}")
        traceback.print_exc()
        print("========================================")

        try:
            await interaction.followup.send(
                f"❌ Erro interno: {e}",
                ephemeral=True
            )
        except Exception:
            pass


# =========================================================
# WRAPPER PARA GOOGLE SHEETS (DEBUG)
# =========================================================
def debug_sheet_call(label: str, func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    debug_log(f"SHEETS::{label}", start)
    return result


# =========================================================
# WRAPPER PARA CACHE (DEBUG)
# =========================================================
def debug_cache_call(label: str, func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    debug_log(f"CACHE::{label}", start)
    return result


# =========================================================
# AUTOCOMPLETE DEBUG (ANTI TIMEOUT)
# =========================================================
async def debug_autocomplete(label: str, interaction, func, *args, **kwargs):
    start = time.perf_counter()

    try:
        if _ac_should_skip(interaction, label):
            return []

        result = await func(*args, **kwargs)

        size = len(result) if result else 0
        debug_log(f"AUTOCOMPLETE::{label}", start, f"| items={size}")

        return result[:25]

    except Exception as e:
        print(f"❌ ERRO AUTOCOMPLETE {label}: {e}")
        return []


# =========================================================
# HEALTHCHECK SERVER (RENDER)
# =========================================================
@app.route("/")
def home():
    return jsonify({
        "ok": True,
        "service": "LEME HOLANDÊS BOT",
        "status": "ready",
    })


@app.route("/ping")
def ping():
    return jsonify({
        "ok": True,
        "service": "LEME HOLANDÊS BOT",
        "status": "alive",
        "discord_ready": bool(client.is_ready()),
    })


@app.route("/healthz")
def healthz():
    now_utc = datetime.now(timezone.utc)

    return jsonify({
        "ok": True,
        "status": "ready" if client.is_ready() else "starting",
        "uptime_seconds": int((now_utc - START_TIME).total_seconds()),
        "discord_ready": bool(client.is_ready()),
        "guild_count": len(client.guilds) if client.is_ready() else 0,
        "latency_ms": round(client.latency * 1000, 2) if client.is_ready() else None,
    })


# =========================================================
# LOGS DE ESTABILIDADE
# =========================================================
@client.event
async def on_ready():
    print("========================================")
    print(f"🔥 BOT ONLINE: {client.user}")
    print(f"🌐 Guilds: {len(client.guilds)}")
    print(f"📶 Latência: {round(client.latency * 1000, 2)} ms")
    print("========================================")


@client.event
async def on_disconnect():
    print("⚠️ Discord desconectado...")


@client.event
async def on_resumed():
    print("✅ Sessão Discord retomada.")


@client.event
async def on_error(event_method, *args, **kwargs):
    print(f"❌ Erro no evento {event_method}")


# =========================================================
# GLOBAL LOCK (ANTI CONCORRÊNCIA)
# =========================================================
GLOBAL_LOCK = asyncio.Lock()


# =========================================================
# RUNNER RESILIENTE
# =========================================================
async def run_bot():
    retry = 0

    while True:
        try:
            print("🚀 Iniciando BOT...")
            await client.start(DISCORD_TOKEN)

        except Exception as e:
            retry += 1

            print("========================================")
            print("❌ CRASH DETECTADO")
            print(f"Erro: {e}")
            print(f"Retry: {retry}")
            print("========================================")

            await asyncio.sleep(5)

        finally:
            try:
                if not client.is_closed():
                    await client.close()
            except Exception:
                pass


# =========================================================
# START FINAL
# =========================================================
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN não configurado.")

keep_alive()
asyncio.run(run_bot())

# =========================================================
# FIM DO BLOCO 22/22
# =========================================================
