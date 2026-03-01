import os
import json
import threading
import random
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

import discord
from discord import app_commands
from flask import Flask

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# LEME HOLANDÊS BOT (Discord + Google Sheets + Render)
#
# Sheets:
# - Players (cadastro permanente)
# - Enrollments (inscrição por ciclo)
# - Cycles (status do ciclo)
# - PodsHistory (pods por ciclo)
# - Matches (confrontos e resultados)
# - Standings (ranking recalculado)
#
# Ciclo:
# 1) Jogadores: /inscrever cycle:X  (somente se ciclo open)
# 2) ADM: /pods_gerar cycle:X tamanho:4  -> trava ciclo (locked), grava PodsHistory e cria Matches pending
# 3) Jogadores: /meus_matches cycle:X para ver match_id
# 4) Jogadores: /resultado match_id:... placar:V-D-E (dropdown) -> pending + 48h
# 5) Oponente: /rejeitar match_id:... (até 48h)
# 6) ADM: /recalcular cycle:X -> auto-confirm + rankings
# 7) ADM: /ciclo_encerrar cycle:X -> completed (não aceita inscrição)
#
# Regras:
# - Ranking: Pontos > OMW% > GW% > OGW%
# - Piso 33,3% em MWP e GWP antes de OMW/OGW
# - Sem incremental: sempre recalcular do zero
# - Placar 3-partes: Vitória-Derrota-Empate (V-D-E) em games
# - Empate de game conta como 0.5 na GWP
# - Anti-repetição: tenta reduzir rematches comparando com matches confirmados anteriores
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
# Configs
# =========================
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")

SHEET_ID = os.getenv("SHEET_ID", "")
SERVICE_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

ROLE_ORGANIZADOR = os.getenv("ROLE_ORGANIZADOR", "Organizador")
ROLE_ADM = os.getenv("ROLE_ADM", "ADM")


# =========================
# Time helpers
# =========================
BR_TZ = timezone(timedelta(hours=-3))

def now_br_str() -> str:
    return datetime.now(BR_TZ).strftime("%Y-%m-%d %H:%M:%S")

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_dt(s: str):
    try:
        return datetime.fromisoformat(str(s).strip())
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

def ensure_worksheet(sh, title: str, header: list[str], rows: int = 2000, cols: int = 20):
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
# Sheets: schema
# =========================
PLAYERS_HEADER = ["discord_id","nick","deck","decklist_url","status","reports_unique","created_at","updated_at"]

ENROLLMENTS_HEADER = ["cycle","player_id","status","created_at","updated_at"]
ENROLLMENTS_REQUIRED = ["cycle","player_id","status","created_at","updated_at"]

PODSHISTORY_HEADER = ["cycle","pod","player_id","created_at"]
PODSHISTORY_REQUIRED = ["cycle","pod","player_id","created_at"]

CYCLES_HEADER = ["cycle","status","created_at","updated_at"]  # status: open | locked | completed
CYCLES_REQUIRED = ["cycle","status","created_at","updated_at"]

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
# Cycles helpers
# =========================
def get_cycle_status(ws_cycles, cycle: int) -> str | None:
    rows = ws_cycles.get_all_records()
    for r in rows:
        if safe_int(r.get("cycle", 0), 0) == cycle:
            return str(r.get("status", "")).strip().lower() or None
    return None

def set_cycle_status(ws_cycles, cycle: int, status: str):
    status = str(status).strip().lower()
    nowb = now_br_str()
    rows = ws_cycles.get_all_values()
    col = ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
    found = None
    for r_i in range(2, len(rows)+1):
        r = rows[r_i-1]
        c = r[col["cycle"]] if col["cycle"] < len(r) else ""
        if safe_int(c, 0) == cycle:
            found = r_i
            break
    if found is None:
        ws_cycles.append_row([str(cycle), status, nowb, nowb], value_input_option="USER_ENTERED")
    else:
        ws_cycles.update([[status]], range_name=f"{col_letter(col['status'])}{found}")
        ws_cycles.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{found}")

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
    """
    Regra simples:
    - ciclos 'completed' não aparecem
    - se não existir nenhum ciclo, sugere [1]
    - sempre sugere também o próximo ciclo (max+1) como 'open' possível
    """
    items = list_cycles(ws_cycles)
    if not items:
        return [1]
    max_cycle = max(c for c, _ in items)
    open_cycles = [c for c, st in items if st in ("open",)]
    # sugerir também o próximo ciclo como opção (ainda não criado)
    if max_cycle + 1 not in open_cycles:
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
# Core: recálculo oficial
# =========================
def recalculate_cycle(cycle: int):
    sh = open_sheet()
    ws_players = sh.worksheet("Players")
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
# Autocomplete: /inscrever cycle e /resultado match_id
# =========================================================
async def ac_cycle_open(interaction: discord.Interaction, current: str):
    try:
        sh = open_sheet()
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=10)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        open_cycles = suggest_open_cycles(ws_cycles, limit=25)
        # filtra pelo texto digitado
        cur = str(current).strip()
        if cur:
            open_cycles = [c for c in open_cycles if str(c).startswith(cur)]

        return [app_commands.Choice(name=f"Ciclo {c} (aberto)", value=str(c)) for c in open_cycles[:25]]
    except Exception:
        # fallback: sugere 1..10
        base = [str(i) for i in range(1, 11)]
        cur = str(current).strip()
        if cur:
            base = [x for x in base if x.startswith(cur)]
        return [app_commands.Choice(name=f"Ciclo {x}", value=x) for x in base[:25]]

async def ac_match_id_user(interaction: discord.Interaction, current: str):
    """
    Mostra matches do usuário (pending/active) com label "Pod X: NickA vs NickB | match_id"
    """
    try:
        user_id = str(interaction.user.id)
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)
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


# =========================
# Básicos
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
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=10)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        st = get_cycle_status(ws_cycles, cycle)
        if st == "completed":
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
        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=10)
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

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=10)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        st = get_cycle_status(ws_cycles, c)
        # se não existir, cria como open automaticamente
        if st is None:
            set_cycle_status(ws_cycles, c, "open")
            st = "open"

        if st == "completed":
            await interaction.followup.send("❌ Este ciclo já foi concluído. Escolha outro ciclo.", ephemeral=True)
            return
        if st == "locked":
            await interaction.followup.send("❌ Este ciclo já teve pods gerados e está LOCKED (inscrição fechada).", ephemeral=True)
            return

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)

        # garante Players
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

@client.tree.command(name="drop", description="Sai do ciclo informado (somente este ciclo).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)", motivo="Opcional: motivo curto")
async def drop(interaction: discord.Interaction, cycle: int, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    nowb = now_br_str()

    try:
        sh = open_sheet()

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=10)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)
        st = get_cycle_status(ws_cycles, cycle)
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
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

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
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

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
# Pods (Jeito 1 profissional)
# =========================
@client.tree.command(
    name="pods_gerar",
    description="(ADM) Sorteia pods do ciclo, grava PodsHistory e cria Matches pending (round-robin)."
)
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

        ws_cycles = ensure_worksheet(sh, "Cycles", CYCLES_HEADER, rows=2000, cols=10)
        ensure_sheet_columns(ws_cycles, CYCLES_REQUIRED)

        st = get_cycle_status(ws_cycles, cycle)
        if st is None:
            set_cycle_status(ws_cycles, cycle, "open")
            st = "open"

        if st == "completed":
            await interaction.followup.send("❌ Ciclo COMPLETED. Não pode gerar pods.", ephemeral=True)
            return
        if st == "locked":
            await interaction.followup.send("❌ Pods já foram gerados. Ciclo está LOCKED. Não pode gerar novamente.", ephemeral=True)
            return

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=8000, cols=20)
        ws_matches = sh.worksheet("Matches")
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # regra anti-regeração: se já existe qualquer linha de PodsHistory desse ciclo => bloqueia
        pods_rows = ws_pods.get_all_records()
        if any(safe_int(r.get("cycle", 0), 0) == cycle for r in pods_rows):
            set_cycle_status(ws_cycles, cycle, "locked")
            await interaction.followup.send(
                f"❌ Já existe PodsHistory para o Ciclo {cycle}. Não é permitido gerar novamente.\n"
                "Status do ciclo foi mantido/ajustado para LOCKED.",
                ephemeral=True
            )
            return

        # pega inscritos ativos do ciclo
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

        # nick map pra mensagem
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

        # resposta com pods + nomes
        lines = [f"🧩 Pods do **Ciclo {cycle}** gerados (tamanho {tamanho})."]
        lines.append(f"♻️ Anti-repetição: penalidade final **{repeat_score}** (quanto menor, melhor).")
        lines.append("🔒 Ciclo agora está **LOCKED** (inscrição fechada).")

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
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=8000, cols=20)
        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

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
# /meus_matches (novo)
# =========================
@client.tree.command(name="meus_matches", description="Lista seus matches do ciclo (com match_id, pod e prazo).")
@app_commands.describe(cycle="Ciclo (ex: 1)")
async def meus_matches(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)
    user_id = str(interaction.user.id)

    try:
        sh = open_sheet()
        ws_matches = sh.worksheet("Matches")
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)
        nick_map = build_players_nick_map(ws_players)

        rows = ws_matches.get_all_records()
        my = []
        now = utc_now_dt()

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
                    secs = int((ac - now).total_seconds())
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
# Resultado / Rejeitar (com autocomplete de match_id)
# =========================
@client.tree.command(name="resultado", description="Registra seu resultado (PENDENTE). O oponente tem 48h para rejeitar.")
@app_commands.describe(match_id="ID do match", placar="Vitória-Derrota-Empate (V-D-E) do reportante")
@app_commands.autocomplete(match_id=ac_match_id_user)
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

        # grava sempre no formato do player_a/player_b
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
# Ranking / Recalcular
# =========================
@client.tree.command(name="recalcular", description="(ADM) Recalcula ranking do ciclo (auto-confirm + standings).")
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
            f"Auto-confirm feitos: **{changed}**\n"
            f"Jogadores no Standings: **{len(rows)}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"⚠️ Erro no recálculo: {e}", ephemeral=True)


# =========================
# Start
# =========================
if not DISCORD_TOKEN:
    raise RuntimeError("Faltou a variável DISCORD_TOKEN no ambiente.")

keep_alive()
client.run(DISCORD_TOKEN)
