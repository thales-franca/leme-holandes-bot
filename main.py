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
# - Players: cadastro permanente (Players)
# - Enrollments: inscrição por CICLO (Enrollments)
# - PodsHistory: histórico de pods por ciclo
# - Matches: confrontos e resultados
#
# Fluxo do ciclo (PRO):
# 1) Jogadores: /inscrever cycle:X
# 2) ADM: /pods_gerar cycle:X tamanho:4
#    -> grava PodsHistory e cria Matches (pending) round-robin
# 3) Jogadores: /resultado match_id + placar (V-D-E) via dropdown
# 4) Oponente: /rejeitar match_id (até 48h)
# 5) ADM: /recalcular cycle:X (auto-confirm + ranking)
#
# Regras:
# - Placar 3-partes: V-D-E (wins-losses-draw games)
# - GW% usa empate como 0.5 (game_draws)
# - Piso 33,3% aplicado em MWP e GWP antes de OMW/OGW
# - Ranking: Pts > OMW% > GW% > OGW%
# - Sem cálculo incremental: sempre recalcular do zero
# - Anti-repetição: pods_gerar tenta minimizar confrontos repetidos
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
    """
    Formato: V-D-E (wins-losses-draw games)
    Ex: 2-1-0 | 1-2-0 | 1-1-1 | 0-0-3
    """
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

        # IMPORTANTÍSSIMO: só auto-confirma se já foi reportado
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
    """
    Confrontos repetidos são contados com base em matches:
    - active TRUE
    - confirmed_status confirmed
    - result_type != bye
    """
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
    # penaliza qualquer confronto dentro do pod que já ocorreu no passado
    penalty = 0
    for pod in pods:
        pairs = round_robin_pairs(pod)
        for a, b in pairs:
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

    # auto-confirm antes de recalcular
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

    # 1º passe: pontos, jogos, games, oponentes
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

    # MWP% e GW% com piso 33,3%
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
            # empate de game vale 0.5
            gwp_raw = (s["game_wins"] + 0.5 * s["game_draws"]) / float(gplayed)
            gwp[pid] = floor_333(gwp_raw)

    # OMW% e OGW%: média simples (piso já aplicado em mwp/gwp)
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

    # Ordenação oficial
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
# Players + Enrollments por ciclo
# =========================
@client.tree.command(name="inscrever", description="Inscreve você em um CICLO (e garante cadastro em Players).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
async def inscrever(interaction: discord.Interaction, cycle: int):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    nick = interaction.user.display_name
    now = now_br_str()

    try:
        sh = open_sheet()

        ws_players = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)
        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)

        # garante Players (cadastro permanente)
        prow = find_player_row(ws_players, discord_id)
        if prow is None:
            ws_players.append_row([str(discord_id), nick, "", "", "active", "0", now, now], value_input_option="USER_ENTERED")
        else:
            ws_players.update([[nick]], range_name=f"B{prow}")
            ws_players.update([["active"]], range_name=f"E{prow}")
            ws_players.update([[now]], range_name=f"H{prow}")

        # inscrição no ciclo (Enrollments)
        # procura linha com cycle+player_id
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
            ws_enr.append_row([str(cycle), str(discord_id), "active", now, now], value_input_option="USER_ENTERED")
            await interaction.followup.send(
                f"✅ Inscrito no **Ciclo {cycle}**.\n"
                "Você pode definir seu deck e decklist **apenas 1 vez** com `/deck` e `/decklist`.\n"
                "Se quiser sair do ciclo: `/drop cycle:...`",
                ephemeral=True
            )
        else:
            ws_enr.update([["active"]], range_name=f"{col_letter(col['status'])}{found_row}")
            ws_enr.update([[now]], range_name=f"{col_letter(col['updated_at'])}{found_row}")
            await interaction.followup.send(
                f"✅ Sua inscrição no **Ciclo {cycle}** foi confirmada/reativada.",
                ephemeral=True
            )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /inscrever: {e}", ephemeral=True)

@client.tree.command(name="drop", description="Sai do ciclo informado (somente este ciclo).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)", motivo="Opcional: motivo curto")
async def drop(interaction: discord.Interaction, cycle: int, motivo: str = ""):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    now = now_br_str()

    try:
        sh = open_sheet()
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
            await interaction.followup.send(
                f"❌ Você não está inscrito no Ciclo {cycle} (nada para dropar).",
                ephemeral=True
            )
            return

        ws_enr.update([["dropped"]], range_name=f"{col_letter(col['status'])}{found_row}")
        ws_enr.update([[now]], range_name=f"{col_letter(col['updated_at'])}{found_row}")

        msg = f"✅ Você saiu do **Ciclo {cycle}**.\nPara jogar um novo ciclo, use `/inscrever cycle:N` novamente."
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
    now = now_br_str()

    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

        row = find_player_row(ws, discord_id)
        if row is None:
            await interaction.followup.send("❌ Você ainda não tem cadastro. Use `/inscrever cycle:...` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current = (fields.get("deck") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu seu deck e não pode alterar.\nSe precisar mudar, peça para um **ADM/Organizador**.",
                ephemeral=True
            )
            return

        ws.update([[nome]], range_name=f"C{row}")
        ws.update([[now]], range_name=f"H{row}")
        await interaction.followup.send(f"✅ Deck salvo.\nDeck: **{nome}**", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar deck: {e}", ephemeral=True)

@client.tree.command(name="decklist", description="Define sua decklist (1 vez). ADM/Organizador podem alterar.")
@app_commands.describe(url="Link da decklist (moxfield.com ou ligamagic.com.br)")
async def decklist(interaction: discord.Interaction, url: str):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    now = now_br_str()

    ok, val = validate_decklist_url(url)
    if not ok:
        await interaction.followup.send(f"❌ {val}", ephemeral=True)
        return

    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

        row = find_player_row(ws, discord_id)
        if row is None:
            await interaction.followup.send("❌ Você ainda não tem cadastro. Use `/inscrever cycle:...` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current = (fields.get("decklist_url") or "").strip()

        if current and not await is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu sua decklist e não pode alterar.\nSe precisar mudar, peça para um **ADM/Organizador**.",
                ephemeral=True
            )
            return

        ws.update([[val]], range_name=f"D{row}")
        ws.update([[now]], range_name=f"H{row}")
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
@app_commands.describe(
    cycle="Ciclo (ex: 1)",
    tamanho="Tamanho do pod (padrão 4)",
)
async def pods_gerar(interaction: discord.Interaction, cycle: int, tamanho: int = 4):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    tamanho = max(2, min(int(tamanho), 8))
    nowb = now_br_str()

    try:
        sh = open_sheet()

        ws_enr = ensure_worksheet(sh, "Enrollments", ENROLLMENTS_HEADER, rows=5000, cols=20)
        ws_pods = ensure_worksheet(sh, "PodsHistory", PODSHISTORY_HEADER, rows=8000, cols=20)
        ws_matches = sh.worksheet("Matches")

        ensure_sheet_columns(ws_enr, ENROLLMENTS_REQUIRED)
        ensure_sheet_columns(ws_pods, PODSHISTORY_REQUIRED)
        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        # trava: não gerar novamente se já existe PodsHistory desse ciclo
        pods_rows = ws_pods.get_all_records()
        if any(safe_int(r.get("cycle", 0), 0) == cycle for r in pods_rows):
            await interaction.followup.send(
                f"❌ Já existe PodsHistory para o Ciclo {cycle}.\n"
                "Regra: para evitar bagunça, não permitimos gerar pods novamente no mesmo ciclo.\n"
                "Se precisar refazer, me diga que eu implemento um comando administrativo de reset seguro.",
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
            await interaction.followup.send("Poucos jogadores ativos inscritos no ciclo para gerar pods.", ephemeral=True)
            return

        # anti-repetição (base: matches confirmados e ativos do histórico)
        past_pairs = get_past_confirmed_pairs(ws_matches)

        pods, repeat_score = best_shuffle_min_repeats(players, tamanho, past_pairs, tries=250)

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        created_matches = 0

        # grava PodsHistory + cria matches
        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"

            for pid in pod_players:
                ws_pods.append_row([str(cycle), pod_name, pid, nowb], value_input_option="USER_ENTERED")

            # round-robin dentro do pod
            if len(pod_players) >= 2:
                pairs = round_robin_pairs(pod_players)
                for a, b in pairs:
                    mid = new_match_id(cycle, pod_name)
                    ac_at = auto_confirm_deadline_iso(utc_now_dt())
                    # match criado como pending mas SEM report (reported_by_id vazio)
                    row = [
                        mid, str(cycle), pod_name,
                        str(a), str(b),
                        "0", "0", "0",                # a_games_won, b_games_won, draw_games
                        "normal",
                        "pending",
                        "", "", "",                   # reported_by_id, confirmed_by_id, message_id
                        "TRUE",
                        nowb, nowb,
                        ac_at
                    ]
                    ws_matches.append_row(row, value_input_option="USER_ENTERED")
                    created_matches += 1

        # resposta
        lines = [f"🧩 Pods do **Ciclo {cycle}** gerados (tamanho {tamanho})."]
        lines.append(f"♻️ Anti-repetição: confrontos repetidos minimizados. Penalidade final: **{repeat_score}** (quanto menor, melhor).")

        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"
            lines.append(f"\n**Pod {pod_name}**")
            for pid in pod_players:
                lines.append(f"• `{pid}`")

        lines.append(f"\n✅ Matches criados automaticamente: **{created_matches}** (status: pending).")
        lines.append("Agora: jogadores reportam com `/resultado match_id:... placar:...`")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /pods_gerar: {e}", ephemeral=True)


# =========================
# Resultado / Rejeitar
# =========================
@client.tree.command(name="resultado", description="Registra seu resultado no match_id (pending). O oponente tem 48h para rejeitar.")
@app_commands.describe(
    match_id="ID do match (gerado no /pods_gerar)",
    placar="Vitória-Derrota-Empate (V-D-E) do reportante"
)
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
            await interaction.followup.send("❌ Este match está inativo/cancelado.", ephemeral=True)
            return

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            await interaction.followup.send(f"❌ Este match não está pending (status atual: {status}).", ephemeral=True)
            return

        a_id = (getc("player_a_id") or "").strip()
        b_id = (getc("player_b_id") or "").strip()

        if reporter_id not in (a_id, b_id):
            await interaction.followup.send("❌ Você não faz parte deste match.", ephemeral=True)
            return

        # Reporta V-D-E do reportante, mas grava a_games_won/b_games_won do player_a/player_b
        if reporter_id == a_id:
            a_gw, b_gw, d_g = v, d, e
            opponent_id = b_id
        else:
            # repórter é B, então a_wins = derrotas do repórter, b_wins = vitórias do repórter
            a_gw, b_gw, d_g = d, v, e
            opponent_id = a_id

        # result_type
        rt = "normal"
        if a_gw == b_gw:
            rt = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            rt = "intentional_draw"

        # atualiza prazo de auto-confirm a partir do report (48h após report)
        ac_at = auto_confirm_deadline_iso(utc_now_dt())

        ws.update([[str(a_gw)]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(col['draw_games'])}{rown}")
        ws.update([[rt]], range_name=f"{col_letter(col['result_type'])}{rown}")

        ws.update([[reporter_id]], range_name=f"{col_letter(col['reported_by_id'])}{rown}")
        ws.update([[""]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")  # limpa se tinha algo
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")
        ws.update([[ac_at]], range_name=f"{col_letter(col['auto_confirm_at'])}{rown}")

        await interaction.followup.send(
            "✅ Resultado registrado como **PENDENTE**.\n"
            f"Match: **{match_id}**\n"
            f"Seu placar (V-D-E): **{v}-{d}-{e}**\n"
            f"Oponente tem **48h** para usar `/rejeitar match_id:{match_id}`.\n"
            "Se não rejeitar, vira oficial automaticamente (na próxima varredura do `/recalcular`).",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /resultado: {e}", ephemeral=True)

@client.tree.command(name="rejeitar", description="Rejeita um resultado pendente (apenas o oponente, até 48h).")
@app_commands.describe(match_id="ID do match", motivo="Opcional: motivo curto")
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
            await interaction.followup.send("❌ Este match está inativo/cancelado.", ephemeral=True)
            return

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            await interaction.followup.send(f"❌ Este match não está pending (status atual: {status}).", ephemeral=True)
            return

        reported_by = (getc("reported_by_id") or "").strip()
        if not reported_by:
            await interaction.followup.send("❌ Ainda não há resultado reportado para rejeitar.", ephemeral=True)
            return

        a_id = (getc("player_a_id") or "").strip()
        b_id = (getc("player_b_id") or "").strip()

        # oponente = o outro (não o reportante)
        opponent_allowed = a_id if reported_by == b_id else b_id
        if user_id != opponent_allowed:
            await interaction.followup.send("❌ Apenas o **oponente** pode rejeitar este resultado.", ephemeral=True)
            return

        ac = parse_iso_dt(getc("auto_confirm_at") or "")
        if ac and utc_now_dt() > ac:
            await interaction.followup.send(
                "❌ Prazo de 48h expirou. Este resultado já deve ser oficial.\n"
                "Peça para um ADM/Organizador revisar se necessário.",
                ephemeral=True
            )
            return

        nowb = now_br_str()
        ws.update([["rejected"]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")
        ws.update([[user_id]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        msg = "✅ Resultado rejeitado. Um ADM/Organizador pode revisar e corrigir."
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro no /rejeitar: {e}", ephemeral=True)


# =========================
# Admin - utilitários
# =========================
@client.tree.command(name="admin_pendentes", description="(ADM) Lista resultados pendentes do ciclo (com prazo).")
@app_commands.describe(cycle="Ciclo", limite="Quantidade máxima (ex: 20)")
async def admin_pendentes(interaction: discord.Interaction, cycle: int, limite: int = 20):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)
        rows = ws.get_all_values()
        if len(rows) <= 1:
            await interaction.followup.send("Nenhum match na planilha.", ephemeral=True)
            return

        now = utc_now_dt()
        out = []

        def getc(r, name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        for rown in range(2, len(rows) + 1):
            r = rows[rown - 1]
            if safe_int(getc(r, "cycle"), 0) != cycle:
                continue
            if not as_bool(getc(r, "active") or "TRUE"):
                continue
            if (getc(r, "confirmed_status") or "").strip().lower() != "pending":
                continue

            mid = getc(r, "match_id")
            pod = getc(r, "pod")
            a = getc(r, "player_a_id")
            b = getc(r, "player_b_id")
            ag = getc(r, "a_games_won")
            bg = getc(r, "b_games_won")
            dg = getc(r, "draw_games")
            rep = getc(r, "reported_by_id")

            ac = parse_iso_dt(getc(r, "auto_confirm_at") or "")
            left = "—"
            if rep:
                if ac:
                    secs = int((ac - now).total_seconds())
                    left = "EXPIRADO" if secs <= 0 else f"{secs//3600}h"
            else:
                left = "aguardando report"

            out.append(f"• `{mid}` Pod {pod} | {a} vs {b} | {ag}-{bg}-{dg} | {left}")

        if not out:
            await interaction.followup.send(f"Nenhum pending no ciclo {cycle}.", ephemeral=True)
            return

        out = out[:max(1, min(limite, 50))]
        await interaction.followup.send("Pendentes:\n" + "\n".join(out), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="admin_resultado_editar", description="(ADM) Edita/corrige um resultado (placar/status/jogadores).")
@app_commands.describe(
    match_id="ID do match",
    placar="Novo placar (V-D-E) do player_a_id (ex: 2-1-0)",
    status="pending | confirmed | rejected",
    active="TRUE/FALSE (opcional)",
    player_a="Opcional: trocar player A",
    player_b="Opcional: trocar player B",
)
async def admin_resultado_editar(
    interaction: discord.Interaction,
    match_id: str,
    placar: str,
    status: str,
    active: str = "",
    player_a: discord.Member | None = None,
    player_b: discord.Member | None = None,
):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    st = str(status).strip().lower()
    if st not in ("pending", "confirmed", "rejected"):
        await interaction.followup.send("❌ Status inválido. Use: pending | confirmed | rejected", ephemeral=True)
        return

    sc = parse_score_3parts(placar)
    if not sc:
        await interaction.followup.send("❌ Placar inválido. Use formato `2-1-0`.", ephemeral=True)
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
        if rown <= 1:
            await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)
            return

        nowb = now_br_str()

        ws.update([[str(a_gw)]], range_name=f"{col_letter(col['a_games_won'])}{rown}")
        ws.update([[str(b_gw)]], range_name=f"{col_letter(col['b_games_won'])}{rown}")
        ws.update([[str(d_g)]], range_name=f"{col_letter(col['draw_games'])}{rown}")

        if player_a is not None:
            ws.update([[str(player_a.id)]], range_name=f"{col_letter(col['player_a_id'])}{rown}")
        if player_b is not None:
            ws.update([[str(player_b.id)]], range_name=f"{col_letter(col['player_b_id'])}{rown}")

        ws.update([[st]], range_name=f"{col_letter(col['confirmed_status'])}{rown}")

        if st == "confirmed":
            ws.update([[str(interaction.user.id)]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")

        if str(active).strip():
            ws.update([[str(active).strip()]], range_name=f"{col_letter(col['active'])}{rown}")

        rt = "normal"
        if a_gw == b_gw:
            rt = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            rt = "intentional_draw"
        ws.update([[rt]], range_name=f"{col_letter(col['result_type'])}{rown}")

        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        await interaction.followup.send(
            "✅ Resultado atualizado.\n"
            f"Match: **{match_id}** | V-D-E(A): **{a_gw}-{b_gw}-{d_g}** | Status: **{st}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao editar: {e}", ephemeral=True)

@client.tree.command(name="admin_resultado_cancelar", description="(ADM) Cancela um match (active=FALSE).")
@app_commands.describe(match_id="ID do match", motivo="Opcional")
async def admin_resultado_cancelar(interaction: discord.Interaction, match_id: str, motivo: str = ""):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)

        cell = ws.find(str(match_id).strip())
        rown = cell.row
        if rown <= 1:
            await interaction.followup.send("❌ Match não encontrado.", ephemeral=True)
            return

        nowb = now_br_str()
        ws.update([["FALSE"]], range_name=f"{col_letter(col['active'])}{rown}")
        ws.update([[nowb]], range_name=f"{col_letter(col['updated_at'])}{rown}")

        msg = f"✅ Match cancelado (active=FALSE): **{match_id}**"
        if motivo.strip():
            msg += f"\nMotivo: {motivo.strip()}"
        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao cancelar: {e}", ephemeral=True)


# =========================
# Ranking / Player
# =========================
@client.tree.command(name="recalcular", description="Recalcula ranking do ciclo (ADM/Organizador).")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
async def recalcular(interaction: discord.Interaction, cycle: int):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
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
            f"Auto-confirm feitos agora: **{changed}**\n"
            f"Jogadores no Standings: **{len(rows)}**",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"⚠️ Erro no recálculo: {e}", ephemeral=True)

@client.tree.command(name="ranking", description="Mostra o ranking do ciclo (Top N).")
@app_commands.describe(cycle="Ciclo", top="Quantidade (ex: 10)")
async def ranking(interaction: discord.Interaction, cycle: int, top: int = 10):
    await interaction.response.defer(ephemeral=True)
    top = max(1, min(int(top), 30))

    try:
        sh = open_sheet()
        ws = sh.worksheet("Standings")
        data = ws.get_all_records()
        if not data:
            await interaction.followup.send("Standings vazio. Rode `/recalcular` primeiro.", ephemeral=True)
            return

        rows = [r for r in data if safe_int(r.get("cycle", 0), 0) == cycle]
        if not rows:
            await interaction.followup.send(f"Nenhum dado de Standings para ciclo {cycle}.", ephemeral=True)
            return

        rows = sorted(rows, key=lambda r: safe_int(r.get("rank_position", 9999), 9999))[:top]

        lines = [f"🏆 **Ranking Ciclo {cycle} (Top {top})**"]
        for r in rows:
            pos = r.get("rank_position", "")
            pid = r.get("player_id", "")
            pts = r.get("match_points", "")
            omw = r.get("omw_percent", "")
            gw = r.get("gw_percent", "")
            ogw = r.get("ogw_percent", "")
            lines.append(f"**{pos}.** `{pid}` | Pts: **{pts}** | OMW: {omw}% | GW: {gw}% | OGW: {ogw}%")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="player", description="Mostra cadastro de um jogador (deck/decklist/status).")
@app_commands.describe(jogador="Opcional: outro jogador (default: você)")
async def player(interaction: discord.Interaction, jogador: discord.Member | None = None):
    await interaction.response.defer(ephemeral=True)
    target = jogador or interaction.user

    try:
        sh = open_sheet()
        ws = ensure_worksheet(sh, "Players", PLAYERS_HEADER, rows=2000, cols=20)

        row = find_player_row(ws, target.id)
        if row is None:
            await interaction.followup.send("Jogador não cadastrado.", ephemeral=True)
            return

        f = get_player_fields(ws, row)
        lines = [
            f"👤 **Player**: {target.display_name}",
            f"ID: `{f.get('discord_id','')}`",
            f"Status: **{f.get('status','')}**",
            f"Deck: **{f.get('deck','') or '—'}**",
            f"Decklist: {f.get('decklist_url','') or '—'}",
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)


# =========================
# Start
# =========================
if not DISCORD_TOKEN:
    raise RuntimeError("Faltou a variável DISCORD_TOKEN no ambiente.")

keep_alive()
client.run(DISCORD_TOKEN)
