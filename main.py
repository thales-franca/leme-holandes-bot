import os
import json
import threading
from datetime import datetime, timezone

import discord
from discord import app_commands

# Servidor HTTP mínimo (Render precisa de uma porta aberta em Web Service)
from flask import Flask

import gspread
from google.oauth2.service_account import Credentials


# =========================
# HTTP keep-alive (Render)
# =========================
app = Flask(__name__)

@app.get("/")
def home():
    return "LEME HOLANDÊS BOT online", 200

def _run_web():
    # Render define a porta na variável PORT
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

def keep_alive():
    t = threading.Thread(target=_run_web, daemon=True)
    t.start()


# =========================
# Configs
# =========================
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))  # opcional
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")

SHEET_ID = os.getenv("SHEET_ID", "")
SERVICE_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")


# =========================
# Google Sheets helpers
# =========================
def get_sheets_client():
    """Cria cliente do Google Sheets usando JSON da conta de serviço em variável de ambiente."""
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

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_int(v, default=0):
    try:
        return int(str(v).strip())
    except:
        return default

def as_bool(v):
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "sim")

def floor_333(x: float) -> float:
    # piso 33,3% = 1/3
    return max(x, 1/3)

def pct1(x: float) -> float:
    # retorna percentual com 1 casa (ex: 50.0)
    return round(x * 100.0, 1)


# =========================
# Core: recálculo oficial
# =========================
def recalculate_cycle(cycle: int):
    sh = open_sheet()
    ws_players = sh.worksheet("Players")
    ws_matches = sh.worksheet("Matches")
    ws_standings = sh.worksheet("Standings")

    # Players: discord_id -> nick
    players_rows = ws_players.get_all_records()
    all_player_ids = set()

    for r in players_rows:
        pid = str(r.get("discord_id", "")).strip()
        if pid:
            all_player_ids.add(pid)

    # Estruturas
    stats = {}
    opponents = {}

    def ensure(pid: str):
        if pid not in stats:
            stats[pid] = {
                "match_points": 0,
                "matches_played": 0,
                "game_wins": 0,
                "game_losses": 0,
                "games_played": 0,
            }
            opponents[pid] = []

    for pid in list(all_player_ids):
        ensure(pid)

    # Matches válidos (ciclo + confirmed + active TRUE)
    matches_rows = ws_matches.get_all_records()
    valid = []

    for r in matches_rows:
        r_cycle = safe_int(r.get("cycle", 0), 0)
        if r_cycle != cycle:
            continue

        confirmed_status = str(r.get("confirmed_status", "")).strip().lower()
        if confirmed_status != "confirmed":
            continue

        active = as_bool(r.get("active", "TRUE"))
        if not active:
            continue

        a = str(r.get("player_a_id", "")).strip()
        b = str(r.get("player_b_id", "")).strip()
        if not a or not b:
            continue

        # Ignorar BYE como oponente (futuro)
        result_type = str(r.get("result_type", "normal")).strip().lower()
        if result_type == "bye":
            continue

        ensure(a)
        ensure(b)

        a_gw = safe_int(r.get("a_games_won", 0), 0)
        b_gw = safe_int(r.get("b_games_won", 0), 0)

        valid.append((a, b, a_gw, b_gw))

    # 1º passe: pontos, jogos, games, oponentes
    for a, b, a_gw, b_gw in valid:
        stats[a]["matches_played"] += 1
        stats[b]["matches_played"] += 1

        # games
        stats[a]["game_wins"] += a_gw
        stats[a]["game_losses"] += b_gw
        stats[a]["games_played"] += (a_gw + b_gw)

        stats[b]["game_wins"] += b_gw
        stats[b]["game_losses"] += a_gw
        stats[b]["games_played"] += (a_gw + b_gw)

        # match points (W=3, D=1, L=0) usando games para decidir
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
        if mplayed == 0:
            mwp[pid] = 1/3
        else:
            mwp[pid] = floor_333(mp / (3.0 * mplayed))

        gplayed = s["games_played"]
        if gplayed == 0:
            gwp[pid] = 1/3
        else:
            gwp[pid] = floor_333(s["game_wins"] / float(gplayed))

    # OMW% e OGW% (média simples dos oponentes; piso aplicado por oponente via mwp/gwp)
    omw = {}
    ogw = {}

    for pid, s in stats.items():
        opps = opponents.get(pid, [])
        if not opps:
            omw[pid] = 1/3
            ogw[pid] = 1/3
        else:
            omw_vals = [mwp.get(oid, 1/3) for oid in opps]
            ogw_vals = [gwp.get(oid, 1/3) for oid in opps]
            omw[pid] = sum(omw_vals) / len(omw_vals)
            ogw[pid] = sum(ogw_vals) / len(ogw_vals)

    # Montar linhas para Standings
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
            "games_played": s["games_played"],
            "gw_percent": pct1(gwp[pid]),
            "omw_percent": pct1(omw[pid]),
            "ogw_percent": pct1(ogw[pid]),
        })

    # Ordenação oficial: Pontos > OMW% > GW% > OGW%
    rows.sort(
        key=lambda r: (r["match_points"], r["omw_percent"], r["gw_percent"], r["ogw_percent"]),
        reverse=True
    )

    # rank_position e timestamp
    ts = now_iso()
    for i, r in enumerate(rows, start=1):
        r["rank_position"] = i
        r["last_recalc_at"] = ts

    # Reescrever Standings do zero
    header = [
        "cycle","player_id","matches_played","match_points","mwp_percent",
        "game_wins","game_losses","games_played","gw_percent",
        "omw_percent","ogw_percent","rank_position","last_recalc_at"
    ]

    ws_standings.clear()
    ws_standings.append_row(header)

    values = []
    for r in rows:
        values.append([
            r["cycle"],
            r["player_id"],
            r["matches_played"],
            r["match_points"],
            r["mwp_percent"],
            r["game_wins"],
            r["game_losses"],
            r["games_played"],
            r["gw_percent"],
            r["omw_percent"],
            r["ogw_percent"],
            r["rank_position"],
            r["last_recalc_at"],
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


@client.tree.command(name="ping", description="Teste do bot")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("⚓ Pong! Bot online.")


@client.tree.command(name="sheets", description="Teste de conexão com Google Sheets (apenas leitura)")
async def sheets(interaction: discord.Interaction):
    gc = get_sheets_client()
    if not gc or not SHEET_ID:
        await interaction.response.send_message(
            "⚠️ Google Sheets ainda não configurado. (Faltando SHEET_ID ou GOOGLE_SERVICE_ACCOUNT_JSON)"
        )
        return

    try:
        sh = gc.open_by_key(SHEET_ID)
        await interaction.response.send_message(f"✅ Conectado na planilha: **{sh.title}**")
    except Exception as e:
        await interaction.response.send_message(f"❌ Erro ao acessar planilha: `{e}`")


@client.tree.command(name="recalcular", description="Recalcula ranking do ciclo (ADM/Organizador)")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
async def recalcular(interaction: discord.Interaction, cycle: int):
    # Permissão simples: admin ou gerenciar servidor
    if not (interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild):
        await interaction.response.send_message("❌ Sem permissão para usar este comando.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        rows = recalculate_cycle(cycle)
        await interaction.followup.send(
            f"✅ Recalculo concluído. Ciclo {cycle} atualizado no Standings. Jogadores: {len(rows)}",
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
