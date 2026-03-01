import os
import json
import threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

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
# Time helpers (BR)
# =========================
BR_TZ = timezone(timedelta(hours=-3))

def now_br_str() -> str:
    # Formato padrão para created_at / updated_at (Brasil)
    return datetime.now(BR_TZ).strftime("%Y-%m-%d %H:%M:%S")

def now_iso_utc() -> str:
    # ISO em UTC para logs internos (ex: last_recalc_at)
    return datetime.now(timezone.utc).isoformat()


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

def find_player_row(ws_players, discord_id: int):
    """
    Procura discord_id na coluna A (discord_id).
    Retorna o número da linha (int) se encontrar, ou None se não encontrar.
    """
    try:
        cell = ws_players.find(str(discord_id))
        return cell.row
    except Exception:
        return None


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

        # Ignorar BYE como oponente (futuro) - por enquanto, não entra no ranking
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

    # Montar linhas para Standings
    rows = []
    for pid, s in stats.items():
        rows.append({
            "cycle": cycle,
            "player_id": pid,  # compat com sua aba Standings
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
    ts = now_iso_utc()
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


@client.tree.command(name="forcesync", description="Força sincronização dos comandos (ADM)")
async def forcesync(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    if not GUILD_ID:
        await interaction.response.send_message("⚠️ DISCORD_GUILD_ID não configurado.", ephemeral=True)
        return

    guild = discord.Object(id=GUILD_ID)
    await client.tree.sync(guild=guild)
    await interaction.response.send_message("🔄 Comandos sincronizados com sucesso.", ephemeral=True)


# =========================
# FASE 2 - /inscrever
# =========================
@client.tree.command(name="inscrever", description="Inscreve você na Liga Leme Holandês (cria/atualiza cadastro).")
async def inscrever(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    nick = interaction.user.display_name
    now = now_br_str()

    try:
        sh = open_sheet()
        ws = sh.worksheet("Players")

        row = find_player_row(ws, discord_id)

        if row is None:
            new_row = [
                str(discord_id),
                nick,
                "",             # deck
                "",             # decklist_url
                "active",       # status
                "0",            # reports_unique
                now,            # created_at
                now             # updated_at
            ]
            ws.append_row(new_row, value_input_option="USER_ENTERED")

            await interaction.followup.send(
                "Inscrição realizada ✅\n"
                f"Nick: **{nick}**\n"
                "Próximo passo: use `/deck` e `/decklist` quando quiser.",
                ephemeral=True
            )
        else:
            ws.update([[nick]], range_name=f"B{row}")
            ws.update([["active"]], range_name=f"E{row}")
            ws.update([[now]], range_name=f"H{row}")

            await interaction.followup.send(
                "Seu cadastro já existia. Atualizei seu nick/status ✅\n"
                f"Nick atual: **{nick}**",
                ephemeral=True
            )

    except Exception as e:
        await interaction.followup.send(
            "❌ Erro ao acessar a planilha. Confirme se o bot tem acesso e se a aba **Players** existe.\n"
            f"Detalhe: `{type(e).__name__}`",
            ephemeral=True
        )


# =========================
# FASE 2 - /deck
# =========================
@client.tree.command(name="deck", description="Define ou altera o nome do seu deck.")
@app_commands.describe(nome="Nome do deck (ex: UR Murktide)")
async def deck(interaction: discord.Interaction, nome: str):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    now = now_br_str()

    try:
        sh = open_sheet()
        ws = sh.worksheet("Players")

        row = find_player_row(ws, discord_id)

        if row is None:
            await interaction.followup.send(
                "❌ Você ainda não está inscrito.\nUse `/inscrever` primeiro.",
                ephemeral=True
            )
            return

        ws.update([[nome]], range_name=f"C{row}")
        ws.update([[now]], range_name=f"H{row}")

        await interaction.followup.send(
            f"✅ Deck atualizado com sucesso.\nDeck atual: **{nome}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(
            f"❌ Erro ao atualizar deck: {e}",
            ephemeral=True
        )


# =========================
# FASE 2 - /decklist (somente Moxfield e LigaMagic)
# =========================
@client.tree.command(name="decklist", description="Define ou altera o link da sua decklist (Moxfield ou LigaMagic).")
@app_commands.describe(url="Link da decklist (moxfield.com ou ligamagic.com.br)")
async def decklist(interaction: discord.Interaction, url: str):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    now = now_br_str()

    try:
        raw = url.strip()

        # Normaliza: se vier sem http/https, adiciona https://
        if not raw.startswith("http://") and not raw.startswith("https://"):
            raw = "https://" + raw

        # Regras básicas
        if " " in raw or len(raw) < 10 or len(raw) > 400:
            await interaction.followup.send(
                "❌ Link inválido. Envie uma URL completa (sem espaços).",
                ephemeral=True
            )
            return

        parsed = urlparse(raw)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]

        allowed_hosts = {"moxfield.com", "ligamagic.com.br"}
        if host not in allowed_hosts:
            await interaction.followup.send(
                "❌ Link não permitido.\nUse apenas:\n• moxfield.com\n• ligamagic.com.br",
                ephemeral=True
            )
            return

        # Validação por domínio
        if host == "moxfield.com":
            if "/decks/" not in (parsed.path or ""):
                await interaction.followup.send(
                    "❌ Link inválido do Moxfield.\nExemplo: https://www.moxfield.com/decks/SEU_ID",
                    ephemeral=True
                )
                return

        if host == "ligamagic.com.br":
            qs = parse_qs(parsed.query or "")
            deck_id = (qs.get("id", [""])[0] or "").strip()
            if not deck_id.isdigit():
                await interaction.followup.send(
                    "❌ Link inválido da LigaMagic.\nUse um link com `id=`.\nExemplo: https://www.ligamagic.com.br/?view=dks/deck&id=123456",
                    ephemeral=True
                )
                return

        sh = open_sheet()
        ws = sh.worksheet("Players")

        row = find_player_row(ws, discord_id)
        if row is None:
            await interaction.followup.send(
                "❌ Você ainda não está inscrito.\nUse `/inscrever` primeiro.",
                ephemeral=True
            )
            return

        ws.update([[raw]], range_name=f"D{row}")
        ws.update([[now]], range_name=f"H{row}")

        await interaction.followup.send(
            "✅ Decklist atualizada com sucesso.\n"
            f"Link: {raw}",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(
            f"❌ Erro ao atualizar decklist: {e}",
            ephemeral=True
        )


@client.tree.command(name="recalcular", description="Recalcula ranking do ciclo (ADM/Organizador)")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
async def recalcular(interaction: discord.Interaction, cycle: int):
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
