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
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

def keep_alive():
    t = threading.Thread(target=_run_web, daemon=True)
    t.start()


# =========================
# Configs
# =========================
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))  # recomendado para sync rápido
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")

SHEET_ID = os.getenv("SHEET_ID", "")
SERVICE_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# Nomes EXATOS dos cargos no Discord (pode sobrescrever no Render se quiser)
ROLE_ORGANIZADOR = os.getenv("ROLE_ORGANIZADOR", "Organizador")
ROLE_ADM = os.getenv("ROLE_ADM", "ADM")


# =========================
# Time helpers (BR)
# =========================
BR_TZ = timezone(timedelta(hours=-3))

def now_br_str() -> str:
    return datetime.now(BR_TZ).strftime("%Y-%m-%d %H:%M:%S")

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# Auth helpers (Discord)
# =========================
def has_role(interaction: discord.Interaction, role_name: str) -> bool:
    if not interaction.guild or not interaction.user:
        return False
    member = interaction.guild.get_member(interaction.user.id)
    if not member:
        return False
    return any(r.name == role_name for r in member.roles)

def is_admin_or_organizer(interaction: discord.Interaction) -> bool:
    # Permite por permissão do Discord OU por cargo
    if interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild:
        return True
    return has_role(interaction, ROLE_ADM) or has_role(interaction, ROLE_ORGANIZADOR)


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
    except:
        return default

def as_bool(v):
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "sim")

def floor_333(x: float) -> float:
    return max(x, 1/3)

def pct1(x: float) -> float:
    return round(x * 100.0, 1)

def find_player_row(ws_players, discord_id: int):
    try:
        cell = ws_players.find(str(discord_id))
        return cell.row
    except Exception:
        return None

def get_player_fields(ws_players, row: int):
    """
    Lê os campos principais do jogador na linha.
    A..H:
    A discord_id
    B nick
    C deck
    D decklist_url
    E status
    F reports_unique
    G created_at
    H updated_at
    """
    # range A..H da linha
    vals = ws_players.get(f"A{row}:H{row}")
    if not vals or not vals[0]:
        return {}
    r = vals[0]
    # garante tamanho 8
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
# Core: recálculo oficial
# =========================
def recalculate_cycle(cycle: int):
    sh = open_sheet()
    ws_players = sh.worksheet("Players")
    ws_matches = sh.worksheet("Matches")
    ws_standings = sh.worksheet("Standings")

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
                "games_played": 0,
            }
            opponents[pid] = []

    for pid in list(all_player_ids):
        ensure(pid)

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

        result_type = str(r.get("result_type", "normal")).strip().lower()
        if result_type == "bye":
            continue

        ensure(a)
        ensure(b)

        a_gw = safe_int(r.get("a_games_won", 0), 0)
        b_gw = safe_int(r.get("b_games_won", 0), 0)

        valid.append((a, b, a_gw, b_gw))

    for a, b, a_gw, b_gw in valid:
        stats[a]["matches_played"] += 1
        stats[b]["matches_played"] += 1

        stats[a]["game_wins"] += a_gw
        stats[a]["game_losses"] += b_gw
        stats[a]["games_played"] += (a_gw + b_gw)

        stats[b]["game_wins"] += b_gw
        stats[b]["game_losses"] += a_gw
        stats[b]["games_played"] += (a_gw + b_gw)

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
        gwp[pid] = 1/3 if gplayed == 0 else floor_333(s["game_wins"] / float(gplayed))

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


@client.tree.command(name="forcesync", description="Força sincronização dos comandos (ADM/Organizador)")
async def forcesync(interaction: discord.Interaction):
    if not is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão.", ephemeral=True)
        return
    if not GUILD_ID:
        await interaction.response.send_message("⚠️ DISCORD_GUILD_ID não configurado.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        guild = discord.Object(id=GUILD_ID)
        await client.tree.sync(guild=guild)
        await interaction.followup.send("🔄 Comandos sincronizados com sucesso.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ Falha ao sincronizar: {type(e).__name__}", ephemeral=True)


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
                "Agora você pode definir seu deck e decklist 1 única vez usando `/deck` e `/decklist`.",
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
# FASE 2 - /deck (jogador só 1x; ADM/Organizador pode sempre)
# =========================
@client.tree.command(name="deck", description="Define seu deck (apenas 1 vez). ADM/Organizador podem alterar.")
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
            await interaction.followup.send("❌ Você ainda não está inscrito. Use `/inscrever` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current_deck = (fields.get("deck") or "").strip()

        # Se já existe deck e não é ADM/Organizador, bloqueia
        if current_deck and not is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu seu deck e não pode alterar.\n"
                "Se precisar mudar, peça para um ADM/Organizador.",
                ephemeral=True
            )
            return

        ws.update([[nome]], range_name=f"C{row}")
        ws.update([[now]], range_name=f"H{row}")

        await interaction.followup.send(
            f"✅ Deck salvo com sucesso.\nDeck: **{nome}**",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar deck: {e}", ephemeral=True)


# =========================
# FASE 2 - /decklist (jogador só 1x; ADM/Organizador pode sempre)
# =========================
@client.tree.command(name="decklist", description="Define sua decklist (apenas 1 vez). ADM/Organizador podem alterar.")
@app_commands.describe(url="Link da decklist (moxfield.com ou ligamagic.com.br)")
async def decklist(interaction: discord.Interaction, url: str):
    await interaction.response.defer(ephemeral=True)

    discord_id = interaction.user.id
    now = now_br_str()

    try:
        raw = url.strip()
        if not raw.startswith("http://") and not raw.startswith("https://"):
            raw = "https://" + raw

        if " " in raw or len(raw) < 10 or len(raw) > 400:
            await interaction.followup.send("❌ Link inválido. Envie uma URL completa (sem espaços).", ephemeral=True)
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
                    "❌ Link inválido da LigaMagic.\nExemplo: https://www.ligamagic.com.br/?view=dks/deck&id=123456",
                    ephemeral=True
                )
                return

        sh = open_sheet()
        ws = sh.worksheet("Players")

        row = find_player_row(ws, discord_id)
        if row is None:
            await interaction.followup.send("❌ Você ainda não está inscrito. Use `/inscrever` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current_link = (fields.get("decklist_url") or "").strip()

        # Se já existe decklist e não é ADM/Organizador, bloqueia
        if current_link and not is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu sua decklist e não pode alterar.\n"
                "Se precisar mudar, peça para um ADM/Organizador.",
                ephemeral=True
            )
            return

        ws.update([[raw]], range_name=f"D{row}")
        ws.update([[now]], range_name=f"H{row}")

        await interaction.followup.send(
            "✅ Decklist salva com sucesso.\n"
            f"Link: {raw}",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar decklist: {e}", ephemeral=True)


# =========================
# /recalcular (ADM/Organizador)
# =========================
@client.tree.command(name="recalcular", description="Recalcula ranking do ciclo (ADM/Organizador)")
@app_commands.describe(cycle="Número do ciclo (ex: 1)")
async def recalcular(interaction: discord.Interaction, cycle: int):
    if not is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
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
