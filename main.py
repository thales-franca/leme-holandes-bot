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
# - Players: /inscrever, /deck (1x), /decklist (1x)
# - Matches: /resultado (pending), /rejeitar (até 48h)
# - Auto-confirm: varredura no /recalcular
# - Admin: editar/cancelar resultados, listar pendentes, player, ranking
# - Fase 3: gerar pods, status de pods
# - PLACAR 3-PARTES: Vitória - Derrota - Empate (games)
#   Ex: 2-1-0 / 1-2-0 / 1-1-1 / 0-0-3
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

def ensure_sheet_columns(ws, required_cols: list[str]):
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"Aba '{ws.title}' sem cabeçalho na linha 1.")
    idx = {name: i for i, name in enumerate(header)}
    missing = [c for c in required_cols if c not in idx]
    if missing:
        raise RuntimeError(f"Aba '{ws.title}' sem colunas: {', '.join(missing)}")
    return idx


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
    Aceita:
      2-1-0  |  1-2-0  |  1-1-1  |  0-0-3
    Retorna:
      (a_wins, b_wins, draws)
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

def validate_3parts_rules(a: int, b: int, d: int) -> tuple[bool, str]:
    """
    Regras básicas (Bo3 + empates de game):
    - games_total = a + b + d
    - deve ser entre 0 e 3 (no máximo 3 games)
    - vitória do match normalmente é 2 wins (mas seu sistema pode aceitar 1-0-0 etc se quiser)
      -> aqui vamos aceitar qualquer dentro de 0..3, mas bloqueamos absurdos.
    """
    total = a + b + d
    if total > 3:
        return (False, "Placar inválido: soma (vitória+derrota+empate) não pode passar de 3.")
    # 0-0-0 permitido (empate sem jogo por tempo/erro) se você quer
    # 0-0-3 permitido (empate intencional: 3 games empatados)
    return (True, "")


# =========================
# Matches helpers
# =========================
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

def new_match_id(cycle: int, pod: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    rnd = random.randint(1000, 9999)
    return f"C{cycle}-P{pod}-{ts}-{rnd}"

def auto_confirm_deadline_iso(created_utc: datetime) -> str:
    return (created_utc + timedelta(hours=48)).isoformat()

def sweep_auto_confirm(sh, cycle: int) -> int:
    ws_matches = sh.worksheet("Matches")
    col = ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)
    rows = ws_matches.get_all_values()
    if len(rows) <= 1:
        return 0

    now = utc_now_dt()
    changed = 0

    def col_letter(col_index_0: int) -> str:
        return chr(65 + col_index_0)  # A..Q (suficiente aqui)

    for i in range(2, len(rows) + 1):
        r = rows[i - 1]

        def getc(name: str) -> str:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        r_cycle = safe_int(getc("cycle"), 0)
        if r_cycle != cycle:
            continue

        active = as_bool(getc("active") or "TRUE")
        if not active:
            continue

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            continue

        ac = parse_iso_dt(getc("auto_confirm_at") or "")
        if not ac:
            continue

        if ac <= now:
            ws_matches.update([["confirmed"]], range_name=f"{col_letter(col['confirmed_status'])}{i}")
            ws_matches.update([["AUTO"]], range_name=f"{col_letter(col['confirmed_by_id'])}{i}")
            ws_matches.update([[now_br_str()]], range_name=f"{col_letter(col['updated_at'])}{i}")
            changed += 1

    return changed


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
        d_g = safe_int(r.get("draw_games", 0), 0)

        valid.append((a, b, a_gw, b_gw, d_g))

    for a, b, a_gw, b_gw, d_g in valid:
        stats[a]["matches_played"] += 1
        stats[b]["matches_played"] += 1

        # games
        stats[a]["game_wins"] += a_gw
        stats[a]["game_losses"] += b_gw
        stats[a]["game_draws"] += d_g
        stats[a]["games_played"] += (a_gw + b_gw + d_g)

        stats[b]["game_wins"] += b_gw
        stats[b]["game_losses"] += a_gw
        stats[b]["game_draws"] += d_g
        stats[b]["games_played"] += (a_gw + b_gw + d_g)

        # match points
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
            # IMPORTANTÍSSIMO: empate de game vale 0.5
            gwp_raw = (s["game_wins"] + 0.5 * s["game_draws"]) / float(gplayed)
            gwp[pid] = floor_333(gwp_raw)

    # OMW% e OGW% (média simples do piso aplicado)
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
            r["cycle"],
            r["player_id"],
            r["matches_played"],
            r["match_points"],
            r["mwp_percent"],
            r["game_wins"],
            r["game_losses"],
            r["game_draws"],
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
# Players
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
            new_row = [str(discord_id), nick, "", "", "active", "0", now, now]
            ws.append_row(new_row, value_input_option="USER_ENTERED")
            await interaction.followup.send(
                "Inscrição realizada ✅\n"
                f"Nick: **{nick}**\n"
                "Você pode definir seu deck e decklist **apenas 1 vez** com `/deck` e `/decklist`.",
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
            "❌ Erro ao acessar a planilha. Confirme acesso do bot e se a aba **Players** existe.\n"
            f"Detalhe: `{type(e).__name__}`",
            ephemeral=True
        )

@client.tree.command(name="deck", description="Define seu deck (1 vez). ADM/Organizador podem alterar.")
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
        current = (fields.get("deck") or "").strip()
        if current and not await is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu seu deck e não pode alterar.\n"
                "Se precisar mudar, peça para um **ADM/Organizador**.",
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
    raw = val

    try:
        sh = open_sheet()
        ws = sh.worksheet("Players")
        row = find_player_row(ws, discord_id)

        if row is None:
            await interaction.followup.send("❌ Você ainda não está inscrito. Use `/inscrever` primeiro.", ephemeral=True)
            return

        fields = get_player_fields(ws, row)
        current = (fields.get("decklist_url") or "").strip()
        if current and not await is_admin_or_organizer(interaction):
            await interaction.followup.send(
                "❌ Você já definiu sua decklist e não pode alterar.\n"
                "Se precisar mudar, peça para um **ADM/Organizador**.",
                ephemeral=True
            )
            return

        ws.update([[raw]], range_name=f"D{row}")
        ws.update([[now]], range_name=f"H{row}")
        await interaction.followup.send(f"✅ Decklist salva.\nLink: {raw}", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao salvar decklist: {e}", ephemeral=True)


# =========================
# Matches (competitivo) - /resultado com opções 3-partes
# =========================
@client.tree.command(name="resultado", description="Registra um resultado (pending). O oponente tem 48h para rejeitar.")
@app_commands.describe(
    cycle="Número do ciclo (ex: 1)",
    pod="Pod (ex: A)",
    oponente="Seu oponente",
    placar="Vitória-Derrota-Empate (selecione)"
)
@app_commands.choices(
    placar=[
        app_commands.Choice(name="2-0-0 (vitória 2-0)", value="2-0-0"),
        app_commands.Choice(name="2-1-0 (vitória 2-1)", value="2-1-0"),
        app_commands.Choice(name="1-2-0 (derrota 1-2)", value="1-2-0"),
        app_commands.Choice(name="0-2-0 (derrota 0-2)", value="0-2-0"),
        app_commands.Choice(name="1-1-1 (empate jogado)", value="1-1-1"),
        app_commands.Choice(name="0-0-0 (empate sem jogo)", value="0-0-0"),
        app_commands.Choice(name="0-0-3 (empate intencional)", value="0-0-3"),
    ]
)
async def resultado(
    interaction: discord.Interaction,
    cycle: int,
    pod: str,
    oponente: discord.Member,
    placar: app_commands.Choice[str]
):
    await interaction.response.defer(ephemeral=True)

    reporter_id = interaction.user.id
    opponent_id = oponente.id

    if reporter_id == opponent_id:
        await interaction.followup.send("❌ Oponente inválido (não pode ser você).", ephemeral=True)
        return

    sc = parse_score_3parts(placar.value)
    if not sc:
        await interaction.followup.send("❌ Placar inválido.", ephemeral=True)
        return

    a_gw, b_gw, d_g = sc
    ok, msg = validate_3parts_rules(a_gw, b_gw, d_g)
    if not ok:
        await interaction.followup.send(f"❌ {msg}", ephemeral=True)
        return

    created_utc = utc_now_dt()
    now_br = now_br_str()

    try:
        sh = open_sheet()
        ws_players = sh.worksheet("Players")
        ws_matches = sh.worksheet("Matches")

        ensure_sheet_columns(ws_matches, MATCHES_REQUIRED_COLS)

        r_row = find_player_row(ws_players, reporter_id)
        o_row = find_player_row(ws_players, opponent_id)
        if r_row is None or o_row is None:
            await interaction.followup.send(
                "❌ Ambos precisam estar inscritos.\nPeça para o oponente usar `/inscrever`.",
                ephemeral=True
            )
            return

        match_id = new_match_id(cycle, str(pod).strip().upper()[:10] or "X")
        ac_at = auto_confirm_deadline_iso(created_utc)

        # result_type (informativo)
        result_type = "normal"
        if a_gw == b_gw:
            result_type = "draw"
        if a_gw == 0 and b_gw == 0 and d_g == 3:
            result_type = "intentional_draw"

        row = [
            match_id,
            str(cycle),
            str(pod).strip().upper(),
            str(reporter_id),
            str(opponent_id),
            str(a_gw),
            str(b_gw),
            str(d_g),
            result_type,
            "pending",
            str(reporter_id),
            "",
            "",
            "TRUE",
            now_br,
            now_br,
            ac_at,
        ]

        ws_matches.append_row(row, value_input_option="USER_ENTERED")

        await interaction.followup.send(
            "✅ Resultado registrado como **PENDENTE**.\n"
            f"Match ID: **{match_id}**\n"
            f"Oponente: **{oponente.display_name}**\n"
            f"Placar (V-D-E): **{a_gw}-{b_gw}-{d_g}**\n"
            "O oponente tem **48h** para usar `/rejeitar`.\n"
            "Se não rejeitar, vira oficial automaticamente (na próxima varredura do `/recalcular`).",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Erro ao registrar resultado: {e}", ephemeral=True)

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

        def col_letter(ci: int) -> str:
            return chr(65 + ci)

        active = as_bool(getc("active") or "TRUE")
        if not active:
            await interaction.followup.send("❌ Este match está inativo/cancelado.", ephemeral=True)
            return

        status = (getc("confirmed_status") or "").strip().lower()
        if status != "pending":
            await interaction.followup.send(f"❌ Este match não está pending (status atual: {status}).", ephemeral=True)
            return

        b_id = (getc("player_b_id") or "").strip()
        if user_id != b_id:
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
        await interaction.followup.send(f"❌ Erro ao rejeitar: {e}", ephemeral=True)


# =========================
# Admin - resultados
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

        for i in range(2, len(rows) + 1):
            r = rows[i - 1]
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

            ac = parse_iso_dt(getc(r, "auto_confirm_at") or "")
            left = ""
            if ac:
                secs = int((ac - now).total_seconds())
                left = "EXPIRADO" if secs <= 0 else f"{secs//3600}h"

            out.append(f"• `{mid}` Pod {pod} | {a} vs {b} | V-D-E: {ag}-{bg}-{dg} | prazo: {left}")

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

        def col_letter(ci: int) -> str:
            return chr(65 + ci)

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
        elif st == "pending":
            ws.update([[""]], range_name=f"{col_letter(col['confirmed_by_id'])}{rown}")

        if str(active).strip():
            ws.update([[str(active).strip()]], range_name=f"{col_letter(col['active'])}{rown}")

        # recalcula result_type pelo novo placar
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

        def col_letter(ci: int) -> str:
            return chr(65 + ci)

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
        rows = recalculate_cycle(cycle)
        await interaction.followup.send(
            f"✅ Recalculo concluído. Ciclo {cycle} atualizado no Standings. Jogadores: {len(rows)}",
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
        ws = sh.worksheet("Players")
        row = find_player_row(ws, target.id)
        if row is None:
            await interaction.followup.send("Jogador não inscrito.", ephemeral=True)
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
# Fase 3
# =========================
@client.tree.command(name="pods_gerar", description="(ADM) Gera pods para o ciclo (somente mensagem; não altera ranking).")
@app_commands.describe(cycle="Ciclo", tamanho="Tamanho do pod (padrão 4)")
async def pods_gerar(interaction: discord.Interaction, cycle: int, tamanho: int = 4):
    if not await is_admin_or_organizer(interaction):
        await interaction.response.send_message("❌ Sem permissão. Apenas ADM/Organizador.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    tamanho = max(2, min(int(tamanho), 8))

    try:
        sh = open_sheet()
        ws = sh.worksheet("Players")
        data = ws.get_all_records()

        players = []
        for r in data:
            pid = str(r.get("discord_id", "")).strip()
            status = str(r.get("status", "")).strip().lower()
            nick = str(r.get("nick", "")).strip()
            if pid and status == "active":
                players.append((pid, nick))

        if len(players) < 2:
            await interaction.followup.send("Poucos jogadores ativos para gerar pods.", ephemeral=True)
            return

        random.shuffle(players)
        pods = []
        for i in range(0, len(players), tamanho):
            pods.append(players[i:i+tamanho])

        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        lines = [f"🧩 **Pods gerados (Ciclo {cycle})** — tamanho {tamanho}"]
        for idx, pod_players in enumerate(pods):
            pod_name = letters[idx] if idx < len(letters) else f"P{idx+1}"
            lines.append(f"\n**Pod {pod_name}**")
            for pid, nick in pod_players:
                lines.append(f"• `{pid}` — {nick}")

        lines.append("\nObs: resultados entram via `/resultado` (pending) e viram oficiais após 48h sem `/rejeitar` ou via revisão ADM.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Erro: {e}", ephemeral=True)

@client.tree.command(name="pods_status", description="Mostra status de matches por pod no ciclo (pending/confirmed/rejected).")
@app_commands.describe(cycle="Ciclo", pod="Opcional: filtrar pod (ex: A)")
async def pods_status(interaction: discord.Interaction, cycle: int, pod: str = ""):
    await interaction.response.defer(ephemeral=True)
    podf = str(pod).strip().upper()

    try:
        sh = open_sheet()
        ws = sh.worksheet("Matches")
        col = ensure_sheet_columns(ws, MATCHES_REQUIRED_COLS)
        rows = ws.get_all_values()
        if len(rows) <= 1:
            await interaction.followup.send("Nenhum match registrado.", ephemeral=True)
            return

        summary = {}
        for i in range(2, len(rows) + 1):
            r = rows[i-1]

            def getc(name: str) -> str:
                idx = col[name]
                return r[idx] if idx < len(r) else ""

            if safe_int(getc("cycle"), 0) != cycle:
                continue
            if not as_bool(getc("active") or "TRUE"):
                continue

            p = (getc("pod") or "").strip().upper()
            if podf and p != podf:
                continue

            st = (getc("confirmed_status") or "").strip().lower() or "unknown"
            if p not in summary:
                summary[p] = {"pending": 0, "confirmed": 0, "rejected": 0, "other": 0}
            if st in summary[p]:
                summary[p][st] += 1
            else:
                summary[p]["other"] += 1

        if not summary:
            await interaction.followup.send("Nenhum match para esse filtro.", ephemeral=True)
            return

        lines = [f"📌 **Status de Pods — Ciclo {cycle}**"]
        for p in sorted(summary.keys()):
            c = summary[p]
            lines.append(f"**Pod {p}** | pending: {c['pending']} | confirmed: {c['confirmed']} | rejected: {c['rejected']}")

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
