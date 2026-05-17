import os
import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.getenv("DATABASE_URL", "")


# =========================================================
# CONNECTION
# =========================================================

def get_conn():

    if not DATABASE_URL:

        raise RuntimeError(
            "DATABASE_URL não configurada no ambiente do bot."
        )

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )


# =========================================================
# PING
# =========================================================

def pg_ping():

    with get_conn() as conn:

        with conn.cursor() as cur:

            cur.execute(
                "select 1 as ok;"
            )

            return cur.fetchone()


# =========================================================
# GENERIC EXECUTE
# =========================================================

def pg_execute(
    query: str,
    params=None,
    fetch: bool = False
):

    with get_conn() as conn:

        with conn.cursor() as cur:

            cur.execute(
                query,
                params or []
            )

            if fetch:

                return cur.fetchall()

            conn.commit()

            return True


# =========================================================
# UPSERT PLAYER
# =========================================================

def pg_upsert_player(
    discord_id: str,
    nickname: str
):

    query = """
    insert into players (
        discord_id,
        nickname
    )
    values (
        %s,
        %s
    )
    on conflict (discord_id)
    do update set
        nickname = excluded.nickname;
    """

    return pg_execute(
        query,
        [
            str(discord_id),
            str(nickname),
        ]
    )
