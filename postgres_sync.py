import os
import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada no ambiente do bot.")

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
    )


def pg_ping():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1 as ok;")
            return cur.fetchone()
