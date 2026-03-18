"""Database connection helper for the UFC data warehouse.

Reads connection parameters from environment variables (or a .env file).
No credentials are hardcoded here — copy .env.example to .env and fill in
your values.

Usage:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
    conn.close()

Or as a context manager via psycopg2:
    with get_connection() as conn:
        ...
"""

import os
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PgConnection

# Load .env from repo root if python-dotenv is available; silently skip if not.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass


def get_connection() -> PgConnection:
    """Return a psycopg2 connection using env-var credentials."""
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


if __name__ == "__main__":
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
    conn.close()
    print(f"Connected: {version}")
