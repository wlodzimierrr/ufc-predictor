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
from typing import Sequence

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

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


def upsert(
    conn: PgConnection,
    table: str,
    rows: Sequence[dict],
    pk_columns: Sequence[str],
    batch_size: int = 500,
) -> int:
    """Batch-upsert rows into table. Returns total rows processed.

    On conflict (pk_columns), all non-PK columns are updated.
    scraped_at is kept as the more recent of the existing and incoming values.
    """
    if not rows:
        return 0

    columns = list(rows[0].keys())
    non_pk = [c for c in columns if c not in pk_columns]

    col_list = ", ".join(f'"{c}"' for c in columns)
    pk_list = ", ".join(f'"{c}"' for c in pk_columns)

    update_parts = []
    for c in non_pk:
        if c == "scraped_at":
            update_parts.append(
                f'"scraped_at" = GREATEST(EXCLUDED."scraped_at", {table}."scraped_at")'
            )
        else:
            update_parts.append(f'"{c}" = EXCLUDED."{c}"')
    update_clause = ", ".join(update_parts)

    sql = (
        f'INSERT INTO {table} ({col_list}) VALUES %s '
        f'ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}'
    )

    total = 0
    with conn.cursor() as cur:
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset : offset + batch_size]
            values = [tuple(r[c] for c in columns) for r in batch]
            execute_values(cur, sql, values)
            total += len(batch)

    return total


if __name__ == "__main__":
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
    conn.close()
    print(f"Connected: {version}")
