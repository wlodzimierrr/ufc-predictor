"""Apply pending SQL migrations in filename order.

Tracks applied files in a schema_migrations table so re-running is a no-op.

Usage:
    python warehouse/migrate.py
"""

from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection

SQL_DIR = Path(__file__).parent / "sql"


def _ensure_migrations_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename   text        PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
    """)


def _applied(cur) -> set[str]:
    cur.execute("SELECT filename FROM schema_migrations")
    return {row[0] for row in cur.fetchall()}


def migrate() -> None:
    files = sorted(SQL_DIR.glob("*.sql"))
    if not files:
        print("No SQL files found in", SQL_DIR)
        return

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _ensure_migrations_table(cur)
                done = _applied(cur)

                for path in files:
                    if path.name in done:
                        print(f"  skip  {path.name}")
                        continue
                    print(f"  apply {path.name} ...", end=" ", flush=True)
                    cur.execute(path.read_text())
                    cur.execute(
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (path.name,),
                    )
                    print("ok")

        print("\nAll migrations applied.")
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
