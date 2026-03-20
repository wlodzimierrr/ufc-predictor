"""Integration test for warehouse.db.upsert.

Requires a live Postgres connection (reads from .env / environment).
Creates and drops a temporary table inside the test so it is self-contained.
"""

import datetime
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.db import get_connection, upsert


@pytest.fixture(scope="module")
def conn():
    c = get_connection()
    yield c
    c.close()


@pytest.fixture(scope="module", autouse=True)
def temp_table(conn):
    """Create a scratch table once for the module; drop it after all tests."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _test_upsert (
                row_id      text        PRIMARY KEY,
                value       text,
                scraped_at  timestamptz
            )
        """)
    conn.commit()
    yield
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _test_upsert")
    conn.commit()


@pytest.fixture(autouse=True)
def truncate_table(conn, temp_table):
    """Truncate between tests so each starts with an empty table."""
    yield
    with conn.cursor() as cur:
        cur.execute("TRUNCATE _test_upsert")
    conn.commit()


def _ts(s: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(s)


def test_insert_single_row(conn):
    rows = [{"row_id": "a", "value": "hello", "scraped_at": _ts("2026-01-01T00:00:00+00:00")}]
    upsert(conn, "_test_upsert", rows, pk_columns=["row_id"])
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM _test_upsert")
        assert cur.fetchone()[0] == 1


def test_duplicate_insert_leaves_one_row(conn):
    row = {"row_id": "b", "value": "original", "scraped_at": _ts("2026-01-01T00:00:00+00:00")}
    upsert(conn, "_test_upsert", [row], pk_columns=["row_id"])
    conn.commit()

    # insert same PK again
    upsert(conn, "_test_upsert", [row], pk_columns=["row_id"])
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM _test_upsert WHERE row_id = 'b'")
        assert cur.fetchone()[0] == 1


def test_upsert_updates_value_on_conflict(conn):
    row = {"row_id": "c", "value": "old", "scraped_at": _ts("2026-01-01T00:00:00+00:00")}
    upsert(conn, "_test_upsert", [row], pk_columns=["row_id"])
    conn.commit()

    updated = {"row_id": "c", "value": "new", "scraped_at": _ts("2026-02-01T00:00:00+00:00")}
    upsert(conn, "_test_upsert", [updated], pk_columns=["row_id"])
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT value FROM _test_upsert WHERE row_id = 'c'")
        assert cur.fetchone()[0] == "new"


def test_scraped_at_keeps_most_recent(conn):
    older = _ts("2026-01-01T00:00:00+00:00")
    newer = _ts("2026-03-01T00:00:00+00:00")

    upsert(conn, "_test_upsert", [{"row_id": "d", "value": "x", "scraped_at": newer}], pk_columns=["row_id"])
    conn.commit()

    # re-insert with older scraped_at — should NOT overwrite
    upsert(conn, "_test_upsert", [{"row_id": "d", "value": "x", "scraped_at": older}], pk_columns=["row_id"])
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT scraped_at FROM _test_upsert WHERE row_id = 'd'")
        result = cur.fetchone()[0]
    assert result.replace(tzinfo=datetime.timezone.utc) == newer.replace(tzinfo=datetime.timezone.utc)


def test_batch_insert(conn):
    rows = [
        {"row_id": str(i), "value": f"v{i}", "scraped_at": _ts("2026-01-01T00:00:00+00:00")}
        for i in range(1200)
    ]
    n = upsert(conn, "_test_upsert", rows, pk_columns=["row_id"], batch_size=500)
    conn.commit()

    assert n == 1200
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM _test_upsert")
        assert cur.fetchone()[0] == 1200
