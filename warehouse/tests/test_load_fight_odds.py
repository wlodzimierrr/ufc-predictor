"""Tests for fight odds CSV validation and loading."""

from __future__ import annotations

import csv
import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
import psycopg2
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.db import get_connection
from warehouse.load_fight_odds import (
    REQUIRED_COLUMNS,
    FightContext,
    OddsValidationContext,
    load_fight_odds,
    read_odds_csv,
    validate_odds_rows,
)


SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
REQUIRED_ENV = {
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
}
EVENT_ID = "11111111-1111-1111-1111-111111111111"
FIGHT_ID = "22222222-2222-2222-2222-222222222222"
FIGHTER_1_ID = "33333333-3333-3333-3333-333333333333"
FIGHTER_2_ID = "44444444-4444-4444-4444-444444444444"


def _context() -> OddsValidationContext:
    return OddsValidationContext(
        event_ids={EVENT_ID},
        fighter_ids={FIGHTER_1_ID, FIGHTER_2_ID},
        fights={
            FIGHT_ID: FightContext(
                event_id=EVENT_ID,
                fighter_1_id=FIGHTER_1_ID,
                fighter_2_id=FIGHTER_2_ID,
            )
        },
    )


def _row(**overrides) -> dict[str, str]:
    row = {
        "fight_id": FIGHT_ID,
        "event_id": EVENT_ID,
        "event_date": "2026-08-01",
        "fighter_id": FIGHTER_1_ID,
        "fighter_name": "Fighter One",
        "opponent_fighter_id": FIGHTER_2_ID,
        "bookmaker": "TestBook",
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": "2026-07-31T12:00:00+00:00",
        "american_odds": "+150",
        "decimal_odds": "",
        "source": "manual",
        "source_url": "",
        "imported_at": "2026-07-31T12:05:00+00:00",
    }
    row.update(overrides)
    return row


def test_read_odds_csv_reports_missing_required_columns(tmp_path):
    path = tmp_path / "odds.csv"
    path.write_text("fight_id,event_id\nabc,def\n", encoding="utf-8")

    rows, missing = read_odds_csv(path)

    assert rows == []
    assert "fighter_id" in missing
    assert REQUIRED_COLUMNS - missing == {"fight_id", "event_id"}


def test_validate_odds_rows_normalizes_american_odds():
    result = validate_odds_rows([(2, _row())], _context())

    assert result.rows_read == 1
    assert result.rejected == []
    assert result.skipped == []
    validated = result.rows[0]
    assert validated.normalized_decimal_odds == Decimal("2.5")
    assert validated.implied_probability == Decimal("0.4")
    assert validated.db_row["american_odds"] == 150
    assert validated.db_row["decimal_odds"] is None


def test_validate_odds_rows_accepts_decimal_odds():
    result = validate_odds_rows(
        [(2, _row(american_odds="", decimal_odds="1.80"))],
        _context(),
    )

    assert result.rejected == []
    assert result.rows[0].normalized_decimal_odds == Decimal("1.80")
    assert result.rows[0].db_row["decimal_odds"] == Decimal("1.80")


def test_validate_odds_rows_rejects_invalid_enum():
    result = validate_odds_rows([(2, _row(market="spread"))], _context())

    assert result.rows == []
    assert result.rejected[0].reason == "invalid market spread"


def test_validate_odds_rows_rejects_unknown_ids():
    result = validate_odds_rows(
        [(2, _row(fighter_id="55555555-5555-5555-5555-555555555555"))],
        _context(),
    )

    assert result.rows == []
    assert "unknown fighter_id" in result.rejected[0].reason


def test_validate_odds_rows_rejects_fighters_that_do_not_match_fight():
    context = OddsValidationContext(
        event_ids={EVENT_ID},
        fighter_ids={
            FIGHTER_1_ID,
            FIGHTER_2_ID,
            "55555555-5555-5555-5555-555555555555",
        },
        fights={
            FIGHT_ID: FightContext(
                event_id=EVENT_ID,
                fighter_1_id=FIGHTER_1_ID,
                fighter_2_id=FIGHTER_2_ID,
            )
        },
    )

    result = validate_odds_rows(
        [(2, _row(opponent_fighter_id="55555555-5555-5555-5555-555555555555"))],
        context,
    )

    assert result.rows == []
    assert "fighter/opponent IDs do not match fight" in result.rejected[0].reason


def test_validate_odds_rows_rejects_fight_without_two_distinct_fighters():
    context = OddsValidationContext(
        event_ids={EVENT_ID},
        fighter_ids={FIGHTER_1_ID, FIGHTER_2_ID},
        fights={
            FIGHT_ID: FightContext(
                event_id=EVENT_ID,
                fighter_1_id=FIGHTER_1_ID,
                fighter_2_id=FIGHTER_1_ID,
            )
        },
    )

    result = validate_odds_rows([(2, _row())], context)

    assert result.rows == []
    assert "does not have exactly two fighters" in result.rejected[0].reason


def test_validate_odds_rows_skips_duplicate_stable_key():
    result = validate_odds_rows([(2, _row()), (3, _row(source_url="different"))], _context())

    assert len(result.rows) == 1
    assert len(result.skipped) == 1
    assert result.skipped[0].reason == "duplicate stable odds key in CSV"


@pytest.fixture()
def migrated_conn():
    missing = sorted(name for name in REQUIRED_ENV if not os.environ.get(name))
    if missing:
        pytest.skip(f"Postgres env not configured: {', '.join(missing)}")

    try:
        conn = get_connection()
    except (KeyError, psycopg2.Error) as exc:
        pytest.skip(f"Postgres unavailable: {exc}")

    schema_name = f"_test_load_fight_odds_{uuid.uuid4().hex}"
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        conn.autocommit = False

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name),
                )
            )
            for filename in [
                "001_events.sql",
                "003_fighters.sql",
                "002_fights.sql",
                "004_fight_stats.sql",
                "005_constraints_and_indexes.sql",
                "017_betting_odds.sql",
            ]:
                cur.execute((SQL_DIR / filename).read_text(encoding="utf-8"))
            _seed_db(cur)
        conn.commit()

        yield conn
    finally:
        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name),
                )
            )
        conn.close()


def _seed_db(cur):
    cur.execute(
        """
        INSERT INTO events (
            event_id,
            event_name,
            event_date,
            event_status,
            source_url
        )
        VALUES (%s, 'UFC Test Card', '2026-08-01', 'completed', 'https://example.com/event')
        """,
        (EVENT_ID,),
    )
    cur.execute(
        """
        INSERT INTO fighters (fighter_id, full_name, source_url)
        VALUES
            (%s, 'Fighter One', 'https://example.com/f1'),
            (%s, 'Fighter Two', 'https://example.com/f2')
        """,
        (FIGHTER_1_ID, FIGHTER_2_ID),
    )
    cur.execute(
        """
        INSERT INTO fights (
            fight_id,
            event_id,
            fighter_1_id,
            fighter_2_id,
            result_type,
            source_url
        )
        VALUES (%s, %s, %s, %s, 'win', 'https://example.com/fight')
        """,
        (FIGHT_ID, EVENT_ID, FIGHTER_1_ID, FIGHTER_2_ID),
    )


def _write_odds_csv(path: Path) -> None:
    rows = [
        _row(),
        _row(
            fighter_id=FIGHTER_2_ID,
            fighter_name="Fighter Two",
            opponent_fighter_id=FIGHTER_1_ID,
            american_odds="-170",
        ),
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sorted(REQUIRED_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def test_load_fight_odds_upserts_idempotently(migrated_conn, tmp_path):
    csv_path = tmp_path / "fight_odds.csv"
    _write_odds_csv(csv_path)

    first = load_fight_odds(migrated_conn, csv_path)
    second = load_fight_odds(migrated_conn, csv_path)

    assert first.imported == 2
    assert first.rejected == 0
    assert second.imported == 2
    with migrated_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fight_odds")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT count(*) FROM fight_odds_no_vig")
        assert cur.fetchone()[0] == 2
