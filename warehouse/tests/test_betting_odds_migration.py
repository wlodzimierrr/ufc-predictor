"""Integration tests for the betting odds warehouse migration.

These tests use a temporary Postgres schema and skip when a warehouse connection
is not configured or unavailable.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest
import psycopg2
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.db import get_connection


SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
REQUIRED_ENV = {
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
}


@pytest.fixture()
def migrated_conn():
    missing = sorted(name for name in REQUIRED_ENV if not os.environ.get(name))
    if missing:
        pytest.skip(f"Postgres env not configured: {', '.join(missing)}")

    try:
        conn = get_connection()
    except (KeyError, psycopg2.Error) as exc:
        pytest.skip(f"Postgres unavailable: {exc}")

    schema_name = f"_test_betting_odds_{uuid.uuid4().hex}"
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


def _seed_fight(cur):
    event_id = "11111111-1111-1111-1111-111111111111"
    fight_id = "22222222-2222-2222-2222-222222222222"
    fighter_1_id = "33333333-3333-3333-3333-333333333333"
    fighter_2_id = "44444444-4444-4444-4444-444444444444"
    fighter_3_id = "55555555-5555-5555-5555-555555555555"

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
        (event_id,),
    )
    cur.execute(
        """
        INSERT INTO fighters (fighter_id, full_name, source_url)
        VALUES
            (%s, 'Fighter One', 'https://example.com/f1'),
            (%s, 'Fighter Two', 'https://example.com/f2'),
            (%s, 'Fighter Three', 'https://example.com/f3')
        """,
        (fighter_1_id, fighter_2_id, fighter_3_id),
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
        (fight_id, event_id, fighter_1_id, fighter_2_id),
    )
    return event_id, fight_id, fighter_1_id, fighter_2_id, fighter_3_id


def _insert_odds(cur, *, event_id, fight_id, fighter_id, opponent_id, timestamp, american_odds):
    cur.execute(
        """
        INSERT INTO fight_odds (
            fight_id,
            event_id,
            fighter_id,
            opponent_fighter_id,
            bookmaker,
            market,
            line_type,
            odds_timestamp,
            american_odds,
            source
        )
        VALUES (%s, %s, %s, %s, 'TestBook', 'moneyline', 'current', %s, %s, 'test')
        """,
        (fight_id, event_id, fighter_id, opponent_id, timestamp, american_odds),
    )


def test_betting_odds_migration_applies_and_creates_objects(migrated_conn):
    with migrated_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('fight_odds')")
        assert cur.fetchone()[0] == "fight_odds"

        cur.execute("SELECT to_regclass('fight_odds_no_vig')")
        assert cur.fetchone()[0] == "fight_odds_no_vig"

        cur.execute("SELECT to_regclass('latest_fight_odds')")
        assert cur.fetchone()[0] == "latest_fight_odds"


def test_fight_odds_constraints_reject_invalid_rows(migrated_conn):
    with migrated_conn.cursor() as cur:
        event_id, fight_id, fighter_1_id, fighter_2_id, _ = _seed_fight(cur)

        cur.execute("SAVEPOINT no_odds_row")
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO fight_odds (
                    fight_id,
                    event_id,
                    fighter_id,
                    opponent_fighter_id,
                    bookmaker,
                    market,
                    line_type,
                    odds_timestamp,
                    source
                )
                VALUES (%s, %s, %s, %s, 'TestBook', 'moneyline', 'current', now(), 'test')
                """,
                (fight_id, event_id, fighter_1_id, fighter_2_id),
            )
        cur.execute("ROLLBACK TO SAVEPOINT no_odds_row")

        cur.execute("SAVEPOINT invalid_market_row")
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO fight_odds (
                    fight_id,
                    event_id,
                    fighter_id,
                    opponent_fighter_id,
                    bookmaker,
                    market,
                    line_type,
                    odds_timestamp,
                    american_odds,
                    source
                )
                VALUES (%s, %s, %s, %s, 'TestBook', 'spread', 'current', now(), 150, 'test')
                """,
                (fight_id, event_id, fighter_1_id, fighter_2_id),
            )
        cur.execute("ROLLBACK TO SAVEPOINT invalid_market_row")


def test_no_vig_view_emits_only_exact_two_sided_markets(migrated_conn):
    with migrated_conn.cursor() as cur:
        event_id, fight_id, fighter_1_id, fighter_2_id, fighter_3_id = _seed_fight(cur)
        two_side_ts = "2026-07-31T12:00:00+00:00"
        one_side_ts = "2026-07-31T13:00:00+00:00"
        three_side_ts = "2026-07-31T14:00:00+00:00"

        _insert_odds(
            cur,
            event_id=event_id,
            fight_id=fight_id,
            fighter_id=fighter_1_id,
            opponent_id=fighter_2_id,
            timestamp=two_side_ts,
            american_odds=-120,
        )
        _insert_odds(
            cur,
            event_id=event_id,
            fight_id=fight_id,
            fighter_id=fighter_2_id,
            opponent_id=fighter_1_id,
            timestamp=two_side_ts,
            american_odds=100,
        )
        _insert_odds(
            cur,
            event_id=event_id,
            fight_id=fight_id,
            fighter_id=fighter_1_id,
            opponent_id=fighter_2_id,
            timestamp=one_side_ts,
            american_odds=-110,
        )
        _insert_odds(
            cur,
            event_id=event_id,
            fight_id=fight_id,
            fighter_id=fighter_1_id,
            opponent_id=fighter_2_id,
            timestamp=three_side_ts,
            american_odds=-110,
        )
        _insert_odds(
            cur,
            event_id=event_id,
            fight_id=fight_id,
            fighter_id=fighter_2_id,
            opponent_id=fighter_1_id,
            timestamp=three_side_ts,
            american_odds=100,
        )
        _insert_odds(
            cur,
            event_id=event_id,
            fight_id=fight_id,
            fighter_id=fighter_3_id,
            opponent_id=fighter_1_id,
            timestamp=three_side_ts,
            american_odds=200,
        )

        cur.execute(
            """
            SELECT count(*), sum(no_vig_implied_probability)::float
            FROM fight_odds_no_vig
            WHERE odds_timestamp = %s
            """,
            (two_side_ts,),
        )
        row_count, probability_sum = cur.fetchone()
        assert row_count == 2
        assert probability_sum == pytest.approx(1.0)

        cur.execute(
            """
            SELECT count(*)
            FROM fight_odds_no_vig
            WHERE odds_timestamp IN (%s, %s)
            """,
            (one_side_ts, three_side_ts),
        )
        assert cur.fetchone()[0] == 0
