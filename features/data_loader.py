"""Load all warehouse data into memory for feature engineering.

Loads four tables (events, fighters, fights, fight_stats_aggregate) in a single
pass and builds lookup indexes so feature modules never hit the DB again.

Usage:
    conn = get_connection()
    data = load_all_data(conn)
    conn.close()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


Row = dict[str, Any]


@dataclass
class WarehouseData:
    """All warehouse data loaded into memory, plus common lookup indexes.

    Attributes:
        events:          list of event rows keyed by event_id
        fighters:        list of fighter rows keyed by fighter_id
        fights:          list of fight rows keyed by fight_id
        fight_stats:     list of fight_stats_aggregate rows

        event_by_id:     event_id  -> event row
        fighter_by_id:   fighter_id -> fighter row
        fight_by_id:     fight_id  -> fight row
        stats_by_fight:  fight_id  -> list of stat rows (2 per fight: one per fighter)
        stats_by_fight_fighter:  (fight_id, fighter_id) -> stat row
    """

    events: list[Row] = field(default_factory=list)
    fighters: list[Row] = field(default_factory=list)
    fights: list[Row] = field(default_factory=list)
    fight_stats: list[Row] = field(default_factory=list)

    event_by_id: dict[str, Row] = field(default_factory=dict)
    fighter_by_id: dict[str, Row] = field(default_factory=dict)
    fight_by_id: dict[str, Row] = field(default_factory=dict)
    stats_by_fight: dict[str, list[Row]] = field(default_factory=dict)
    stats_by_fight_fighter: dict[tuple[str, str], Row] = field(default_factory=dict)


def load_all_data(conn) -> WarehouseData:
    """Load events, fighters, fights, and fight_stats_aggregate into memory.

    All four tables are fetched once with cursor.fetchall() and stored as
    lists of plain dicts. Lookup indexes are built before returning.

    Args:
        conn: open psycopg2 connection.

    Returns:
        WarehouseData with all rows and indexes populated.
    """
    data = WarehouseData()

    with conn.cursor() as cur:
        # events
        cur.execute("""
            SELECT event_id::text, event_name, event_date, city, state, country,
                   event_status, source_url
            FROM events
            ORDER BY event_date
        """)
        cols = [d[0] for d in cur.description]
        data.events = [dict(zip(cols, row)) for row in cur.fetchall()]
        data.event_by_id = {r["event_id"]: r for r in data.events}

        # fighters
        cur.execute("""
            SELECT fighter_id::text, full_name, first_name, last_name, nickname,
                   height_cm, weight_lbs, reach_cm, stance, dob, source_url
            FROM fighters
        """)
        cols = [d[0] for d in cur.description]
        data.fighters = [dict(zip(cols, row)) for row in cur.fetchall()]
        data.fighter_by_id = {r["fighter_id"]: r for r in data.fighters}

        # fights
        cur.execute("""
            SELECT fight_id::text, event_id::text,
                   fighter_1_id::text, fighter_2_id::text,
                   winner_fighter_id::text,
                   result_type, weight_class,
                   is_title_fight, is_interim_title,
                   scheduled_rounds, finish_method, finish_detail,
                   finish_round, finish_time_seconds, referee, source_url
            FROM fights
        """)
        cols = [d[0] for d in cur.description]
        data.fights = [dict(zip(cols, row)) for row in cur.fetchall()]
        data.fight_by_id = {r["fight_id"]: r for r in data.fights}

        # fight_stats_aggregate
        cur.execute("""
            SELECT fight_stat_id::text, fight_id::text, fighter_id::text,
                   knockdowns,
                   total_strikes_landed, total_strikes_attempted,
                   sig_strikes_landed, sig_strikes_attempted,
                   sig_strikes_head_landed, sig_strikes_head_attempted,
                   sig_strikes_body_landed, sig_strikes_body_attempted,
                   sig_strikes_leg_landed, sig_strikes_leg_attempted,
                   sig_strikes_distance_landed, sig_strikes_distance_attempted,
                   sig_strikes_clinch_landed, sig_strikes_clinch_attempted,
                   sig_strikes_ground_landed, sig_strikes_ground_attempted,
                   takedowns_landed, takedowns_attempted,
                   control_time_seconds, submissions_attempted, reversals
            FROM fight_stats_aggregate
        """)
        cols = [d[0] for d in cur.description]
        data.fight_stats = [dict(zip(cols, row)) for row in cur.fetchall()]

        for stat in data.fight_stats:
            fid = stat["fight_id"]
            data.stats_by_fight.setdefault(fid, []).append(stat)
            key = (fid, stat["fighter_id"])
            data.stats_by_fight_fighter[key] = stat

    # Attach event_date to every fight for easy access
    for fight in data.fights:
        event = data.event_by_id.get(fight["event_id"])
        fight["event_date"] = event["event_date"] if event else None

    return data
