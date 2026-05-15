"""Load upcoming fight cards into the warehouse.

Reads fight_urls from upcoming events in the events manifest, matches them
against fights.csv, and inserts as upcoming fights. For fighters not yet in
the warehouse, inserts stubs from fighters.csv.

Upcoming fights have result_type='upcoming', no winner, no finish details.

Usage:
    python warehouse/load_upcoming_fights.py

    Or manually specify fights via CSV:
    python warehouse/load_upcoming_fights.py --csv path/to/upcoming.csv

    The CSV format is:
    event_name,fighter_1_name,fighter_2_name,weight_class,scheduled_rounds,is_title_fight
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.csv_utils import iter_data_rows
from warehouse.db import get_connection, upsert
from warehouse.transform import _extract_bout_flags, _str

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_CSV = REPO_ROOT / "data" / "manifests" / "events_manifest.csv"
FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"


def _load_upcoming_events(conn) -> list[dict]:
    """Get upcoming events from the warehouse."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_id, event_name, event_date
            FROM events
            WHERE event_status = 'upcoming'
            ORDER BY event_date ASC
        """)
        rows = cur.fetchall()
    return [{"event_id": str(r[0]), "event_name": r[1], "event_date": r[2]} for r in rows]


def _load_fight_urls_from_manifest() -> dict[str, list[str]]:
    """Return {event_id: [fight_url, ...]} for upcoming events from manifest."""
    if not MANIFEST_CSV.exists():
        return {}

    result: dict[str, list[str]] = {}
    for row in iter_data_rows(MANIFEST_CSV):
        if row.get("event_status") == "upcoming" and row.get("fight_urls"):
            urls = [u.strip() for u in row["fight_urls"].split(",") if u.strip()]
            if urls:
                result[row["event_id"]] = urls
    return result


def _load_fights_csv_index() -> dict[str, dict]:
    """Index fights.csv by fight URL for quick lookup."""
    if not FIGHTS_CSV.exists():
        return {}

    index = {}
    for row in iter_data_rows(FIGHTS_CSV):
        url = _str(row.get("url"))
        if url:
            index[url] = row
    return index


def _fighter_name_to_id(conn) -> dict[str, str]:
    """Return {lowercase full_name: fighter_id} from the fighters table."""
    with conn.cursor() as cur:
        cur.execute("SELECT fighter_id, full_name FROM fighters")
        return {r[1].lower().strip(): str(r[0]) for r in cur.fetchall() if r[1]}


def _existing_fight_ids(conn) -> set[str]:
    """Return set of fight_ids already in the fights table."""
    with conn.cursor() as cur:
        cur.execute("SELECT fight_id FROM fights")
        return {str(r[0]) for r in cur.fetchall()}


def _make_fight_id(event_id: str, fighter_1_id: str, fighter_2_id: str) -> str:
    """Generate a deterministic fight ID for manually entered fights."""
    combined = f"{event_id}:{fighter_1_id}:{fighter_2_id}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, combined))


def load_from_scraped_data(conn) -> int:
    """Load upcoming fights from scraped data (fights.csv + manifest)."""
    upcoming_events = _load_upcoming_events(conn)
    if not upcoming_events:
        print("  No upcoming events in the warehouse.")
        print("  Run the scraper first: cd scraper/UFC-Web-Scraping-main && make update_events")
        return 0

    print(f"  Found {len(upcoming_events)} upcoming event(s):")
    for e in upcoming_events:
        print(f"    {e['event_date']}  {e['event_name']}")

    fight_urls = _load_fight_urls_from_manifest()
    fights_index = _load_fights_csv_index()
    existing = _existing_fight_ids(conn)

    rows = []
    for event in upcoming_events:
        eid = event["event_id"]
        urls = fight_urls.get(eid, [])

        for url in urls:
            fight_data = fights_index.get(url)
            if not fight_data:
                continue

            fid = fight_data["fight_id"]
            if fid in existing:
                continue

            bout_type = _str(fight_data.get("bout_type"))
            weight_class, is_title, is_interim = _extract_bout_flags(bout_type)

            rows.append({
                "fight_id": fid,
                "event_id": eid,
                "fighter_1_id": fight_data["fighter_1_id"],
                "fighter_2_id": fight_data["fighter_2_id"],
                "winner_fighter_id": None,
                "result_type": "upcoming",
                "weight_class": weight_class,
                "is_title_fight": is_title,
                "is_interim_title": is_interim,
                "scheduled_rounds": int(fight_data.get("num_rounds", 3)) if fight_data.get("num_rounds") else 3,
                "finish_method": None,
                "finish_detail": None,
                "finish_round": None,
                "finish_time_seconds": None,
                "referee": None,
                "source_url": url,
                "scraped_at": datetime.now(timezone.utc),
            })

    if not rows:
        print("  No new upcoming fights to load.")
        return 0

    with conn:
        n = upsert(conn, "fights", rows, pk_columns=["fight_id"])
    print(f"  Loaded {n} upcoming fight(s)")
    return n


def load_from_csv(conn, csv_path: Path) -> int:
    """Load upcoming fights from a manual CSV file.

    CSV format:
    event_name,fighter_1_name,fighter_2_name,weight_class,scheduled_rounds,is_title_fight
    """
    name_to_id = _fighter_name_to_id(conn)
    existing = _existing_fight_ids(conn)

    # Get event name → event_id mapping
    with conn.cursor() as cur:
        cur.execute("SELECT event_id, event_name FROM events")
        event_map = {r[1].lower().strip(): str(r[0]) for r in cur.fetchall() if r[1]}

    rows = []
    warnings = []
    for raw in iter_data_rows(csv_path):
        event_name = _str(raw.get("event_name", ""))
        f1_name = _str(raw.get("fighter_1_name", ""))
        f2_name = _str(raw.get("fighter_2_name", ""))

        if not all([event_name, f1_name, f2_name]):
            continue

        event_id = event_map.get(event_name.lower())
        if not event_id:
            warnings.append(f"Unknown event: {event_name}")
            continue

        f1_id = name_to_id.get(f1_name.lower())
        f2_id = name_to_id.get(f2_name.lower())

        if not f1_id:
            warnings.append(f"Unknown fighter: {f1_name}")
            continue
        if not f2_id:
            warnings.append(f"Unknown fighter: {f2_name}")
            continue

        fight_id = _make_fight_id(event_id, f1_id, f2_id)
        if fight_id in existing:
            continue

        wc = _str(raw.get("weight_class"))
        rounds = int(raw.get("scheduled_rounds", 3)) if raw.get("scheduled_rounds") else 3
        is_title = raw.get("is_title_fight", "").lower() in ("true", "1", "yes")

        rows.append({
            "fight_id": fight_id,
            "event_id": event_id,
            "fighter_1_id": f1_id,
            "fighter_2_id": f2_id,
            "winner_fighter_id": None,
            "result_type": "upcoming",
            "weight_class": wc,
            "is_title_fight": is_title,
            "is_interim_title": False,
            "scheduled_rounds": rounds,
            "finish_method": None,
            "finish_detail": None,
            "finish_round": None,
            "finish_time_seconds": None,
            "referee": None,
            "source_url": None,
            "scraped_at": datetime.now(timezone.utc),
        })

    for w in warnings:
        print(f"  warn  {w}")

    if not rows:
        print("  No valid upcoming fights to load from CSV.")
        return 0

    with conn:
        n = upsert(conn, "fights", rows, pk_columns=["fight_id"])
    print(f"  Loaded {n} upcoming fight(s) from {csv_path.name}")
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Load upcoming UFC fights")
    parser.add_argument("--csv", type=Path, help="Manual CSV file with upcoming fights")
    args = parser.parse_args()

    print("Loading upcoming fights...")
    conn = get_connection()
    try:
        if args.csv:
            load_from_csv(conn, args.csv)
        else:
            load_from_scraped_data(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
