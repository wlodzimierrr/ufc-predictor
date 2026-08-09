"""Apply manual active-card rows for catch-up prediction windows.

Use this when an old pre-result UFCStats event cache was only a partial card.
The script updates local CSVs so selected event-date windows can be scored as
``upcoming`` before loading completed results.
"""

from __future__ import annotations

import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"

JULY_18_EVENT_ID = "68a758a6-bd6a-5ec7-933e-72251614d52f"
AUG_01_EVENT_ID = "6952ee17-a74d-5432-832f-b9c6d22f8e65"

ACTIVE_BOUTS = [
    # UFC Fight Night: Du Plessis vs. Usman, July 18, 2026
    (JULY_18_EVENT_ID, "Dricus Du Plessis", "Kamaru Usman", "Middleweight", 5),
    (JULY_18_EVENT_ID, "Jared Cannonier", "Christian Leroy Duncan", "Middleweight", 3),
    (JULY_18_EVENT_ID, "Chase Hooper", "Mitch Ramirez", "Lightweight", 3),
    (JULY_18_EVENT_ID, "Tabatha Ricci", "Fatima Kline", "Women's Strawweight", 3),
    (JULY_18_EVENT_ID, "Tommy McMillen", "Alberto Montes", "Featherweight", 3),
    (JULY_18_EVENT_ID, "Austin Bashi", "Jose Miguel Delgado", "Featherweight", 3),
    (JULY_18_EVENT_ID, "Jean-Paul Lebosnoyani", "Seokhyeon Ko", "Welterweight", 3),
    (JULY_18_EVENT_ID, "Levi Rodrigues Jr.", "Felipe Franco", "Light Heavyweight", 3),
    (JULY_18_EVENT_ID, "Ezra Elliott", "Damien Anderson", "Featherweight", 3),
    (JULY_18_EVENT_ID, "Alden Coria", "Stewart Nicoll", "Flyweight", 3),
    (JULY_18_EVENT_ID, "RJ Harris", "Alvin Hines", "Heavyweight", 3),
    (JULY_18_EVENT_ID, "Anna Melisano", "Dione Barbosa", "Women's Flyweight", 3),
    # UFC Fight Night: Medic vs. Rodriguez, August 1, 2026
    (AUG_01_EVENT_ID, "Uros Medic", "Daniel Rodriguez", "Welterweight", 5),
    (AUG_01_EVENT_ID, "Jan Blachowicz", "Navajo Stirling", "Light Heavyweight", 3),
    (AUG_01_EVENT_ID, "Aleksandar Rakic", "Marcin Tybura", "Heavyweight", 3),
    (AUG_01_EVENT_ID, "Dusko Todorovic", "Robert Valentin", "Middleweight", 3),
    (AUG_01_EVENT_ID, "Vlasto Cepo", "Gilbert Urbina", "Middleweight", 3),
    (AUG_01_EVENT_ID, "Milos Janicic", "Noah Gugnon", "Lightweight", 3),
    (AUG_01_EVENT_ID, "Ludovit Klein", "Tofiq Musayev", "Lightweight", 3),
    (AUG_01_EVENT_ID, "Oban Elliott", "Michael Oliveira", "Welterweight", 3),
    (AUG_01_EVENT_ID, "Borislav Nikolic", "Mark Vologdin", "Bantamweight", 3),
    (AUG_01_EVENT_ID, "Dennis Buzukja", "Bogdan Grad", "Featherweight", 3),
    (AUG_01_EVENT_ID, "Mateusz Rebecki", "Kyle Prepolec", "Lightweight", 3),
    (AUG_01_EVENT_ID, "Nina Milosevic", "Hailey Cowan", "Women's Bantamweight", 3),
    (AUG_01_EVENT_ID, "Jovan Leka", "Alexander Poppeck", "Heavyweight", 3),
    (AUG_01_EVENT_ID, "Marina Spasic", "Stephanie Luciano", "Women's Strawweight", 3),
]

STALE_BOUTS = [
    (JULY_18_EVENT_ID, "Brad Tavares", "Marc-Andre Barriault"),
    (JULY_18_EVENT_ID, "Kevin Holland", "Jacobe Smith"),
    (AUG_01_EVENT_ID, "Ante Delija", "Johnny Walker"),
    (AUG_01_EVENT_ID, "Jovan Leka", "Max Gimenis"),
]


def _uuid(value: str) -> str:
    return str(uuid5(namespace=NAMESPACE_URL, name=value))


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.split()
    if len(parts) <= 1:
        return full_name, ""
    return parts[0], " ".join(parts[1:])


def _bout_type(weight_class: str) -> str:
    return f"{weight_class} Bout"


def _fighter_rows_by_name(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    by_name = {}
    for row in rows:
        name = row.get("full_name", "").strip()
        if name:
            by_name[name] = row
    return by_name


def _load_warehouse_fighters(names: list[str]) -> dict[str, dict[str, str]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fighter_id::text, full_name, source_url
                FROM fighters
                WHERE full_name = ANY(%s)
                """,
                (names,),
            )
            return {
                row[1]: {
                    "fighter_id": row[0],
                    "full_name": row[1],
                    "url": row[2] or "",
                }
                for row in cur.fetchall()
            }


def _ensure_fighter(
    name: str,
    local_by_name: dict[str, dict[str, str]],
    warehouse_by_name: dict[str, dict[str, str]],
    fighter_rows: list[dict[str, str]],
    scraped_at: str,
) -> str:
    if name in warehouse_by_name:
        return warehouse_by_name[name]["fighter_id"]
    if name in local_by_name:
        return local_by_name[name]["fighter_id"]

    fighter_id = _uuid(f"manual:fighter:{name.lower()}")
    first_name, last_names = _split_name(name)
    row = {
        "scraped_at": scraped_at,
        "fighter_id": fighter_id,
        "url": f"manual://fighter/{_slug(name)}",
        "full_name": name,
        "first_name": first_name,
        "last_names": last_names,
        "nickname": "",
        "height_ft": "",
        "height_in": "",
        "height_cm": "",
        "weight_lbs": "",
        "reach_in": "",
        "reach_cm": "",
        "stance": "",
        "dob": "",
        "dob_formatted": "",
        "record": "",
        "wins": "",
        "losses": "",
        "draws": "",
        "no_contests": "",
        "fight_ids": "",
    }
    fighter_rows.append(row)
    local_by_name[name] = row
    return fighter_id


def _pair_key(event_id: str, fighter_1_id: str, fighter_2_id: str) -> tuple[str, frozenset[str]]:
    return event_id, frozenset((fighter_1_id, fighter_2_id))


def main() -> None:
    event_fields, event_rows = _read_csv(EVENTS_CSV)
    fighter_fields, fighter_rows = _read_csv(FIGHTERS_CSV)
    fight_fields, fight_rows = _read_csv(FIGHTS_CSV)
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    names = sorted({name for _, f1, f2, *_ in ACTIVE_BOUTS for name in (f1, f2)}
                   | {name for _, f1, f2 in STALE_BOUTS for name in (f1, f2)})
    local_by_name = _fighter_rows_by_name(fighter_rows)
    warehouse_by_name = _load_warehouse_fighters(names)

    fighter_ids = {
        name: _ensure_fighter(name, local_by_name, warehouse_by_name, fighter_rows, scraped_at)
        for name in names
    }

    fight_by_pair = {}
    for row in fight_rows:
        f1_id = row.get("fighter_1_id", "")
        f2_id = row.get("fighter_2_id", "")
        if row.get("event_id") and f1_id and f2_id:
            fight_by_pair[_pair_key(row["event_id"], f1_id, f2_id)] = row

    added = 0
    updated = 0
    active_fight_ids = set()
    for event_id, f1_name, f2_name, weight_class, rounds in ACTIVE_BOUTS:
        f1_id = fighter_ids[f1_name]
        f2_id = fighter_ids[f2_name]
        key = _pair_key(event_id, f1_id, f2_id)
        row = fight_by_pair.get(key)
        if row is None:
            fight_id = _uuid(f"manual:fight:{event_id}:{f1_id}:{f2_id}")
            row = {field: "" for field in fight_fields}
            row.update({
                "scraped_at": scraped_at,
                "fight_id": fight_id,
                "event_id": event_id,
                "url": f"manual://fight/{event_id}/{_slug(f1_name)}-vs-{_slug(f2_name)}",
                "fighter_1_id": f1_id,
                "fighter_2_id": f2_id,
            })
            fight_rows.append(row)
            fight_by_pair[key] = row
            added += 1
        else:
            updated += 1

        row.update({
            "fighter_1_outcome": "",
            "fighter_2_outcome": "",
            "bout_type": _bout_type(weight_class),
            "weight_class": weight_class.lower().replace("women's ", "women_").replace(" ", "_"),
            "num_rounds": str(rounds),
            "finish_method": "",
            "primary_finish_method": "",
            "secondary_finish_method": "",
            "finish_round": "",
            "finish_time_minute": "",
            "finish_time_second": "",
            "referee": "",
            "judge_1": "",
            "judge_2": "",
            "judge_3": "",
            "event_status": "upcoming",
        })
        active_fight_ids.add(row["fight_id"])

    stale_updated = 0
    for event_id, f1_name, f2_name in STALE_BOUTS:
        f1_id = fighter_ids[f1_name]
        f2_id = fighter_ids[f2_name]
        row = fight_by_pair.get(_pair_key(event_id, f1_id, f2_id))
        if row and row["fight_id"] not in active_fight_ids:
            row["event_status"] = "canceled"
            stale_updated += 1

    fights_by_event: dict[str, list[dict[str, str]]] = {}
    for row in fight_rows:
        if row.get("event_id") in {JULY_18_EVENT_ID, AUG_01_EVENT_ID} and row.get("event_status") == "upcoming":
            fights_by_event.setdefault(row["event_id"], []).append(row)

    for row in event_rows:
        if row.get("event_id") == JULY_18_EVENT_ID:
            row["name"] = "UFC Fight Night: Du Plessis vs. Usman"
            row["event_status"] = "upcoming"
        elif row.get("event_id") == AUG_01_EVENT_ID:
            row["event_status"] = "upcoming"
        else:
            continue

        event_fights = fights_by_event.get(row["event_id"], [])
        row["fights"] = ", ".join(fight["fight_id"] for fight in event_fights)
        row["fight_urls"] = ", ".join(fight["url"] for fight in event_fights)

    _write_csv(FIGHTERS_CSV, fighter_fields, fighter_rows)
    _write_csv(FIGHTS_CSV, fight_fields, fight_rows)
    _write_csv(EVENTS_CSV, event_fields, event_rows)

    print(f"Active bouts: {len(ACTIVE_BOUTS)}")
    print(f"Added fight rows: {added}")
    print(f"Updated active fight rows: {updated}")
    print(f"Marked stale rows canceled: {stale_updated}")
    print(f"Fighter rows total: {len(fighter_rows)}")


if __name__ == "__main__":
    main()
