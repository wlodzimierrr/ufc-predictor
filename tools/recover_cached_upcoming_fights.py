"""Recover upcoming fight rows from cached UFCStats event pages.

This is for pre-scrape catch-up prediction runs. It reads cached event detail
HTML under data/raw/ufcstats/events, extracts matchup rows, and appends missing
fighter stubs/fight rows as event_status=upcoming without reading result pages.
"""

from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

REPO_ROOT = Path(__file__).resolve().parent.parent
EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
RAW_EVENTS_DIR = REPO_ROOT / "data" / "raw" / "ufcstats" / "events"


def _format_href(url: str) -> str:
    return re.sub(r"(?<=://)www\.", "", url)


def _uuid(url: str) -> str:
    return str(uuid5(namespace=NAMESPACE_URL, name=_format_href(url)))


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


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
    return f"{weight_class} Bout" if weight_class else ""


def _event_ids_after(start_after: str, through: str) -> list[str]:
    _, events = _read_csv(EVENTS_CSV)
    ids = []
    for row in events:
        event_date = row.get("date_formatted", "")
        if start_after < event_date <= through and row.get("event_status") == "upcoming":
            ids.append(row["event_id"])
    return ids


def _parse_event_fights(event_id: str) -> list[dict[str, str]]:
    path = RAW_EVENTS_DIR / f"{event_id}.html"
    if not path.exists():
        return []

    html = path.read_text(encoding="utf-8")
    fights = []
    rows = re.findall(r"<tr class=\"b-fight-details__table-row.*?</tr>", html, re.S)
    for row in rows:
        fight_match = re.search(
            r"data-link=\"(http://www\.ufcstats\.com/fight-details/[a-z0-9]+)\"",
            row,
        )
        fighter_matches = re.findall(
            r"<a class=\"b-link b-link_style_black\" href=\""
            r"(http://www\.ufcstats\.com/fighter-details/[a-z0-9]+)\">\s*([^<]+?)\s*</a>",
            row,
            re.S,
        )
        weight_match = re.search(
            r"<td class=\"b-fight-details__table-col l-page_align_left\">\s*"
            r"<p class=\"b-fight-details__table-text\">\s*([^<\n]+)",
            row,
            re.S,
        )
        if not fight_match or len(fighter_matches) < 2:
            continue

        fights.append({
            "event_id": event_id,
            "url": fight_match.group(1),
            "fight_id": _uuid(fight_match.group(1)),
            "fighter_1_url": fighter_matches[0][0],
            "fighter_1_name": _clean(fighter_matches[0][1]),
            "fighter_1_id": _uuid(fighter_matches[0][0]),
            "fighter_2_url": fighter_matches[1][0],
            "fighter_2_name": _clean(fighter_matches[1][1]),
            "fighter_2_id": _uuid(fighter_matches[1][0]),
            "weight_class_label": _clean(weight_match.group(1)) if weight_match else "",
        })
    return fights


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover cached upcoming fight rows.")
    parser.add_argument("--after", required=True, help="Only events after this date, YYYY-MM-DD")
    parser.add_argument("--through", required=True, help="Events through this date, YYYY-MM-DD")
    args = parser.parse_args()

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    event_fields, event_rows = _read_csv(EVENTS_CSV)
    event_ids = _event_ids_after(args.after, args.through)
    recovered = []
    for event_id in event_ids:
        recovered.extend(_parse_event_fights(event_id))

    fighter_fields, fighter_rows = _read_csv(FIGHTERS_CSV)
    fight_fields, fight_rows = _read_csv(FIGHTS_CSV)
    existing_fighters = {row["fighter_id"] for row in fighter_rows}
    existing_fights = {row["fight_id"] for row in fight_rows}

    new_fighters = []
    for fight in recovered:
        for side in ("1", "2"):
            fighter_id = fight[f"fighter_{side}_id"]
            if fighter_id in existing_fighters:
                continue
            full_name = fight[f"fighter_{side}_name"]
            first_name, last_names = _split_name(full_name)
            new_fighters.append({
                "scraped_at": scraped_at,
                "fighter_id": fighter_id,
                "url": _format_href(fight[f"fighter_{side}_url"]),
                "full_name": full_name,
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
            })
            existing_fighters.add(fighter_id)

    new_fights = []
    for fight in recovered:
        if fight["fight_id"] in existing_fights:
            continue
        new_fights.append({
            "scraped_at": scraped_at,
            "fight_id": fight["fight_id"],
            "event_id": fight["event_id"],
            "url": fight["url"],
            "fighter_1_id": fight["fighter_1_id"],
            "fighter_2_id": fight["fighter_2_id"],
            "fighter_1_outcome": "",
            "fighter_2_outcome": "",
            "bout_type": _bout_type(fight["weight_class_label"]),
            "weight_class": fight["weight_class_label"].lower().replace(" ", "_"),
            "num_rounds": "",
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
        existing_fights.add(fight["fight_id"])

    if new_fighters:
        fighter_rows.extend(new_fighters)
        _write_csv(FIGHTERS_CSV, fighter_fields, fighter_rows)
    if new_fights:
        fight_rows.extend(new_fights)
        _write_csv(FIGHTS_CSV, fight_fields, fight_rows)

    by_event: dict[str, list[dict[str, str]]] = {}
    for fight in recovered:
        by_event.setdefault(fight["event_id"], []).append(fight)
    updated_events = 0
    for row in event_rows:
        fights = by_event.get(row["event_id"])
        if not fights:
            continue
        row["fights"] = ", ".join(fight["fight_id"] for fight in fights)
        row["fight_urls"] = ", ".join(fight["url"] for fight in fights)
        row["event_status"] = "upcoming"
        updated_events += 1
    if updated_events:
        _write_csv(EVENTS_CSV, event_fields, event_rows)

    print(f"Recovered cached matchups: {len(recovered)}")
    print(f"Added fighter stubs: {len(new_fighters)}")
    print(f"Added upcoming fight rows: {len(new_fights)}")
    print(f"Updated event rows: {updated_events}")


if __name__ == "__main__":
    main()
