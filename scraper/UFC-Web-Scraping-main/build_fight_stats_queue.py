#!/usr/bin/env python3
"""Build the canonical fight stats queue (T1.5.1).

Consolidates completed fight URLs from local data sources into a single,
deduplicated queue that the fight-stats spiders read instead of rediscovering
fights independently.  No network calls are made.

Sources (consulted in priority order):
  1. data/fights.csv                    — already-scraped fight rows; provides
                                           fight_id, event_id, url, finish_method
  2. data/manifests/events_manifest.csv — canonical event registry; fight_urls
                                           column added in T1.3.2; used to ingest
                                           fights from events not yet in fights.csv
  3. data/events.csv                    — parsed event output; fight_urls column
                                           added in T1.3.3; secondary fallback

Event filtering:
  Only fights whose parent event has event_status == "completed" are queued.
  Future fights (upcoming events) are excluded.  Fights from events absent in
  events.csv are included conservatively (defaulting to completed).

stats_status values:
  pending   — fight should have a stats page; not yet fetched
  no_stats  — (reserved for future use by the stats spider after a 404 or
               explicit empty-stats detection)

Output:
  data/manifests/fight_stats_queue.csv

Schema:
  fight_id, event_id, fight_url, finish_method, stats_status, queued_at

Idempotency:
  Re-running never duplicates entries.  Existing rows retain their
  original queued_at and stats_status.  New fights are appended.

Usage:
    python3 build_fight_stats_queue.py    # from scraper/UFC-Web-Scraping-main/
    make build_stats_queue
"""

import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

_QUEUE_PATH = DATA_DIR / "manifests" / "fight_stats_queue.csv"
_FIGHTS_CSV = DATA_DIR / "fights.csv"
_EVENTS_CSV = DATA_DIR / "events.csv"
_EVENTS_MANIFEST = DATA_DIR / "manifests" / "events_manifest.csv"

_QUEUE_FIELDS = [
    "fight_id",
    "event_id",
    "fight_url",
    "finish_method",
    "stats_status",
    "queued_at",
]


# ---------------------------------------------------------------------------
# UUID helper — mirrors ufc_scraper/utils.py without the Scrapy dependency
# ---------------------------------------------------------------------------


def _normalise_url(url: str) -> str:
    return re.sub(r"(?<=://)www\.", "", url.strip())


def _fight_id(url: str) -> str:
    return str(uuid5(NAMESPACE_URL, _normalise_url(url)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _parse_fight_urls(raw: str) -> list[str]:
    """Split a comma-separated fight_urls cell into individual URLs."""
    return [u.strip() for u in raw.split(",") if u.strip() and "fight-details" in u]


# ---------------------------------------------------------------------------
# Event status index
# ---------------------------------------------------------------------------


def _build_event_status_index() -> dict[str, str]:
    """Return {event_id: event_status} from events.csv and events_manifest.csv."""
    index: dict[str, str] = {}

    for row in _load_csv(_EVENTS_CSV):
        eid = row.get("event_id", "").strip()
        status = row.get("event_status", "completed").strip()
        if eid:
            index[eid] = status

    # events_manifest.csv takes precedence (more up-to-date status)
    for row in _load_csv(_EVENTS_MANIFEST):
        eid = row.get("event_id", "").strip()
        status = row.get("event_status", "completed").strip()
        if eid:
            index[eid] = status

    return index


# ---------------------------------------------------------------------------
# Source loaders
# ---------------------------------------------------------------------------


def _load_fights_csv(event_status: dict[str, str]) -> dict[str, dict]:
    """Source 1: fights.csv — authoritative for already-scraped fights."""
    rows: dict[str, dict] = {}
    for row in _load_csv(_FIGHTS_CSV):
        url = row.get("url", "").strip()
        if not url or "fight-details" not in url:
            continue
        event_id = row.get("event_id", "").strip()
        # Exclude fights whose event is known to be upcoming.
        if event_status.get(event_id, "completed") == "upcoming":
            continue
        fid = row.get("fight_id", "").strip() or _fight_id(url)
        rows[fid] = {
            "fight_id": fid,
            "event_id": event_id,
            "fight_url": _normalise_url(url),
            "finish_method": row.get("finish_method", "").strip(),
        }
    return rows


def _load_event_fight_urls(
    source_rows: list[dict], event_status: dict[str, str]
) -> dict[str, dict]:
    """Sources 2 & 3: extract fight URLs from events_manifest or events.csv rows."""
    rows: dict[str, dict] = {}
    for row in source_rows:
        event_id = row.get("event_id", "").strip()
        if event_status.get(event_id, "completed") == "upcoming":
            continue
        fight_urls_raw = row.get("fight_urls", "").strip()
        if not fight_urls_raw:
            continue
        for url in _parse_fight_urls(fight_urls_raw):
            fid = _fight_id(url)
            if fid not in rows:
                rows[fid] = {
                    "fight_id": fid,
                    "event_id": event_id,
                    "fight_url": _normalise_url(url),
                    "finish_method": "",  # not available from event source
                }
    return rows


# ---------------------------------------------------------------------------
# Existing queue
# ---------------------------------------------------------------------------


def _load_existing_queue() -> dict[str, dict]:
    rows: dict[str, dict] = {}
    if not _QUEUE_PATH.exists():
        return rows
    with _QUEUE_PATH.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            fid = row.get("fight_id", "").strip()
            if fid:
                rows[fid] = dict(row)
    return rows


# ---------------------------------------------------------------------------
# Merge and write
# ---------------------------------------------------------------------------


def build_queue() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    event_status = _build_event_status_index()
    existing = _load_existing_queue()

    # Build candidates from all sources (lower priority first, higher overwrites).
    candidates: dict[str, dict] = {}

    # Source 3: events.csv fight_urls (added in T1.3.3; may not exist yet)
    candidates.update(_load_event_fight_urls(_load_csv(_EVENTS_CSV), event_status))

    # Source 2: events_manifest.csv fight_urls (added in T1.3.2)
    candidates.update(_load_event_fight_urls(_load_csv(_EVENTS_MANIFEST), event_status))

    # Source 1: fights.csv (highest priority — has finish_method)
    candidates.update(_load_fights_csv(event_status))

    # Merge with existing queue.
    merged: dict[str, dict] = {}
    for fid, candidate in candidates.items():
        if fid in existing:
            # Preserve queued_at and stats_status; update fight details.
            merged[fid] = {
                **existing[fid],
                "fight_url": candidate["fight_url"],
                "event_id": candidate["event_id"],
                "finish_method": candidate.get("finish_method") or existing[fid].get("finish_method", ""),
            }
        else:
            merged[fid] = {
                **candidate,
                "stats_status": "pending",
                "queued_at": now,
            }

    # Carry forward any existing entries not found in current sources.
    for fid, row in existing.items():
        if fid not in merged:
            merged[fid] = row

    total = len(merged)
    new_count = total - len(existing)

    # Source breakdown
    src_counts = {
        "fights_csv": sum(1 for r in merged.values() if r.get("finish_method", "")),
        "event_source": sum(1 for r in merged.values() if not r.get("finish_method", "")),
    }

    _QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _QUEUE_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_QUEUE_FIELDS)
        writer.writeheader()
        writer.writerows(
            sorted(merged.values(), key=lambda r: (r.get("event_id", ""), r.get("fight_id", "")))
        )

    print(f"Fight stats queue written to {_QUEUE_PATH}")
    print(f"  Total entries   : {total}")
    print(f"  New this run    : {new_count}")
    print(f"  From fights.csv : {src_counts['fights_csv']}")
    print(f"  From events src : {src_counts['event_source']}")
    print(f"  Excluded future : {sum(1 for s in event_status.values() if s == 'upcoming')} upcoming events filtered")


if __name__ == "__main__":
    try:
        build_queue()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
