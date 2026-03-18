#!/usr/bin/env python3
"""Build the canonical fighter profile queue (T1.4.1).

Consolidates fighter profile URLs from two local sources into a single,
deduplicated manifest.  No network calls are made; only existing artifacts
in data/ are read.

Sources (in priority order):
  1. data/fighters.csv          — already-scraped fighter profiles (url column)
  2. data/raw/ufcstats/fights/  — raw fight HTML artifacts, each page embeds
                                   two fighter-details URLs

Output:
  data/manifests/fighter_queue.csv

Schema:
  fighter_id   — UUID5 of the normalised profile URL (stable dedupe key)
  fighter_url  — canonical profile URL (www. stripped)
  source       — first source that introduced this fighter
                 ("fighters_csv" | "fight_page")
  queued_at    — ISO-8601 UTC timestamp set once on first appearance

Idempotency:
  Re-running the script never duplicates entries.  Existing rows retain their
  original queued_at timestamp.  New fighters discovered since the last run
  are appended.

Usage:
    python3 build_fighter_queue.py            # from scraper/UFC-Web-Scraping-main/
    make build_queue
"""

import csv
import re
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

_QUEUE_PATH = DATA_DIR / "manifests" / "fighter_queue.csv"
_FIGHTERS_CSV = DATA_DIR / "fighters.csv"
_FIGHTS_RAW_DIR = DATA_DIR / "raw" / "ufcstats" / "fights"

_QUEUE_FIELDS = ["fighter_id", "fighter_url", "source", "queued_at"]


# ---------------------------------------------------------------------------
# UUID helpers  (mirrors ufc_scraper/utils.py without the Scrapy dependency)
# ---------------------------------------------------------------------------


def _normalise_url(url: str) -> str:
    """Strip www. subdomain so UUID generation matches the spider's logic."""
    return re.sub(r"(?<=://)www\.", "", url.strip())


def _fighter_id(url: str) -> str:
    return str(uuid5(NAMESPACE_URL, _normalise_url(url)))


# ---------------------------------------------------------------------------
# Source 1 — fighters.csv
# ---------------------------------------------------------------------------


def _load_fighters_csv() -> dict[str, dict]:
    """Return {fighter_id: queue_row} from data/fighters.csv."""
    rows: dict[str, dict] = {}
    if not _FIGHTERS_CSV.exists():
        return rows

    with _FIGHTERS_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            url = row.get("url", "").strip()
            if not url or "fighter-details" not in url:
                continue
            fid = _fighter_id(url)
            canonical = _normalise_url(url)
            rows[fid] = {
                "fighter_id": fid,
                "fighter_url": canonical,
                "source": "fighters_csv",
            }

    return rows


# ---------------------------------------------------------------------------
# Source 2 — raw fight HTML
# ---------------------------------------------------------------------------


class _FighterUrlExtractor(HTMLParser):
    """Collect all href values that contain 'fighter-details'."""

    def __init__(self) -> None:
        super().__init__()
        self.urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for attr, value in attrs:
            if attr == "href" and value and "fighter-details" in value:
                self.urls.append(value)


def _load_fight_pages() -> dict[str, dict]:
    """Return {fighter_id: queue_row} from raw fight HTML artifacts."""
    rows: dict[str, dict] = {}
    if not _FIGHTS_RAW_DIR.exists():
        return rows

    for html_file in _FIGHTS_RAW_DIR.glob("*.html"):
        try:
            content = html_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        extractor = _FighterUrlExtractor()
        extractor.feed(content)

        for url in extractor.urls:
            fid = _fighter_id(url)
            if fid not in rows:
                rows[fid] = {
                    "fighter_id": fid,
                    "fighter_url": _normalise_url(url),
                    "source": "fight_page",
                }

    return rows


# ---------------------------------------------------------------------------
# Existing queue  (preserves queued_at for returning entries)
# ---------------------------------------------------------------------------


def _load_existing_queue() -> dict[str, dict]:
    """Return {fighter_id: queue_row} from the current queue file, if any."""
    rows: dict[str, dict] = {}
    if not _QUEUE_PATH.exists():
        return rows

    with _QUEUE_PATH.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            fid = row.get("fighter_id", "").strip()
            if fid:
                rows[fid] = dict(row)

    return rows


# ---------------------------------------------------------------------------
# Merge and write
# ---------------------------------------------------------------------------


def build_queue() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    existing = _load_existing_queue()
    source1 = _load_fighters_csv()
    source2 = _load_fight_pages()

    # Merge: sources in priority order; existing queued_at always preserved.
    merged: dict[str, dict] = {}
    for fid, row in {**source2, **source1}.items():
        if fid in existing:
            # Preserve queued_at; allow source to be updated if higher-priority
            # source now knows about this fighter.
            merged[fid] = {**existing[fid], "source": row["source"],
                           "fighter_url": row["fighter_url"]}
        else:
            merged[fid] = {**row, "queued_at": now}

    # Carry over any existing entries not found in either source this run
    # (e.g. fighter_queue was seeded from a listing-page run previously).
    for fid, row in existing.items():
        if fid not in merged:
            merged[fid] = row

    total = len(merged)
    new_count = total - len(existing)

    _QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _QUEUE_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_QUEUE_FIELDS)
        writer.writeheader()
        writer.writerows(
            sorted(merged.values(), key=lambda r: r.get("queued_at", ""))
        )

    src1_count = sum(1 for r in merged.values() if r.get("source") == "fighters_csv")
    src2_count = sum(1 for r in merged.values() if r.get("source") == "fight_page")

    print(f"Fighter queue written to {_QUEUE_PATH}")
    print(f"  Total entries : {total}")
    print(f"  New this run  : {new_count}")
    print(f"  fighters_csv  : {src1_count}")
    print(f"  fight_page    : {src2_count}")


if __name__ == "__main__":
    try:
        build_queue()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
