#!/usr/bin/env python3
"""Fighter missing-data and identity review report (T1.4.3).

Reads data/fighters.csv and flags rows that warrant human or downstream
review before fighter identity is used for joins or deduplication.

Flags applied:
  missing_name        — full_name is blank (critical identity field)
  missing_id          — fighter_id is blank (critical dedupe key)
  duplicate_name      — full_name shared by two or more distinct fighter_ids
  sparse_bio          — 3 or more of {height_cm, weight_lbs, reach_in,
                        stance, dob_formatted} are missing
  missing_dob         — dob_formatted is blank (standalone; useful for
                        age-based feature engineering downstream)

A fighter can carry multiple flags (separated by "; ").

Output:
  data/reports/fighter_review.csv

Schema:
  fighter_id, fighter_url, full_name, flags, flag_count

Exit code is always 0 — the presence of flagged fighters is expected and
must not block the scraper run.

Usage:
    python3 fighter_review.py          # from scraper/UFC-Web-Scraping-main/
    make review_fighters
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

_FIGHTERS_CSV = DATA_DIR / "fighters.csv"
_REPORT_DIR = DATA_DIR / "reports"
_REPORT_PATH = _REPORT_DIR / "fighter_review.csv"

_REPORT_FIELDS = ["fighter_id", "fighter_url", "full_name", "flags", "flag_count"]

# Physical attributes considered for sparse-bio detection.
_PHYSICAL_FIELDS = ["height_cm", "weight_lbs", "reach_in", "stance", "dob_formatted"]
_SPARSE_THRESHOLD = 3  # flag when this many or more physical fields are blank


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _blank(row: dict, field: str) -> bool:
    return not row.get(field, "").strip()


def _info(msg: str) -> None:
    print(f"  INFO  {msg}")


def _warn(msg: str) -> None:
    print(f"  WARN  {msg}")


# ---------------------------------------------------------------------------
# Flagging logic
# ---------------------------------------------------------------------------


def _build_duplicate_name_index(rows: list[dict]) -> dict[str, list[str]]:
    """Return {full_name: [fighter_id, ...]} for names with >1 distinct ID."""
    name_to_ids: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        name = row.get("full_name", "").strip()
        fid = row.get("fighter_id", "").strip()
        if name and fid:
            name_to_ids[name].add(fid)
    return {name: sorted(ids) for name, ids in name_to_ids.items() if len(ids) > 1}


def flag_rows(rows: list[dict]) -> list[dict]:
    """Return flagged rows; fighters with no flags are excluded."""
    dup_names = _build_duplicate_name_index(rows)
    flagged: list[dict] = []

    for row in rows:
        flags: list[str] = []

        if _blank(row, "full_name"):
            flags.append("missing_name")

        if _blank(row, "fighter_id"):
            flags.append("missing_id")

        name = row.get("full_name", "").strip()
        if name in dup_names:
            flags.append("duplicate_name")

        missing_physical = sum(1 for f in _PHYSICAL_FIELDS if _blank(row, f))
        if missing_physical >= _SPARSE_THRESHOLD:
            flags.append("sparse_bio")
        elif _blank(row, "dob_formatted"):
            # Surface missing DOB even when the rest of the bio is present.
            flags.append("missing_dob")

        if flags:
            flagged.append({
                "fighter_id": row.get("fighter_id", ""),
                "fighter_url": row.get("url", ""),
                "full_name": row.get("full_name", ""),
                "flags": "; ".join(flags),
                "flag_count": len(flags),
            })

    return flagged


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _flag_summary(flagged: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in flagged:
        for flag in row["flags"].split("; "):
            counts[flag.strip()] += 1
    return dict(counts)


def run() -> int:
    print(f"\nFighter review report  ·  data dir: {DATA_DIR}\n")

    if not _FIGHTERS_CSV.exists():
        print(f"  ERROR  fighters.csv not found at {_FIGHTERS_CSV}")
        print("  Run 'make crawl_csv_fighters' first.\n")
        return 0  # not a hard failure — just nothing to review yet

    with _FIGHTERS_CSV.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    _info(f"fighters.csv loaded  ({len(rows)} rows)")
    print()

    flagged = flag_rows(rows)
    summary = _flag_summary(flagged)

    print(f"── Flag summary {'─' * 43}")
    if not flagged:
        _info("No fighters flagged — all rows pass review checks.")
    else:
        _info(f"Total flagged fighters : {len(flagged)} / {len(rows)}  "
              f"({len(flagged)/len(rows):.1%})")
        for flag, count in sorted(summary.items(), key=lambda x: -x[1]):
            _info(f"  {flag:<20} {count:>5} fighters")

    print()
    print(f"── Duplicate name details {'─' * 33}")
    dup_index = _build_duplicate_name_index(rows)
    if not dup_index:
        _info("No duplicate full_name values found.")
    else:
        for name, ids in sorted(dup_index.items()):
            _warn(f"'{name}'  →  {len(ids)} distinct IDs: {ids}")

    print()
    print(f"── Output {'─' * 49}")
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with _REPORT_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_REPORT_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(flagged, key=lambda r: (-r["flag_count"], r["full_name"])))
    _info(f"Review file written: {_REPORT_PATH}  ({len(flagged)} rows)")

    print()
    print("  NOTE  Exit code is always 0 — flagged fighters do not block the pipeline.")
    print()

    return 0  # flags are informational; never block the scraper run


if __name__ == "__main__":
    sys.exit(run())
