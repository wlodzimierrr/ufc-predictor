#!/usr/bin/env python3
"""Smoke-run output validator for Phase 1 acquisition.

Checks that a recent crawl produced all expected artifacts in the correct
locations.  Run after any acquisition job to confirm the full pipeline is
working end-to-end.

Usage:
    python3 smoke_check.py        # from scraper/UFC-Web-Scraping-main/
    make check                    # same, via Makefile
    make smoke                    # crawl (events N=5) then check

Exit codes:
    0  all mandatory checks passed
    1  one or more mandatory checks failed
"""

import csv
import sys
from pathlib import Path

# smoke_check.py lives at scraper/UFC-Web-Scraping-main/;
# parents[2] is the repo root (ufc-data/).
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

_MANIFEST_FIELDS = {
    "job_run_id",
    "entity_type",
    "source_url",
    "fetched_at",
    "http_status",
    "content_hash",
    "storage_path",
    "fetch_status",
    "error_message",
}
_CAPTURED_STATUSES = {"fetched", "unchanged", "updated"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pass(label: str) -> bool:
    print(f"  PASS  {label}")
    return True


def _fail(label: str, detail: str = "") -> bool:
    msg = f"  FAIL  {label}"
    if detail:
        msg += f"\n        → {detail}"
    print(msg)
    return False


# ---------------------------------------------------------------------------
# Check sections
# ---------------------------------------------------------------------------


def check_manifest() -> list[bool]:
    """Verify fetch_manifest.csv exists, has correct headers, and captured rows."""
    results: list[bool] = []
    manifest = DATA_DIR / "manifests" / "fetch_manifest.csv"

    if not manifest.exists():
        results.append(_fail("fetch_manifest.csv exists", str(manifest)))
        return results  # remaining checks are meaningless without the file
    results.append(_pass("fetch_manifest.csv exists"))

    with manifest.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    if not rows:
        results.append(_fail("fetch_manifest.csv has data rows"))
        return results
    results.append(_pass(f"fetch_manifest.csv has data rows ({len(rows)} total)"))

    missing = _MANIFEST_FIELDS - set(rows[0].keys())
    if missing:
        results.append(_fail("manifest has all required fields", f"missing: {missing}"))
    else:
        results.append(_pass("manifest has all required fields"))

    captured = [r for r in rows if r.get("fetch_status") in _CAPTURED_STATUSES]
    failed = len(rows) - len(captured)
    if not captured:
        results.append(_fail(
            "manifest has at least one captured row (fetched/unchanged/updated)"
        ))
    else:
        results.append(_pass(
            f"manifest has captured rows  "
            f"({len(captured)} captured, {failed} failed/other)"
        ))

    return results


def check_raw_artifacts() -> list[bool]:
    """Check data/raw/ufcstats/ subdirectories for HTML files.

    event_listing and events are mandatory after any events spider run.
    fights and fighters are optional; they are reported but never fail.
    """
    results: list[bool] = []

    for subdir in ("event_listing", "events"):
        raw_dir = DATA_DIR / "raw" / "ufcstats" / subdir
        files = sorted(raw_dir.glob("*.html")) if raw_dir.exists() else []
        if files:
            results.append(_pass(
                f"data/raw/ufcstats/{subdir}/  ({len(files)} .html files)"
            ))
        else:
            results.append(_fail(
                f"data/raw/ufcstats/{subdir}/ has .html files",
                f"directory: {raw_dir}",
            ))

    for subdir in ("fights", "fighters"):
        raw_dir = DATA_DIR / "raw" / "ufcstats" / subdir
        files = sorted(raw_dir.glob("*.html")) if raw_dir.exists() else []
        results.append(_pass(
            f"data/raw/ufcstats/{subdir}/  ({len(files)} .html files)  [optional]"
        ))

    return results


def check_parsed_csvs() -> list[bool]:
    """Check parsed CSV outputs.

    events.csv is mandatory after an events smoke run.
    The remaining CSVs are optional; they are reported but never fail.
    """
    results: list[bool] = []

    for name in ("events.csv",):
        path = DATA_DIR / name
        if not path.exists():
            results.append(_fail(f"data/{name} exists", str(path)))
            continue
        with path.open(newline="", encoding="utf-8") as fh:
            count = sum(1 for _ in csv.DictReader(fh))
        if count > 0:
            results.append(_pass(f"data/{name}  ({count} rows)"))
        else:
            results.append(_fail(
                f"data/{name} has data rows",
                "file exists but contains no data rows",
            ))

    for name in ("fights.csv", "fight_stats.csv", "fight_stats_by_round.csv", "fighters.csv"):
        path = DATA_DIR / name
        if path.exists():
            with path.open(newline="", encoding="utf-8") as fh:
                count = sum(1 for _ in csv.DictReader(fh))
            results.append(_pass(f"data/{name}  ({count} rows)  [optional]"))
        else:
            results.append(_pass(f"data/{name}  not present  [optional]"))

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    print(f"\nSmoke check  ·  data dir: {DATA_DIR}\n")

    sections = [
        ("Manifest", check_manifest),
        ("Raw artifacts", check_raw_artifacts),
        ("Parsed CSV outputs", check_parsed_csvs),
    ]

    all_results: list[bool] = []
    for title, fn in sections:
        print(f"── {title} {'─' * (56 - len(title))}")
        results = fn()
        all_results.extend(results)
        print()

    failures = all_results.count(False)
    bar = "═" * 60
    print(bar)
    if failures == 0:
        print("  ALL CHECKS PASSED")
    else:
        print(f"  {failures} CHECK(S) FAILED  ·  {all_results.count(True)} passed")
    print(bar + "\n")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
