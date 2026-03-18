#!/usr/bin/env python3
"""Event acquisition validation and coverage report (T1.3.4).

Reconciles three data sources to confirm that event discovery, raw capture,
and CSV parsing are all in sync before downstream fighter / fight acquisition
begins.

Sources read:
  data/manifests/events_manifest.csv   canonical event registry
  data/manifests/fetch_manifest.csv    per-fetch audit trail
  data/events.csv                      parsed event output

Checks performed:
  Coverage  — discovered vs. fetched vs. parsed counts
  Quality   — duplicate event IDs, blank dates, malformed fight-card listings
  Threshold — parse-miss rate and blank-date rate vs. a configurable limit

Usage:
    python3 event_coverage_report.py            # from scraper/UFC-Web-Scraping-main/
    python3 event_coverage_report.py --threshold 0.10
    make report_events

Exit codes:
    0  all checks passed (within threshold)
    1  one or more checks failed
"""

import argparse
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

_CAPTURED_STATUSES = {"fetched", "unchanged", "updated"}

DEFAULT_THRESHOLD = 0.05  # 5 % tolerance for parse-miss and blank-date rates


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def load_sources() -> tuple[list[dict], list[dict], list[dict]]:
    events_manifest = _load_csv(DATA_DIR / "manifests" / "events_manifest.csv")
    fetch_manifest = _load_csv(DATA_DIR / "manifests" / "fetch_manifest.csv")
    events_csv = _load_csv(DATA_DIR / "events.csv")
    return events_manifest, fetch_manifest, events_csv


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


def _info(label: str) -> None:
    print(f"  INFO  {label}")


def _pct(n: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{n / total:.1%}"


# ---------------------------------------------------------------------------
# Check sections
# ---------------------------------------------------------------------------


def check_sources_present(
    events_manifest: list[dict],
    fetch_manifest: list[dict],
    events_csv: list[dict],
) -> list[bool]:
    """Verify all three source files exist and have data."""
    results: list[bool] = []

    for label, rows, path_suffix in [
        ("events_manifest.csv", events_manifest, "manifests/events_manifest.csv"),
        ("fetch_manifest.csv", fetch_manifest, "manifests/fetch_manifest.csv"),
        ("events.csv", events_csv, "events.csv"),
    ]:
        path = DATA_DIR / path_suffix
        if not path.exists():
            results.append(_fail(f"data/{path_suffix} exists", str(path)))
        elif not rows:
            results.append(_fail(f"data/{path_suffix} has data rows"))
        else:
            results.append(_pass(f"data/{path_suffix}  ({len(rows)} rows)"))

    return results


def check_coverage(
    events_manifest: list[dict],
    fetch_manifest: list[dict],
    events_csv: list[dict],
) -> list[bool]:
    """Reconcile discovered → fetched → parsed counts."""
    results: list[bool] = []

    # --- Discovered events ---
    discovered_ids = {
        r["event_id"].strip()
        for r in events_manifest
        if r.get("event_id", "").strip()
    }
    total_discovered = len(discovered_ids)
    _info(f"Discovered events (events_manifest.csv):  {total_discovered}")

    # --- Fetched event pages ---
    fetched_event_urls = {
        r["source_url"].strip()
        for r in fetch_manifest
        if r.get("entity_type", "").strip() == "event"
        and r.get("fetch_status", "").strip() in _CAPTURED_STATUSES
        and r.get("source_url", "").strip()
    }
    failed_event_fetches = sum(
        1
        for r in fetch_manifest
        if r.get("entity_type", "").strip() == "event"
        and r.get("fetch_status", "").strip() == "failed"
    )
    total_fetched = len(fetched_event_urls)
    _info(f"Fetched event pages (fetch_manifest.csv): {total_fetched}  "
          f"({failed_event_fetches} failed)")

    if failed_event_fetches > 0:
        results.append(_fail(
            f"No failed event fetches",
            f"{failed_event_fetches} event page(s) failed to fetch — "
            "re-run with incremental=1 to retry",
        ))
    else:
        if total_fetched > 0:
            results.append(_pass("No failed event fetches"))

    # --- Parsed events ---
    parsed_ids = {
        r["event_id"].strip()
        for r in events_csv
        if r.get("event_id", "").strip()
    }
    total_parsed = len(parsed_ids)
    _info(f"Parsed events (events.csv):                {total_parsed}")

    # Events in manifest but not in parsed CSV (parse-miss)
    manifest_not_parsed = discovered_ids - parsed_ids
    if manifest_not_parsed:
        sample = sorted(manifest_not_parsed)[:5]
        results.append(_fail(
            f"All manifest events are parsed",
            f"{len(manifest_not_parsed)} event(s) in manifest but missing from "
            f"events.csv — sample IDs: {sample}",
        ))
    else:
        if total_discovered > 0:
            results.append(_pass("All manifest events appear in events.csv"))

    # Events in CSV but not in manifest (orphan rows)
    csv_not_manifest = parsed_ids - discovered_ids
    if csv_not_manifest:
        _info(
            f"events.csv has {len(csv_not_manifest)} ID(s) not in events_manifest.csv "
            f"(may be pre-manifest legacy rows)"
        )

    return results


def check_data_quality(events_csv: list[dict]) -> list[bool]:
    """Flag duplicates, blank dates, and malformed fight-card listings."""
    results: list[bool] = []

    if not events_csv:
        return results

    # --- Duplicate event IDs ---
    all_ids = [r.get("event_id", "").strip() for r in events_csv if r.get("event_id", "").strip()]
    seen: set[str] = set()
    duplicates: set[str] = set()
    for eid in all_ids:
        if eid in seen:
            duplicates.add(eid)
        seen.add(eid)

    if duplicates:
        results.append(_fail(
            "No duplicate event IDs in events.csv",
            f"{len(duplicates)} duplicate ID(s): {sorted(duplicates)[:5]}",
        ))
    else:
        results.append(_pass(f"No duplicate event IDs  ({len(all_ids)} rows checked)"))

    # --- Blank event dates ---
    blank_date = [
        r for r in events_csv
        if not r.get("date_formatted", "").strip()
        and r.get("event_status", "").strip() == "completed"
    ]
    if blank_date:
        sample = [r.get("event_id", "?") for r in blank_date[:5]]
        results.append(_fail(
            "No completed events with blank date_formatted",
            f"{len(blank_date)} completed event(s) missing date_formatted — "
            f"sample IDs: {sample}",
        ))
    else:
        results.append(_pass("No completed events have blank date_formatted"))

    # --- Malformed fight-card listings ---
    # Completed events should have at least one fight URL.
    bad_fight_card = [
        r for r in events_csv
        if r.get("event_status", "").strip() == "completed"
        and not r.get("fight_urls", "").strip()
    ]
    if bad_fight_card:
        sample = [r.get("event_id", "?") for r in bad_fight_card[:5]]
        results.append(_fail(
            "No completed events with empty fight_urls",
            f"{len(bad_fight_card)} completed event(s) have no fight_urls — "
            f"sample IDs: {sample}",
        ))
    else:
        results.append(_pass("All completed events have fight_urls"))

    return results


def check_thresholds(
    events_manifest: list[dict],
    events_csv: list[dict],
    threshold: float,
) -> list[bool]:
    """Fail if parse-miss rate or blank-date rate exceeds threshold."""
    results: list[bool] = []
    threshold_pct = f"{threshold:.0%}"

    discovered_ids = {
        r["event_id"].strip()
        for r in events_manifest
        if r.get("event_id", "").strip()
    }
    parsed_ids = {
        r["event_id"].strip()
        for r in events_csv
        if r.get("event_id", "").strip()
    }

    # Parse-miss rate
    total = len(discovered_ids)
    missed = len(discovered_ids - parsed_ids)
    miss_rate = missed / total if total else 0.0
    label = (
        f"Parse-miss rate ({_pct(missed, total)}) within {threshold_pct} threshold"
    )
    if miss_rate > threshold:
        results.append(_fail(
            label,
            f"{missed}/{total} manifest events not in events.csv",
        ))
    else:
        results.append(_pass(label))

    # Blank-date rate (completed events only)
    completed = [r for r in events_csv if r.get("event_status", "").strip() == "completed"]
    blank = sum(1 for r in completed if not r.get("date_formatted", "").strip())
    blank_rate = blank / len(completed) if completed else 0.0
    label = (
        f"Blank-date rate ({_pct(blank, len(completed))}) within {threshold_pct} threshold"
    )
    if blank_rate > threshold:
        results.append(_fail(
            label,
            f"{blank}/{len(completed)} completed events have no date_formatted",
        ))
    else:
        results.append(_pass(label))

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(threshold: float = DEFAULT_THRESHOLD) -> int:
    print(f"\nEvent coverage report  ·  data dir: {DATA_DIR}")
    print(f"Threshold: {threshold:.0%}\n")

    events_manifest, fetch_manifest, events_csv = load_sources()

    sections = [
        ("Sources present", lambda: check_sources_present(
            events_manifest, fetch_manifest, events_csv
        )),
        ("Coverage reconciliation", lambda: check_coverage(
            events_manifest, fetch_manifest, events_csv
        )),
        ("Data quality", lambda: check_data_quality(events_csv)),
        (f"Threshold checks (≤ {threshold:.0%})", lambda: check_thresholds(
            events_manifest, events_csv, threshold
        )),
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        metavar="RATE",
        help=f"max tolerated parse-miss and blank-date rate (default: {DEFAULT_THRESHOLD})",
    )
    args = parser.parse_args()
    sys.exit(main(threshold=args.threshold))
