#!/usr/bin/env python3
"""Fight stats coverage reconciliation report (T1.5.4).

Reconciles four sources to show whether aggregate and round-level stats
acquisition is complete enough for Phase 2 work to proceed.

Sources read:
  data/manifests/fight_stats_queue.csv   canonical queue of completed fights
  data/manifests/fetch_manifest.csv      per-fetch audit trail
  data/fight_stats.csv                   parsed aggregate stats
  data/fight_stats_by_round.csv          parsed round-level stats

Failure classification (per fight, from manifests alone):
  not_fetched          — URL absent from fetch_manifest entirely
  fetch_failed         — URL in fetch_manifest with fetch_status="failed"
  missing_stats        — fetched successfully, but fight_id absent from
                         fight_stats.csv (covers both no-stats-table and
                         parse failure; distinguishing these requires logs)
  ok_aggregate         — fight_id present in fight_stats.csv
  ok_by_round          — fight_id present in fight_stats_by_round.csv

Outputs:
  data/reports/stats_coverage.csv  — per-fight gap rows (non-ok fights only)
  stdout                            — summary table + threshold result

Exit codes:
  0  all threshold checks pass
  1  missing-aggregate-stats rate or missing-round-stats rate exceeds threshold

Usage:
    python3 stats_coverage_report.py                 # default 5% threshold
    python3 stats_coverage_report.py --threshold 0.10
    make report_stats
"""

import argparse
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"

_QUEUE_PATH = DATA_DIR / "manifests" / "fight_stats_queue.csv"
_FETCH_MANIFEST = DATA_DIR / "manifests" / "fetch_manifest.csv"
_FIGHT_STATS_CSV = DATA_DIR / "fight_stats.csv"
_FIGHT_STATS_BY_ROUND_CSV = DATA_DIR / "fight_stats_by_round.csv"
_REPORT_PATH = DATA_DIR / "reports" / "stats_coverage.csv"

_CAPTURED_STATUSES = {"fetched", "unchanged", "updated"}
_REPORT_FIELDS = ["fight_id", "event_id", "fight_url", "finish_method",
                  "fetch_status", "agg_stats", "round_stats", "gap_reason"]

DEFAULT_THRESHOLD = 0.05


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


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
    return f"{n / total:.1%}" if total else "n/a"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_fetched_fight_urls() -> tuple[set[str], set[str]]:
    """Return (captured_urls, failed_urls) from fetch_manifest for fights."""
    captured, failed = set(), set()
    for row in _load_csv(_FETCH_MANIFEST):
        if row.get("entity_type", "").strip() != "fight":
            continue
        url = row.get("source_url", "").strip()
        if not url:
            continue
        status = row.get("fetch_status", "").strip()
        if status in _CAPTURED_STATUSES:
            captured.add(url)
        elif status == "failed":
            failed.add(url)
    return captured, failed


def _load_parsed_fight_ids(path: Path) -> set[str]:
    return {
        row["fight_id"].strip()
        for row in _load_csv(path)
        if row.get("fight_id", "").strip()
    }


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------


def check_sources(queue: list[dict], fight_stats: list[dict],
                  fight_stats_by_round: list[dict]) -> list[bool]:
    results: list[bool] = []
    for label, rows, path in [
        ("fight_stats_queue.csv", queue, _QUEUE_PATH),
        ("fight_stats.csv", fight_stats, _FIGHT_STATS_CSV),
        ("fight_stats_by_round.csv", fight_stats_by_round, _FIGHT_STATS_BY_ROUND_CSV),
    ]:
        if not path.exists():
            results.append(_fail(f"data/{path.relative_to(DATA_DIR)} exists", str(path)))
        elif not rows:
            results.append(_fail(f"data/{path.relative_to(DATA_DIR)} has data rows"))
        else:
            results.append(_pass(f"data/{path.relative_to(DATA_DIR)}  ({len(rows)} rows)"))
    # fetch_manifest is optional for this report but worth noting
    if _FETCH_MANIFEST.exists():
        _info(f"data/manifests/fetch_manifest.csv  (present — used for failure classification)")
    else:
        _info(f"data/manifests/fetch_manifest.csv  (absent — fetch failure classification unavailable)")
    return results


def check_coverage(
    queue: list[dict],
    captured_urls: set[str],
    failed_urls: set[str],
    agg_ids: set[str],
    round_ids: set[str],
) -> tuple[list[bool], list[dict]]:
    """Returns (check_results, gap_rows) where gap_rows feed the CSV report."""
    results: list[bool] = []
    gap_rows: list[dict] = []

    total = len(queue)
    if total == 0:
        _info("fight_stats_queue.csv is empty — nothing to reconcile")
        return results, gap_rows

    _info(f"Total queued fights                : {total}")

    agg_count = 0
    round_count = 0
    not_fetched_count = 0
    fetch_failed_count = 0
    missing_stats_count = 0

    for row in queue:
        fight_id = row.get("fight_id", "").strip()
        fight_url = row.get("fight_url", "").strip()
        event_id = row.get("event_id", "").strip()
        finish_method = row.get("finish_method", "").strip()

        has_agg = fight_id in agg_ids
        has_round = fight_id in round_ids
        was_captured = fight_url in captured_urls
        was_failed = fight_url in failed_urls

        if has_agg:
            agg_count += 1
        if has_round:
            round_count += 1

        # Classify failures
        if has_agg:
            gap_reason = ""
            fetch_status = "captured"
        elif was_failed:
            gap_reason = "fetch_failed"
            fetch_status = "failed"
            fetch_failed_count += 1
        elif not was_captured:
            gap_reason = "not_fetched"
            fetch_status = "not_fetched"
            not_fetched_count += 1
        else:
            # Fetched but not parsed — no-stats-table or parse error
            gap_reason = "missing_stats"
            fetch_status = "captured"
            missing_stats_count += 1

        if gap_reason:
            gap_rows.append({
                "fight_id": fight_id,
                "event_id": event_id,
                "fight_url": fight_url,
                "finish_method": finish_method,
                "fetch_status": fetch_status,
                "agg_stats": "yes" if has_agg else "no",
                "round_stats": "yes" if has_round else "no",
                "gap_reason": gap_reason,
            })

    _info(f"Fights with aggregate stats        : {agg_count}  ({_pct(agg_count, total)})")
    _info(f"Fights with round-level stats      : {round_count}  ({_pct(round_count, total)})")
    _info(f"Fights — not fetched               : {not_fetched_count}")
    _info(f"Fights — fetch failed              : {fetch_failed_count}")
    _info(f"Fights — fetched, no stats parsed  : {missing_stats_count}  "
          f"(no-stats-table or parse failure; check spider logs for details)")

    if fetch_failed_count:
        results.append(_fail(
            "No failed fight page fetches",
            f"{fetch_failed_count} fight page(s) failed — re-run incremental to retry",
        ))
    if not_fetched_count:
        results.append(_fail(
            "All queued fights have been fetched",
            f"{not_fetched_count} fight(s) not in fetch_manifest — run crawl_fight_stats",
        ))

    return results, gap_rows


def check_thresholds(
    queue: list[dict],
    agg_ids: set[str],
    round_ids: set[str],
    threshold: float,
) -> list[bool]:
    results: list[bool] = []
    total = len(queue)
    queued_ids = {r["fight_id"].strip() for r in queue if r.get("fight_id", "").strip()}

    agg_miss = len(queued_ids - agg_ids)
    agg_rate = agg_miss / total if total else 0.0
    label = f"Agg-stats miss rate ({_pct(agg_miss, total)}) within {threshold:.0%} threshold"
    if agg_rate > threshold:
        results.append(_fail(label, f"{agg_miss}/{total} fights missing aggregate stats"))
    else:
        results.append(_pass(label))

    round_miss = len(queued_ids - round_ids)
    round_rate = round_miss / total if total else 0.0
    label = f"Round-stats miss rate ({_pct(round_miss, total)}) within {threshold:.0%} threshold"
    if round_rate > threshold:
        results.append(_fail(label, f"{round_miss}/{total} fights missing round-level stats"))
    else:
        results.append(_pass(label))

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(threshold: float = DEFAULT_THRESHOLD) -> int:
    print(f"\nStats coverage report  ·  data dir: {DATA_DIR}")
    print(f"Threshold: {threshold:.0%}\n")

    queue = _load_csv(_QUEUE_PATH)
    fight_stats = _load_csv(_FIGHT_STATS_CSV)
    fight_stats_by_round = _load_csv(_FIGHT_STATS_BY_ROUND_CSV)
    captured_urls, failed_urls = _load_fetched_fight_urls()
    agg_ids = _load_parsed_fight_ids(_FIGHT_STATS_CSV)
    round_ids = _load_parsed_fight_ids(_FIGHT_STATS_BY_ROUND_CSV)

    sections = [
        ("Sources present", lambda: check_sources(queue, fight_stats, fight_stats_by_round)),
        ("Coverage reconciliation", lambda: check_coverage(
            queue, captured_urls, failed_urls, agg_ids, round_ids
        )),
        (f"Threshold checks (≤ {threshold:.0%})", lambda: check_thresholds(
            queue, agg_ids, round_ids, threshold
        )),
    ]

    all_results: list[bool] = []
    gap_rows: list[dict] = []

    for title, fn in sections:
        print(f"── {title} {'─' * (56 - len(title))}")
        result = fn()
        # coverage section returns a tuple
        if isinstance(result, tuple):
            check_results, rows = result
            all_results.extend(check_results)
            gap_rows.extend(rows)
        else:
            all_results.extend(result)
        print()

    # Write gap report
    print(f"── Output {'─' * 49}")
    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _REPORT_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_REPORT_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(gap_rows, key=lambda r: (r["gap_reason"], r["event_id"])))
    print(f"  INFO  Gap report written: {_REPORT_PATH}  ({len(gap_rows)} rows)")
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
        help=f"max tolerated miss rate for agg and round stats (default: {DEFAULT_THRESHOLD})",
    )
    args = parser.parse_args()
    sys.exit(main(threshold=args.threshold))
