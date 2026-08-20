"""Build a reviewed-target manifest for bounded BFO historical backfill.

This ticket is planning-only: it reads local CSVs, selects completed fights that
need BFO review, and writes a manifest for human URL/matchup lookup. It does not
fetch BestFightOdds, decode payloads, or promote odds rows.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.bfo_mean_market_backtest import DEFAULT_BOOKMAKER, DEFAULT_SOURCE

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PREDICTIONS = REPO_ROOT / "data" / "reports" / "pre_event_prediction_fights.csv"
DEFAULT_ODDS = REPO_ROOT / "data" / "odds" / "fight_odds.csv"
DEFAULT_FIGHTS = REPO_ROOT / "data" / "fights.csv"
DEFAULT_OUTPUT = (
    REPO_ROOT
    / "data"
    / "odds"
    / "raw"
    / "bestfightodds"
    / "manifests"
    / "bfo_mean_historical_targets.review.csv"
)
DEFAULT_MAX_FIGHTS = 10

REVIEW_MANIFEST_COLUMNS = [
    "target_id",
    "target_status",
    "purpose",
    "local_event_id",
    "local_fight_id",
    "event_date",
    "event_name",
    "fighter_1_id",
    "fighter_1_name",
    "fighter_2_id",
    "fighter_2_name",
    "scored_at",
    "resolved",
    "actual_winner_name",
    "confidence_tier",
    "has_existing_bfo_mean_rows",
    "review_priority",
    "bfo_event_url",
    "bfo_matchup_id",
    "bfo_payload_p1_url",
    "bfo_payload_p2_url",
    "capture_manifest_ready",
    "manual_lookup_notes",
]


@dataclass(frozen=True)
class HistoricalManifestResult:
    """Result from writing the BFO historical review manifest."""

    output_path: Path
    rows: tuple[dict[str, str], ...]
    counters: Counter


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Build a local-only BFO historical target review manifest."
    )
    parser.add_argument("--predictions", type=Path, default=DEFAULT_PREDICTIONS)
    parser.add_argument("--odds", type=Path, default=DEFAULT_ODDS)
    parser.add_argument("--fights", type=Path, default=DEFAULT_FIGHTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--max-fights", type=int, default=DEFAULT_MAX_FIGHTS)
    parser.add_argument("--as-of-date", help="Date used to define completed/past fights, YYYY-MM-DD. Defaults to today UTC.")
    return parser


def build_bestfightodds_historical_review_manifest(
    *,
    predictions_path: Path = DEFAULT_PREDICTIONS,
    odds_path: Path = DEFAULT_ODDS,
    fights_path: Path = DEFAULT_FIGHTS,
    output_path: Path = DEFAULT_OUTPUT,
    max_fights: int = DEFAULT_MAX_FIGHTS,
    as_of_date: date | str | None = None,
) -> HistoricalManifestResult:
    """Select completed prediction fights needing BFO review and write a manifest."""
    if max_fights < 1:
        raise ValueError("max_fights must be at least 1")

    cutoff = _date_from_value(as_of_date) or datetime.now(timezone.utc).date()
    prediction_rows = _read_csv(predictions_path)
    odds_rows = _read_csv(odds_path)
    fight_rows = _latest_rows_by_id(_read_csv(fights_path), "fight_id")
    existing_bfo_fight_ids = _existing_bfo_fight_ids(odds_rows)

    candidates, counters = _select_candidates(
        prediction_rows,
        fight_rows=fight_rows,
        existing_bfo_fight_ids=existing_bfo_fight_ids,
        as_of_date=cutoff,
    )
    rows = tuple(
        _review_row(candidate, priority=index)
        for index, candidate in enumerate(candidates[:max_fights], start=1)
    )
    counters["manifest_rows_written"] = len(rows)
    counters["max_fights"] = max_fights
    _write_csv(output_path, REVIEW_MANIFEST_COLUMNS, rows)
    return HistoricalManifestResult(output_path=output_path, rows=rows, counters=counters)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_arg_parser().parse_args(argv)
    result = build_bestfightodds_historical_review_manifest(
        predictions_path=args.predictions,
        odds_path=args.odds,
        fights_path=args.fights,
        output_path=args.output,
        max_fights=args.max_fights,
        as_of_date=args.as_of_date,
    )
    print("BestFightOdds historical review manifest.")
    print("No network fetch, payload decode, canonical promotion, or scheduler was run.")
    for key in sorted(result.counters):
        print(f"{key}: {result.counters[key]}")
    print(f"Wrote: {result.output_path}")
    return 0


def _select_candidates(
    prediction_rows: Iterable[Mapping[str, str]],
    *,
    fight_rows: Mapping[str, Mapping[str, str]],
    existing_bfo_fight_ids: set[str],
    as_of_date: date,
) -> tuple[list[dict[str, str]], Counter]:
    counters: Counter = Counter()
    by_fight: dict[str, dict[str, str]] = {}
    for prediction in prediction_rows:
        counters["prediction_rows_read"] += 1
        fight_id = _text(prediction.get("fight_id"))
        if not fight_id:
            counters["skipped_missing_fight_id"] += 1
            continue
        if fight_id in existing_bfo_fight_ids:
            counters["skipped_existing_bfo_mean"] += 1
            continue

        event_date = _date_from_value(prediction.get("event_date"))
        if event_date is None:
            counters["skipped_missing_event_date"] += 1
            continue
        if event_date >= as_of_date:
            counters["skipped_not_past_event"] += 1
            continue
        if not _truthy(prediction.get("resolved")):
            counters["skipped_unresolved_prediction"] += 1
            continue
        if not _text(prediction.get("scored_at")):
            counters["skipped_missing_scored_at"] += 1
            continue

        fight = fight_rows.get(fight_id)
        if fight is None:
            counters["skipped_missing_local_fight"] += 1
            continue
        if _text(fight.get("event_status")) != "completed":
            counters["skipped_non_completed_local_fight"] += 1
            continue
        if not _text(fight.get("fighter_1_id")) or not _text(fight.get("fighter_2_id")):
            counters["skipped_missing_local_fighter_ids"] += 1
            continue

        candidate = dict(prediction)
        candidate["fighter_1_id"] = _text(fight.get("fighter_1_id"))
        candidate["fighter_2_id"] = _text(fight.get("fighter_2_id"))
        candidate["local_event_id"] = _text(prediction.get("event_id")) or _text(fight.get("event_id"))
        candidate["local_fight_id"] = fight_id
        current = by_fight.get(fight_id)
        if current is None or _datetime_sort_value(candidate.get("scored_at")) > _datetime_sort_value(current.get("scored_at")):
            by_fight[fight_id] = candidate

    candidates = sorted(
        by_fight.values(),
        key=lambda row: (
            _date_from_value(row.get("event_date")) or date.min,
            _datetime_sort_value(row.get("scored_at")),
            _text(row.get("fight_id")),
        ),
        reverse=True,
    )
    counters["candidate_fights"] = len(candidates)
    return candidates, counters


def _review_row(candidate: Mapping[str, str], *, priority: int) -> dict[str, str]:
    event_date = _text(candidate.get("event_date"))
    fight_id = _text(candidate.get("local_fight_id")) or _text(candidate.get("fight_id"))
    target_id = f"hist_{event_date.replace('-', '')}_{fight_id[:8]}"
    fighter_1 = _text(candidate.get("fighter_1_name"))
    fighter_2 = _text(candidate.get("fighter_2_name"))
    return {
        "target_id": target_id,
        "target_status": "needs_manual_bfo_lookup",
        "purpose": "historical_backfill",
        "local_event_id": _text(candidate.get("local_event_id")),
        "local_fight_id": fight_id,
        "event_date": event_date,
        "event_name": _text(candidate.get("event_name")),
        "fighter_1_id": _text(candidate.get("fighter_1_id")),
        "fighter_1_name": fighter_1,
        "fighter_2_id": _text(candidate.get("fighter_2_id")),
        "fighter_2_name": fighter_2,
        "scored_at": _text(candidate.get("scored_at")),
        "resolved": _text(candidate.get("resolved")),
        "actual_winner_name": _text(candidate.get("actual_winner_name")),
        "confidence_tier": _text(candidate.get("confidence_tier")),
        "has_existing_bfo_mean_rows": "false",
        "review_priority": str(priority),
        "bfo_event_url": "",
        "bfo_matchup_id": "",
        "bfo_payload_p1_url": "",
        "bfo_payload_p2_url": "",
        "capture_manifest_ready": "false",
        "manual_lookup_notes": (
            "Review BFO event/fighter pages manually; add exact event URL and "
            f"paired matchup payload URLs for {fighter_1} vs. {fighter_2} before live capture."
        ),
    }


def _existing_bfo_fight_ids(rows: Iterable[Mapping[str, str]]) -> set[str]:
    return {
        _text(row.get("fight_id"))
        for row in rows
        if row.get("bookmaker") == DEFAULT_BOOKMAKER
        and row.get("source") == DEFAULT_SOURCE
        and _text(row.get("fight_id"))
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _latest_rows_by_id(rows: Iterable[Mapping[str, str]], key: str) -> dict[str, Mapping[str, str]]:
    output: dict[str, Mapping[str, str]] = {}
    for row in sorted(rows, key=lambda item: _datetime_sort_value(item.get("scraped_at"))):
        row_key = _text(row.get(key))
        if row_key:
            output[row_key] = row
    return output


def _date_from_value(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _datetime_sort_value(value: object) -> datetime:
    text = _text(value)
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    text = text.replace(" UTC", "+00:00").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _truthy(value: object) -> bool:
    return _text(value).casefold() in {"true", "1", "yes", "y"}


def _text(value: object) -> str:
    return str(value or "").strip()


if __name__ == "__main__":
    raise SystemExit(main())
