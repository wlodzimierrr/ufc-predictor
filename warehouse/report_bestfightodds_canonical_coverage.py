"""Write a compact QA report for canonical BestFightOdds Mean coverage."""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.bfo_mean_market_backtest import DEFAULT_BOOKMAKER, DEFAULT_SOURCE

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ODDS = REPO_ROOT / "data" / "odds" / "fight_odds.csv"
DEFAULT_EVENTS = REPO_ROOT / "data" / "events.csv"
DEFAULT_OUTPUT = (
    REPO_ROOT
    / "data"
    / "reports"
    / "bfo_mean_market_benchmark"
    / "bfo_mean_canonical_coverage.csv"
)

COVERAGE_COLUMNS = [
    "event_id",
    "event_name",
    "event_date",
    "fight_id",
    "fighter_names",
    "side_count",
    "row_count",
    "market_group_count",
    "first_odds_timestamp",
    "last_odds_timestamp",
    "has_two_sides",
    "source_url_count",
]


@dataclass(frozen=True)
class BfoCoverageResult:
    """Result from writing the BFO canonical coverage report."""

    output_path: Path
    rows: tuple[dict[str, str], ...]
    counters: Counter


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Write canonical BestFightOdds Mean coverage by event/fight."
    )
    parser.add_argument("--odds", type=Path, default=DEFAULT_ODDS)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def write_bestfightodds_canonical_coverage(
    *,
    odds_path: Path = DEFAULT_ODDS,
    events_path: Path = DEFAULT_EVENTS,
    output_path: Path = DEFAULT_OUTPUT,
) -> BfoCoverageResult:
    """Read canonical odds and write BFO Mean fight-level coverage rows."""
    event_names = _event_names_by_id(_read_csv(events_path))
    odds_rows = [
        row for row in _read_csv(odds_path)
        if row.get("bookmaker") == DEFAULT_BOOKMAKER
        and row.get("source") == DEFAULT_SOURCE
    ]
    rows = tuple(_coverage_rows(odds_rows, event_names=event_names))
    counters: Counter = Counter({
        "canonical_bfo_rows": len(odds_rows),
        "covered_fights": len(rows),
        "covered_events": len({row["event_id"] for row in rows}),
        "two_sided_fights": sum(1 for row in rows if row["has_two_sides"] == "true"),
    })
    _write_csv(output_path, COVERAGE_COLUMNS, rows)
    return BfoCoverageResult(output_path=output_path, rows=rows, counters=counters)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_arg_parser().parse_args(argv)
    result = write_bestfightodds_canonical_coverage(
        odds_path=args.odds,
        events_path=args.events,
        output_path=args.output,
    )
    print("BestFightOdds Mean canonical coverage QA.")
    for key in sorted(result.counters):
        print(f"{key}: {result.counters[key]}")
    print(f"Wrote: {result.output_path}")
    return 0


def _coverage_rows(
    odds_rows: Iterable[Mapping[str, str]],
    *,
    event_names: Mapping[str, str],
) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[Mapping[str, str]]] = {}
    for row in odds_rows:
        event_id = row.get("event_id", "")
        fight_id = row.get("fight_id", "")
        if not event_id or not fight_id:
            continue
        grouped.setdefault((event_id, fight_id), []).append(row)

    output = []
    for (event_id, fight_id), group in sorted(
        grouped.items(),
        key=lambda item: (
            _date_sort_value(item[1][0].get("event_date")),
            event_names.get(item[0][0], ""),
            item[0][1],
        ),
    ):
        timestamps = sorted(
            timestamp for timestamp in (_timestamp_text(row.get("odds_timestamp")) for row in group)
            if timestamp
        )
        sides = {
            row.get("fighter_id", "")
            for row in group
            if row.get("fighter_id")
        }
        market_groups = {
            (
                row.get("bookmaker", ""),
                row.get("market", ""),
                row.get("line_type", ""),
                row.get("odds_timestamp", ""),
            )
            for row in group
        }
        output.append({
            "event_id": event_id,
            "event_name": event_names.get(event_id, ""),
            "event_date": group[0].get("event_date", ""),
            "fight_id": fight_id,
            "fighter_names": " vs. ".join(sorted({
                row.get("fighter_name", "")
                for row in group
                if row.get("fighter_name")
            })),
            "side_count": str(len(sides)),
            "row_count": str(len(group)),
            "market_group_count": str(len(market_groups)),
            "first_odds_timestamp": timestamps[0] if timestamps else "",
            "last_odds_timestamp": timestamps[-1] if timestamps else "",
            "has_two_sides": str(len(sides) == 2).lower(),
            "source_url_count": str(len({
                row.get("source_url", "")
                for row in group
                if row.get("source_url")
            })),
        })
    return output


def _event_names_by_id(rows: Iterable[Mapping[str, str]]) -> dict[str, str]:
    output = {}
    for row in rows:
        event_id = row.get("event_id", "")
        event_name = row.get("name", "") or row.get("event_name", "")
        if event_id and event_name:
            output[event_id] = event_name
    return output


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _timestamp_text(value: object) -> str:
    parsed = _datetime_from_value(value)
    return parsed.isoformat() if parsed else ""


def _date_sort_value(value: object) -> str:
    return str(value or "")


def _datetime_from_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
