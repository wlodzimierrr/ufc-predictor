"""Promote a reviewed source-specific odds CSV into canonical fight_odds.csv.

The promoter is intentionally small: it validates source rows against local
warehouse identity CSVs, keeps only the canonical columns, de-duplicates by the
loader's stable odds key, and writes a reviewed canonical CSV. It does not load
the database.

Usage:
    python3 warehouse/promote_odds_source_to_canonical.py \
        --source-csv data/odds/sources/bestfightodds_line_history_fight_odds.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS
from warehouse.load_fight_odds import (
    FightContext,
    OddsValidationContext,
    PK_COLUMNS,
    REQUIRED_COLUMNS,
    validate_odds_rows,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CANONICAL_CSV = REPO_ROOT / "data" / "odds" / "fight_odds.csv"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"


@dataclass(frozen=True)
class PromotionSummary:
    """Summary of source rows promoted into a canonical odds CSV."""

    existing_rows: int
    source_rows_read: int
    source_rows_valid: int
    appended_rows: int
    duplicate_rows: int
    output_path: Path


def promote_source_to_canonical(
    *,
    source_csv: Path,
    canonical_csv: Path = DEFAULT_CANONICAL_CSV,
    output_csv: Path | None = None,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
) -> PromotionSummary:
    """Validate and append reviewed source rows to the canonical odds CSV."""
    target_csv = output_csv or canonical_csv
    existing_rows = _read_canonical_rows(canonical_csv)
    source_rows, source_fieldnames = _read_source_rows(source_csv)
    missing = REQUIRED_COLUMNS - set(source_fieldnames)
    if missing:
        raise ValueError(f"{source_csv} is missing required columns: {', '.join(sorted(missing))}")

    context = _validation_context(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )
    validation = validate_odds_rows(
        [(index, row) for index, row in enumerate(source_rows, start=2)],
        context,
    )
    if validation.rejected:
        first = validation.rejected[0]
        raise ValueError(f"source validation rejected row {first.row_number}: {first.reason}")
    if validation.skipped:
        first = validation.skipped[0]
        raise ValueError(f"source validation skipped row {first.row_number}: {first.reason}")

    existing_keys = {_stable_key(row) for row in existing_rows}
    appended: list[dict[str, str]] = []
    duplicate_rows = 0
    for row in source_rows:
        canonical = {column: row.get(column, "") for column in CANONICAL_COLUMNS}
        key = _stable_key(canonical)
        if key in existing_keys:
            duplicate_rows += 1
            continue
        existing_keys.add(key)
        appended.append(canonical)

    _write_canonical_rows(target_csv, [*existing_rows, *appended])
    return PromotionSummary(
        existing_rows=len(existing_rows),
        source_rows_read=len(source_rows),
        source_rows_valid=len(validation.rows),
        appended_rows=len(appended),
        duplicate_rows=duplicate_rows,
        output_path=target_csv,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote a reviewed source-specific odds CSV into canonical fight_odds.csv."
    )
    parser.add_argument("--source-csv", type=Path, required=True)
    parser.add_argument("--canonical-csv", type=Path, default=DEFAULT_CANONICAL_CSV)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        summary = promote_source_to_canonical(
            source_csv=args.source_csv,
            canonical_csv=args.canonical_csv,
            output_csv=args.output_csv,
            events_csv=args.events_csv,
            fights_csv=args.fights_csv,
            fighters_csv=args.fighters_csv,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    print(f"Existing canonical rows: {summary.existing_rows}")
    print(f"Source rows read: {summary.source_rows_read}")
    print(f"Source rows valid: {summary.source_rows_valid}")
    print(f"Appended rows: {summary.appended_rows}")
    print(f"Duplicate rows skipped: {summary.duplicate_rows}")
    print(f"Wrote: {summary.output_path}")
    return 0


def _read_canonical_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        missing = set(CANONICAL_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing canonical columns: {', '.join(sorted(missing))}")
        return [
            {column: row.get(column, "") for column in CANONICAL_COLUMNS}
            for row in reader
            if not _is_repeated_header(row)
        ]


def _read_source_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [row for row in reader if not _is_repeated_header(row)]
    return rows, fieldnames


def _write_canonical_rows(path: Path, rows: list[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _validation_context(
    *,
    events_csv: Path,
    fights_csv: Path,
    fighters_csv: Path,
) -> OddsValidationContext:
    event_ids = {row["event_id"] for row in _read_csv(events_csv)}
    fighter_ids = {row["fighter_id"] for row in _read_csv(fighters_csv)}
    fights = {
        row["fight_id"]: FightContext(
            event_id=row["event_id"],
            fighter_1_id=row["fighter_1_id"],
            fighter_2_id=row["fighter_2_id"],
        )
        for row in _read_csv(fights_csv)
    }
    return OddsValidationContext(event_ids=event_ids, fighter_ids=fighter_ids, fights=fights)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _stable_key(row: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(row[column] for column in PK_COLUMNS)


def _is_repeated_header(row: Mapping[str, str]) -> bool:
    return all(row.get(column) == column for column in CANONICAL_COLUMNS)


if __name__ == "__main__":
    raise SystemExit(main())
