"""Load fight moneyline odds from data/odds/fight_odds.csv.

The loader validates IDs against the warehouse, normalizes odds for audit/debug
checks, and upserts raw observations into fight_odds by a stable market key.

Usage:
    python warehouse/load_fight_odds.py
    python warehouse/load_fight_odds.py --csv data/odds/fight_odds.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import (
    implied_probability,
    normalize_decimal_odds,
    parse_optional_american_odds,
    parse_optional_decimal,
)
from warehouse.db import get_connection, upsert

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ODDS_CSV = REPO_ROOT / "data" / "odds" / "fight_odds.csv"

ALLOWED_MARKETS = {"moneyline"}
ALLOWED_LINE_TYPES = {"opening", "current", "closing", "unknown"}
REQUIRED_COLUMNS = {
    "fight_id",
    "event_id",
    "event_date",
    "fighter_id",
    "fighter_name",
    "opponent_fighter_id",
    "bookmaker",
    "market",
    "line_type",
    "odds_timestamp",
    "american_odds",
    "decimal_odds",
    "source",
    "source_url",
    "imported_at",
}
PK_COLUMNS = [
    "fight_id",
    "fighter_id",
    "bookmaker",
    "market",
    "line_type",
    "odds_timestamp",
]


@dataclass(frozen=True)
class FightContext:
    """Warehouse fight identity needed to validate odds rows."""

    event_id: str
    fighter_1_id: str
    fighter_2_id: str

    @property
    def fighter_ids(self) -> set[str]:
        return {self.fighter_1_id, self.fighter_2_id}


@dataclass(frozen=True)
class OddsValidationContext:
    """Warehouse IDs used by pure CSV validation."""

    event_ids: set[str]
    fighter_ids: set[str]
    fights: dict[str, FightContext]


@dataclass(frozen=True)
class ValidationIssue:
    """Rejected or skipped odds row with a human-readable reason."""

    row_number: int
    reason: str


@dataclass(frozen=True)
class ValidatedOddsRow:
    """Validated odds row plus derived odds values."""

    row_number: int
    db_row: dict
    normalized_decimal_odds: Decimal
    implied_probability: Decimal


@dataclass(frozen=True)
class ValidationResult:
    """CSV validation result."""

    rows_read: int
    rows: list[ValidatedOddsRow]
    skipped: list[ValidationIssue]
    rejected: list[ValidationIssue]


@dataclass(frozen=True)
class LoadOddsSummary:
    """Database load result."""

    rows_read: int
    imported: int
    skipped: int
    rejected: int


def read_odds_csv(path: Path) -> tuple[list[tuple[int, dict[str, str]]], set[str]]:
    """Read odds CSV rows and return missing required columns, if any."""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            return [], missing

        rows = []
        for row_number, row in enumerate(reader, start=2):
            if _is_repeated_header_row(row):
                continue
            rows.append((row_number, row))
    return rows, set()


def validate_odds_rows(
    raw_rows: Iterable[tuple[int, Mapping[str, object]]],
    context: OddsValidationContext,
) -> ValidationResult:
    """Validate imported odds rows without touching the database."""
    validated_rows: list[ValidatedOddsRow] = []
    skipped: list[ValidationIssue] = []
    rejected: list[ValidationIssue] = []
    seen_keys: set[tuple] = set()
    rows_read = 0

    for row_number, raw in raw_rows:
        rows_read += 1
        try:
            validated = validate_odds_row(row_number, raw, context)
        except ValueError as exc:
            rejected.append(ValidationIssue(row_number, str(exc)))
            continue

        key = tuple(validated.db_row[column] for column in PK_COLUMNS)
        if key in seen_keys:
            skipped.append(ValidationIssue(row_number, "duplicate stable odds key in CSV"))
            continue
        seen_keys.add(key)
        validated_rows.append(validated)

    return ValidationResult(
        rows_read=rows_read,
        rows=validated_rows,
        skipped=skipped,
        rejected=rejected,
    )


def validate_odds_row(
    row_number: int,
    raw: Mapping[str, object],
    context: OddsValidationContext,
) -> ValidatedOddsRow:
    """Validate and normalize one raw odds CSV row."""
    fight_id = _uuid_text(_required(raw, "fight_id"), "fight_id")
    event_id = _uuid_text(_required(raw, "event_id"), "event_id")
    fighter_id = _uuid_text(_required(raw, "fighter_id"), "fighter_id")
    opponent_fighter_id = _uuid_text(
        _required(raw, "opponent_fighter_id"),
        "opponent_fighter_id",
    )

    fight = context.fights.get(fight_id)
    if fight is None:
        raise ValueError(f"unknown fight_id {fight_id}")
    if len(fight.fighter_ids) != 2:
        raise ValueError(f"fight {fight_id} does not have exactly two fighters")
    if event_id not in context.event_ids:
        raise ValueError(f"unknown event_id {event_id}")
    if fighter_id not in context.fighter_ids:
        raise ValueError(f"unknown fighter_id {fighter_id}")
    if opponent_fighter_id not in context.fighter_ids:
        raise ValueError(f"unknown opponent_fighter_id {opponent_fighter_id}")
    if event_id != fight.event_id:
        raise ValueError(f"event_id {event_id} does not match fight {fight_id}")
    if {fighter_id, opponent_fighter_id} != fight.fighter_ids:
        raise ValueError(f"fighter/opponent IDs do not match fight {fight_id}")
    if fighter_id == opponent_fighter_id:
        raise ValueError("fighter_id and opponent_fighter_id must differ")

    _parse_date(_required(raw, "event_date"), "event_date")
    _required(raw, "fighter_name")
    bookmaker = _required(raw, "bookmaker")
    source = _required(raw, "source")
    market = _required(raw, "market").lower()
    line_type = _required(raw, "line_type").lower()
    if market not in ALLOWED_MARKETS:
        raise ValueError(f"invalid market {market}")
    if line_type not in ALLOWED_LINE_TYPES:
        raise ValueError(f"invalid line_type {line_type}")

    odds_timestamp = _parse_datetime(_required(raw, "odds_timestamp"), "odds_timestamp")
    imported_at = _parse_datetime(_required(raw, "imported_at"), "imported_at")
    american_odds = parse_optional_american_odds(raw.get("american_odds"))
    decimal_odds = parse_optional_decimal(raw.get("decimal_odds"))
    normalized_decimal = normalize_decimal_odds(
        american_odds=american_odds,
        decimal_odds=decimal_odds,
    )
    raw_implied_probability = implied_probability(normalized_decimal)

    db_row = {
        "fight_id": fight_id,
        "event_id": event_id,
        "fighter_id": fighter_id,
        "opponent_fighter_id": opponent_fighter_id,
        "bookmaker": bookmaker,
        "market": market,
        "line_type": line_type,
        "odds_timestamp": odds_timestamp,
        "american_odds": american_odds,
        "decimal_odds": decimal_odds,
        "source": source,
        "source_url": _optional(raw.get("source_url")),
        "imported_at": imported_at,
    }
    return ValidatedOddsRow(
        row_number=row_number,
        db_row=db_row,
        normalized_decimal_odds=normalized_decimal,
        implied_probability=raw_implied_probability,
    )


def load_fight_odds(conn, csv_path: Path = DEFAULT_ODDS_CSV) -> LoadOddsSummary:
    """Validate and upsert fight odds into the warehouse."""
    rows, missing_columns = read_odds_csv(csv_path)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"{csv_path} is missing required columns: {missing}")

    context = load_validation_context(conn)
    result = validate_odds_rows(rows, context)

    for issue in result.rejected:
        print(f"  reject row {issue.row_number}: {issue.reason}")
    for issue in result.skipped:
        print(f"  skip   row {issue.row_number}: {issue.reason}")

    db_rows = [validated.db_row for validated in result.rows]
    imported = 0
    if db_rows:
        imported = upsert(conn, "fight_odds", db_rows, pk_columns=PK_COLUMNS)

    print(f"  read      {result.rows_read} rows from {csv_path.name}")
    print(f"  imported  {imported} rows into fight_odds")
    print(f"  skipped   {len(result.skipped)} rows")
    print(f"  rejected  {len(result.rejected)} rows")

    return LoadOddsSummary(
        rows_read=result.rows_read,
        imported=imported,
        skipped=len(result.skipped),
        rejected=len(result.rejected),
    )


def load_validation_context(conn) -> OddsValidationContext:
    """Load warehouse IDs needed for odds CSV validation."""
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM events")
        event_ids = {str(row[0]) for row in cur.fetchall()}

        cur.execute("SELECT fighter_id FROM fighters")
        fighter_ids = {str(row[0]) for row in cur.fetchall()}

        cur.execute("SELECT fight_id, event_id, fighter_1_id, fighter_2_id FROM fights")
        fights = {
            str(row[0]): FightContext(
                event_id=str(row[1]),
                fighter_1_id=str(row[2]),
                fighter_2_id=str(row[3]),
            )
            for row in cur.fetchall()
        }

    return OddsValidationContext(
        event_ids=event_ids,
        fighter_ids=fighter_ids,
        fights=fights,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load fight odds into the warehouse.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_ODDS_CSV)
    args = parser.parse_args(argv)

    conn = get_connection()
    try:
        with conn:
            load_fight_odds(conn, args.csv)
    finally:
        conn.close()
    return 0


def _required(row: Mapping[str, object], column: str) -> str:
    value = _optional(row.get(column))
    if value is None:
        raise ValueError(f"missing required value for {column}")
    return value


def _optional(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _uuid_text(value: str, column: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise ValueError(f"invalid UUID for {column}: {value}") from exc


def _parse_datetime(value: str, column: str) -> datetime:
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp for {column}: {value}") from exc


def _parse_date(value: str, column: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date for {column}: {value}") from exc


def _is_repeated_header_row(row: Mapping[str, object] | None) -> bool:
    if not row:
        return False
    matches = sum(1 for key, value in row.items() if value == key)
    return matches >= max(3, len(row) // 3)


if __name__ == "__main__":
    raise SystemExit(main())
