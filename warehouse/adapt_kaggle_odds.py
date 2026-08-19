"""Adapt Kaggle UFC/MMA daily odds into the canonical fight_odds CSV.

The Kaggle source is treated as raw external input. This adapter maps rows to
existing warehouse IDs using UFCStats fight/fighter URLs and writes only V1
two-sided moneyline rows into the canonical odds contract.

Usage:
    python warehouse/adapt_kaggle_odds.py
    python warehouse/adapt_kaggle_odds.py --raw-csv data/odds/raw/UFC_betting_odds.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import validate_decimal_odds

REPO_ROOT = Path(__file__).resolve().parent.parent
KAGGLE_DATASET_URL = "https://www.kaggle.com/datasets/jerzyszocik/ufc-betting-odds-daily-dataset"
KAGGLE_SOURCE_LABEL = "kaggle_ufc_betting_odds_daily"

DEFAULT_RAW_CSV = REPO_ROOT / "data" / "odds" / "raw" / "UFC_betting_odds.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_SOURCE_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "kaggle_fight_odds.csv"
DEFAULT_CANONICAL_OUTPUT = REPO_ROOT / "data" / "odds" / "fight_odds.csv"
DEFAULT_UNMATCHED_OUTPUT = REPO_ROOT / "data" / "odds" / "unmatched_odds.csv"
DEFAULT_QA_OUTPUT = REPO_ROOT / "data" / "reports" / "odds_matching_qa.csv"

KAGGLE_REQUIRED_COLUMNS = {
    "fight_url",
    "fighter_1_url",
    "fighter_2_url",
    "fighter_1",
    "fighter_2",
    "odds_1",
    "odds_2",
    "event_date",
    "adding_date",
    "source",
}

CANONICAL_COLUMNS = [
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
]

UNMATCHED_COLUMNS = [
    "source",
    "row_number",
    "rejection_reason",
    "source_event_name",
    "source_event_date",
    "source_fighter_name",
    "source_opponent_name",
    "source_bookmaker",
    "source_market",
    "source_line_type",
    "source_odds_timestamp",
    "american_odds",
    "decimal_odds",
    "candidate_event_id",
    "candidate_fight_id",
    "candidate_fighter_id",
    "candidate_opponent_fighter_id",
    "notes",
]

QA_STATUS_VALUES = ("matched", "unmatched", "duplicate", "ambiguous", "rejected")

QA_COUNT_COLUMNS = [
    "matched_count",
    "unmatched_count",
    "duplicate_count",
    "ambiguous_count",
    "rejected_count",
]

QA_COLUMNS = [
    "source",
    "row_number",
    "row_status",
    "match_reason",
    "rejection_reason",
    "source_event_name",
    "source_event_date",
    "source_fighter_1_name",
    "source_fighter_2_name",
    "source_bookmaker",
    "source_region",
    "source_market",
    "source_line_type",
    "source_odds_timestamp",
    "odds_1",
    "odds_2",
    "candidate_event_id",
    "candidate_event_name",
    "candidate_event_date",
    "candidate_fight_id",
    "candidate_fighter_1_id",
    "candidate_fighter_1_name",
    "candidate_fighter_2_id",
    "candidate_fighter_2_name",
    "canonical_rows",
    "notes",
    *QA_COUNT_COLUMNS,
]


@dataclass(frozen=True)
class WarehouseEvent:
    """Event identity available from the warehouse snapshot."""

    event_id: str
    event_date: str | None
    event_name: str | None = None


@dataclass(frozen=True)
class WarehouseFight:
    """Fight identity available from the warehouse snapshot."""

    fight_id: str
    event_id: str
    url: str
    fighter_1_id: str
    fighter_2_id: str


@dataclass(frozen=True)
class WarehouseFighter:
    """Fighter identity available from the warehouse snapshot."""

    fighter_id: str
    url: str
    full_name: str | None = None


@dataclass(frozen=True)
class OddsIdentityMap:
    """URL-keyed warehouse IDs used by the Kaggle adapter."""

    events_by_id: dict[str, WarehouseEvent]
    fights_by_url: dict[str, tuple[WarehouseFight, ...]]
    fighters_by_url: dict[str, tuple[WarehouseFighter, ...]]


@dataclass(frozen=True)
class KaggleAdaptResult:
    """Converted canonical odds rows plus review rows."""

    rows_read: int
    matched_rows: tuple[dict[str, str], ...]
    unmatched_rows: tuple[dict[str, str], ...]
    qa_rows: tuple[dict[str, str], ...]
    skipped_rows: int


def read_csv_rows(path: Path) -> tuple[list[tuple[int, dict[str, str]]], set[str]]:
    """Read a CSV as numbered dict rows and report missing Kaggle columns."""
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = KAGGLE_REQUIRED_COLUMNS - fieldnames
        if missing:
            return [], missing
        return [(row_number, row) for row_number, row in enumerate(reader, start=2)], set()


def build_identity_map_from_csvs(
    *,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
) -> OddsIdentityMap:
    """Build URL-keyed warehouse mappings from local warehouse CSV snapshots."""
    return build_identity_map(
        events=_read_plain_csv(events_csv),
        fights=_read_plain_csv(fights_csv),
        fighters=_read_plain_csv(fighters_csv),
    )


def build_identity_map(
    *,
    events: Iterable[Mapping[str, object]],
    fights: Iterable[Mapping[str, object]],
    fighters: Iterable[Mapping[str, object]],
) -> OddsIdentityMap:
    """Build URL-keyed mappings from warehouse-like event/fight/fighter rows."""
    events_by_id: dict[str, WarehouseEvent] = {}
    for row in events:
        event_id = _text(row.get("event_id"))
        if event_id is None:
            continue
        events_by_id[event_id] = WarehouseEvent(
            event_id=event_id,
            event_date=_event_date(row),
            event_name=_text(row.get("event_name")) or _text(row.get("name")),
        )

    fights_by_url: dict[str, list[WarehouseFight]] = {}
    for row in fights:
        url = _normalized_url(row.get("url") or row.get("source_url"))
        fight_id = _text(row.get("fight_id"))
        event_id = _text(row.get("event_id"))
        fighter_1_id = _text(row.get("fighter_1_id"))
        fighter_2_id = _text(row.get("fighter_2_id"))
        if None in {url, fight_id, event_id, fighter_1_id, fighter_2_id}:
            continue
        fights_by_url.setdefault(url, []).append(WarehouseFight(
            fight_id=fight_id,
            event_id=event_id,
            url=url,
            fighter_1_id=fighter_1_id,
            fighter_2_id=fighter_2_id,
        ))

    fighters_by_url: dict[str, list[WarehouseFighter]] = {}
    for row in fighters:
        url = _normalized_url(row.get("url") or row.get("source_url"))
        fighter_id = _text(row.get("fighter_id"))
        if url is None or fighter_id is None:
            continue
        fighters_by_url.setdefault(url, []).append(WarehouseFighter(
            fighter_id=fighter_id,
            url=url,
            full_name=_text(row.get("full_name")) or _text(row.get("name")),
        ))

    return OddsIdentityMap(
        events_by_id=events_by_id,
        fights_by_url={key: tuple(value) for key, value in fights_by_url.items()},
        fighters_by_url={key: tuple(value) for key, value in fighters_by_url.items()},
    )


def adapt_kaggle_odds_rows(
    raw_rows: Iterable[tuple[int, Mapping[str, object]]],
    identity_map: OddsIdentityMap,
) -> KaggleAdaptResult:
    """Convert Kaggle rows into canonical odds rows and unmatched review rows."""
    matched_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    qa_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    rows_read = 0
    skipped_rows = 0

    for row_number, raw in raw_rows:
        rows_read += 1
        try:
            canonical_rows = _adapt_kaggle_row(row_number, raw, identity_map)
        except ValueError as exc:
            reason = str(exc)
            unmatched_rows.append(_unmatched_row(row_number, raw, reason))
            qa_rows.append(_qa_row(
                row_number,
                raw,
                identity_map,
                row_status=_qa_status_for_reason(reason),
                match_reason="",
                rejection_reason=reason,
            ))
            continue

        keys = [_stable_key(row) for row in canonical_rows]
        if any(key in seen_keys for key in keys):
            skipped_rows += 1
            unmatched_rows.append(_unmatched_row(
                row_number,
                raw,
                "duplicate_stable_odds_key",
                canonical_rows=canonical_rows,
            ))
            qa_rows.append(_qa_row(
                row_number,
                raw,
                identity_map,
                row_status="duplicate",
                match_reason="exact_url_match",
                rejection_reason="duplicate_stable_odds_key",
                canonical_rows=canonical_rows,
            ))
            continue

        seen_keys.update(keys)
        matched_rows.extend(canonical_rows)
        qa_rows.append(_qa_row(
            row_number,
            raw,
            identity_map,
            row_status="matched",
            match_reason="exact_url_match",
            rejection_reason="",
            canonical_rows=canonical_rows,
        ))

    return KaggleAdaptResult(
        rows_read=rows_read,
        matched_rows=tuple(matched_rows),
        unmatched_rows=tuple(unmatched_rows),
        qa_rows=tuple(qa_rows),
        skipped_rows=skipped_rows,
    )


def adapt_kaggle_odds_file(
    *,
    raw_csv: Path = DEFAULT_RAW_CSV,
    source_output: Path = DEFAULT_SOURCE_OUTPUT,
    canonical_output: Path = DEFAULT_CANONICAL_OUTPUT,
    unmatched_output: Path = DEFAULT_UNMATCHED_OUTPUT,
    qa_output: Path = DEFAULT_QA_OUTPUT,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
) -> KaggleAdaptResult:
    """Read raw Kaggle odds and write source, canonical, unmatched, and QA CSVs."""
    raw_rows, missing = read_csv_rows(raw_csv)
    if missing:
        raise ValueError(f"{raw_csv} is missing required columns: {', '.join(sorted(missing))}")

    identity_map = build_identity_map_from_csvs(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )
    result = adapt_kaggle_odds_rows(raw_rows, identity_map)
    write_adapted_outputs(
        result,
        source_output=source_output,
        canonical_output=canonical_output,
        unmatched_output=unmatched_output,
        qa_output=qa_output,
    )
    return result


def write_adapted_outputs(
    result: KaggleAdaptResult,
    *,
    source_output: Path = DEFAULT_SOURCE_OUTPUT,
    canonical_output: Path = DEFAULT_CANONICAL_OUTPUT,
    unmatched_output: Path = DEFAULT_UNMATCHED_OUTPUT,
    qa_output: Path = DEFAULT_QA_OUTPUT,
) -> None:
    """Write matched rows, unmatched review rows, and the QA report."""
    _write_csv(source_output, CANONICAL_COLUMNS, result.matched_rows)
    _write_csv(canonical_output, CANONICAL_COLUMNS, result.matched_rows)
    _write_csv(unmatched_output, UNMATCHED_COLUMNS, result.unmatched_rows)
    _write_csv(qa_output, QA_COLUMNS, qa_rows_with_counts(result.qa_rows))


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the Kaggle odds adapter CLI parser."""
    parser = argparse.ArgumentParser(
        description="Adapt raw Kaggle UFC odds into the canonical fight_odds CSV contract."
    )
    parser.add_argument("--raw-csv", type=Path, default=DEFAULT_RAW_CSV)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    parser.add_argument("--source-output", type=Path, default=DEFAULT_SOURCE_OUTPUT)
    parser.add_argument("--canonical-output", type=Path, default=DEFAULT_CANONICAL_OUTPUT)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT)
    parser.add_argument("--qa-output", type=Path, default=DEFAULT_QA_OUTPUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the Kaggle odds adapter."""
    args = build_arg_parser().parse_args(argv)
    result = adapt_kaggle_odds_file(
        raw_csv=args.raw_csv,
        source_output=args.source_output,
        canonical_output=args.canonical_output,
        unmatched_output=args.unmatched_output,
        qa_output=args.qa_output,
        events_csv=args.events_csv,
        fights_csv=args.fights_csv,
        fighters_csv=args.fighters_csv,
    )
    qa_counts = qa_summary_counts(result.qa_rows)
    print(f"Rows read: {result.rows_read}")
    print(f"Matched canonical rows: {len(result.matched_rows)}")
    print(f"Unmatched/review rows: {len(result.unmatched_rows)}")
    print(f"Skipped duplicate source rows: {result.skipped_rows}")
    print(
        "QA source-row counts: "
        + ", ".join(f"{status}={qa_counts[status]}" for status in QA_STATUS_VALUES)
    )
    print(f"Wrote: {args.source_output}")
    print(f"Wrote: {args.canonical_output}")
    print(f"Wrote: {args.unmatched_output}")
    print(f"Wrote: {args.qa_output}")
    return 0


def _adapt_kaggle_row(
    row_number: int,
    raw: Mapping[str, object],
    identity_map: OddsIdentityMap,
) -> tuple[dict[str, str], dict[str, str]]:
    fight = _single_match(
        identity_map.fights_by_url,
        raw.get("fight_url"),
        missing_reason="unknown_fight_url",
        ambiguous_reason="ambiguous_fight_url",
    )
    fighter_1 = _single_match(
        identity_map.fighters_by_url,
        raw.get("fighter_1_url"),
        missing_reason="unknown_fighter_1_url",
        ambiguous_reason="ambiguous_fighter_1_url",
    )
    fighter_2 = _single_match(
        identity_map.fighters_by_url,
        raw.get("fighter_2_url"),
        missing_reason="unknown_fighter_2_url",
        ambiguous_reason="ambiguous_fighter_2_url",
    )

    if {fighter_1.fighter_id, fighter_2.fighter_id} != {fight.fighter_1_id, fight.fighter_2_id}:
        raise ValueError("fighter_urls_do_not_match_fight")

    event = identity_map.events_by_id.get(fight.event_id)
    event_date = _required_date(raw.get("event_date"), "event_date")
    if event is not None and event.event_date is not None and event.event_date != event_date:
        raise ValueError("event_date_mismatch")

    odds_timestamp = _required_timestamp(raw.get("adding_date"), "adding_date")
    source = _required_text(raw, "source")
    odds_1 = _decimal_odds(raw.get("odds_1"), "odds_1")
    odds_2 = _decimal_odds(raw.get("odds_2"), "odds_2")
    bookmaker = _bookmaker(source, raw.get("region"))

    return (
        _canonical_row(
            fight=fight,
            event_date=event_date,
            fighter_id=fighter_1.fighter_id,
            fighter_name=_required_text(raw, "fighter_1"),
            opponent_fighter_id=fighter_2.fighter_id,
            bookmaker=bookmaker,
            odds_timestamp=odds_timestamp,
            decimal_odds=odds_1,
        ),
        _canonical_row(
            fight=fight,
            event_date=event_date,
            fighter_id=fighter_2.fighter_id,
            fighter_name=_required_text(raw, "fighter_2"),
            opponent_fighter_id=fighter_1.fighter_id,
            bookmaker=bookmaker,
            odds_timestamp=odds_timestamp,
            decimal_odds=odds_2,
        ),
    )


def _canonical_row(
    *,
    fight: WarehouseFight,
    event_date: str,
    fighter_id: str,
    fighter_name: str,
    opponent_fighter_id: str,
    bookmaker: str,
    odds_timestamp: str,
    decimal_odds: str,
) -> dict[str, str]:
    return {
        "fight_id": fight.fight_id,
        "event_id": fight.event_id,
        "event_date": event_date,
        "fighter_id": fighter_id,
        "fighter_name": fighter_name,
        "opponent_fighter_id": opponent_fighter_id,
        "bookmaker": bookmaker,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": odds_timestamp,
        "american_odds": "",
        "decimal_odds": decimal_odds,
        "source": KAGGLE_SOURCE_LABEL,
        "source_url": KAGGLE_DATASET_URL,
        "imported_at": odds_timestamp,
    }


def _unmatched_row(
    row_number: int,
    raw: Mapping[str, object],
    reason: str,
    *,
    canonical_rows: tuple[Mapping[str, str], ...] = (),
) -> dict[str, str]:
    first = canonical_rows[0] if canonical_rows else {}
    second = canonical_rows[1] if len(canonical_rows) > 1 else {}
    return {
        "source": KAGGLE_SOURCE_LABEL,
        "row_number": str(row_number),
        "rejection_reason": reason,
        "source_event_name": "",
        "source_event_date": _text(raw.get("event_date")) or "",
        "source_fighter_name": _text(raw.get("fighter_1")) or "",
        "source_opponent_name": _text(raw.get("fighter_2")) or "",
        "source_bookmaker": _text(raw.get("source")) or "",
        "source_market": "h2h",
        "source_line_type": "current",
        "source_odds_timestamp": _text(raw.get("adding_date")) or "",
        "american_odds": "",
        "decimal_odds": f"odds_1={_text(raw.get('odds_1')) or ''};odds_2={_text(raw.get('odds_2')) or ''}",
        "candidate_event_id": _text(first.get("event_id")) or "",
        "candidate_fight_id": _text(first.get("fight_id")) or "",
        "candidate_fighter_id": _text(first.get("fighter_id")) or "",
        "candidate_opponent_fighter_id": _text(second.get("fighter_id")) or "",
        "notes": (
            f"fight_url={_text(raw.get('fight_url')) or ''};"
            f"fighter_1_url={_text(raw.get('fighter_1_url')) or ''};"
            f"fighter_2_url={_text(raw.get('fighter_2_url')) or ''}"
        ),
    }


def qa_summary_counts(qa_rows: Iterable[Mapping[str, str]]) -> dict[str, int]:
    """Return deterministic source-row QA counts by status."""
    counts = {status: 0 for status in QA_STATUS_VALUES}
    for row in qa_rows:
        status = row.get("row_status", "")
        counts[status] = counts.get(status, 0) + 1
    return counts


def qa_rows_with_counts(
    qa_rows: tuple[dict[str, str], ...],
) -> tuple[dict[str, str], ...]:
    """Add repeated summary counts to each QA row for spreadsheet review."""
    counts = qa_summary_counts(qa_rows)
    count_columns = {
        f"{status}_count": str(counts.get(status, 0))
        for status in QA_STATUS_VALUES
    }
    return tuple({**row, **count_columns} for row in qa_rows)


def _qa_row(
    row_number: int,
    raw: Mapping[str, object],
    identity_map: OddsIdentityMap,
    *,
    row_status: str,
    match_reason: str,
    rejection_reason: str,
    canonical_rows: tuple[Mapping[str, str], ...] = (),
) -> dict[str, str]:
    context = _candidate_context(raw, identity_map)
    notes = context["notes"]
    if canonical_rows:
        notes = f"{notes};canonical_keys={len(canonical_rows)}"
    return {
        "source": KAGGLE_SOURCE_LABEL,
        "row_number": str(row_number),
        "row_status": row_status,
        "match_reason": match_reason,
        "rejection_reason": rejection_reason,
        "source_event_name": "",
        "source_event_date": _text(raw.get("event_date")) or "",
        "source_fighter_1_name": _text(raw.get("fighter_1")) or "",
        "source_fighter_2_name": _text(raw.get("fighter_2")) or "",
        "source_bookmaker": _text(raw.get("source")) or "",
        "source_region": _text(raw.get("region")) or "",
        "source_market": "h2h",
        "source_line_type": "current",
        "source_odds_timestamp": _text(raw.get("adding_date")) or "",
        "odds_1": _text(raw.get("odds_1")) or "",
        "odds_2": _text(raw.get("odds_2")) or "",
        "candidate_event_id": context["event_ids"],
        "candidate_event_name": context["event_names"],
        "candidate_event_date": context["event_dates"],
        "candidate_fight_id": context["fight_ids"],
        "candidate_fighter_1_id": context["fighter_1_ids"],
        "candidate_fighter_1_name": context["fighter_1_names"],
        "candidate_fighter_2_id": context["fighter_2_ids"],
        "candidate_fighter_2_name": context["fighter_2_names"],
        "canonical_rows": str(len(canonical_rows)),
        "notes": notes,
    }


def _candidate_context(raw: Mapping[str, object], identity_map: OddsIdentityMap) -> dict[str, str]:
    fight_url = _normalized_url(raw.get("fight_url"))
    fighter_1_url = _normalized_url(raw.get("fighter_1_url"))
    fighter_2_url = _normalized_url(raw.get("fighter_2_url"))
    fight_matches = identity_map.fights_by_url.get(fight_url or "", ())
    fighter_1_matches = identity_map.fighters_by_url.get(fighter_1_url or "", ())
    fighter_2_matches = identity_map.fighters_by_url.get(fighter_2_url or "", ())
    event_matches = tuple(
        event
        for event_id in _unique_text(fight.event_id for fight in fight_matches)
        for event in [identity_map.events_by_id.get(event_id)]
        if event is not None
    )

    return {
        "event_ids": _join_unique(event.event_id for event in event_matches),
        "event_names": _join_unique(event.event_name for event in event_matches),
        "event_dates": _join_unique(event.event_date for event in event_matches),
        "fight_ids": _join_unique(fight.fight_id for fight in fight_matches),
        "fighter_1_ids": _join_unique(fighter.fighter_id for fighter in fighter_1_matches),
        "fighter_1_names": _join_unique(fighter.full_name for fighter in fighter_1_matches),
        "fighter_2_ids": _join_unique(fighter.fighter_id for fighter in fighter_2_matches),
        "fighter_2_names": _join_unique(fighter.full_name for fighter in fighter_2_matches),
        "notes": (
            f"fight_url={_text(raw.get('fight_url')) or ''};"
            f"fighter_1_url={_text(raw.get('fighter_1_url')) or ''};"
            f"fighter_2_url={_text(raw.get('fighter_2_url')) or ''};"
            f"fight_url_matches={len(fight_matches)};"
            f"fighter_1_url_matches={len(fighter_1_matches)};"
            f"fighter_2_url_matches={len(fighter_2_matches)}"
        ),
    }


def _qa_status_for_reason(reason: str) -> str:
    if reason == "duplicate_stable_odds_key":
        return "duplicate"
    if reason.startswith("ambiguous_"):
        return "ambiguous"
    if reason.startswith("unknown_"):
        return "unmatched"
    return "rejected"


def _join_unique(values: Iterable[object]) -> str:
    return "|".join(_unique_text(values))


def _unique_text(values: Iterable[object]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique_values: list[str] = []
    for value in values:
        text = _text(value)
        if text is None or text in seen:
            continue
        seen.add(text)
        unique_values.append(text)
    return tuple(unique_values)


def _single_match(
    mapping: Mapping[str, tuple],
    raw_url: object,
    *,
    missing_reason: str,
    ambiguous_reason: str,
):
    url = _normalized_url(raw_url)
    if url is None:
        raise ValueError(missing_reason)
    matches = mapping.get(url, ())
    if not matches:
        raise ValueError(missing_reason)
    if len(matches) > 1:
        raise ValueError(ambiguous_reason)
    return matches[0]


def _stable_key(row: Mapping[str, str]) -> tuple[str, str, str, str, str, str]:
    return (
        row["fight_id"],
        row["fighter_id"],
        row["bookmaker"],
        row["market"],
        row["line_type"],
        row["odds_timestamp"],
    )


def _bookmaker(source: str, region: object) -> str:
    region_text = _text(region)
    return f"{source}/{region_text}" if region_text else source


def _decimal_odds(value: object, column: str) -> str:
    text = _required_value(value, column)
    try:
        decimal_odds = validate_decimal_odds(Decimal(text))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid_{column}") from exc
    return format(decimal_odds, "f")


def _required_timestamp(value: object, column: str) -> str:
    text = _required_value(value, column)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid_{column}") from exc


def _required_date(value: object, column: str) -> str:
    text = _required_value(value, column)
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid_{column}") from exc


def _required_text(row: Mapping[str, object], column: str) -> str:
    return _required_value(row.get(column), column)


def _required_value(value: object, column: str) -> str:
    text = _text(value)
    if text is None:
        raise ValueError(f"missing_{column}")
    return text


def _event_date(row: Mapping[str, object]) -> str | None:
    date_text = _text(row.get("event_date")) or _text(row.get("date_formatted"))
    if date_text is not None:
        return _date_value(date_text)
    verbose_date = _text(row.get("date"))
    if verbose_date is None:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(verbose_date, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _date_value(value: str) -> str | None:
    try:
        return datetime.fromisoformat(value[:10]).date().isoformat()
    except ValueError:
        return None


def _normalized_url(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    parts = urlsplit(text)
    netloc = parts.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = parts.path.rstrip("/")
    return urlunsplit((parts.scheme.lower() or "http", netloc, path, "", ""))


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read_plain_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, columns: list[str], rows: tuple[Mapping[str, str], ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


__all__ = [
    "CANONICAL_COLUMNS",
    "DEFAULT_CANONICAL_OUTPUT",
    "DEFAULT_QA_OUTPUT",
    "DEFAULT_RAW_CSV",
    "DEFAULT_SOURCE_OUTPUT",
    "DEFAULT_UNMATCHED_OUTPUT",
    "KAGGLE_DATASET_URL",
    "KAGGLE_SOURCE_LABEL",
    "KAGGLE_REQUIRED_COLUMNS",
    "KaggleAdaptResult",
    "OddsIdentityMap",
    "QA_COLUMNS",
    "QA_COUNT_COLUMNS",
    "QA_STATUS_VALUES",
    "UNMATCHED_COLUMNS",
    "WarehouseEvent",
    "WarehouseFight",
    "WarehouseFighter",
    "adapt_kaggle_odds_file",
    "adapt_kaggle_odds_rows",
    "build_arg_parser",
    "build_identity_map",
    "build_identity_map_from_csvs",
    "main",
    "qa_rows_with_counts",
    "qa_summary_counts",
    "read_csv_rows",
    "write_adapted_outputs",
]


if __name__ == "__main__":
    raise SystemExit(main())
