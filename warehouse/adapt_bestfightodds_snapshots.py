"""Adapt reviewed BestFightOdds raw snapshots into source-specific odds rows.

This adapter is offline-only. It reads HTML snapshots and adjacent metadata
captured by ``probe_bestfightodds_snapshot.py``; it never fetches network
content. Rows without precise observed timestamps are kept in the unmatched
review output instead of being turned into point-in-time odds observations.

Usage:
    python3 warehouse/adapt_bestfightodds_snapshots.py
    python3 warehouse/adapt_bestfightodds_snapshots.py --raw-dir data/odds/raw/bestfightodds
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import american_to_decimal_odds, validate_american_odds
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS

REPO_ROOT = Path(__file__).resolve().parent.parent
BESTFIGHTODDS_SOURCE_LABEL = "bestfightodds_raw_snapshot"
DEFAULT_BOOKMAKER = "BestFightOdds aggregate"

DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "bestfightodds"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_SOURCE_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "bestfightodds_fight_odds.csv"
DEFAULT_UNMATCHED_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "bestfightodds_unmatched_odds.csv"

BFO_AUDIT_COLUMNS = [
    "source_snapshot_path",
    "source_snapshot_sha256",
    "source_event_name",
    "source_fighter_name",
    "source_opponent_name",
    "source_timestamp_quality",
    "source_line_value",
]

BFO_SOURCE_COLUMNS = [*CANONICAL_COLUMNS, *BFO_AUDIT_COLUMNS]

UNMATCHED_COLUMNS = [
    "source",
    "row_number",
    "rejection_reason",
    "source_snapshot_path",
    "source_snapshot_sha256",
    "source_url",
    "source_event_name",
    "source_event_date",
    "source_fighter_name",
    "source_opponent_name",
    "source_bookmaker",
    "source_market",
    "source_line_type",
    "source_odds_timestamp",
    "source_timestamp_quality",
    "american_odds",
    "candidate_event_id",
    "candidate_event_name",
    "candidate_fight_id",
    "candidate_fighter_id",
    "candidate_opponent_fighter_id",
    "notes",
]


@dataclass(frozen=True)
class LocalFight:
    """Local warehouse fight identity used for conservative BFO matching."""

    event_id: str
    event_name: str
    event_date: str
    fight_id: str
    fighter_1_id: str
    fighter_1_name: str
    fighter_2_id: str
    fighter_2_name: str


@dataclass(frozen=True)
class BfoSnapshot:
    """Raw snapshot contents plus metadata needed for audit output."""

    path: Path
    html_text: str
    source_url: str
    fetched_at: str
    content_sha256: str


@dataclass(frozen=True)
class BfoSourceRow:
    """One parsed fighter-side BFO moneyline summary row."""

    row_number: int
    snapshot_path: str
    snapshot_sha256: str
    source_url: str
    source_event_name: str
    source_event_date: str
    fighter_name: str
    opponent_name: str
    bookmaker: str
    market: str
    open_odds: str
    close_odds: str
    open_timestamp: str
    close_timestamp: str
    timestamp_quality: str
    raw_cells: tuple[str, ...]


@dataclass(frozen=True)
class BestFightOddsAdaptResult:
    """Adapted BestFightOdds rows and review rows."""

    snapshots_read: int
    source_rows_read: int
    matched_rows: tuple[dict[str, str], ...]
    unmatched_rows: tuple[dict[str, str], ...]
    skipped_rows: int


def adapt_bestfightodds_snapshots(
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
    source_output: Path = DEFAULT_SOURCE_OUTPUT,
    unmatched_output: Path = DEFAULT_UNMATCHED_OUTPUT,
    imported_at: str | None = None,
) -> BestFightOddsAdaptResult:
    """Read local BFO snapshots and write source-specific normalized outputs."""
    local_fights = _local_fights_by_date_pair(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )
    snapshots = read_snapshots(raw_dir)
    source_rows = tuple(
        row
        for snapshot in snapshots
        for row in parse_snapshot_rows(snapshot)
    )
    result = adapt_bestfightodds_rows(
        source_rows,
        local_fights,
        imported_at=imported_at,
    )
    _write_csv(source_output, BFO_SOURCE_COLUMNS, result.matched_rows)
    _write_csv(unmatched_output, UNMATCHED_COLUMNS, result.unmatched_rows)
    return BestFightOddsAdaptResult(
        snapshots_read=len(snapshots),
        source_rows_read=result.source_rows_read,
        matched_rows=result.matched_rows,
        unmatched_rows=result.unmatched_rows,
        skipped_rows=result.skipped_rows,
    )


def read_snapshots(raw_dir: Path) -> tuple[BfoSnapshot, ...]:
    """Read local HTML snapshots and optional adjacent metadata."""
    snapshots: list[BfoSnapshot] = []
    if not raw_dir.exists():
        return ()
    for path in sorted(raw_dir.glob("*.html")):
        metadata = _read_metadata(path)
        snapshots.append(BfoSnapshot(
            path=path,
            html_text=path.read_text(encoding="utf-8"),
            source_url=_text(metadata.get("source_url")) or _text(metadata.get("final_url")) or "",
            fetched_at=_timestamp_text(metadata.get("fetched_at")) or "",
            content_sha256=_text(metadata.get("content_sha256")) or "",
        ))
    return tuple(snapshots)


def parse_snapshot_rows(snapshot: BfoSnapshot) -> tuple[BfoSourceRow, ...]:
    """Extract supported BFO fighter-history moneyline rows from one snapshot."""
    soup = BeautifulSoup(snapshot.html_text, "html.parser")
    parsed_rows: list[BfoSourceRow] = []
    row_number = 0
    for table in soup.find_all("table"):
        header_cells = _table_headers(table)
        if not header_cells:
            continue
        header_map = _header_map(header_cells)
        if not _is_supported_table(header_map):
            continue

        for tr in table.find_all("tr"):
            cells = [_clean_cell(cell.get_text(" ", strip=True)) for cell in tr.find_all(["td", "th"])]
            if not cells or cells == header_cells:
                continue
            row = _row_by_headers(header_cells, cells)
            row_number += 1
            market = _field(row, "market") or "moneyline"
            if _normalize_market(market) != "moneyline":
                parsed_rows.append(_source_row_from_fields(
                    snapshot=snapshot,
                    row_number=row_number,
                    row=row,
                    cells=tuple(cells),
                    market=market,
                ))
                continue
            parsed_rows.append(_source_row_from_fields(
                snapshot=snapshot,
                row_number=row_number,
                row=row,
                cells=tuple(cells),
                market="moneyline",
            ))
    if parsed_rows:
        return tuple(parsed_rows)

    for table in soup.find_all("table", class_="team-stats-table"):
        for row in _parse_native_fighter_history_table(snapshot, table, start_row_number=row_number):
            row_number = row.row_number
            parsed_rows.append(row)
    return tuple(parsed_rows)


def adapt_bestfightodds_rows(
    source_rows: Iterable[BfoSourceRow],
    local_fights_by_date_pair: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    *,
    imported_at: str | None = None,
) -> BestFightOddsAdaptResult:
    """Convert parsed BFO source rows into canonical-compatible odds rows."""
    matched_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    rows_read = 0
    skipped_rows = 0
    imported_at_value = imported_at or datetime.now(timezone.utc).isoformat()

    for source_row in source_rows:
        rows_read += 1
        try:
            canonical_rows = _adapt_source_row(
                source_row,
                local_fights_by_date_pair,
                imported_at=imported_at_value,
            )
        except ValueError as exc:
            unmatched_rows.append(_unmatched_row(source_row, str(exc)))
            continue

        if not canonical_rows:
            unmatched_rows.append(_unmatched_row(source_row, "missing_moneyline_odds"))
            continue

        duplicate = False
        for row in canonical_rows:
            key = _stable_key(row)
            if key in seen_keys:
                duplicate = True
                break
        if duplicate:
            skipped_rows += 1
            unmatched_rows.append(_unmatched_row(
                source_row,
                "duplicate_stable_odds_key",
                canonical_rows=canonical_rows,
            ))
            continue

        seen_keys.update(_stable_key(row) for row in canonical_rows)
        matched_rows.extend(canonical_rows)

    return BestFightOddsAdaptResult(
        snapshots_read=0,
        source_rows_read=rows_read,
        matched_rows=tuple(matched_rows),
        unmatched_rows=tuple(unmatched_rows),
        skipped_rows=skipped_rows,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Adapt stored BestFightOdds raw snapshots into source-specific odds CSVs."
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    parser.add_argument("--source-output", type=Path, default=DEFAULT_SOURCE_OUTPUT)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = adapt_bestfightodds_snapshots(
        raw_dir=args.raw_dir,
        events_csv=args.events_csv,
        fights_csv=args.fights_csv,
        fighters_csv=args.fighters_csv,
        source_output=args.source_output,
        unmatched_output=args.unmatched_output,
    )
    print(f"Snapshots read: {result.snapshots_read}")
    print(f"Source rows read: {result.source_rows_read}")
    print(f"Matched canonical-compatible rows: {len(result.matched_rows)}")
    print(f"Unmatched/review rows: {len(result.unmatched_rows)}")
    print(f"Skipped duplicate source rows: {result.skipped_rows}")
    print(f"Wrote: {args.source_output}")
    print(f"Wrote: {args.unmatched_output}")
    return 0


def _adapt_source_row(
    source_row: BfoSourceRow,
    local_fights_by_date_pair: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    *,
    imported_at: str,
) -> tuple[dict[str, str], ...]:
    if _normalize_market(source_row.market) != "moneyline":
        raise ValueError("non_moneyline_market")

    source_event_date = _date_iso(source_row.source_event_date)
    if source_event_date is None:
        raise ValueError("missing_source_event_date")

    local_matches = local_fights_by_date_pair.get(
        (source_event_date, _pair_key(source_row.fighter_name, source_row.opponent_name)),
        (),
    )
    if not local_matches:
        raise ValueError("unknown_local_fight_pair")
    event_filtered = _filter_by_event_name(local_matches, source_row.source_event_name)
    if not event_filtered:
        raise ValueError("unknown_local_event_name")
    if len(event_filtered) > 1:
        raise ValueError("ambiguous_local_fight_pair")
    local = event_filtered[0]

    side = _local_side(local, source_row.fighter_name)
    opponent = _local_side(local, source_row.opponent_name)
    if side is None or opponent is None or side[0] == opponent[0]:
        raise ValueError("matched_pair_but_side_names_do_not_align")

    canonical_rows: list[dict[str, str]] = []
    for line_type, raw_odds, timestamp in [
        ("opening", source_row.open_odds, source_row.open_timestamp),
        ("closing", source_row.close_odds, source_row.close_timestamp),
    ]:
        odds_text = _text(raw_odds)
        if odds_text is None:
            continue
        if _text(timestamp) is None:
            raise ValueError(f"missing_{line_type}_observed_timestamp_open_close_label_only")
        american_odds = _american_odds(odds_text, line_type)
        odds_timestamp = _timestamp(timestamp, f"{line_type}_timestamp")
        canonical_rows.append(_canonical_row(
            source_row=source_row,
            local=local,
            fighter_id=side[0],
            fighter_name=side[1],
            opponent_fighter_id=opponent[0],
            line_type=line_type,
            odds_timestamp=odds_timestamp,
            american_odds=american_odds,
            imported_at=imported_at,
            source_event_date=source_event_date,
            raw_line_value=odds_text,
        ))

    return tuple(canonical_rows)


def _canonical_row(
    *,
    source_row: BfoSourceRow,
    local: LocalFight,
    fighter_id: str,
    fighter_name: str,
    opponent_fighter_id: str,
    line_type: str,
    odds_timestamp: str,
    american_odds: int,
    imported_at: str,
    source_event_date: str,
    raw_line_value: str,
) -> dict[str, str]:
    decimal_odds = american_to_decimal_odds(american_odds)
    return {
        "fight_id": local.fight_id,
        "event_id": local.event_id,
        "event_date": source_event_date,
        "fighter_id": fighter_id,
        "fighter_name": fighter_name,
        "opponent_fighter_id": opponent_fighter_id,
        "bookmaker": source_row.bookmaker or DEFAULT_BOOKMAKER,
        "market": "moneyline",
        "line_type": line_type,
        "odds_timestamp": odds_timestamp,
        "american_odds": str(american_odds),
        "decimal_odds": format(decimal_odds, "f"),
        "source": BESTFIGHTODDS_SOURCE_LABEL,
        "source_url": source_row.source_url,
        "imported_at": imported_at,
        "source_snapshot_path": source_row.snapshot_path,
        "source_snapshot_sha256": source_row.snapshot_sha256,
        "source_event_name": source_row.source_event_name,
        "source_fighter_name": source_row.fighter_name,
        "source_opponent_name": source_row.opponent_name,
        "source_timestamp_quality": "observed_timestamp",
        "source_line_value": raw_line_value,
    }


def _unmatched_row(
    source_row: BfoSourceRow,
    reason: str,
    *,
    canonical_rows: tuple[Mapping[str, str], ...] = (),
) -> dict[str, str]:
    first = canonical_rows[0] if canonical_rows else {}
    return {
        "source": BESTFIGHTODDS_SOURCE_LABEL,
        "row_number": str(source_row.row_number),
        "rejection_reason": reason,
        "source_snapshot_path": source_row.snapshot_path,
        "source_snapshot_sha256": source_row.snapshot_sha256,
        "source_url": source_row.source_url,
        "source_event_name": source_row.source_event_name,
        "source_event_date": source_row.source_event_date,
        "source_fighter_name": source_row.fighter_name,
        "source_opponent_name": source_row.opponent_name,
        "source_bookmaker": source_row.bookmaker,
        "source_market": source_row.market,
        "source_line_type": _line_type_context(source_row),
        "source_odds_timestamp": _timestamp_context(source_row),
        "source_timestamp_quality": source_row.timestamp_quality or "open_close_label_only",
        "american_odds": f"open={source_row.open_odds};close={source_row.close_odds}",
        "candidate_event_id": _text(first.get("event_id")) or "",
        "candidate_event_name": "",
        "candidate_fight_id": _text(first.get("fight_id")) or "",
        "candidate_fighter_id": _text(first.get("fighter_id")) or "",
        "candidate_opponent_fighter_id": _text(first.get("opponent_fighter_id")) or "",
        "notes": "raw_cells=" + " | ".join(source_row.raw_cells),
    }


def _source_row_from_fields(
    *,
    snapshot: BfoSnapshot,
    row_number: int,
    row: Mapping[str, str],
    cells: tuple[str, ...],
    market: str,
) -> BfoSourceRow:
    event_date = (
        _field(row, "event_date")
        or _date_from_event_text(_field(row, "event") or "")
        or ""
    )
    event_name = _event_name_without_date(_field(row, "event") or "")
    if _field(row, "event_name"):
        event_name = _field(row, "event_name") or event_name
    close_raw = _field(row, "closing_range") or _field(row, "closing") or ""
    open_timestamp = _field(row, "open_timestamp") or _field(row, "opening_timestamp") or ""
    close_timestamp = _field(row, "close_timestamp") or _field(row, "closing_timestamp") or ""
    timestamp_quality = "observed_timestamp" if open_timestamp or close_timestamp else "open_close_label_only"
    return BfoSourceRow(
        row_number=row_number,
        snapshot_path=str(snapshot.path),
        snapshot_sha256=snapshot.content_sha256,
        source_url=snapshot.source_url,
        source_event_name=event_name,
        source_event_date=event_date,
        fighter_name=_field(row, "fighter") or _field(row, "matchup") or "",
        opponent_name=_field(row, "opponent") or "",
        bookmaker=_field(row, "bookmaker") or DEFAULT_BOOKMAKER,
        market=market,
        open_odds=_american_odds_text(_field(row, "open") or _field(row, "opening") or ""),
        close_odds=_closing_odds_text(close_raw),
        open_timestamp=open_timestamp,
        close_timestamp=close_timestamp,
        timestamp_quality=timestamp_quality,
        raw_cells=cells,
    )


def _parse_native_fighter_history_table(
    snapshot: BfoSnapshot,
    table,
    *,
    start_row_number: int,
) -> tuple[BfoSourceRow, ...]:
    rows: list[BfoSourceRow] = []
    row_number = start_row_number
    event_name = ""
    event_date = ""
    pending_main: dict[str, object] | None = None

    for tr in table.find_all("tr"):
        cells = [_clean_cell(cell.get_text(" ", strip=True)) for cell in tr.find_all(["td", "th"])]
        if not cells:
            continue
        classes = set(tr.get("class") or [])
        if "event-header" in classes:
            event_text = cells[0]
            event_name = _event_name_without_date(event_text)
            event_date = _date_from_event_text(event_text) or ""
            pending_main = None
            continue
        if "main-row" in classes:
            fighter_name = _fighter_name_from_row(tr)
            moneyline_values = _moneyline_values_from_row(tr)
            event_cell = _event_cell_from_row(tr)
            if event_cell:
                event_name = _event_name_without_date(event_cell)
            pending_main = {
                "fighter_name": fighter_name,
                "open_odds": moneyline_values[0] if moneyline_values else "",
                "close_odds": moneyline_values[-1] if moneyline_values else "",
                "raw_cells": tuple(cells),
                "event_name": event_name,
                "event_date": event_date,
            }
            continue
        if pending_main is None:
            continue

        opponent_name = _fighter_name_from_row(tr)
        moneyline_values = _moneyline_values_from_row(tr)
        row_event_date = _date_from_cells(cells) or str(pending_main["event_date"])
        if row_event_date:
            event_date = row_event_date
        if not opponent_name:
            pending_main = None
            continue

        row_number += 1
        rows.append(BfoSourceRow(
            row_number=row_number,
            snapshot_path=str(snapshot.path),
            snapshot_sha256=snapshot.content_sha256,
            source_url=snapshot.source_url,
            source_event_name=str(pending_main["event_name"]),
            source_event_date=row_event_date,
            fighter_name=str(pending_main["fighter_name"]),
            opponent_name=opponent_name,
            bookmaker=DEFAULT_BOOKMAKER,
            market="moneyline",
            open_odds=str(pending_main["open_odds"]),
            close_odds=str(pending_main["close_odds"]),
            open_timestamp="",
            close_timestamp="",
            timestamp_quality="open_close_label_only",
            raw_cells=tuple(pending_main["raw_cells"]),
        ))
        row_number += 1
        rows.append(BfoSourceRow(
            row_number=row_number,
            snapshot_path=str(snapshot.path),
            snapshot_sha256=snapshot.content_sha256,
            source_url=snapshot.source_url,
            source_event_name=str(pending_main["event_name"]),
            source_event_date=row_event_date,
            fighter_name=opponent_name,
            opponent_name=str(pending_main["fighter_name"]),
            bookmaker=DEFAULT_BOOKMAKER,
            market="moneyline",
            open_odds=moneyline_values[0] if moneyline_values else "",
            close_odds=moneyline_values[-1] if moneyline_values else "",
            open_timestamp="",
            close_timestamp="",
            timestamp_quality="open_close_label_only",
            raw_cells=tuple(cells),
        ))
        pending_main = None

    return tuple(rows)


def _fighter_name_from_row(tr) -> str:
    cell = tr.find(["th", "td"], class_="oppcell")
    if cell is None:
        cell = tr.find(["th", "td"])
    if cell is None:
        return ""
    link = cell.find("a")
    return _clean_cell((link or cell).get_text(" ", strip=True))


def _moneyline_values_from_row(tr) -> list[str]:
    values: list[str] = []
    for cell in tr.find_all(["td", "th"], class_="moneyline"):
        value = _american_odds_text(cell.get_text(" ", strip=True))
        if value:
            values.append(value)
    return values


def _event_cell_from_row(tr) -> str:
    for cell in tr.find_all(["td", "th"], class_="item-non-mobile"):
        link = cell.find("a")
        if link is not None:
            return _clean_cell(link.get_text(" ", strip=True))
    return ""


def _date_from_cells(cells: Iterable[str]) -> str | None:
    for cell in cells:
        date_value = _date_iso(cell)
        if date_value is not None:
            return date_value
    return None


def _table_headers(table) -> list[str]:
    first_row = table.find("tr")
    if first_row is None:
        return []
    return [_clean_cell(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]


def _header_map(headers: Iterable[str]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for header in headers:
        key = _normalize_header(header)
        canonical = {
            "event": "event",
            "event name": "event_name",
            "date": "event_date",
            "event date": "event_date",
            "matchup": "matchup",
            "fighter": "fighter",
            "opponent": "opponent",
            "open": "open",
            "opening": "opening",
            "closing range": "closing_range",
            "close": "closing",
            "closing": "closing",
            "bookmaker": "bookmaker",
            "source": "bookmaker",
            "market": "market",
            "open timestamp": "open_timestamp",
            "opening timestamp": "opening_timestamp",
            "close timestamp": "close_timestamp",
            "closing timestamp": "closing_timestamp",
        }.get(key)
        if canonical:
            mapped[header] = canonical
    return mapped


def _is_supported_table(header_map: Mapping[str, str]) -> bool:
    canonical_headers = set(header_map.values())
    has_event = bool({"event", "event_name"} & canonical_headers)
    has_date = "event_date" in canonical_headers or "event" in canonical_headers
    has_fighters = "fighter" in canonical_headers and "opponent" in canonical_headers
    has_odds = bool({"open", "opening", "closing", "closing_range"} & canonical_headers)
    return has_event and has_date and has_fighters and has_odds


def _row_by_headers(headers: list[str], cells: list[str]) -> dict[str, str]:
    header_map = _header_map(headers)
    row: dict[str, str] = {}
    for index, header in enumerate(headers):
        key = header_map.get(header)
        if key is None:
            continue
        row[key] = cells[index] if index < len(cells) else ""
    return row


def _local_fights_by_date_pair(
    *,
    events_csv: Path,
    fights_csv: Path,
    fighters_csv: Path,
) -> dict[tuple[str, tuple[str, str]], tuple[LocalFight, ...]]:
    events = _read_csv(events_csv)
    fights = _read_csv(fights_csv)
    fighters = _read_csv(fighters_csv)
    event_by_id = {
        _text(row.get("event_id")): {
            "event_name": _text(row.get("event_name")) or _text(row.get("name")) or "",
            "event_date": _event_date(row),
        }
        for row in events
        if _text(row.get("event_id"))
    }
    fighter_name_by_id = {
        _text(row.get("fighter_id")): _text(row.get("full_name")) or _text(row.get("name")) or ""
        for row in fighters
        if _text(row.get("fighter_id"))
    }

    grouped: dict[tuple[str, tuple[str, str]], list[LocalFight]] = {}
    seen: set[tuple[tuple[str, tuple[str, str]], str]] = set()
    for row in fights:
        event_id = _text(row.get("event_id"))
        fight_id = _text(row.get("fight_id"))
        fighter_1_id = _text(row.get("fighter_1_id"))
        fighter_2_id = _text(row.get("fighter_2_id"))
        if not all([event_id, fight_id, fighter_1_id, fighter_2_id]):
            continue
        event = event_by_id.get(event_id)
        fighter_1_name = fighter_name_by_id.get(fighter_1_id, "")
        fighter_2_name = fighter_name_by_id.get(fighter_2_id, "")
        if event is None or event["event_date"] is None or not fighter_1_name or not fighter_2_name:
            continue
        local = LocalFight(
            event_id=event_id or "",
            event_name=event["event_name"],
            event_date=event["event_date"],
            fight_id=fight_id or "",
            fighter_1_id=fighter_1_id or "",
            fighter_1_name=fighter_1_name,
            fighter_2_id=fighter_2_id or "",
            fighter_2_name=fighter_2_name,
        )
        key = (local.event_date, _pair_key(local.fighter_1_name, local.fighter_2_name))
        seen_key = (key, local.fight_id)
        if seen_key in seen:
            continue
        seen.add(seen_key)
        grouped.setdefault(key, []).append(local)
    return {key: tuple(value) for key, value in grouped.items()}


def _filter_by_event_name(local_matches: tuple[LocalFight, ...], source_event_name: str) -> tuple[LocalFight, ...]:
    source_name = _normalize_name(source_event_name)
    if not source_name:
        return local_matches
    return tuple(
        match for match in local_matches
        if source_name == _normalize_name(match.event_name)
        or source_name in _normalize_name(match.event_name)
        or _normalize_name(match.event_name) in source_name
    )


def _local_side(local: LocalFight, source_name: str) -> tuple[str, str] | None:
    normalized = _normalize_name(source_name)
    if normalized == _normalize_name(local.fighter_1_name):
        return local.fighter_1_id, local.fighter_1_name
    if normalized == _normalize_name(local.fighter_2_name):
        return local.fighter_2_id, local.fighter_2_name
    return None


def _stable_key(row: Mapping[str, str]) -> tuple[str, str, str, str, str, str]:
    return (
        row["fight_id"],
        row["fighter_id"],
        row["bookmaker"],
        row["market"],
        row["line_type"],
        row["odds_timestamp"],
    )


def _line_type_context(source_row: BfoSourceRow) -> str:
    line_types = []
    if source_row.open_odds:
        line_types.append("opening")
    if source_row.close_odds:
        line_types.append("closing")
    return "|".join(line_types)


def _timestamp_context(source_row: BfoSourceRow) -> str:
    values = []
    if source_row.open_timestamp:
        values.append(f"opening={source_row.open_timestamp}")
    if source_row.close_timestamp:
        values.append(f"closing={source_row.close_timestamp}")
    return ";".join(values)


def _field(row: Mapping[str, str], key: str) -> str:
    return _text(row.get(key)) or ""


def _normalize_market(value: object) -> str:
    text = _normalize_name(value)
    if text in {"moneyline", "money line", "h2h"}:
        return "moneyline"
    return text


def _american_odds_text(value: str) -> str:
    text = _text(value)
    if text is None or text.lower() == "n/a":
        return ""
    matches = re.findall(r"(?<!\d)[+-]\d{2,5}(?!\d)", text)
    return matches[0] if matches else ""


def _closing_odds_text(value: str) -> str:
    text = _text(value)
    if text is None or text.lower() == "n/a":
        return ""
    matches = re.findall(r"(?<!\d)[+-]\d{2,5}(?!\d)", text)
    return matches[-1] if matches else ""


def _american_odds(value: object, column: str) -> int:
    text = _text(value)
    if text is None:
        raise ValueError(f"missing_{column}_odds")
    try:
        return validate_american_odds(Decimal(text))
    except ValueError as exc:
        raise ValueError(f"invalid_{column}_odds") from exc


def _timestamp(value: object, column: str) -> str:
    text = _text(value)
    if text is None:
        raise ValueError(f"missing_{column}")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid_{column}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _date_iso(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    text = _strip_day_suffixes(text)
    for fmt in ("%Y-%m-%d", "%b %d %Y", "%B %d %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt).date().isoformat()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except ValueError:
        return None


def _event_date(row: Mapping[str, object]) -> str | None:
    date_text = _text(row.get("event_date")) or _text(row.get("date_formatted"))
    if date_text is not None:
        return _date_iso(date_text)
    return _date_iso(row.get("date"))


def _date_from_event_text(value: str) -> str | None:
    text = _strip_day_suffixes(value)
    match = re.search(r"([A-Z][a-z]{2,8}\s+\d{1,2},?\s+\d{4})", text)
    return _date_iso(match.group(1)) if match else None


def _event_name_without_date(value: str) -> str:
    text = _strip_day_suffixes(value)
    text = re.sub(r"\s+[A-Z][a-z]{2,8}\s+\d{1,2},?\s+\d{4}\s*$", "", text)
    return text.strip()


def _strip_day_suffixes(value: str) -> str:
    return re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", value.strip())


def _pair_key(first: str, second: str) -> tuple[str, str]:
    return tuple(sorted([_normalize_name(first), _normalize_name(second)]))


def _normalize_header(value: object) -> str:
    return _normalize_name(value)


def _normalize_name(value: object) -> str:
    text = unicodedata.normalize("NFKD", _text(value) or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("'", "").replace(".", "")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_cell(value: str) -> str:
    return html.unescape(re.sub(r"\s+", " ", value)).strip()


def _timestamp_text(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return _timestamp(text, "timestamp")
    except ValueError:
        return text


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read_metadata(html_path: Path) -> dict[str, object]:
    metadata_path = html_path.with_suffix(".metadata.json")
    if not metadata_path.exists():
        return {}
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, columns: list[str], rows: tuple[Mapping[str, str], ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


__all__ = [
    "BESTFIGHTODDS_SOURCE_LABEL",
    "BFO_SOURCE_COLUMNS",
    "DEFAULT_RAW_DIR",
    "DEFAULT_SOURCE_OUTPUT",
    "DEFAULT_UNMATCHED_OUTPUT",
    "UNMATCHED_COLUMNS",
    "BestFightOddsAdaptResult",
    "BfoSnapshot",
    "BfoSourceRow",
    "LocalFight",
    "adapt_bestfightodds_rows",
    "adapt_bestfightodds_snapshots",
    "build_arg_parser",
    "main",
    "parse_snapshot_rows",
    "read_snapshots",
]


if __name__ == "__main__":
    raise SystemExit(main())
