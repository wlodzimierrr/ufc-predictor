"""Decode stored BestFightOdds line-history payloads into reviewable odds rows.

This adapter is offline-only. It reads stored fighter-history HTML snapshots and
stored ``/api/ggd`` payload responses; it never fetches network content. The
currently supported payload shape is the fighter-history ``Mean`` odds series
resolved from ``data-li="[matchup_id, side]"``.

Usage:
    python3 warehouse/adapt_bestfightodds_line_history_payloads.py
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import validate_decimal_odds
from warehouse.adapt_bestfightodds_snapshots import (
    BfoSnapshot,
    LocalFight,
    _clean_cell,
    _date_from_cells,
    _date_from_event_text,
    _date_iso,
    _event_cell_from_row,
    _event_name_without_date,
    _fighter_name_from_row,
    _filter_by_event_name,
    _local_fights_by_date_pair,
    _local_side,
    _pair_key,
    _read_metadata,
    _stable_key,
    _text,
    _write_csv,
    read_snapshots,
)
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "bestfightodds"
DEFAULT_PAYLOAD_DIR = DEFAULT_RAW_DIR / "payloads"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_SOURCE_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "bestfightodds_line_history_fight_odds.csv"
DEFAULT_UNMATCHED_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "bestfightodds_line_history_unmatched_odds.csv"

SOURCE_LABEL = "bestfightodds_line_history_payload"
DEFAULT_BOOKMAKER = "BestFightOdds Mean"
PRINTABLE_ASCII = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

LINE_HISTORY_AUDIT_COLUMNS = [
    "source_payload_path",
    "source_payload_sha256",
    "source_snapshot_path",
    "source_snapshot_sha256",
    "source_matchup_id",
    "source_side",
    "source_series_name",
    "source_timestamp_quality",
    "source_decimal_odds",
    "source_event_name",
    "source_fighter_name",
    "source_opponent_name",
]

SOURCE_COLUMNS = [*CANONICAL_COLUMNS, *LINE_HISTORY_AUDIT_COLUMNS]

UNMATCHED_COLUMNS = [
    "source",
    "row_number",
    "rejection_reason",
    "source_payload_path",
    "source_payload_sha256",
    "source_url",
    "source_matchup_id",
    "source_side",
    "source_series_name",
    "source_odds_timestamp",
    "source_decimal_odds",
    "source_event_name",
    "source_event_date",
    "source_fighter_name",
    "source_opponent_name",
    "candidate_event_id",
    "candidate_event_name",
    "candidate_fight_id",
    "candidate_fighter_id",
    "candidate_opponent_fighter_id",
    "notes",
]


@dataclass(frozen=True)
class LineHistoryContext:
    """Fighter-history row context for one BFO ``data-li`` key."""

    matchup_id: str
    side: str
    source_snapshot_path: str
    source_snapshot_sha256: str
    source_url: str
    source_event_name: str
    source_event_date: str
    fighter_name: str
    opponent_name: str


@dataclass(frozen=True)
class LineHistoryPayload:
    """Stored BFO line-history payload plus decoded identity fields."""

    path: Path
    source_url: str
    content_sha256: str
    matchup_id: str
    side: str
    decoded_json: str


@dataclass(frozen=True)
class LineHistoryPoint:
    """One timestamped decimal-odds point from a BFO line-history series."""

    row_number: int
    payload_path: str
    payload_sha256: str
    source_url: str
    matchup_id: str
    side: str
    series_name: str
    odds_timestamp: str
    decimal_odds: Decimal


@dataclass(frozen=True)
class BestFightOddsLineHistoryAdaptResult:
    """Adapted BFO line-history rows and review rows."""

    payloads_read: int
    source_points_read: int
    matched_rows: tuple[dict[str, str], ...]
    unmatched_rows: tuple[dict[str, str], ...]
    skipped_rows: int


def adapt_bestfightodds_line_history_payloads(
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    payload_dir: Path = DEFAULT_PAYLOAD_DIR,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
    source_output: Path = DEFAULT_SOURCE_OUTPUT,
    unmatched_output: Path = DEFAULT_UNMATCHED_OUTPUT,
    imported_at: str | None = None,
) -> BestFightOddsLineHistoryAdaptResult:
    """Decode stored payloads and write source-specific normalized outputs."""
    local_fights = _local_fights_by_date_pair(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )
    contexts = build_line_history_contexts(read_snapshots(raw_dir))
    payloads = read_line_history_payloads(payload_dir)
    result = adapt_line_history_payloads(
        payloads,
        contexts,
        local_fights,
        imported_at=imported_at,
    )
    _write_csv(source_output, SOURCE_COLUMNS, result.matched_rows)
    _write_csv(unmatched_output, UNMATCHED_COLUMNS, result.unmatched_rows)
    return BestFightOddsLineHistoryAdaptResult(
        payloads_read=len(payloads),
        source_points_read=result.source_points_read,
        matched_rows=result.matched_rows,
        unmatched_rows=result.unmatched_rows,
        skipped_rows=result.skipped_rows,
    )


def build_line_history_contexts(
    snapshots: Iterable[BfoSnapshot],
) -> dict[tuple[str, str], tuple[LineHistoryContext, ...]]:
    """Build ``(matchup_id, side)`` context records from stored BFO HTML."""
    contexts: dict[tuple[str, str], list[LineHistoryContext]] = {}
    for snapshot in snapshots:
        for context in _contexts_from_snapshot(snapshot):
            contexts.setdefault((context.matchup_id, context.side), []).append(context)
    return {
        key: tuple(_dedupe_contexts(value))
        for key, value in contexts.items()
    }


def read_line_history_payloads(payload_dir: Path) -> tuple[LineHistoryPayload, ...]:
    """Read stored BFO ``/api/ggd`` payloads and decode their JSON bodies."""
    payloads: list[LineHistoryPayload] = []
    if not payload_dir.exists():
        return ()
    for path in sorted(payload_dir.iterdir()):
        if not path.is_file() or path.name.endswith(".metadata.json") or path.suffix == ".js":
            continue
        metadata = _read_metadata(path)
        source_url = _text(metadata.get("source_url")) or _text(metadata.get("final_url")) or ""
        matchup_id, side = _matchup_and_side_from_url(source_url)
        if matchup_id is None or side is None:
            continue
        raw_text = path.read_text(encoding="utf-8")
        payloads.append(LineHistoryPayload(
            path=path,
            source_url=source_url,
            content_sha256=_text(metadata.get("content_sha256")) or _sha256_text(raw_text),
            matchup_id=matchup_id,
            side=side,
            decoded_json=decode_notin_payload(raw_text),
        ))
    return tuple(payloads)


def decode_notin_payload(raw_text: str) -> str:
    """Decode the BFO ``notIn`` payload encoding used before ``JSON.parse``."""
    cleaned = re.sub(r"[^A-Za-z0-9+/=]", "", raw_text)
    decoded = base64.b64decode(cleaned).decode("utf-8")
    half = len(PRINTABLE_ASCII) // 2
    translated: list[str] = []
    for character in decoded:
        index = PRINTABLE_ASCII.find(character)
        if index >= 0:
            translated.append(PRINTABLE_ASCII[(index + half) % len(PRINTABLE_ASCII)])
        else:
            translated.append(character)
    return "".join(translated)


def parse_line_history_points(payload: LineHistoryPayload) -> tuple[LineHistoryPoint, ...]:
    """Extract timestamped decimal odds from one decoded BFO line-history payload."""
    parsed = json.loads(payload.decoded_json)
    if not isinstance(parsed, list):
        raise ValueError("line_history_payload_not_list")

    points: list[LineHistoryPoint] = []
    row_number = 0
    for series in parsed:
        if not isinstance(series, Mapping):
            raise ValueError("line_history_series_not_object")
        series_name = _text(series.get("name")) or ""
        if series_name != "Mean":
            raise ValueError(f"unsupported_line_history_series_{series_name or 'blank'}")
        data = series.get("data")
        if not isinstance(data, list):
            raise ValueError("line_history_series_missing_data")
        for item in data:
            if not isinstance(item, Mapping):
                raise ValueError("line_history_point_not_object")
            row_number += 1
            points.append(LineHistoryPoint(
                row_number=row_number,
                payload_path=str(payload.path),
                payload_sha256=payload.content_sha256,
                source_url=payload.source_url,
                matchup_id=payload.matchup_id,
                side=payload.side,
                series_name=series_name,
                odds_timestamp=_timestamp_from_unix_millis(item.get("x")),
                decimal_odds=validate_decimal_odds(item.get("y")),
            ))
    return tuple(points)


def adapt_line_history_payloads(
    payloads: Iterable[LineHistoryPayload],
    contexts_by_key: Mapping[tuple[str, str], tuple[LineHistoryContext, ...]],
    local_fights_by_date_pair: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    *,
    imported_at: str | None = None,
) -> BestFightOddsLineHistoryAdaptResult:
    """Convert decoded BFO line-history payloads into canonical-compatible rows."""
    imported_at_value = imported_at or datetime.now(timezone.utc).isoformat()
    matched_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    payload_count = 0
    points_read = 0
    skipped_rows = 0

    for payload in payloads:
        payload_count += 1
        try:
            points = parse_line_history_points(payload)
        except ValueError as exc:
            unmatched_rows.append(_unmatched_row_for_payload(payload, str(exc)))
            continue

        contexts = contexts_by_key.get((payload.matchup_id, payload.side), ())
        for point in points:
            points_read += 1
            if not contexts:
                unmatched_rows.append(_unmatched_row(point, "missing_line_history_context"))
                continue
            if len(contexts) > 1:
                unmatched_rows.append(_unmatched_row(
                    point,
                    "ambiguous_line_history_context",
                    context=contexts[0],
                ))
                continue
            context = contexts[0]
            try:
                canonical = _canonical_row_for_point(
                    point,
                    context,
                    local_fights_by_date_pair,
                    imported_at=imported_at_value,
                )
            except ValueError as exc:
                unmatched_rows.append(_unmatched_row(point, str(exc), context=context))
                continue

            key = _stable_key(canonical)
            if key in seen_keys:
                skipped_rows += 1
                unmatched_rows.append(_unmatched_row(
                    point,
                    "duplicate_stable_odds_key",
                    context=context,
                    canonical_row=canonical,
                ))
                continue
            seen_keys.add(key)
            matched_rows.append(canonical)

    return BestFightOddsLineHistoryAdaptResult(
        payloads_read=payload_count,
        source_points_read=points_read,
        matched_rows=tuple(matched_rows),
        unmatched_rows=tuple(unmatched_rows),
        skipped_rows=skipped_rows,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Adapt stored BestFightOdds line-history payloads into source-specific odds CSVs."
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--payload-dir", type=Path, default=DEFAULT_PAYLOAD_DIR)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    parser.add_argument("--source-output", type=Path, default=DEFAULT_SOURCE_OUTPUT)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = adapt_bestfightodds_line_history_payloads(
        raw_dir=args.raw_dir,
        payload_dir=args.payload_dir,
        events_csv=args.events_csv,
        fights_csv=args.fights_csv,
        fighters_csv=args.fighters_csv,
        source_output=args.source_output,
        unmatched_output=args.unmatched_output,
    )
    print(f"Payloads read: {result.payloads_read}")
    print(f"Source points read: {result.source_points_read}")
    print(f"Matched canonical-compatible rows: {len(result.matched_rows)}")
    print(f"Unmatched/review rows: {len(result.unmatched_rows)}")
    print(f"Skipped duplicate source rows: {result.skipped_rows}")
    print(f"Wrote: {args.source_output}")
    print(f"Wrote: {args.unmatched_output}")
    return 0


def _contexts_from_snapshot(snapshot: BfoSnapshot) -> tuple[LineHistoryContext, ...]:
    soup = BeautifulSoup(snapshot.html_text, "html.parser")
    contexts: list[LineHistoryContext] = []
    contexts.extend(_event_contexts_from_snapshot(snapshot, soup))
    for table in soup.find_all("table", class_="team-stats-table"):
        event_name = ""
        event_date = ""
        pending_main: dict[str, str] | None = None
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
                data_li = _data_li_from_row(tr)
                fighter_name = _fighter_name_from_row(tr)
                event_cell = _event_cell_from_row(tr)
                if event_cell:
                    event_name = _event_name_without_date(event_cell)
                pending_main = {
                    "fighter_name": fighter_name,
                    "event_name": event_name,
                    "event_date": event_date,
                    "matchup_id": data_li[0] if data_li else "",
                    "side": data_li[1] if data_li else "",
                }
                continue
            if pending_main is None:
                continue
            opponent_name = _fighter_name_from_row(tr)
            row_event_date = _date_from_cells(cells) or pending_main["event_date"]
            if row_event_date:
                event_date = row_event_date
            if not pending_main["matchup_id"] or not pending_main["side"] or not opponent_name:
                pending_main = None
                continue
            contexts.append(LineHistoryContext(
                matchup_id=pending_main["matchup_id"],
                side=pending_main["side"],
                source_snapshot_path=str(snapshot.path),
                source_snapshot_sha256=snapshot.content_sha256,
                source_url=snapshot.source_url,
                source_event_name=pending_main["event_name"],
                source_event_date=row_event_date,
                fighter_name=pending_main["fighter_name"],
                opponent_name=opponent_name,
            ))
            pending_main = None
    return tuple(contexts)


def _event_contexts_from_snapshot(snapshot: BfoSnapshot, soup: BeautifulSoup) -> tuple[LineHistoryContext, ...]:
    event_name, event_date = _event_identity_from_event_snapshot(soup)
    if not event_date:
        return ()

    contexts: list[LineHistoryContext] = []
    for tr in soup.find_all("tr", id=re.compile(r"^mu-\d+$")):
        matchup_id = (_text(tr.get("id")) or "").replace("mu-", "")
        fighter_name = _event_page_fighter_name_from_row(tr)
        opponent_row = tr.find_next_sibling("tr")
        opponent_name = _event_page_fighter_name_from_row(opponent_row) if opponent_row else ""
        if not matchup_id or not fighter_name or not opponent_name:
            continue
        contexts.extend([
            LineHistoryContext(
                matchup_id=matchup_id,
                side="1",
                source_snapshot_path=str(snapshot.path),
                source_snapshot_sha256=snapshot.content_sha256,
                source_url=snapshot.source_url,
                source_event_name=event_name,
                source_event_date=event_date,
                fighter_name=fighter_name,
                opponent_name=opponent_name,
            ),
            LineHistoryContext(
                matchup_id=matchup_id,
                side="2",
                source_snapshot_path=str(snapshot.path),
                source_snapshot_sha256=snapshot.content_sha256,
                source_url=snapshot.source_url,
                source_event_name=event_name,
                source_event_date=event_date,
                fighter_name=opponent_name,
                opponent_name=fighter_name,
            ),
        ])
    return tuple(contexts)


def _event_identity_from_event_snapshot(soup: BeautifulSoup) -> tuple[str, str]:
    title = _text(soup.title.get_text(" ", strip=True) if soup.title else "") or ""
    event_name = re.sub(r"\s+Odds:.*$", "", title).strip()
    description = soup.find("meta", attrs={"name": "description"})
    description_text = _text(description.get("content")) if description else ""
    event_date = _date_from_event_text(description_text) or ""
    return event_name, event_date


def _event_page_fighter_name_from_row(row) -> str:
    fighter = row.find("span", class_="t-b-fcc") if row else None
    return _clean_cell(fighter.get_text(" ", strip=True)) if fighter else _fighter_name_from_row(row)


def _canonical_row_for_point(
    point: LineHistoryPoint,
    context: LineHistoryContext,
    local_fights_by_date_pair: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    *,
    imported_at: str,
) -> dict[str, str]:
    source_event_date = _date_iso(context.source_event_date)
    if source_event_date is None:
        raise ValueError("missing_source_event_date")

    local_matches = local_fights_by_date_pair.get(
        (source_event_date, _pair_key(context.fighter_name, context.opponent_name)),
        (),
    )
    if not local_matches:
        raise ValueError("unknown_local_fight_pair")
    event_filtered = _filter_by_event_name(local_matches, context.source_event_name)
    if not event_filtered:
        if len(local_matches) == 1:
            event_filtered = local_matches
        else:
            raise ValueError("unknown_local_event_name")
    if len(event_filtered) > 1:
        raise ValueError("ambiguous_local_fight_pair")
    local = event_filtered[0]

    side = _local_side(local, context.fighter_name)
    opponent = _local_side(local, context.opponent_name)
    if side is None or opponent is None or side[0] == opponent[0]:
        raise ValueError("matched_pair_but_side_names_do_not_align")

    return {
        "fight_id": local.fight_id,
        "event_id": local.event_id,
        "event_date": source_event_date,
        "fighter_id": side[0],
        "fighter_name": side[1],
        "opponent_fighter_id": opponent[0],
        "bookmaker": DEFAULT_BOOKMAKER,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": point.odds_timestamp,
        "american_odds": "",
        "decimal_odds": format(point.decimal_odds, "f"),
        "source": SOURCE_LABEL,
        "source_url": point.source_url,
        "imported_at": imported_at,
        "source_payload_path": point.payload_path,
        "source_payload_sha256": point.payload_sha256,
        "source_snapshot_path": context.source_snapshot_path,
        "source_snapshot_sha256": context.source_snapshot_sha256,
        "source_matchup_id": point.matchup_id,
        "source_side": point.side,
        "source_series_name": point.series_name,
        "source_timestamp_quality": "observed_timestamp",
        "source_decimal_odds": format(point.decimal_odds, "f"),
        "source_event_name": context.source_event_name,
        "source_fighter_name": context.fighter_name,
        "source_opponent_name": context.opponent_name,
    }


def _unmatched_row(
    point: LineHistoryPoint,
    reason: str,
    *,
    context: LineHistoryContext | None = None,
    canonical_row: Mapping[str, str] | None = None,
) -> dict[str, str]:
    first = canonical_row or {}
    return {
        "source": SOURCE_LABEL,
        "row_number": str(point.row_number),
        "rejection_reason": reason,
        "source_payload_path": point.payload_path,
        "source_payload_sha256": point.payload_sha256,
        "source_url": point.source_url,
        "source_matchup_id": point.matchup_id,
        "source_side": point.side,
        "source_series_name": point.series_name,
        "source_odds_timestamp": point.odds_timestamp,
        "source_decimal_odds": format(point.decimal_odds, "f"),
        "source_event_name": context.source_event_name if context else "",
        "source_event_date": context.source_event_date if context else "",
        "source_fighter_name": context.fighter_name if context else "",
        "source_opponent_name": context.opponent_name if context else "",
        "candidate_event_id": _text(first.get("event_id")) or "",
        "candidate_event_name": "",
        "candidate_fight_id": _text(first.get("fight_id")) or "",
        "candidate_fighter_id": _text(first.get("fighter_id")) or "",
        "candidate_opponent_fighter_id": _text(first.get("opponent_fighter_id")) or "",
        "notes": "timestamp_quality=observed_timestamp;bookmaker=BestFightOdds Mean",
    }


def _unmatched_row_for_payload(payload: LineHistoryPayload, reason: str) -> dict[str, str]:
    return {
        "source": SOURCE_LABEL,
        "row_number": "",
        "rejection_reason": reason,
        "source_payload_path": str(payload.path),
        "source_payload_sha256": payload.content_sha256,
        "source_url": payload.source_url,
        "source_matchup_id": payload.matchup_id,
        "source_side": payload.side,
        "source_series_name": "",
        "source_odds_timestamp": "",
        "source_decimal_odds": "",
        "source_event_name": "",
        "source_event_date": "",
        "source_fighter_name": "",
        "source_opponent_name": "",
        "candidate_event_id": "",
        "candidate_event_name": "",
        "candidate_fight_id": "",
        "candidate_fighter_id": "",
        "candidate_opponent_fighter_id": "",
        "notes": "payload_decode_or_shape_failure",
    }


def _data_li_from_row(tr) -> tuple[str, str] | None:
    candidate = tr.find(attrs={"data-li": True})
    if candidate is None:
        return None
    raw = candidate.get("data-li")
    try:
        values = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(values, list) or len(values) < 2:
        return None
    matchup_id = _text(values[0])
    side = _text(values[1])
    if matchup_id is None or side is None:
        return None
    return matchup_id, side


def _dedupe_contexts(contexts: Iterable[LineHistoryContext]) -> list[LineHistoryContext]:
    seen: dict[tuple[str, ...], int] = {}
    unique: list[LineHistoryContext] = []
    for context in contexts:
        key = (
            context.matchup_id,
            context.side,
            context.source_event_date,
            context.fighter_name,
            context.opponent_name,
        )
        existing_index = seen.get(key)
        if existing_index is not None:
            existing = unique[existing_index]
            if len(context.source_event_name) > len(existing.source_event_name):
                unique[existing_index] = context
            continue
        seen[key] = len(unique)
        unique.append(context)
    return unique


def _matchup_and_side_from_url(source_url: str) -> tuple[str | None, str | None]:
    parsed = urllib.parse.urlparse(source_url)
    if parsed.path != "/api/ggd":
        return None, None
    query = urllib.parse.parse_qs(parsed.query)
    matchup_id = _first_query_value(query, "m")
    side = _first_query_value(query, "p")
    return matchup_id, side


def _first_query_value(query: Mapping[str, list[str]], key: str) -> str | None:
    values = query.get(key, [])
    if not values:
        return None
    return _text(values[0])


def _timestamp_from_unix_millis(value: object) -> str:
    if value is None:
        raise ValueError("missing_line_history_timestamp")
    try:
        millis = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"invalid_line_history_timestamp {value}") from exc
    seconds = millis / Decimal("1000")
    return datetime.fromtimestamp(float(seconds), timezone.utc).isoformat()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
