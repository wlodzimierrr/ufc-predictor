"""Adapt BestFightOdds event-page snapshots into current per-bookmaker odds rows.

This adapter is offline-only. It reads event-page HTML snapshots and adjacent
metadata captured by ``probe_bestfightodds_snapshot.py``; it never fetches
network content.

Unlike ``adapt_bestfightodds_snapshots.py`` (which reads fighter-history
open/close tables and emits ``BestFightOdds aggregate`` rows), this adapter
reads the per-bookmaker moneyline grid on an event page. Each odds cell carries
``data-li="[bookmaker_id, side, matchup_id]"``, so bookmaker and fighter side
are read from the markup rather than from column position. Rows are emitted as
``line_type=current`` observations timestamped with the snapshot capture time,
under the ``bestfightodds_live_snapshot`` source label.

Prop and total rows (for example "Over 1.5 rounds") are skipped: only rows whose
header cell links to a fighter page are treated as moneyline sides.

Usage:
    python3 warehouse/adapt_bestfightodds_live_snapshots.py
    python3 warehouse/adapt_bestfightodds_live_snapshots.py --raw-dir data/odds/raw/bestfightodds
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import american_to_decimal_odds
from warehouse.adapt_bestfightodds_snapshots import (
    BfoSnapshot,
    LocalFight,
    _american_odds,
    _date_iso,
    _filter_by_event_name,
    _local_fights_by_date_pair,
    _normalize_name,
    _pair_key,
    _read_metadata,
    _text,
    _write_csv,
)
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS

REPO_ROOT = Path(__file__).resolve().parent.parent
BESTFIGHTODDS_LIVE_SOURCE_LABEL = "bestfightodds_live_snapshot"
LIVE_LINE_TYPE = "current"

DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "bestfightodds"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_ALIASES_CSV = REPO_ROOT / "data" / "odds" / "bfo_name_aliases.csv"
DEFAULT_SOURCE_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "bestfightodds_live_fight_odds.csv"
DEFAULT_UNMATCHED_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "bestfightodds_live_unmatched_odds.csv"

LIVE_AUDIT_COLUMNS = [
    "source_snapshot_path",
    "source_snapshot_sha256",
    "source_event_name",
    "source_fighter_name",
    "source_opponent_name",
    "source_timestamp_quality",
    "source_line_value",
]

LIVE_SOURCE_COLUMNS = [*CANONICAL_COLUMNS, *LIVE_AUDIT_COLUMNS]

UNMATCHED_COLUMNS = [
    "source",
    "row_number",
    "rejection_reason",
    "source_snapshot_path",
    "source_snapshot_sha256",
    "source_url",
    "source_event_name",
    "source_event_date",
    "source_matchup_id",
    "source_fighter_name",
    "source_opponent_name",
    "source_bookmaker",
    "source_market",
    "source_line_type",
    "source_odds_timestamp",
    "american_odds",
    "notes",
]

# Suffixes dropped before comparing names. BestFightOdds routinely omits the
# generational suffix that UFCStats records ("Michael Aswell" / "Michael Aswell Jr.").
GENERATIONAL_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})

MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


@dataclass(frozen=True)
class LiveOddsRow:
    """One parsed fighter-side current moneyline cell from an event page."""

    row_number: int
    snapshot_path: str
    snapshot_sha256: str
    source_url: str
    source_event_name: str
    source_event_date: str
    odds_timestamp: str
    matchup_id: str
    fighter_name: str
    opponent_name: str
    bookmaker: str
    odds_text: str


@dataclass(frozen=True)
class LiveAdaptResult:
    """Adapted live snapshot rows and review rows."""

    snapshots_read: int
    source_rows_read: int
    matched_rows: tuple[dict[str, str], ...]
    unmatched_rows: tuple[dict[str, str], ...]
    skipped_rows: int


def adapt_bestfightodds_live_snapshots(
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
    aliases_csv: Path | None = DEFAULT_ALIASES_CSV,
    source_output: Path = DEFAULT_SOURCE_OUTPUT,
    unmatched_output: Path = DEFAULT_UNMATCHED_OUTPUT,
    imported_at: str | None = None,
) -> LiveAdaptResult:
    """Read local BFO event snapshots and write current per-bookmaker odds rows."""
    local_fights = _local_fights_by_date_pair(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )
    aliases = read_name_aliases(aliases_csv) if aliases_csv else {}
    match_index = _match_index(local_fights, aliases)

    snapshots = read_event_snapshots(raw_dir)
    source_rows = tuple(
        row
        for snapshot in snapshots
        for row in parse_live_snapshot_rows(snapshot)
    )
    result = adapt_live_rows(
        source_rows,
        match_index,
        aliases,
        imported_at=imported_at,
    )
    _write_csv(source_output, LIVE_SOURCE_COLUMNS, result.matched_rows)
    _write_csv(unmatched_output, UNMATCHED_COLUMNS, result.unmatched_rows)
    return LiveAdaptResult(
        snapshots_read=len(snapshots),
        source_rows_read=result.source_rows_read,
        matched_rows=result.matched_rows,
        unmatched_rows=result.unmatched_rows,
        skipped_rows=result.skipped_rows,
    )


def read_event_snapshots(raw_dir: Path) -> tuple[BfoSnapshot, ...]:
    """Load event-page snapshots from ``raw_dir``, newest capture last."""
    snapshots: list[BfoSnapshot] = []
    for html_path in sorted(raw_dir.glob("*.html")):
        metadata = _read_metadata(html_path)
        if _text(metadata.get("target_type")) != "event":
            continue
        snapshots.append(BfoSnapshot(
            path=html_path,
            html_text=html_path.read_text(encoding="utf-8", errors="replace"),
            source_url=_text(metadata.get("source_url")) or "",
            fetched_at=_text(metadata.get("fetched_at")) or "",
            content_sha256=_text(metadata.get("content_sha256")) or "",
        ))
    return tuple(snapshots)


def parse_live_snapshot_rows(snapshot: BfoSnapshot) -> tuple[LiveOddsRow, ...]:
    """Parse per-bookmaker current moneyline cells out of one event snapshot."""
    soup = BeautifulSoup(snapshot.html_text, "html.parser")
    event_name = _event_name_from_event_snapshot(soup)
    event_date = _event_date_from_event_snapshot(soup, snapshot.fetched_at)
    if not event_date:
        return ()

    bookmakers = _bookmakers_by_id(soup)
    sides = _sides_by_matchup(soup, bookmakers)

    rows: list[LiveOddsRow] = []
    row_number = 0
    for matchup_id in sorted(sides):
        matchup = sides[matchup_id]
        if "1" not in matchup or "2" not in matchup:
            continue
        for side, (fighter_name, odds_by_bookmaker) in sorted(matchup.items()):
            opponent_name = matchup["2" if side == "1" else "1"][0]
            for bookmaker in sorted(odds_by_bookmaker):
                row_number += 1
                rows.append(LiveOddsRow(
                    row_number=row_number,
                    snapshot_path=str(snapshot.path),
                    snapshot_sha256=snapshot.content_sha256,
                    source_url=snapshot.source_url,
                    source_event_name=event_name,
                    source_event_date=event_date,
                    odds_timestamp=snapshot.fetched_at,
                    matchup_id=matchup_id,
                    fighter_name=fighter_name,
                    opponent_name=opponent_name,
                    bookmaker=bookmaker,
                    odds_text=odds_by_bookmaker[bookmaker],
                ))
    return tuple(rows)


def adapt_live_rows(
    source_rows: Iterable[LiveOddsRow],
    match_index: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    aliases: Mapping[str, str],
    *,
    imported_at: str | None = None,
) -> LiveAdaptResult:
    """Convert parsed live rows into canonical-compatible odds rows."""
    matched_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    rows_read = 0
    skipped_rows = 0
    imported_at_value = imported_at or datetime.now(timezone.utc).isoformat()

    for source_row in source_rows:
        rows_read += 1
        try:
            canonical = _adapt_live_row(
                source_row,
                match_index,
                aliases,
                imported_at=imported_at_value,
            )
        except ValueError as exc:
            unmatched_rows.append(_unmatched_row(source_row, str(exc)))
            continue

        key = _stable_key(canonical)
        if key in seen_keys:
            skipped_rows += 1
            unmatched_rows.append(_unmatched_row(source_row, "duplicate_stable_odds_key"))
            continue
        seen_keys.add(key)
        matched_rows.append(canonical)

    return LiveAdaptResult(
        snapshots_read=0,
        source_rows_read=rows_read,
        matched_rows=tuple(matched_rows),
        unmatched_rows=tuple(unmatched_rows),
        skipped_rows=skipped_rows,
    )


def read_name_aliases(path: Path) -> dict[str, str]:
    """Read reviewed BFO-to-local fighter name aliases, keyed by normalized name."""
    if not path.exists():
        return {}
    aliases: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            source_name = _normalize_name(row.get("source_name"))
            local_name = _normalize_name(row.get("local_name"))
            if source_name and local_name:
                aliases[source_name] = local_name
    return aliases


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Adapt stored BestFightOdds event snapshots into current per-bookmaker odds CSVs."
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=DEFAULT_ALIASES_CSV)
    parser.add_argument("--source-output", type=Path, default=DEFAULT_SOURCE_OUTPUT)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = adapt_bestfightodds_live_snapshots(
        raw_dir=args.raw_dir,
        events_csv=args.events_csv,
        fights_csv=args.fights_csv,
        fighters_csv=args.fighters_csv,
        aliases_csv=args.aliases_csv,
        source_output=args.source_output,
        unmatched_output=args.unmatched_output,
    )
    print(f"Event snapshots read: {result.snapshots_read}")
    print(f"Source rows read: {result.source_rows_read}")
    print(f"Matched canonical-compatible rows: {len(result.matched_rows)}")
    print(f"Unmatched/review rows: {len(result.unmatched_rows)}")
    print(f"Skipped duplicate source rows: {result.skipped_rows}")
    print(f"Wrote: {args.source_output}")
    print(f"Wrote: {args.unmatched_output}")
    return 0


def _adapt_live_row(
    source_row: LiveOddsRow,
    match_index: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    aliases: Mapping[str, str],
    *,
    imported_at: str,
) -> dict[str, str]:
    source_event_date = _date_iso(source_row.source_event_date)
    if source_event_date is None:
        raise ValueError("missing_source_event_date")

    pair = _pair_key(
        _match_name(source_row.fighter_name, aliases),
        _match_name(source_row.opponent_name, aliases),
    )
    local_matches = _matches_near_date(match_index, source_event_date, pair)
    if not local_matches:
        raise ValueError("unknown_local_fight_pair")
    event_filtered = _filter_by_event_name(local_matches, source_row.source_event_name)
    if not event_filtered:
        # BestFightOdds names Fight Nights by host city ("UFC Sacramento") where
        # UFCStats names them by main event, so a unique date+pair hit stands on
        # its own. This mirrors adapt_bestfightodds_line_history_payloads.py.
        if len(local_matches) == 1:
            event_filtered = local_matches
        else:
            raise ValueError("unknown_local_event_name")
    if len(event_filtered) > 1:
        raise ValueError("ambiguous_local_fight_pair")
    local = event_filtered[0]

    side = _match_side(local, source_row.fighter_name, aliases)
    opponent = _match_side(local, source_row.opponent_name, aliases)
    if side is None or opponent is None or side[0] == opponent[0]:
        raise ValueError("matched_pair_but_side_names_do_not_align")

    odds_timestamp = _text(source_row.odds_timestamp)
    if odds_timestamp is None:
        raise ValueError("missing_snapshot_capture_timestamp")

    american_odds = _american_odds(source_row.odds_text, "current")
    decimal_odds = american_to_decimal_odds(american_odds)
    return {
        "fight_id": local.fight_id,
        "event_id": local.event_id,
        "event_date": local.event_date,
        "fighter_id": side[0],
        "fighter_name": side[1],
        "opponent_fighter_id": opponent[0],
        "bookmaker": source_row.bookmaker,
        "market": "moneyline",
        "line_type": LIVE_LINE_TYPE,
        "odds_timestamp": odds_timestamp,
        "american_odds": str(american_odds),
        "decimal_odds": format(decimal_odds, "f"),
        "source": BESTFIGHTODDS_LIVE_SOURCE_LABEL,
        "source_url": source_row.source_url,
        "imported_at": imported_at,
        "source_snapshot_path": source_row.snapshot_path,
        "source_snapshot_sha256": source_row.snapshot_sha256,
        "source_event_name": source_row.source_event_name,
        "source_fighter_name": source_row.fighter_name,
        "source_opponent_name": source_row.opponent_name,
        "source_timestamp_quality": "observed_snapshot_capture_time",
        "source_line_value": source_row.odds_text,
    }


def _matches_near_date(
    match_index: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    source_event_date: str,
    pair: tuple[str, str],
) -> tuple[LocalFight, ...]:
    """Look up a fight pair on the labelled date, then one day either side.

    BestFightOdds labels an event with its local calendar date, which can sit a
    day either side of the UFCStats date for late-night US cards. The pair key
    is specific enough that the first date with a hit is taken.
    """
    labelled = _date_from_timestamp(source_event_date)
    if labelled is None:
        return ()
    for offset in (0, 1, -1):
        candidate = (labelled + timedelta(days=offset)).isoformat()
        matches = match_index.get((candidate, pair), ())
        if matches:
            return matches
    return ()


def _unmatched_row(source_row: LiveOddsRow, reason: str) -> dict[str, str]:
    return {
        "source": BESTFIGHTODDS_LIVE_SOURCE_LABEL,
        "row_number": str(source_row.row_number),
        "rejection_reason": reason,
        "source_snapshot_path": source_row.snapshot_path,
        "source_snapshot_sha256": source_row.snapshot_sha256,
        "source_url": source_row.source_url,
        "source_event_name": source_row.source_event_name,
        "source_event_date": source_row.source_event_date,
        "source_matchup_id": source_row.matchup_id,
        "source_fighter_name": source_row.fighter_name,
        "source_opponent_name": source_row.opponent_name,
        "source_bookmaker": source_row.bookmaker,
        "source_market": "moneyline",
        "source_line_type": LIVE_LINE_TYPE,
        "source_odds_timestamp": source_row.odds_timestamp,
        "american_odds": source_row.odds_text,
        "notes": f"matchup_id={source_row.matchup_id}",
    }


def _bookmakers_by_id(soup: BeautifulSoup) -> dict[str, str]:
    """Map BFO bookmaker ids to display names from odds-table headers."""
    bookmakers: dict[str, str] = {}
    for table in soup.find_all("table", class_="odds-table"):
        for th in table.select("thead th[data-b]"):
            bookmaker_id = _text(th.get("data-b"))
            if not bookmaker_id or bookmaker_id in bookmakers:
                continue
            link = th.find("a")
            name = _text(link.get_text(" ", strip=True) if link else th.get_text(" ", strip=True))
            if name:
                bookmakers[bookmaker_id] = name
    return bookmakers


def _sides_by_matchup(
    soup: BeautifulSoup,
    bookmakers: Mapping[str, str],
) -> dict[str, dict[str, tuple[str, dict[str, str]]]]:
    """Group fighter-side names and per-bookmaker odds by BFO matchup id."""
    sides: dict[str, dict[str, tuple[str, dict[str, str]]]] = {}
    for table in soup.find_all("table", class_="odds-table"):
        for tr in table.select("tbody tr"):
            header_cell = tr.find("th", attrs={"scope": "row"})
            if header_cell is None:
                continue
            link = header_cell.find("a", href=re.compile(r"^/fighters/"))
            if link is None:
                # Prop/total row such as "Over 1.5 rounds"; not a moneyline side.
                continue
            fighter_name = _text(link.get_text(" ", strip=True))
            if not fighter_name:
                continue

            odds_by_bookmaker: dict[str, str] = {}
            matchup_id = ""
            side = ""
            for cell in tr.select("td.but-sg[data-li]"):
                parsed = _parse_line_identifier(cell.get("data-li"))
                if parsed is None:
                    continue
                bookmaker_id, cell_side, cell_matchup_id = parsed
                odds_text = _odds_text_from_cell(cell)
                if odds_text is None:
                    continue
                matchup_id = cell_matchup_id
                side = cell_side
                bookmaker = bookmakers.get(bookmaker_id)
                if bookmaker:
                    odds_by_bookmaker[bookmaker] = odds_text

            if not matchup_id or not side or not odds_by_bookmaker:
                continue
            existing = sides.setdefault(matchup_id, {}).get(side)
            if existing is None:
                sides[matchup_id][side] = (fighter_name, odds_by_bookmaker)
            else:
                merged = {**existing[1], **odds_by_bookmaker}
                sides[matchup_id][side] = (existing[0], merged)
    return sides


def _parse_line_identifier(value: object) -> tuple[str, str, str] | None:
    """Read ``data-li="[bookmaker_id, side, matchup_id]"`` from an odds cell."""
    text = _text(value)
    if text is None:
        return None
    try:
        parsed = json.loads(text)
    except ValueError:
        return None
    if not isinstance(parsed, list) or len(parsed) != 3:
        return None
    return str(parsed[0]), str(parsed[1]), str(parsed[2])


def _odds_text_from_cell(cell) -> str | None:
    span = cell.find("span")
    if span is None:
        return None
    return _text(span.get_text(" ", strip=True))


def _event_name_from_event_snapshot(soup: BeautifulSoup) -> str:
    """Read the event name, preferring the uniform ``<Event> Odds`` page header.

    Event-page titles come in two shapes -- "UFC 330 Odds: Makhachev vs. Machado
    Garry" and "UFC Sacramento for August 22 | Best Fight Odds" -- while the
    header is always "<Event> Odds".
    """
    header = soup.select_one(".table-header h1")
    if header is not None:
        name = re.sub(r"\s+Odds$", "", header.get_text(" ", strip=True)).strip()
        if name:
            return name
    title = _text(soup.title.get_text(" ", strip=True) if soup.title else "") or ""
    title = re.sub(r"\s*\|\s*Best Fight Odds\s*$", "", title).strip()
    return re.sub(r"\s+Odds(?::.*)?$", "", title).strip()


def _event_date_from_event_snapshot(soup: BeautifulSoup, fetched_at: str) -> str:
    """Resolve the event date from the year-less header label plus capture time."""
    label = soup.select_one(".table-header-date")
    if label is None:
        return ""
    return _date_from_header_label(label.get_text(" ", strip=True), fetched_at)


def _date_from_header_label(label: str, fetched_at: str) -> str:
    """Pick the calendar year that puts a year-less label closest to capture time."""
    text = _text(label)
    if text is None:
        return ""
    match = re.search(
        rf"({'|'.join(MONTHS)})\s+(\d{{1,2}})(?:st|nd|rd|th)?",
        text,
        flags=re.IGNORECASE,
    )
    if match is None:
        return ""
    month = next(
        index for index, name in enumerate(MONTHS, start=1)
        if name.lower() == match.group(1).lower()
    )
    day = int(match.group(2))

    fetched_date = _date_from_timestamp(fetched_at)
    if fetched_date is None:
        return ""

    candidates: list[date] = []
    for year in (fetched_date.year - 1, fetched_date.year, fetched_date.year + 1):
        try:
            candidates.append(date(year, month, day))
        except ValueError:
            continue
    if not candidates:
        return ""
    return min(candidates, key=lambda value: abs((value - fetched_date).days)).isoformat()


def _date_from_timestamp(value: str) -> date | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _match_name(value: object, aliases: Mapping[str, str]) -> str:
    """Normalize a name for matching across BFO and UFCStats spelling variants.

    Applies reviewed aliases, drops generational suffixes, and collapses internal
    whitespace so "Doo Ho Choi" and "Dooho Choi" compare equal.
    """
    normalized = _normalize_name(value)
    normalized = aliases.get(normalized, normalized)
    tokens = [token for token in normalized.split() if token not in GENERATIONAL_SUFFIXES]
    return "".join(tokens)


def _match_index(
    local_fights: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    aliases: Mapping[str, str],
) -> dict[tuple[str, tuple[str, str]], tuple[LocalFight, ...]]:
    """Re-key the local fight index under the variant-tolerant match name."""
    grouped: dict[tuple[str, tuple[str, str]], list[LocalFight]] = {}
    seen: set[tuple[tuple[str, tuple[str, str]], str]] = set()
    for locals_ in local_fights.values():
        for local in locals_:
            key = (local.event_date, _pair_key(
                _match_name(local.fighter_1_name, aliases),
                _match_name(local.fighter_2_name, aliases),
            ))
            seen_key = (key, local.fight_id)
            if seen_key in seen:
                continue
            seen.add(seen_key)
            grouped.setdefault(key, []).append(local)
    return {key: tuple(value) for key, value in grouped.items()}


def _match_side(
    local: LocalFight,
    source_name: str,
    aliases: Mapping[str, str],
) -> tuple[str, str] | None:
    normalized = _match_name(source_name, aliases)
    if normalized == _match_name(local.fighter_1_name, aliases):
        return local.fighter_1_id, local.fighter_1_name
    if normalized == _match_name(local.fighter_2_name, aliases):
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


if __name__ == "__main__":
    raise SystemExit(main())
