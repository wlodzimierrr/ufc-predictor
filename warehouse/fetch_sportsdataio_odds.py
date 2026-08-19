"""Fetch SportsDataIO MMA odds and adapt them to the canonical odds CSV.

SportsDataIO schedule/fighter endpoints may not be available for every API key,
so this adapter discovers events through the odds endpoint and matches fights to
local warehouse CSVs by event date plus normalized fighter-name pair.

Usage:
    python3 warehouse/fetch_sportsdataio_odds.py
    python3 warehouse/fetch_sportsdataio_odds.py --event-id-start 891 --event-id-end 920
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import unicodedata
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import american_to_decimal_odds, validate_american_odds
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PREDICTION_EVENTS_CSV = REPO_ROOT / "data" / "reports" / "pre_event_prediction_events.csv"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "sportsdataio"
DEFAULT_SOURCE_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "sportsdataio_fight_odds.csv"
DEFAULT_UNMATCHED_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "sportsdataio_unmatched_odds.csv"
DEFAULT_QA_OUTPUT = REPO_ROOT / "data" / "reports" / "sportsdataio_odds_matching_qa.csv"

SPORTSDATAIO_SOURCE_LABEL = "sportsdataio_mma_event_odds_line_movement"
SPORTSDATAIO_API_BASE = "https://api.sportsdata.io/v3/mma/odds/json"
EASTERN = ZoneInfo("America/New_York")

UNMATCHED_COLUMNS = [
    "source",
    "sportsdataio_event_id",
    "sportsdataio_event_name",
    "sportsdataio_event_date",
    "sportsdataio_fight_id",
    "sportsdataio_fighter_a_id",
    "sportsdataio_fighter_a_name",
    "sportsdataio_fighter_b_id",
    "sportsdataio_fighter_b_name",
    "sportsbook",
    "created",
    "updated",
    "rejection_reason",
]

QA_COLUMNS = [
    "source",
    "row_status",
    "match_reason",
    "rejection_reason",
    "sportsdataio_event_id",
    "sportsdataio_event_name",
    "sportsdataio_event_date",
    "sportsdataio_fight_id",
    "sportsdataio_fighter_a_id",
    "sportsdataio_fighter_a_name",
    "sportsdataio_fighter_b_id",
    "sportsdataio_fighter_b_name",
    "sportsbook",
    "created",
    "updated",
    "local_event_id",
    "local_event_name",
    "local_fight_id",
    "local_fighter_1_id",
    "local_fighter_1_name",
    "local_fighter_2_id",
    "local_fighter_2_name",
    "canonical_rows",
]


@dataclass(frozen=True)
class LocalFight:
    event_id: str
    event_name: str
    event_date: str
    fight_id: str
    fighter_1_id: str
    fighter_1_name: str
    fighter_2_id: str
    fighter_2_name: str


@dataclass(frozen=True)
class FetchResult:
    discovered_events: int
    fetched_events: int
    api_rows: int
    matched_rows: tuple[dict[str, str], ...]
    unmatched_rows: tuple[dict[str, str], ...]
    qa_rows: tuple[dict[str, str], ...]
    skipped_duplicate_rows: int


def fetch_sportsdataio_odds(
    *,
    api_key: str,
    prediction_events_csv: Path = DEFAULT_PREDICTION_EVENTS_CSV,
    events_csv: Path = DEFAULT_EVENTS_CSV,
    fights_csv: Path = DEFAULT_FIGHTS_CSV,
    fighters_csv: Path = DEFAULT_FIGHTERS_CSV,
    raw_dir: Path = DEFAULT_RAW_DIR,
    source_output: Path = DEFAULT_SOURCE_OUTPUT,
    unmatched_output: Path = DEFAULT_UNMATCHED_OUTPUT,
    qa_output: Path = DEFAULT_QA_OUTPUT,
    event_id_start: int = 850,
    event_id_end: int = 950,
) -> FetchResult:
    """Fetch, match, and write SportsDataIO line-movement odds."""
    wanted_dates = _wanted_prediction_dates(prediction_events_csv)
    local_fights_by_date_pair = _local_fights_by_date_pair(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )

    event_ids = _discover_event_ids(
        api_key=api_key,
        wanted_dates=wanted_dates,
        event_id_start=event_id_start,
        event_id_end=event_id_end,
        raw_dir=raw_dir,
    )

    matched_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    qa_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    skipped_duplicate_rows = 0
    api_rows = 0
    imported_at = datetime.now(timezone.utc).isoformat()

    for event_id in sorted(event_ids.values()):
        payload = _fetch_json(api_key, f"{SPORTSDATAIO_API_BASE}/EventOddsLineMovement/{event_id}")
        _write_raw_json(raw_dir / f"event_odds_line_movement_{event_id}.json", payload)
        event = payload.get("Event") or {}
        odds_rows = payload.get("FightOdds") or []
        api_rows += len(odds_rows)
        for odds_row in odds_rows:
            canonical, unmatched, qa = _adapt_line_movement_row(
                event=event,
                odds_row=odds_row,
                local_fights_by_date_pair=local_fights_by_date_pair,
                imported_at=imported_at,
            )
            qa_rows.append(qa)
            if unmatched is not None:
                unmatched_rows.append(unmatched)
                continue
            if canonical is None:
                continue
            keys = [_stable_key(row) for row in canonical]
            if any(key in seen_keys for key in keys):
                skipped_duplicate_rows += 1
                qa_rows[-1] = {**qa_rows[-1], "row_status": "duplicate", "rejection_reason": "duplicate_stable_odds_key"}
                unmatched_rows.append(_unmatched_row(event, odds_row, "duplicate_stable_odds_key"))
                continue
            seen_keys.update(keys)
            matched_rows.extend(canonical)

    result = FetchResult(
        discovered_events=len(event_ids),
        fetched_events=len(event_ids),
        api_rows=api_rows,
        matched_rows=tuple(matched_rows),
        unmatched_rows=tuple(unmatched_rows),
        qa_rows=tuple(qa_rows),
        skipped_duplicate_rows=skipped_duplicate_rows,
    )
    _write_csv(source_output, CANONICAL_COLUMNS, result.matched_rows)
    _write_csv(unmatched_output, UNMATCHED_COLUMNS, result.unmatched_rows)
    _write_csv(qa_output, QA_COLUMNS, result.qa_rows)
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch SportsDataIO MMA odds line movement and write canonical odds rows."
    )
    parser.add_argument("--api-key-env", default="SportsDataIO_API", help="Environment variable containing the SportsDataIO API key.")
    parser.add_argument("--prediction-events-csv", type=Path, default=DEFAULT_PREDICTION_EVENTS_CSV)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--source-output", type=Path, default=DEFAULT_SOURCE_OUTPUT)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT)
    parser.add_argument("--qa-output", type=Path, default=DEFAULT_QA_OUTPUT)
    parser.add_argument("--event-id-start", type=int, default=850)
    parser.add_argument("--event-id-end", type=int, default=950)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    api_key = _api_key(args.api_key_env)
    result = fetch_sportsdataio_odds(
        api_key=api_key,
        prediction_events_csv=args.prediction_events_csv,
        events_csv=args.events_csv,
        fights_csv=args.fights_csv,
        fighters_csv=args.fighters_csv,
        raw_dir=args.raw_dir,
        source_output=args.source_output,
        unmatched_output=args.unmatched_output,
        qa_output=args.qa_output,
        event_id_start=args.event_id_start,
        event_id_end=args.event_id_end,
    )
    print(f"Discovered SportsDataIO events: {result.discovered_events}")
    print(f"Fetched SportsDataIO events: {result.fetched_events}")
    print(f"Source line-movement rows: {result.api_rows}")
    print(f"Matched canonical rows: {len(result.matched_rows)}")
    print(f"Unmatched/review rows: {len(result.unmatched_rows)}")
    print(f"Skipped duplicate source rows: {result.skipped_duplicate_rows}")
    print(f"Wrote: {args.source_output}")
    print(f"Wrote: {args.unmatched_output}")
    print(f"Wrote: {args.qa_output}")
    print(f"Wrote raw JSON under: {args.raw_dir}")
    return 0


def _discover_event_ids(
    *,
    api_key: str,
    wanted_dates: set[str],
    event_id_start: int,
    event_id_end: int,
    raw_dir: Path,
) -> dict[str, int]:
    event_ids: dict[str, int] = {}
    for event_id in range(event_id_start, event_id_end + 1):
        try:
            payload = _fetch_json(api_key, f"{SPORTSDATAIO_API_BASE}/EventOdds/{event_id}")
        except urllib.error.HTTPError as exc:
            if exc.code in {400, 404}:
                continue
            raise
        event = payload.get("Event") or {}
        event_date = _date_iso(event.get("Day"))
        if event_date not in wanted_dates:
            continue
        event_ids[event_date] = event_id
        _write_raw_json(raw_dir / f"event_odds_{event_id}.json", payload)
    return event_ids


def _adapt_line_movement_row(
    *,
    event: Mapping[str, object],
    odds_row: Mapping[str, object],
    local_fights_by_date_pair: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    imported_at: str,
) -> tuple[tuple[dict[str, str], dict[str, str]] | None, dict[str, str] | None, dict[str, str]]:
    event_date = _date_iso(event.get("Day"))
    fighter_a = odds_row.get("FighterA") or {}
    fighter_b = odds_row.get("FighterB") or {}
    fighter_a_name = _fighter_name(fighter_a)
    fighter_b_name = _fighter_name(fighter_b)
    pair_key = _pair_key(fighter_a_name, fighter_b_name)
    local_matches = local_fights_by_date_pair.get((event_date, pair_key), ()) if event_date else ()

    if event_date is None:
        return None, _unmatched_row(event, odds_row, "missing_event_date"), _qa_row(event, odds_row, "rejected", "missing_event_date")
    if not local_matches:
        return None, _unmatched_row(event, odds_row, "unknown_local_fight_pair"), _qa_row(event, odds_row, "unmatched", "unknown_local_fight_pair")
    if len(local_matches) > 1:
        return None, _unmatched_row(event, odds_row, "ambiguous_local_fight_pair"), _qa_row(event, odds_row, "ambiguous", "ambiguous_local_fight_pair", local_matches=local_matches)

    local = local_matches[0]
    try:
        timestamp = _sportsdataio_timestamp(odds_row.get("Created"))
        fighter_a_odds = _american_odds(odds_row.get("FighterAMoneyline"), "FighterAMoneyline")
        fighter_b_odds = _american_odds(odds_row.get("FighterBMoneyline"), "FighterBMoneyline")
    except ValueError as exc:
        return None, _unmatched_row(event, odds_row, str(exc)), _qa_row(event, odds_row, "rejected", str(exc), local_matches=local_matches)

    side_a = _local_side(local, fighter_a_name)
    side_b = _local_side(local, fighter_b_name)
    if side_a is None or side_b is None or side_a[0] == side_b[0]:
        return None, _unmatched_row(event, odds_row, "matched_pair_but_side_names_do_not_align"), _qa_row(event, odds_row, "rejected", "matched_pair_but_side_names_do_not_align", local_matches=local_matches)

    canonical = (
        _canonical_row(
            local=local,
            fighter_id=side_a[0],
            fighter_name=side_a[1],
            opponent_fighter_id=side_b[0],
            bookmaker=_required_text(odds_row.get("SportsbookName"), "SportsbookName"),
            odds_timestamp=timestamp,
            american_odds=fighter_a_odds,
            imported_at=imported_at,
            source_url=f"{SPORTSDATAIO_API_BASE}/EventOddsLineMovement/{event.get('EventId')}",
        ),
        _canonical_row(
            local=local,
            fighter_id=side_b[0],
            fighter_name=side_b[1],
            opponent_fighter_id=side_a[0],
            bookmaker=_required_text(odds_row.get("SportsbookName"), "SportsbookName"),
            odds_timestamp=timestamp,
            american_odds=fighter_b_odds,
            imported_at=imported_at,
            source_url=f"{SPORTSDATAIO_API_BASE}/EventOddsLineMovement/{event.get('EventId')}",
        ),
    )
    return canonical, None, _qa_row(event, odds_row, "matched", "", match_reason="event_date_and_fighter_pair", local_matches=local_matches, canonical_rows=2)


def _canonical_row(
    *,
    local: LocalFight,
    fighter_id: str,
    fighter_name: str,
    opponent_fighter_id: str,
    bookmaker: str,
    odds_timestamp: str,
    american_odds: int,
    imported_at: str,
    source_url: str,
) -> dict[str, str]:
    decimal_odds = american_to_decimal_odds(american_odds)
    return {
        "fight_id": local.fight_id,
        "event_id": local.event_id,
        "event_date": local.event_date,
        "fighter_id": fighter_id,
        "fighter_name": fighter_name,
        "opponent_fighter_id": opponent_fighter_id,
        "bookmaker": bookmaker,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": odds_timestamp,
        "american_odds": str(american_odds),
        "decimal_odds": format(decimal_odds, "f"),
        "source": SPORTSDATAIO_SOURCE_LABEL,
        "source_url": source_url,
        "imported_at": imported_at,
    }


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
            "event_name": _text(row.get("name")) or "",
            "event_date": _event_date(row),
        }
        for row in events
        if _text(row.get("event_id"))
    }
    fighter_name_by_id = {
        _text(row.get("fighter_id")): _text(row.get("full_name")) or ""
        for row in fighters
        if _text(row.get("fighter_id"))
    }

    grouped: dict[tuple[str, tuple[str, str]], list[LocalFight]] = {}
    seen_group_fights: set[tuple[tuple[str, tuple[str, str]], str]] = set()
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
            event_id=event_id,
            event_name=event["event_name"],
            event_date=event["event_date"],
            fight_id=fight_id,
            fighter_1_id=fighter_1_id,
            fighter_1_name=fighter_1_name,
            fighter_2_id=fighter_2_id,
            fighter_2_name=fighter_2_name,
        )
        group_key = (local.event_date, _pair_key(fighter_1_name, fighter_2_name))
        seen_key = (group_key, local.fight_id)
        if seen_key in seen_group_fights:
            continue
        seen_group_fights.add(seen_key)
        grouped.setdefault(group_key, []).append(local)
    return {key: tuple(value) for key, value in grouped.items()}


def _wanted_prediction_dates(path: Path) -> set[str]:
    rows = _read_csv(path)
    return {
        date_value
        for date_value in (_date_iso(row.get("event_date")) for row in rows)
        if date_value is not None
    }


def _api_key(env_name: str) -> str:
    value = os.environ.get(env_name) or os.environ.get(env_name.upper())
    if value:
        return value
    dotenv = REPO_ROOT / ".env"
    if dotenv.exists():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            if key.strip() == env_name:
                return raw_value.strip().strip('"').strip("'")
    raise ValueError(f"SportsDataIO API key not found in ${env_name} or {dotenv}")


def _fetch_json(api_key: str, url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "Ocp-Apim-Subscription-Key": api_key,
            "User-Agent": "ufc-data-sportsdataio-adapter/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _write_raw_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def _write_csv(path: Path, columns: list[str], rows: Iterable[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _qa_row(
    event: Mapping[str, object],
    odds_row: Mapping[str, object],
    row_status: str,
    rejection_reason: str,
    *,
    match_reason: str = "",
    local_matches: tuple[LocalFight, ...] = (),
    canonical_rows: int = 0,
) -> dict[str, str]:
    local = local_matches[0] if len(local_matches) == 1 else None
    return {
        "source": SPORTSDATAIO_SOURCE_LABEL,
        "row_status": row_status,
        "match_reason": match_reason,
        "rejection_reason": rejection_reason,
        **_sportsdataio_context(event, odds_row),
        "local_event_id": local.event_id if local else "",
        "local_event_name": local.event_name if local else "",
        "local_fight_id": local.fight_id if local else "",
        "local_fighter_1_id": local.fighter_1_id if local else "",
        "local_fighter_1_name": local.fighter_1_name if local else "",
        "local_fighter_2_id": local.fighter_2_id if local else "",
        "local_fighter_2_name": local.fighter_2_name if local else "",
        "canonical_rows": str(canonical_rows),
    }


def _unmatched_row(
    event: Mapping[str, object],
    odds_row: Mapping[str, object],
    reason: str,
) -> dict[str, str]:
    return {
        "source": SPORTSDATAIO_SOURCE_LABEL,
        **_sportsdataio_context(event, odds_row),
        "rejection_reason": reason,
    }


def _sportsdataio_context(event: Mapping[str, object], odds_row: Mapping[str, object]) -> dict[str, str]:
    fighter_a = odds_row.get("FighterA") or {}
    fighter_b = odds_row.get("FighterB") or {}
    return {
        "sportsdataio_event_id": str(event.get("EventId") or ""),
        "sportsdataio_event_name": _text(event.get("Name")) or "",
        "sportsdataio_event_date": _date_iso(event.get("Day")) or "",
        "sportsdataio_fight_id": str(odds_row.get("FightId") or ""),
        "sportsdataio_fighter_a_id": str(fighter_a.get("FighterId") or ""),
        "sportsdataio_fighter_a_name": _fighter_name(fighter_a),
        "sportsdataio_fighter_b_id": str(fighter_b.get("FighterId") or ""),
        "sportsdataio_fighter_b_name": _fighter_name(fighter_b),
        "sportsbook": _text(odds_row.get("SportsbookName")) or "",
        "created": _text(odds_row.get("Created")) or "",
        "updated": _text(odds_row.get("Updated")) or "",
    }


def _local_side(local: LocalFight, source_name: str) -> tuple[str, str] | None:
    normalized = _normalize_name(source_name)
    if normalized == _normalize_name(local.fighter_1_name):
        return local.fighter_1_id, local.fighter_1_name
    if normalized == _normalize_name(local.fighter_2_name):
        return local.fighter_2_id, local.fighter_2_name
    return None


def _fighter_name(fighter: Mapping[str, object]) -> str:
    return " ".join(
        part
        for part in [
            _text(fighter.get("FirstName")),
            _text(fighter.get("LastName")),
        ]
        if part
    )


def _pair_key(first: str, second: str) -> tuple[str, str]:
    return tuple(sorted([_normalize_name(first), _normalize_name(second)]))


def _normalize_name(value: object) -> str:
    text = unicodedata.normalize("NFKD", _text(value) or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("'", "").replace(".", "")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _sportsdataio_timestamp(value: object) -> str:
    text = _required_text(value, "Created")
    try:
        naive = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("invalid_created_timestamp") from exc
    if naive.tzinfo is None:
        return naive.replace(tzinfo=EASTERN).astimezone(timezone.utc).isoformat()
    return naive.astimezone(timezone.utc).isoformat()


def _american_odds(value: object, column: str) -> int:
    text = _required_text(value, column)
    try:
        return validate_american_odds(Decimal(text))
    except ValueError as exc:
        raise ValueError(f"invalid_{column}") from exc


def _required_text(value: object, column: str) -> str:
    text = _text(value)
    if text is None:
        raise ValueError(f"missing_{column}")
    return text


def _event_date(row: Mapping[str, object]) -> str | None:
    date_text = _text(row.get("event_date")) or _text(row.get("date_formatted"))
    if date_text is not None:
        return _date_iso(date_text)
    verbose_date = _text(row.get("date"))
    if verbose_date is None:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(verbose_date, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _date_iso(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except ValueError:
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


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "DEFAULT_QA_OUTPUT",
    "DEFAULT_RAW_DIR",
    "DEFAULT_SOURCE_OUTPUT",
    "DEFAULT_UNMATCHED_OUTPUT",
    "SPORTSDATAIO_SOURCE_LABEL",
    "FetchResult",
    "fetch_sportsdataio_odds",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
