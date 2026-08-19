"""Fetch BALLDONTLIE MMA odds and adapt them to the canonical odds CSV.

Usage:
    python3 warehouse/fetch_balldontlie_odds.py
    python3 warehouse/fetch_balldontlie_odds.py --line-types opening,current
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.odds import american_to_decimal_odds, validate_american_odds
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PREDICTION_EVENTS_CSV = REPO_ROOT / "data" / "reports" / "pre_event_prediction_events.csv"
DEFAULT_EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "balldontlie"
DEFAULT_SOURCE_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "balldontlie_fight_odds.csv"
DEFAULT_UNMATCHED_OUTPUT = REPO_ROOT / "data" / "odds" / "sources" / "balldontlie_unmatched_odds.csv"
DEFAULT_QA_OUTPUT = REPO_ROOT / "data" / "reports" / "balldontlie_odds_matching_qa.csv"

BALLDONTLIE_SOURCE_LABEL = "balldontlie_mma_odds"
BALLDONTLIE_API_BASE = "https://api.balldontlie.io/mma/v1"
SUPPORTED_LINE_TYPES = ("opening", "current")

UNMATCHED_COLUMNS = [
    "source",
    "line_type",
    "balldontlie_event_id",
    "balldontlie_event_name",
    "balldontlie_event_date",
    "balldontlie_fight_id",
    "balldontlie_odds_id",
    "balldontlie_fighter_1_id",
    "balldontlie_fighter_1_name",
    "balldontlie_fighter_2_id",
    "balldontlie_fighter_2_name",
    "vendor",
    "odds_timestamp",
    "rejection_reason",
]

QA_COLUMNS = [
    "source",
    "line_type",
    "row_status",
    "match_reason",
    "rejection_reason",
    "balldontlie_event_id",
    "balldontlie_event_name",
    "balldontlie_event_date",
    "balldontlie_fight_id",
    "balldontlie_odds_id",
    "balldontlie_fighter_1_id",
    "balldontlie_fighter_1_name",
    "balldontlie_fighter_2_id",
    "balldontlie_fighter_2_name",
    "vendor",
    "odds_timestamp",
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
    matched_events: int
    fetched_event_line_types: int
    api_rows: int
    matched_rows: tuple[dict[str, str], ...]
    unmatched_rows: tuple[dict[str, str], ...]
    qa_rows: tuple[dict[str, str], ...]
    skipped_duplicate_rows: int


def fetch_balldontlie_odds(
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
    line_types: tuple[str, ...] = ("opening",),
    request_interval_seconds: float = 13.0,
) -> FetchResult:
    """Fetch BALLDONTLIE odds for local prediction-event dates."""
    wanted_events = _wanted_prediction_events(prediction_events_csv)
    wanted_dates = {event_date for event_date, _ in wanted_events}
    wanted_dates_by_name = {
        _normalize_name(event_name): event_date
        for event_date, event_name in wanted_events
        if event_name
    }
    years = sorted({int(date_value[:4]) for date_value in wanted_dates})
    local_fights_by_date_pair = _local_fights_by_date_pair(
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )

    bdl_events_by_date = _fetch_matching_events(
        api_key=api_key,
        years=years,
        wanted_dates=wanted_dates,
        wanted_dates_by_name=wanted_dates_by_name,
        raw_dir=raw_dir,
        request_interval_seconds=request_interval_seconds,
    )

    matched_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    qa_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    skipped_duplicate_rows = 0
    api_rows = 0
    fetched_event_line_types = 0
    imported_at = datetime.now(timezone.utc).isoformat()

    for event_date, event in sorted(bdl_events_by_date.items()):
        for line_type in line_types:
            payload = _fetch_odds_payload(
                api_key=api_key,
                event_id=str(event["id"]),
                line_type=line_type,
                request_interval_seconds=request_interval_seconds,
            )
            fetched_event_line_types += 1
            _write_raw_json(raw_dir / f"odds_{line_type}_event_{event['id']}.json", payload)
            odds_rows = payload.get("data") or []
            api_rows += len(odds_rows)
            for odds_row in odds_rows:
                canonical, unmatched, qa = _adapt_odds_row(
                    event=event,
                    odds_row=odds_row,
                    line_type=line_type,
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
                    unmatched_rows.append(_unmatched_row(event, odds_row, line_type, "duplicate_stable_odds_key"))
                    continue
                seen_keys.update(keys)
                matched_rows.extend(canonical)

    result = FetchResult(
        matched_events=len(bdl_events_by_date),
        fetched_event_line_types=fetched_event_line_types,
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
        description="Fetch BALLDONTLIE MMA odds and write canonical odds rows."
    )
    parser.add_argument("--api-key-env", default="BALLDONTLIE_API")
    parser.add_argument("--prediction-events-csv", type=Path, default=DEFAULT_PREDICTION_EVENTS_CSV)
    parser.add_argument("--events-csv", type=Path, default=DEFAULT_EVENTS_CSV)
    parser.add_argument("--fights-csv", type=Path, default=DEFAULT_FIGHTS_CSV)
    parser.add_argument("--fighters-csv", type=Path, default=DEFAULT_FIGHTERS_CSV)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--source-output", type=Path, default=DEFAULT_SOURCE_OUTPUT)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT)
    parser.add_argument("--qa-output", type=Path, default=DEFAULT_QA_OUTPUT)
    parser.add_argument("--line-types", default="opening", help="Comma-separated list: opening,current")
    parser.add_argument("--request-interval-seconds", type=float, default=13.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    line_types = _parse_line_types(args.line_types)
    result = fetch_balldontlie_odds(
        api_key=_api_key(args.api_key_env),
        prediction_events_csv=args.prediction_events_csv,
        events_csv=args.events_csv,
        fights_csv=args.fights_csv,
        fighters_csv=args.fighters_csv,
        raw_dir=args.raw_dir,
        source_output=args.source_output,
        unmatched_output=args.unmatched_output,
        qa_output=args.qa_output,
        line_types=line_types,
        request_interval_seconds=args.request_interval_seconds,
    )
    print(f"Matched BALLDONTLIE events: {result.matched_events}")
    print(f"Fetched event/line-type payloads: {result.fetched_event_line_types}")
    print(f"Source odds rows: {result.api_rows}")
    print(f"Matched canonical rows: {len(result.matched_rows)}")
    print(f"Unmatched/review rows: {len(result.unmatched_rows)}")
    print(f"Skipped duplicate source rows: {result.skipped_duplicate_rows}")
    print(f"Wrote: {args.source_output}")
    print(f"Wrote: {args.unmatched_output}")
    print(f"Wrote: {args.qa_output}")
    print(f"Wrote raw JSON under: {args.raw_dir}")
    return 0


def _fetch_matching_events(
    *,
    api_key: str,
    years: Iterable[int],
    wanted_dates: set[str],
    wanted_dates_by_name: Mapping[str, str],
    raw_dir: Path,
    request_interval_seconds: float,
) -> dict[str, Mapping[str, object]]:
    events_by_date: dict[str, Mapping[str, object]] = {}
    for year in years:
        payload = _fetch_paginated(
            api_key=api_key,
            path="/events",
            params={"year": year, "per_page": 100},
            request_interval_seconds=request_interval_seconds,
        )
        _write_raw_json(raw_dir / f"events_{year}.json", payload)
        for event in payload.get("data") or []:
            league = event.get("league") or {}
            event_date = _date_iso(event.get("date"))
            event_name = _normalize_name(event.get("name"))
            local_event_date = event_date
            candidate_date = _candidate_date_by_name(event_name, wanted_dates_by_name)
            if event_date not in wanted_dates and candidate_date is not None:
                if event_date is not None and _date_distance_days(candidate_date, event_date) <= 1:
                    local_event_date = candidate_date
            if league.get("abbreviation") != "UFC" or local_event_date not in wanted_dates:
                continue
            events_by_date[local_event_date] = {**event, "_local_event_date": local_event_date}
    return events_by_date


def _fetch_odds_payload(
    *,
    api_key: str,
    event_id: str,
    line_type: str,
    request_interval_seconds: float,
) -> dict[str, object]:
    endpoint = "/odds/opening" if line_type == "opening" else "/odds"
    return _fetch_paginated(
        api_key=api_key,
        path=endpoint,
        params={"event_id": event_id, "per_page": 100},
        request_interval_seconds=request_interval_seconds,
    )


def _fetch_paginated(
    *,
    api_key: str,
    path: str,
    params: Mapping[str, object],
    request_interval_seconds: float,
) -> dict[str, object]:
    data: list[object] = []
    meta: dict[str, object] = {}
    cursor: object | None = None
    while True:
        page_params = dict(params)
        if cursor is not None:
            page_params["cursor"] = cursor
        payload = _fetch_json(api_key, path, page_params)
        data.extend(payload.get("data") or [])
        meta = payload.get("meta") or {}
        cursor = meta.get("next_cursor")
        if not cursor:
            return {"data": data, "meta": meta}
        _sleep(request_interval_seconds)


def _fetch_json(api_key: str, path: str, params: Mapping[str, object]) -> dict[str, object]:
    query = urllib.parse.urlencode(params, doseq=True)
    url = f"{BALLDONTLIE_API_BASE}{path}?{query}" if query else f"{BALLDONTLIE_API_BASE}{path}"
    while True:
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": api_key,
                "User-Agent": "ufc-data-balldontlie-adapter/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code != 429:
                raise
            retry_after = exc.headers.get("Retry-After")
            seconds = float(retry_after) if retry_after else 65.0
            time.sleep(seconds)


def _adapt_odds_row(
    *,
    event: Mapping[str, object],
    odds_row: Mapping[str, object],
    line_type: str,
    local_fights_by_date_pair: Mapping[tuple[str, tuple[str, str]], tuple[LocalFight, ...]],
    imported_at: str,
) -> tuple[tuple[dict[str, str], dict[str, str]] | None, dict[str, str] | None, dict[str, str]]:
    event_date = _local_event_date(event)
    fighter_1 = odds_row.get("fighter1") or {}
    fighter_2 = odds_row.get("fighter2") or {}
    fighter_1_name = _fighter_name(fighter_1)
    fighter_2_name = _fighter_name(fighter_2)
    local_matches = (
        local_fights_by_date_pair.get((event_date, _pair_key(fighter_1_name, fighter_2_name)), ())
        if event_date
        else ()
    )

    if event_date is None:
        return None, _unmatched_row(event, odds_row, line_type, "missing_event_date"), _qa_row(event, odds_row, line_type, "rejected", "missing_event_date")
    if not local_matches:
        return None, _unmatched_row(event, odds_row, line_type, "unknown_local_fight_pair"), _qa_row(event, odds_row, line_type, "unmatched", "unknown_local_fight_pair")
    if len(local_matches) > 1:
        return None, _unmatched_row(event, odds_row, line_type, "ambiguous_local_fight_pair"), _qa_row(event, odds_row, line_type, "ambiguous", "ambiguous_local_fight_pair", local_matches=local_matches)

    local = local_matches[0]
    timestamp_key = "opened_at" if line_type == "opening" else "updated_at"
    try:
        timestamp = _timestamp(odds_row.get(timestamp_key), timestamp_key)
        fighter_1_odds = _american_odds(odds_row.get("fighter1_odds"), "fighter1_odds")
        fighter_2_odds = _american_odds(odds_row.get("fighter2_odds"), "fighter2_odds")
        vendor = _required_text(odds_row.get("vendor"), "vendor")
    except ValueError as exc:
        return None, _unmatched_row(event, odds_row, line_type, str(exc)), _qa_row(event, odds_row, line_type, "rejected", str(exc), local_matches=local_matches)

    side_1 = _local_side(local, fighter_1_name)
    side_2 = _local_side(local, fighter_2_name)
    if side_1 is None or side_2 is None or side_1[0] == side_2[0]:
        return None, _unmatched_row(event, odds_row, line_type, "matched_pair_but_side_names_do_not_align"), _qa_row(event, odds_row, line_type, "rejected", "matched_pair_but_side_names_do_not_align", local_matches=local_matches)

    source_url = f"{BALLDONTLIE_API_BASE}/odds" + ("/opening" if line_type == "opening" else "")
    canonical = (
        _canonical_row(
            local=local,
            fighter_id=side_1[0],
            fighter_name=side_1[1],
            opponent_fighter_id=side_2[0],
            bookmaker=vendor,
            line_type=line_type,
            odds_timestamp=timestamp,
            american_odds=fighter_1_odds,
            imported_at=imported_at,
            source_url=source_url,
        ),
        _canonical_row(
            local=local,
            fighter_id=side_2[0],
            fighter_name=side_2[1],
            opponent_fighter_id=side_1[0],
            bookmaker=vendor,
            line_type=line_type,
            odds_timestamp=timestamp,
            american_odds=fighter_2_odds,
            imported_at=imported_at,
            source_url=source_url,
        ),
    )
    return canonical, None, _qa_row(event, odds_row, line_type, "matched", "", match_reason="event_date_and_fighter_pair", local_matches=local_matches, canonical_rows=2)


def _canonical_row(
    *,
    local: LocalFight,
    fighter_id: str,
    fighter_name: str,
    opponent_fighter_id: str,
    bookmaker: str,
    line_type: str,
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
        "line_type": line_type,
        "odds_timestamp": odds_timestamp,
        "american_odds": str(american_odds),
        "decimal_odds": format(decimal_odds, "f"),
        "source": BALLDONTLIE_SOURCE_LABEL,
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


def _wanted_prediction_events(path: Path) -> set[tuple[str, str]]:
    rows = _read_csv(path)
    return {
        (date_value, _text(row.get("event_name")) or "")
        for row in rows
        for date_value in [_date_iso(row.get("event_date"))]
        if date_value is not None
    }


def _parse_line_types(value: str) -> tuple[str, ...]:
    line_types = tuple(part.strip().lower() for part in value.split(",") if part.strip())
    invalid = [line_type for line_type in line_types if line_type not in SUPPORTED_LINE_TYPES]
    if invalid:
        raise ValueError(f"unsupported line type(s): {', '.join(invalid)}")
    if not line_types:
        raise ValueError("at least one line type is required")
    return line_types


def _candidate_date_by_name(
    event_name: str,
    wanted_dates_by_name: Mapping[str, str],
) -> str | None:
    if event_name in wanted_dates_by_name:
        return wanted_dates_by_name[event_name]
    matches = [
        event_date
        for wanted_name, event_date in wanted_dates_by_name.items()
        if event_name.startswith(wanted_name) or wanted_name.startswith(event_name)
    ]
    return matches[0] if len(matches) == 1 else None


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
    raise ValueError(f"BALLDONTLIE API key not found in ${env_name} or {dotenv}")


def _qa_row(
    event: Mapping[str, object],
    odds_row: Mapping[str, object],
    line_type: str,
    row_status: str,
    rejection_reason: str,
    *,
    match_reason: str = "",
    local_matches: tuple[LocalFight, ...] = (),
    canonical_rows: int = 0,
) -> dict[str, str]:
    local = local_matches[0] if len(local_matches) == 1 else None
    return {
        "source": BALLDONTLIE_SOURCE_LABEL,
        "line_type": line_type,
        "row_status": row_status,
        "match_reason": match_reason,
        "rejection_reason": rejection_reason,
        **_balldontlie_context(event, odds_row, line_type),
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
    line_type: str,
    reason: str,
) -> dict[str, str]:
    return {
        "source": BALLDONTLIE_SOURCE_LABEL,
        "line_type": line_type,
        **_balldontlie_context(event, odds_row, line_type),
        "rejection_reason": reason,
    }


def _balldontlie_context(event: Mapping[str, object], odds_row: Mapping[str, object], line_type: str) -> dict[str, str]:
    fighter_1 = odds_row.get("fighter1") or {}
    fighter_2 = odds_row.get("fighter2") or {}
    timestamp_key = "opened_at" if line_type == "opening" else "updated_at"
    return {
        "balldontlie_event_id": str(event.get("id") or ""),
        "balldontlie_event_name": _text(event.get("name")) or "",
        "balldontlie_event_date": _date_iso(event.get("date")) or "",
        "balldontlie_fight_id": str(odds_row.get("fight_id") or ""),
        "balldontlie_odds_id": str(odds_row.get("id") or ""),
        "balldontlie_fighter_1_id": str(fighter_1.get("id") or ""),
        "balldontlie_fighter_1_name": _fighter_name(fighter_1),
        "balldontlie_fighter_2_id": str(fighter_2.get("id") or ""),
        "balldontlie_fighter_2_name": _fighter_name(fighter_2),
        "vendor": _text(odds_row.get("vendor")) or "",
        "odds_timestamp": _text(odds_row.get(timestamp_key)) or "",
    }


def _local_side(local: LocalFight, source_name: str) -> tuple[str, str] | None:
    normalized = _normalize_name(source_name)
    if normalized == _normalize_name(local.fighter_1_name):
        return local.fighter_1_id, local.fighter_1_name
    if normalized == _normalize_name(local.fighter_2_name):
        return local.fighter_2_id, local.fighter_2_name
    return None


def _fighter_name(fighter: Mapping[str, object]) -> str:
    name = _text(fighter.get("name"))
    if name:
        return name
    return " ".join(
        part
        for part in [
            _text(fighter.get("first_name")),
            _text(fighter.get("last_name")),
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


def _timestamp(value: object, column: str) -> str:
    text = _required_text(value, column)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid_{column}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


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


def _local_event_date(event: Mapping[str, object]) -> str | None:
    return _text(event.get("_local_event_date")) or _date_iso(event.get("date"))


def _date_iso(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except ValueError:
        return None


def _date_distance_days(first: str, second: str) -> int:
    first_date = date.fromisoformat(first)
    second_date = date.fromisoformat(second)
    return abs((first_date - second_date).days)


def _stable_key(row: Mapping[str, str]) -> tuple[str, str, str, str, str, str]:
    return (
        row["fight_id"],
        row["fighter_id"],
        row["bookmaker"],
        row["market"],
        row["line_type"],
        row["odds_timestamp"],
    )


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


def _sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "BALLDONTLIE_SOURCE_LABEL",
    "DEFAULT_QA_OUTPUT",
    "DEFAULT_RAW_DIR",
    "DEFAULT_SOURCE_OUTPUT",
    "DEFAULT_UNMATCHED_OUTPUT",
    "FetchResult",
    "fetch_balldontlie_odds",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
