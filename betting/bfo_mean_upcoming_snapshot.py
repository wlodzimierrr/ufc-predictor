"""Write upcoming BestFightOdds Mean market-benchmark comparison rows."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.bfo_mean_market_backtest import (
    DEFAULT_BOOKMAKER,
    DEFAULT_FIGHTERS,
    DEFAULT_FIGHTS,
    DEFAULT_ODDS,
    DEFAULT_PREDICTIONS,
    DEFAULT_REPORT_DIR,
    DEFAULT_SOURCE,
    _latest_fighter_names,
    _latest_rows_by_id,
    _prepare_bfo_no_vig_odds_rows,
    _prepare_prediction_rows,
    _read_csv,
)
from betting.config import (
    BettingConfig,
    apply_cli_overrides,
    default_config,
    load_config_file,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = DEFAULT_REPORT_DIR / "bfo_mean_upcoming_market_snapshot.csv"
BENCHMARK_LABEL = "market-benchmark-not-sportsbook-executable"

OUTPUT_COLUMNS = [
    "benchmark_label",
    "benchmark_freshness_status",
    "benchmark_exclusion_reason",
    "as_of",
    "max_odds_age_hours",
    "event_id",
    "event_name",
    "event_date",
    "fight_id",
    "fighter_id",
    "fighter_name",
    "opponent_fighter_id",
    "opponent_fighter_name",
    "bookmaker",
    "market",
    "line_type",
    "odds_timestamp",
    "odds_age_hours",
    "scored_at",
    "model_probability",
    "market_implied_probability",
    "no_vig_market_probability",
    "edge",
    "ev_per_unit",
    "offered_decimal_odds",
]


@dataclass(frozen=True)
class BenchmarkSelection:
    status: str
    reason: str
    odds_rows: tuple[Mapping[str, str], ...]
    odds_age_hours: Decimal | None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write upcoming BFO Mean market-benchmark comparison rows."
    )
    parser.add_argument("--predictions", type=Path, default=DEFAULT_PREDICTIONS)
    parser.add_argument("--odds", type=Path, default=DEFAULT_ODDS)
    parser.add_argument("--fights", type=Path, default=DEFAULT_FIGHTS)
    parser.add_argument("--fighters", type=Path, default=DEFAULT_FIGHTERS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--as-of", help="Timestamp cutoff for current odds, ISO-8601. Defaults to now UTC.")
    parser.add_argument("--config", help="Optional .json or .toml betting config file.")
    parser.add_argument("--max-odds-age-hours-current", type=int, help="Override current BFO benchmark freshness cap.")
    return parser


def build_upcoming_snapshot_rows(
    *,
    predictions_path: Path = DEFAULT_PREDICTIONS,
    odds_path: Path = DEFAULT_ODDS,
    fights_path: Path = DEFAULT_FIGHTS,
    fighters_path: Path = DEFAULT_FIGHTERS,
    as_of: datetime | str | None = None,
    config: BettingConfig | None = None,
    max_odds_age_hours: int | None = None,
) -> list[dict[str, str]]:
    """Return upcoming BFO Mean benchmark comparison rows."""
    as_of_dt = _datetime_from_value(as_of) or datetime.now(timezone.utc)
    config = config or default_config()
    if max_odds_age_hours is not None:
        config = config.with_overrides({"max_odds_age_hours_current": max_odds_age_hours})
    max_age = config.risk.max_odds_age_hours_current

    raw_odds_rows = _read_csv(odds_path)
    odds_rows, _ = _prepare_bfo_no_vig_odds_rows(raw_odds_rows)
    prediction_rows, _ = _prepare_prediction_rows(
        _read_csv(predictions_path),
        fight_rows=_latest_rows_by_id(_read_csv(fights_path), "fight_id"),
        fighter_names=_latest_fighter_names(_read_csv(fighters_path)),
    )
    upcoming_predictions = [
        row for row in prediction_rows
        if _date_from_value(row.get("event_date")) is not None
        and _date_from_value(row.get("event_date")) >= as_of_dt.date()
    ]
    selected_odds = _select_bfo_benchmarks(
        raw_odds_rows,
        odds_rows,
        as_of=as_of_dt,
        max_odds_age_hours=max_age,
    )

    output = []
    for prediction in sorted(upcoming_predictions, key=lambda row: (row.get("event_date", ""), row.get("fight_id", ""))):
        fight_id = prediction.get("fight_id", "")
        selection = selected_odds.get(fight_id)
        if selection is None:
            selection = BenchmarkSelection(
                status="missing",
                reason="missing_bfo_rows",
                odds_rows=(),
                odds_age_hours=None,
            )
        if selection.odds_rows:
            for odds_row in sorted(selection.odds_rows, key=lambda row: row.get("fighter_id", "")):
                output.append(_snapshot_row(
                    prediction,
                    odds_row,
                    selection=selection,
                    as_of=as_of_dt,
                    max_odds_age_hours=max_age,
                ))
            continue
        output.extend(_placeholder_rows(
            prediction,
            selection=selection,
            as_of=as_of_dt,
            max_odds_age_hours=max_age,
        ))
    return output


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    config = load_config_file(args.config) if args.config else default_config()
    config = apply_cli_overrides(config, args)
    rows = build_upcoming_snapshot_rows(
        predictions_path=args.predictions,
        odds_path=args.odds,
        fights_path=args.fights,
        fighters_path=args.fighters,
        as_of=args.as_of,
        config=config,
    )
    _write_csv(args.output, rows)
    print("BestFightOdds Mean upcoming market snapshot.")
    print("Label: market-benchmark, not sportsbook-executable P/L.")
    print(f"Rows written: {len(rows)}")
    print(f"Wrote: {args.output}")
    return 0


def _latest_odds_by_fight(
    odds_rows: list[Mapping[str, str]],
    *,
    as_of: datetime,
) -> dict[str, list[Mapping[str, str]]]:
    groups: dict[tuple[str, str], list[Mapping[str, str]]] = {}
    for row in odds_rows:
        timestamp = _datetime_from_value(row.get("odds_timestamp"))
        if timestamp is None or timestamp > as_of:
            continue
        fight_id = row.get("fight_id", "")
        if not fight_id:
            continue
        groups.setdefault((fight_id, timestamp.isoformat()), []).append(row)

    by_fight: dict[str, list[Mapping[str, str]]] = {}
    for (fight_id, _), group in groups.items():
        if len(group) != 2:
            continue
        existing = by_fight.get(fight_id)
        if existing is None or _datetime_from_value(group[0].get("odds_timestamp")) > _datetime_from_value(existing[0].get("odds_timestamp")):
            by_fight[fight_id] = group
    return by_fight


def _select_bfo_benchmarks(
    raw_odds_rows: list[Mapping[str, str]],
    prepared_odds_rows: list[Mapping[str, str]],
    *,
    as_of: datetime,
    max_odds_age_hours: int,
) -> dict[str, BenchmarkSelection]:
    raw_groups: dict[str, dict[str, list[Mapping[str, str]]]] = {}
    for row in raw_odds_rows:
        if not _is_relevant_bfo_row(row):
            continue
        timestamp = _datetime_from_value(row.get("odds_timestamp"))
        fight_id = row.get("fight_id", "")
        if timestamp is None or not fight_id:
            continue
        raw_groups.setdefault(fight_id, {}).setdefault(timestamp.isoformat(), []).append(row)

    valid_groups: dict[tuple[str, str], list[Mapping[str, str]]] = {}
    for row in prepared_odds_rows:
        timestamp = _datetime_from_value(row.get("odds_timestamp"))
        fight_id = row.get("fight_id", "")
        if timestamp is None or not fight_id:
            continue
        valid_groups.setdefault((fight_id, timestamp.isoformat()), []).append(row)

    selections: dict[str, BenchmarkSelection] = {}
    for fight_id, groups in raw_groups.items():
        timestamp_values = [
            (_datetime_from_value(timestamp), timestamp)
            for timestamp in groups
        ]
        past = [(timestamp, key) for timestamp, key in timestamp_values if timestamp is not None and timestamp <= as_of]
        if not past:
            selections[fight_id] = BenchmarkSelection(
                status="future_only",
                reason="future_only_bfo_rows",
                odds_rows=(),
                odds_age_hours=None,
            )
            continue
        latest_timestamp, latest_key = max(past, key=lambda item: item[0])
        raw_group = groups[latest_key]
        age = _odds_age_hours(as_of, latest_timestamp)
        valid_group = tuple(valid_groups.get((fight_id, latest_key), ()))
        if len(raw_group) != 2:
            selections[fight_id] = BenchmarkSelection(
                status="one_sided",
                reason=f"expected_two_bfo_sides_got_{len(raw_group)}",
                odds_rows=(),
                odds_age_hours=age,
            )
            continue
        if len(valid_group) != 2:
            selections[fight_id] = BenchmarkSelection(
                status="invalid",
                reason="invalid_two_sided_no_vig_group",
                odds_rows=(),
                odds_age_hours=age,
            )
            continue
        if age > Decimal(max_odds_age_hours):
            selections[fight_id] = BenchmarkSelection(
                status="stale",
                reason="odds_age_exceeds_max",
                odds_rows=valid_group,
                odds_age_hours=age,
            )
            continue
        selections[fight_id] = BenchmarkSelection(
            status="fresh",
            reason="",
            odds_rows=valid_group,
            odds_age_hours=age,
        )
    return selections


def _snapshot_row(
    prediction: Mapping[str, str],
    odds_row: Mapping[str, str],
    *,
    selection: BenchmarkSelection,
    as_of: datetime,
    max_odds_age_hours: int,
) -> dict[str, str]:
    model_probability = _model_probability(prediction, odds_row)
    no_vig = Decimal(odds_row["no_vig_implied_probability"])
    offered = Decimal(odds_row["normalized_decimal_odds"])
    edge = model_probability - no_vig
    ev_per_unit = (model_probability * offered) - Decimal("1")
    return {
        "benchmark_label": BENCHMARK_LABEL,
        "benchmark_freshness_status": selection.status,
        "benchmark_exclusion_reason": selection.reason,
        "as_of": as_of.isoformat(),
        "max_odds_age_hours": str(max_odds_age_hours),
        "event_id": prediction.get("event_id", ""),
        "event_name": prediction.get("event_name", ""),
        "event_date": prediction.get("event_date", ""),
        "fight_id": prediction.get("fight_id", ""),
        "fighter_id": odds_row.get("fighter_id", ""),
        "fighter_name": odds_row.get("fighter_name", ""),
        "opponent_fighter_id": odds_row.get("opponent_fighter_id", ""),
        "opponent_fighter_name": _opponent_name(prediction, odds_row),
        "bookmaker": odds_row.get("bookmaker", ""),
        "market": odds_row.get("market", ""),
        "line_type": odds_row.get("line_type", ""),
        "odds_timestamp": odds_row.get("odds_timestamp", ""),
        "odds_age_hours": _format_optional_decimal(selection.odds_age_hours),
        "scored_at": prediction.get("scored_at", ""),
        "model_probability": _format_decimal(model_probability),
        "market_implied_probability": odds_row.get("implied_probability", ""),
        "no_vig_market_probability": odds_row.get("no_vig_implied_probability", ""),
        "edge": _format_decimal(edge),
        "ev_per_unit": _format_decimal(ev_per_unit),
        "offered_decimal_odds": odds_row.get("normalized_decimal_odds", ""),
    }


def _placeholder_rows(
    prediction: Mapping[str, str],
    *,
    selection: BenchmarkSelection,
    as_of: datetime,
    max_odds_age_hours: int,
) -> list[dict[str, str]]:
    rows = []
    sides = [
        (
            prediction.get("fighter_1_id", ""),
            prediction.get("fighter_1_name", ""),
            prediction.get("fighter_2_id", ""),
            prediction.get("fighter_2_name", ""),
        ),
        (
            prediction.get("fighter_2_id", ""),
            prediction.get("fighter_2_name", ""),
            prediction.get("fighter_1_id", ""),
            prediction.get("fighter_1_name", ""),
        ),
    ]
    for fighter_id, fighter_name, opponent_id, opponent_name in sides:
        rows.append({
            "benchmark_label": BENCHMARK_LABEL,
            "benchmark_freshness_status": selection.status,
            "benchmark_exclusion_reason": selection.reason,
            "as_of": as_of.isoformat(),
            "max_odds_age_hours": str(max_odds_age_hours),
            "event_id": prediction.get("event_id", ""),
            "event_name": prediction.get("event_name", ""),
            "event_date": prediction.get("event_date", ""),
            "fight_id": prediction.get("fight_id", ""),
            "fighter_id": fighter_id,
            "fighter_name": fighter_name,
            "opponent_fighter_id": opponent_id,
            "opponent_fighter_name": opponent_name,
            "bookmaker": DEFAULT_BOOKMAKER,
            "market": "moneyline",
            "line_type": "current",
            "odds_timestamp": "",
            "odds_age_hours": _format_optional_decimal(selection.odds_age_hours),
            "scored_at": prediction.get("scored_at", ""),
            "model_probability": _format_decimal(_model_probability_for_fighter(prediction, fighter_id)),
            "market_implied_probability": "",
            "no_vig_market_probability": "",
            "edge": "",
            "ev_per_unit": "",
            "offered_decimal_odds": "",
        })
    return rows


def _model_probability(prediction: Mapping[str, str], odds_row: Mapping[str, str]) -> Decimal:
    return _model_probability_for_fighter(prediction, odds_row.get("fighter_id"))


def _model_probability_for_fighter(prediction: Mapping[str, str], fighter_id: str | None) -> Decimal:
    f1_id = prediction.get("fighter_1_id")
    f1_probability = Decimal(prediction["calibrated_prob_f1"])
    if fighter_id == f1_id:
        return f1_probability
    return Decimal("1") - f1_probability


def _opponent_name(prediction: Mapping[str, str], odds_row: Mapping[str, str]) -> str:
    if odds_row.get("opponent_fighter_id") == prediction.get("fighter_1_id"):
        return prediction.get("fighter_1_name", "")
    if odds_row.get("opponent_fighter_id") == prediction.get("fighter_2_id"):
        return prediction.get("fighter_2_name", "")
    return ""


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _date_from_value(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _datetime_from_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _is_relevant_bfo_row(row: Mapping[str, str]) -> bool:
    return (
        row.get("bookmaker") == DEFAULT_BOOKMAKER
        and row.get("source") == DEFAULT_SOURCE
        and (row.get("market") or "moneyline") == "moneyline"
        and row.get("line_type") == "current"
    )


def _odds_age_hours(as_of: datetime, odds_timestamp: datetime) -> Decimal:
    seconds = Decimal(str((as_of - odds_timestamp).total_seconds()))
    return seconds / Decimal("3600")


def _format_decimal(value: Decimal) -> str:
    return format(value, "f")


def _format_optional_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.quantize(Decimal("0.000001")), "f")


if __name__ == "__main__":
    raise SystemExit(main())
