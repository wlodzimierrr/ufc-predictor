"""Research-only betting simulation using retro predictions and Kaggle odds.

This intentionally is not a leakage-safe historical betting backtest. The
Kaggle odds file contains collection timestamps, not confirmed pre-fight
availability timestamps for old events. To reuse the regular betting engine
without mixing results, this script shifts each odds snapshot to a synthetic
pre-event timestamp and writes separate ``kaggle_research_backtest_*`` reports.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.backtest import (
    BACKTEST_DETAIL_DECISIONS,
    BACKTEST_DETAIL_MODES,
    LINE_POLICY_LATEST_CURRENT,
    build_historical_betting_dataset,
    filter_historical_prediction_rows,
    generate_backtest_reports,
    print_backtest_summary,
)
from betting.config import (
    BettingConfig,
    apply_cli_overrides,
    default_config,
    load_config_file,
)
from betting.odds import calculate_no_vig_probabilities

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PREDICTIONS = REPO_ROOT / "models" / "backtests" / "past_event_predictions.csv"
DEFAULT_ODDS = REPO_ROOT / "data" / "odds" / "sources" / "kaggle_fight_odds.csv"


def load_kaggle_research_dataset(
    *,
    predictions_path: Path = DEFAULT_PREDICTIONS,
    odds_path: Path = DEFAULT_ODDS,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    bookmaker: str | None = None,
) -> tuple[object, dict[str, int]]:
    """Return a research dataset plus small construction counters."""
    prediction_rows = _prepare_prediction_rows(_read_csv(predictions_path))
    prediction_rows = filter_historical_prediction_rows(
        prediction_rows,
        start_date=start_date,
        end_date=end_date,
    )
    odds_rows, counters = _prepare_no_vig_odds_rows(_read_csv(odds_path))
    odds_rows = _filter_odds_rows(
        odds_rows,
        start_date=start_date,
        end_date=end_date,
        bookmaker=bookmaker,
    )
    dataset = build_historical_betting_dataset(
        prediction_rows,
        odds_rows,
        line_policy=LINE_POLICY_LATEST_CURRENT,
        require_odds_before_prediction=True,
    )
    counters.update({
        "prediction_rows": len(prediction_rows),
        "no_vig_odds_rows_after_filters": len(odds_rows),
        "dataset_rows": len(dataset.rows),
        "dataset_issues": len(dataset.issues),
    })
    return dataset, counters


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the research backtest CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Run a research-only betting simulation with retro model scores "
            "and Kaggle odds snapshots."
        )
    )
    parser.add_argument("--predictions", default=str(DEFAULT_PREDICTIONS), help="Retro prediction CSV.")
    parser.add_argument("--odds", default=str(DEFAULT_ODDS), help="Kaggle canonical fight odds CSV.")
    parser.add_argument("--start-date", help="First event date to include, in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Last event date to include, in YYYY-MM-DD format.")
    parser.add_argument("--bookmaker", help="Restrict odds to one bookmaker/source.")
    parser.add_argument("--initial-bankroll", type=_decimal_arg, default=Decimal("1000"), help="Starting bankroll.")
    parser.add_argument("--detail-mode", choices=BACKTEST_DETAIL_MODES, default=BACKTEST_DETAIL_DECISIONS)
    parser.add_argument(
        "--allow-multiple-books-per-fight",
        action="store_true",
        help="Allow more than one positive-EV bet per fight across books.",
    )
    parser.add_argument("--config", help="Optional .json or .toml betting config file.")
    parser.add_argument("--report-dir", help="Override report output directory.")
    parser.add_argument("--kelly-fraction", type=float, help="Override fractional Kelly multiplier.")
    parser.add_argument("--min-edge", type=float, help="Override minimum model-vs-no-vig edge.")
    parser.add_argument("--min-ev", type=float, help="Override minimum expected value per unit.")
    parser.add_argument("--max-single-bet-fraction", type=float, help="Override max bankroll fraction for any single bet.")
    parser.add_argument("--max-event-fraction", type=float, help="Override max cumulative bankroll fraction per event.")
    parser.add_argument("--medium-tier-cap", type=float, help="Override max bankroll fraction for medium-confidence bets.")
    parser.add_argument("--high-tier-cap", type=float, help="Override max bankroll fraction for high-confidence bets.")
    parser.add_argument("--toss-up-tier-cap", type=float, help="Override max bankroll fraction for toss-up tier.")
    parser.add_argument("--drawdown-protection-threshold", type=float, help="Enable drawdown protection at this drawdown fraction.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = load_config_file(args.config) if args.config else default_config()
    config = _research_report_config(apply_cli_overrides(config, args))
    max_one_bet_per_fight = not args.allow_multiple_books_per_fight

    dataset, counters = load_kaggle_research_dataset(
        predictions_path=Path(args.predictions),
        odds_path=Path(args.odds),
        start_date=args.start_date,
        end_date=args.end_date,
        bookmaker=args.bookmaker,
    )
    result = generate_backtest_reports(
        dataset,
        starting_bankroll=args.initial_bankroll,
        config=config,
        detail_mode=args.detail_mode,
        max_one_bet_per_fight=max_one_bet_per_fight,
    )

    print("Research-only Kaggle odds simulation.")
    print("Synthetic timing: odds snapshots were shifted to before each event.")
    for key in sorted(counters):
        print(f"{key}: {counters[key]}")
    print_backtest_summary(result)
    return 0


def _prepare_prediction_rows(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    prepared = []
    for row in rows:
        event_date = _date_from_value(row.get("event_date"))
        if event_date is None:
            continue
        synthetic_scored_at = _event_cutoff(event_date) - timedelta(seconds=1)
        output = dict(row)
        output["event_date"] = event_date.isoformat()
        output["scored_at"] = synthetic_scored_at.isoformat()
        output["actual_label"] = output.get("actual_label") or output.get("label")
        output["resolved"] = "true"
        prepared.append(output)
    return prepared


def _prepare_no_vig_odds_rows(
    rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], Counter]:
    counters: Counter = Counter()
    raw_groups: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        if row.get("line_type") != "current":
            counters["skipped_non_current_line"] += 1
            continue
        if not _has_decimal_odds(row.get("decimal_odds")):
            counters["skipped_missing_decimal_odds"] += 1
            continue
        key = (
            row.get("fight_id", ""),
            row.get("bookmaker", ""),
            row.get("market") or "moneyline",
            row.get("line_type", ""),
            row.get("odds_timestamp", ""),
        )
        raw_groups.setdefault(key, []).append(row)

    snapshots_by_market: dict[tuple[str, str, str, str], list[tuple[datetime, list[dict[str, str]]]]] = {}
    for key, group in raw_groups.items():
        result = calculate_no_vig_probabilities(group)
        if not result.valid:
            counters[f"invalid_no_vig_{result.reason or 'unknown'}"] += 1
            continue

        source_by_fighter = {row["fighter_id"]: row for row in group}
        no_vig_group = []
        for no_vig in result.rows:
            source_row = source_by_fighter[no_vig.fighter_id]
            output = dict(source_row)
            output["market"] = no_vig.market
            output["normalized_decimal_odds"] = str(no_vig.decimal_odds)
            output["implied_probability"] = str(no_vig.implied_probability)
            output["no_vig_implied_probability"] = str(no_vig.no_vig_implied_probability)
            output["overround"] = str(no_vig.overround)
            no_vig_group.append(output)

        original_timestamp = _datetime_from_value(key[4])
        if original_timestamp is None:
            counters["invalid_original_timestamp"] += 1
            continue
        market_key = key[:4]
        snapshots_by_market.setdefault(market_key, []).append((original_timestamp, no_vig_group))

    prepared = []
    for snapshots in snapshots_by_market.values():
        snapshots.sort(key=lambda item: item[0])
        snapshot_count = len(snapshots)
        for index, (_, group) in enumerate(snapshots):
            event_date = _date_from_value(group[0].get("event_date"))
            if event_date is None:
                counters["invalid_event_date"] += 1
                continue
            synthetic_timestamp = _event_cutoff(event_date) - timedelta(
                seconds=snapshot_count - index
            )
            for row in group:
                output = dict(row)
                output["event_date"] = event_date.isoformat()
                output["odds_timestamp"] = synthetic_timestamp.isoformat()
                prepared.append(output)

    counters["raw_odds_rows"] = len(rows)
    counters["raw_market_groups"] = len(raw_groups)
    counters["valid_no_vig_odds_rows"] = len(prepared)
    return prepared, counters


def _filter_odds_rows(
    rows: list[dict[str, str]],
    *,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    bookmaker: str | None = None,
) -> list[dict[str, str]]:
    start = _date_from_value(start_date)
    end = _date_from_value(end_date)
    bookmaker_key = bookmaker.casefold() if bookmaker else None
    output = []
    for row in rows:
        event_date = _date_from_value(row.get("event_date"))
        if event_date is None:
            continue
        if start is not None and event_date < start:
            continue
        if end is not None and event_date > end:
            continue
        if bookmaker_key is not None and row.get("bookmaker", "").casefold() != bookmaker_key:
            continue
        output.append(row)
    return output


def _research_report_config(config: BettingConfig) -> BettingConfig:
    return config.with_overrides({
        "backtest_fights_report": "kaggle_research_backtest_fights.csv",
        "backtest_events_report": "kaggle_research_backtest_events.csv",
        "backtest_summary_report": "kaggle_research_backtest_summary.csv",
    })


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _event_cutoff(event_date: date) -> datetime:
    return datetime.combine(event_date, time.min, tzinfo=timezone.utc)


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


def _has_decimal_odds(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    if not text or text == "nan":
        return False
    try:
        return Decimal(text) > Decimal("1")
    except InvalidOperation:
        return False


def _decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
