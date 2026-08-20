"""Run a BestFightOdds Mean market-benchmark backtest from local CSVs.

The BFO ``Mean`` line is an aggregate market benchmark, not a sportsbook quote.
This runner is offline-only: it reads canonical odds and saved pre-event
prediction CSVs, computes two-sided no-vig probabilities, and delegates staking
and settlement to the standard historical backtest engine.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from datetime import date, datetime, timezone
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
DEFAULT_PREDICTIONS = REPO_ROOT / "data" / "reports" / "pre_event_prediction_fights.csv"
DEFAULT_ODDS = REPO_ROOT / "data" / "odds" / "fight_odds.csv"
DEFAULT_FIGHTS = REPO_ROOT / "data" / "fights.csv"
DEFAULT_FIGHTERS = REPO_ROOT / "data" / "fighters.csv"
DEFAULT_REPORT_DIR = REPO_ROOT / "data" / "reports" / "bfo_mean_market_benchmark"
DEFAULT_BOOKMAKER = "BestFightOdds Mean"
DEFAULT_SOURCE = "bestfightodds_line_history_payload"


def load_bfo_mean_market_benchmark_dataset(
    *,
    predictions_path: Path = DEFAULT_PREDICTIONS,
    odds_path: Path = DEFAULT_ODDS,
    fights_path: Path = DEFAULT_FIGHTS,
    fighters_path: Path = DEFAULT_FIGHTERS,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> tuple[object, Counter]:
    """Return a BFO Mean market-benchmark dataset plus construction counters."""
    canonical_odds_rows = _read_csv(odds_path)
    no_vig_odds_rows, counters = _prepare_bfo_no_vig_odds_rows(canonical_odds_rows)
    no_vig_odds_rows = _filter_odds_rows(
        no_vig_odds_rows,
        start_date=start_date,
        end_date=end_date,
    )
    bfo_fight_ids = {
        row["fight_id"]
        for row in no_vig_odds_rows
        if row.get("fight_id")
    }

    fight_rows = _latest_rows_by_id(_read_csv(fights_path), "fight_id")
    fighter_names = _latest_fighter_names(_read_csv(fighters_path))
    raw_prediction_rows = _read_csv(predictions_path)
    prediction_rows, prediction_counters = _prepare_prediction_rows(
        [
            row for row in raw_prediction_rows
            if row.get("fight_id") in bfo_fight_ids
        ],
        fight_rows=fight_rows,
        fighter_names=fighter_names,
    )
    prediction_rows = filter_historical_prediction_rows(
        prediction_rows,
        start_date=start_date,
        end_date=end_date,
    )
    prediction_rows = [
        row for row in prediction_rows
        if row.get("fight_id") in bfo_fight_ids
    ]

    dataset = build_historical_betting_dataset(
        prediction_rows,
        no_vig_odds_rows,
        line_policy=LINE_POLICY_LATEST_CURRENT,
        require_odds_before_prediction=True,
    )
    counters.update(prediction_counters)
    counters.update({
        "saved_prediction_rows_read": len(raw_prediction_rows),
        "prediction_rows_after_filters": len(prediction_rows),
        "bfo_fights_with_no_vig_odds": len(bfo_fight_ids),
        "no_vig_odds_rows_after_filters": len(no_vig_odds_rows),
        "dataset_rows": len(dataset.rows),
        "dataset_issues": len(dataset.issues),
    })
    return dataset, counters


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the BFO Mean market-benchmark CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Run an offline BestFightOdds Mean market-benchmark backtest from "
            "canonical odds and saved pre-event predictions."
        )
    )
    parser.add_argument("--predictions", default=str(DEFAULT_PREDICTIONS), help="Saved pre-event fight prediction CSV.")
    parser.add_argument("--odds", default=str(DEFAULT_ODDS), help="Canonical fight odds CSV.")
    parser.add_argument("--fights", default=str(DEFAULT_FIGHTS), help="Local fights CSV used to restore fighter IDs.")
    parser.add_argument("--fighters", default=str(DEFAULT_FIGHTERS), help="Local fighters CSV used to restore fighter names.")
    parser.add_argument("--start-date", help="First event date to include, in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Last event date to include, in YYYY-MM-DD format.")
    parser.add_argument("--initial-bankroll", type=_decimal_arg, default=Decimal("1000"), help="Starting bankroll.")
    parser.add_argument("--detail-mode", choices=BACKTEST_DETAIL_MODES, default=BACKTEST_DETAIL_DECISIONS)
    parser.add_argument(
        "--max-one-bet-per-fight",
        action="store_true",
        help="Conservatively keep only the highest-EV bet candidate per fight.",
    )
    parser.add_argument("--config", help="Optional .json or .toml betting config file.")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="Report output directory.")
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
    config = _market_benchmark_report_config(apply_cli_overrides(config, args))

    dataset, counters = load_bfo_mean_market_benchmark_dataset(
        predictions_path=Path(args.predictions),
        odds_path=Path(args.odds),
        fights_path=Path(args.fights),
        fighters_path=Path(args.fighters),
        start_date=args.start_date,
        end_date=args.end_date,
    )
    result = generate_backtest_reports(
        dataset,
        starting_bankroll=args.initial_bankroll,
        config=config,
        detail_mode=args.detail_mode,
        max_one_bet_per_fight=args.max_one_bet_per_fight,
    )
    notice_path = _write_market_benchmark_notice(
        Path(config.report_dir),
        counters=counters,
        result=result,
    )

    print("BestFightOdds Mean market-benchmark backtest.")
    print("Label: market-benchmark, not sportsbook-executable P/L.")
    for key in sorted(counters):
        print(f"{key}: {counters[key]}")
    print_backtest_summary(result)
    print(f"Wrote: {notice_path}")
    return 0


def _prepare_prediction_rows(
    rows: list[dict[str, str]],
    *,
    fight_rows: Mapping[str, Mapping[str, str]],
    fighter_names: Mapping[str, str],
) -> tuple[list[dict[str, str]], Counter]:
    counters: Counter = Counter()
    prepared = []
    for row in rows:
        output = dict(row)
        fight = fight_rows.get(row.get("fight_id", ""))
        if fight is None:
            counters["prediction_rows_missing_local_fight"] += 1
        else:
            _add_prediction_fighter_ids(output, fight=fight, fighter_names=fighter_names)

        normalized_actual_label = _normalize_int_label(row.get("actual_label"))
        output["actual_label"] = normalized_actual_label or ""
        if "result_type" not in output or not output.get("result_type"):
            output["result_type"] = "win" if normalized_actual_label in {"0", "1"} else "unresolved"
        prepared.append(output)

    counters["prediction_rows_read"] = len(rows)
    counters["prediction_rows_prepared"] = len(prepared)
    return prepared, counters


def _prepare_bfo_no_vig_odds_rows(
    rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], Counter]:
    counters: Counter = Counter({"canonical_odds_rows_read": len(rows)})
    raw_groups: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        if row.get("bookmaker") != DEFAULT_BOOKMAKER:
            continue
        if row.get("source") != DEFAULT_SOURCE:
            counters["skipped_bfo_wrong_source"] += 1
            continue
        if row.get("market", "moneyline") != "moneyline":
            counters["skipped_bfo_non_moneyline"] += 1
            continue
        if row.get("line_type") != "current":
            counters["skipped_bfo_non_current"] += 1
            continue
        if not _has_decimal_odds(row.get("decimal_odds")):
            counters["skipped_bfo_missing_decimal_odds"] += 1
            continue
        key = (
            row.get("fight_id", ""),
            row.get("bookmaker", ""),
            row.get("market") or "moneyline",
            row.get("line_type", ""),
            row.get("odds_timestamp", ""),
        )
        raw_groups.setdefault(key, []).append(row)

    prepared = []
    for group in raw_groups.values():
        result = calculate_no_vig_probabilities(group)
        if not result.valid:
            counters[f"invalid_no_vig_{result.reason or 'unknown'}"] += 1
            continue
        source_by_fighter = {row["fighter_id"]: row for row in group}
        for no_vig in result.rows:
            source_row = source_by_fighter[no_vig.fighter_id]
            output = dict(source_row)
            output["market"] = no_vig.market
            output["normalized_decimal_odds"] = str(no_vig.decimal_odds)
            output["implied_probability"] = str(no_vig.implied_probability)
            output["no_vig_implied_probability"] = str(no_vig.no_vig_implied_probability)
            output["overround"] = str(no_vig.overround)
            prepared.append(output)

    counters["bfo_mean_rows"] = sum(1 for row in rows if row.get("bookmaker") == DEFAULT_BOOKMAKER)
    counters["bfo_market_groups"] = len(raw_groups)
    counters["valid_no_vig_odds_rows"] = len(prepared)
    return prepared, counters


def _add_prediction_fighter_ids(
    row: dict[str, str],
    *,
    fight: Mapping[str, str],
    fighter_names: Mapping[str, str],
) -> None:
    fight_fighter_ids = [
        fight.get("fighter_1_id", ""),
        fight.get("fighter_2_id", ""),
    ]
    name_to_id = {
        _name_key(fighter_names.get(fighter_id)): fighter_id
        for fighter_id in fight_fighter_ids
        if fighter_id
    }
    row["fighter_1_id"] = name_to_id.get(
        _name_key(row.get("fighter_1_name")),
        fight.get("fighter_1_id", ""),
    )
    row["fighter_2_id"] = name_to_id.get(
        _name_key(row.get("fighter_2_name")),
        fight.get("fighter_2_id", ""),
    )


def _filter_odds_rows(
    rows: list[dict[str, str]],
    *,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> list[dict[str, str]]:
    start = _date_from_value(start_date)
    end = _date_from_value(end_date)
    output = []
    for row in rows:
        event_date = _date_from_value(row.get("event_date"))
        if event_date is None:
            continue
        if start is not None and event_date < start:
            continue
        if end is not None and event_date > end:
            continue
        output.append(row)
    return output


def _market_benchmark_report_config(config: BettingConfig) -> BettingConfig:
    return config.with_overrides({
        "backtest_fights_report": "bfo_mean_market_benchmark_fights.csv",
        "backtest_events_report": "bfo_mean_market_benchmark_events.csv",
        "backtest_summary_report": "bfo_mean_market_benchmark_summary.csv",
    })


def _write_market_benchmark_notice(
    report_dir: Path,
    *,
    counters: Mapping[str, int],
    result,
) -> Path:
    path = report_dir if report_dir.is_absolute() else REPO_ROOT / report_dir
    path.mkdir(parents=True, exist_ok=True)
    notice_path = path / "market_benchmark_notice.md"
    overall = next(
        (
            row for row in result.rows.summary_rows
            if row["summary_type"] == "overall" and row["group"] == "all"
        ),
        {},
    )
    lines = [
        "# BestFightOdds Mean Market Benchmark",
        "",
        "Label: market-benchmark, not sportsbook-executable P/L.",
        "",
        "Source rows are canonical `BestFightOdds Mean` aggregate market lines from `data/odds/fight_odds.csv`.",
        "The run joins only to saved pre-event predictions and selects the latest line observed before each prediction timestamp.",
        "",
        "## Outputs",
        "",
        f"- Fights: `{result.fights_path}`",
        f"- Events: `{result.events_path}`",
        f"- Summary: `{result.summary_path}`",
        "",
        "## Counters",
        "",
    ]
    for key in sorted(counters):
        lines.append(f"- {key}: {counters[key]}")
    lines.extend([
        "",
        "## Overall",
        "",
        f"- Total bets: {overall.get('total_bets', '0')}",
        f"- Total staked: {overall.get('total_staked', '0')}",
        f"- Profit/Loss: {overall.get('profit_loss', '0')}",
        f"- ROI: {overall.get('roi', '')}",
        f"- Ending bankroll: {overall.get('ending_bankroll', '')}",
    ])
    notice_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return notice_path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _latest_rows_by_id(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    output = {}
    for row in sorted(rows, key=lambda item: _datetime_sort_value(item.get("scraped_at"))):
        row_key = row.get(key, "")
        if row_key:
            output[row_key] = row
    return output


def _latest_fighter_names(rows: list[dict[str, str]]) -> dict[str, str]:
    latest = _latest_rows_by_id(rows, "fighter_id")
    return {
        fighter_id: row.get("full_name", "")
        for fighter_id, row in latest.items()
    }


def _normalize_int_label(value: object) -> str | None:
    decimal_value = _decimal_from_value(value)
    if decimal_value is None:
        return None
    if decimal_value == Decimal("0"):
        return "0"
    if decimal_value == Decimal("1"):
        return "1"
    return None


def _has_decimal_odds(value: object) -> bool:
    decimal_value = _decimal_from_value(value)
    return decimal_value is not None and decimal_value > Decimal("1")


def _decimal_from_value(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text or text == "nan":
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


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


def _datetime_sort_value(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    text = text.replace(" UTC", "+00:00").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _name_key(value: object) -> str:
    return " ".join(str(value or "").casefold().split())


def _decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
