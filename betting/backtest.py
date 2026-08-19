"""Leakage-safe historical betting dataset construction and reports."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.config import (
    BettingConfig,
    apply_cli_overrides,
    default_config,
    load_config_file,
)
from betting.reasons import ReasonCode, format_reason_codes
from betting.recommend import BetDecision
from betting.risk import StakedBetDecision, apply_staking_caps
from betting.value import evaluate_value

REPO_ROOT = Path(__file__).resolve().parent.parent

CLI_ODDS_POLICY_LATEST_BEFORE_EVENT = "latest-before-event"
CLI_ODDS_POLICY_LATEST_BEFORE_PREDICTION = "latest-before-prediction"
CLI_ODDS_POLICY_OPENING = "opening"
CLI_ODDS_POLICY_CLOSING = "closing"
CLI_ODDS_POLICIES = (
    CLI_ODDS_POLICY_LATEST_BEFORE_EVENT,
    CLI_ODDS_POLICY_LATEST_BEFORE_PREDICTION,
    CLI_ODDS_POLICY_OPENING,
    CLI_ODDS_POLICY_CLOSING,
)

LINE_POLICY_LATEST_CURRENT = "latest_current"
LINE_POLICY_CLOSING = "closing"
LINE_POLICY_OPENING = "opening"
LINE_POLICIES = (
    LINE_POLICY_LATEST_CURRENT,
    LINE_POLICY_CLOSING,
    LINE_POLICY_OPENING,
)

BACKTEST_DETAIL_DECISIONS = "decisions"
BACKTEST_DETAIL_EVALUATED = "evaluated"
BACKTEST_DETAIL_MODES = (BACKTEST_DETAIL_DECISIONS, BACKTEST_DETAIL_EVALUATED)

EDGE_BUCKETS = (
    ("0-3%", Decimal("0"), Decimal("0.03")),
    ("3-5%", Decimal("0.03"), Decimal("0.05")),
    ("5-10%", Decimal("0.05"), Decimal("0.10")),
    ("10%+", Decimal("0.10"), None),
)


@dataclass(frozen=True)
class HistoricalBettingRow:
    """One historical fighter-side row or fight-level pass row for backtesting."""

    decision: str
    reason_codes: tuple[ReasonCode, ...]
    event_id: str | None
    event_name: str | None
    event_date: date
    fight_id: str
    scored_at: datetime
    prediction_source: str
    fighter_id: str | None = None
    fighter_name: str | None = None
    opponent_fighter_id: str | None = None
    opponent_fighter_name: str | None = None
    fighter_slot: str | None = None
    model_probability: Decimal | None = None
    predicted_prob_f1: Decimal | None = None
    calibrated_prob_f1: Decimal | None = None
    confidence_tier: str | None = None
    is_uncertain: object | None = None
    model_name: str | None = None
    model_artifact: str | None = None
    bookmaker: str | None = None
    market: str | None = None
    line_type: str | None = None
    odds_timestamp: datetime | None = None
    american_odds: object | None = None
    decimal_odds: Decimal | None = None
    normalized_decimal_odds: Decimal | None = None
    market_implied_probability: Decimal | None = None
    no_vig_market_probability: Decimal | None = None
    overround: Decimal | None = None
    actual_winner_fighter_id: str | None = None
    actual_winner_name: str | None = None
    actual_label: int | None = None
    result_type: str | None = None
    resolved: bool = False


@dataclass(frozen=True)
class HistoricalDatasetIssue:
    """Prediction or odds row excluded before producing fighter-side rows."""

    fight_id: str | None
    reason_code: ReasonCode
    detail: str
    event_id: str | None = None
    event_name: str | None = None
    event_date: date | None = None
    scored_at: datetime | None = None


@dataclass(frozen=True)
class HistoricalBettingDataset:
    """Leakage-safe historical betting input rows plus excluded-row issues."""

    rows: tuple[HistoricalBettingRow, ...]
    issues: tuple[HistoricalDatasetIssue, ...]
    line_policy: str
    require_odds_before_prediction: bool


@dataclass(frozen=True)
class BacktestBetResult:
    """One settled historical betting decision."""

    event_id: str | None
    event_name: str | None
    event_date: date
    fight_id: str | None
    fighter_id: str | None
    fighter_name: str | None
    opponent_fighter_id: str | None
    opponent_fighter_name: str | None
    bookmaker: str | None
    market: str | None
    line_type: str | None
    odds_timestamp: datetime | None
    scored_at: datetime | None
    decision: str
    recommended_fighter_id: str | None
    recommended_fighter_name: str | None
    reason_codes: tuple[ReasonCode, ...]
    confidence_tier: str | None
    model_probability: Decimal | None
    market_implied_probability: Decimal | None
    no_vig_market_probability: Decimal | None
    edge: Decimal | None
    ev_per_unit: Decimal | None
    offered_decimal_odds: Decimal | None
    uncapped_kelly_fraction: Decimal
    fractional_kelly_fraction: Decimal
    final_stake_fraction: Decimal
    stake_amount: Decimal
    bet_result: str
    profit_loss_amount: Decimal
    bankroll_before_event: Decimal
    bankroll_after_event: Decimal
    peak_bankroll: Decimal
    drawdown: Decimal
    max_drawdown: Decimal
    actual_winner_fighter_id: str | None
    actual_winner_name: str | None
    result_type: str | None
    resolved: bool


@dataclass(frozen=True)
class BacktestEventResult:
    """One event-level bankroll step after same-card settlement."""

    event_id: str | None
    event_name: str | None
    event_date: date
    bankroll_before_event: Decimal
    total_staked: Decimal
    profit_loss_amount: Decimal
    bankroll_after_event: Decimal
    peak_bankroll: Decimal
    drawdown: Decimal
    max_drawdown: Decimal
    bets: int
    wins: int
    losses: int
    pushes: int
    passes: int


@dataclass(frozen=True)
class BacktestSimulationResult:
    """Chronological betting backtest with bankroll path metrics."""

    bets: tuple[BacktestBetResult, ...]
    events: tuple[BacktestEventResult, ...]
    starting_bankroll: Decimal
    ending_bankroll: Decimal
    peak_bankroll: Decimal
    max_drawdown: Decimal


@dataclass(frozen=True)
class BacktestReportRows:
    """CSV-ready backtest report rows."""

    fight_rows: tuple[dict[str, str], ...]
    event_rows: tuple[dict[str, str], ...]
    summary_rows: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class BacktestReportResult:
    """Generated backtest report paths plus CSV-ready rows."""

    fights_path: Path
    events_path: Path
    summary_path: Path
    rows: BacktestReportRows


def build_historical_betting_dataset(
    prediction_rows: list[Mapping[str, object]],
    odds_rows: list[Mapping[str, object]],
    *,
    line_policy: str = LINE_POLICY_LATEST_CURRENT,
    require_odds_before_prediction: bool = True,
) -> HistoricalBettingDataset:
    """Join pre-event predictions to historical no-vig odds and outcomes.

    ``prediction_rows`` should come from ``pre_event_prediction_fights``. Rows
    scored on or after the event date are excluded to protect against leakage.
    Eligible odds must be observed before the event date and, by default, at or
    before the exact prediction timestamp.
    """
    if line_policy not in LINE_POLICIES:
        raise ValueError(f"line_policy must be one of: {', '.join(LINE_POLICIES)}")

    rows: list[HistoricalBettingRow] = []
    issues: list[HistoricalDatasetIssue] = []
    odds_by_fight = _group_odds_by_fight(odds_rows)

    for prediction in sorted(prediction_rows, key=_prediction_sort_key):
        try:
            context = _prediction_context(prediction)
        except ValueError as exc:
            issues.append(HistoricalDatasetIssue(
                fight_id=_text(prediction.get("fight_id")),
                reason_code=ReasonCode.MISSING_PREDICTION,
                detail=str(exc),
                event_id=_text(prediction.get("event_id")),
                event_name=_text(prediction.get("event_name")),
            ))
            continue

        if context["scored_at"].date() >= context["event_date"]:
            issues.append(HistoricalDatasetIssue(
                fight_id=context["fight_id"],
                reason_code=ReasonCode.MISSING_PREDICTION,
                detail="prediction scored on or after event date",
                event_id=context["event_id"],
                event_name=context["event_name"],
                event_date=context["event_date"],
                scored_at=context["scored_at"],
            ))
            continue

        outcome_reason = _outcome_pass_reason(prediction, context)
        if outcome_reason is not None:
            rows.append(_pass_row(
                context,
                reason_code=outcome_reason,
                detail="fight outcome is not eligible for betting settlement",
            ))
            continue

        eligible_groups, ambiguous_seen = _eligible_market_groups(
            odds_by_fight.get(context["fight_id"], []),
            context,
            line_policy=line_policy,
            require_odds_before_prediction=require_odds_before_prediction,
        )
        selected_groups = _select_market_groups(eligible_groups, line_policy)

        if not selected_groups:
            rows.append(_pass_row(
                context,
                reason_code=(
                    ReasonCode.AMBIGUOUS_ODDS
                    if ambiguous_seen
                    else ReasonCode.MISSING_ODDS
                ),
                detail="no eligible two-sided historical odds group",
            ))
            continue

        for group in selected_groups:
            for odds_row in sorted(group, key=lambda row: _text(row.get("fighter_id")) or ""):
                rows.append(_historical_side_row(context, odds_row))

    return HistoricalBettingDataset(
        rows=tuple(rows),
        issues=tuple(issues),
        line_policy=line_policy,
        require_odds_before_prediction=require_odds_before_prediction,
    )


def simulate_betting_backtest(
    dataset: HistoricalBettingDataset,
    *,
    starting_bankroll: Decimal | int | float | str = Decimal("1000"),
    config: BettingConfig | None = None,
    max_one_bet_per_fight: bool = False,
) -> BacktestSimulationResult:
    """Settle historical betting rows event-by-event in chronological order."""
    config = config or default_config()
    bankroll = _positive_decimal(starting_bankroll, "starting_bankroll")
    peak_bankroll = bankroll
    max_drawdown = Decimal("0")
    bet_results: list[BacktestBetResult] = []
    event_results: list[BacktestEventResult] = []

    for _, event_rows in sorted(_group_rows_by_event(dataset.rows).items()):
        bankroll_before_event = bankroll
        event_drawdown_before = calculate_drawdown(bankroll_before_event, peak_bankroll)
        decisions = build_backtest_bet_decisions(
            event_rows,
            config=config,
            max_one_bet_per_fight=max_one_bet_per_fight,
        )
        staked = apply_staking_caps(
            decisions,
            config=config,
            bankroll=bankroll_before_event,
            current_drawdown=event_drawdown_before,
        ).decisions
        settlements = [_settle_staked_decision(decision) for decision in staked]
        event_profit_loss = sum(
            (settlement["profit_loss_amount"] for settlement in settlements),
            Decimal("0"),
        )
        bankroll_after_event = bankroll_before_event + event_profit_loss
        peak_bankroll = max(peak_bankroll, bankroll_after_event)
        drawdown = calculate_drawdown(bankroll_after_event, peak_bankroll)
        max_drawdown = max(max_drawdown, drawdown)

        bet_results.extend(
            _backtest_bet_result(
                decision,
                settlement,
                bankroll_before_event=bankroll_before_event,
                bankroll_after_event=bankroll_after_event,
                peak_bankroll=peak_bankroll,
                drawdown=drawdown,
                max_drawdown=max_drawdown,
            )
            for decision, settlement in zip(staked, settlements)
        )
        event_results.append(_backtest_event_result(
            event_rows,
            settlements,
            bankroll_before_event=bankroll_before_event,
            bankroll_after_event=bankroll_after_event,
            peak_bankroll=peak_bankroll,
            drawdown=drawdown,
            max_drawdown=max_drawdown,
        ))
        bankroll = bankroll_after_event

    return BacktestSimulationResult(
        bets=tuple(bet_results),
        events=tuple(event_results),
        starting_bankroll=_positive_decimal(starting_bankroll, "starting_bankroll"),
        ending_bankroll=bankroll,
        peak_bankroll=peak_bankroll,
        max_drawdown=max_drawdown,
    )


def generate_backtest_reports(
    dataset: HistoricalBettingDataset,
    *,
    starting_bankroll: Decimal | int | float | str = Decimal("1000"),
    config: BettingConfig | None = None,
    detail_mode: str = BACKTEST_DETAIL_DECISIONS,
    max_one_bet_per_fight: bool = False,
) -> BacktestReportResult:
    """Simulate a historical dataset and write all backtest CSV reports."""
    config = config or default_config()
    simulation = simulate_betting_backtest(
        dataset,
        starting_bankroll=starting_bankroll,
        config=config,
        max_one_bet_per_fight=max_one_bet_per_fight,
    )
    return write_backtest_reports(
        simulation,
        dataset=dataset,
        config=config,
        line_policy=dataset.line_policy,
        require_odds_before_prediction=dataset.require_odds_before_prediction,
        detail_mode=detail_mode,
        max_one_bet_per_fight=max_one_bet_per_fight,
    )


def write_backtest_reports(
    simulation: BacktestSimulationResult,
    *,
    dataset: HistoricalBettingDataset | None = None,
    config: BettingConfig | None = None,
    line_policy: str = LINE_POLICY_LATEST_CURRENT,
    require_odds_before_prediction: bool = True,
    detail_mode: str = BACKTEST_DETAIL_DECISIONS,
    max_one_bet_per_fight: bool = False,
) -> BacktestReportResult:
    """Write fight-, event-, and summary-level backtest reports."""
    config = config or default_config()
    rows = build_backtest_report_rows(
        simulation,
        dataset=dataset,
        config=config,
        line_policy=line_policy,
        require_odds_before_prediction=require_odds_before_prediction,
        detail_mode=detail_mode,
        max_one_bet_per_fight=max_one_bet_per_fight,
    )
    report_dir = _report_dir(config)
    report_dir.mkdir(parents=True, exist_ok=True)
    fights_path = report_dir / config.backtest_fights_report
    events_path = report_dir / config.backtest_events_report
    summary_path = report_dir / config.backtest_summary_report
    _write_csv(fights_path, BACKTEST_FIGHT_COLUMNS, rows.fight_rows)
    _write_csv(events_path, BACKTEST_EVENT_COLUMNS, rows.event_rows)
    _write_csv(summary_path, BACKTEST_SUMMARY_COLUMNS, rows.summary_rows)
    return BacktestReportResult(
        fights_path=fights_path,
        events_path=events_path,
        summary_path=summary_path,
        rows=rows,
    )


def build_backtest_report_rows(
    simulation: BacktestSimulationResult,
    *,
    dataset: HistoricalBettingDataset | None = None,
    config: BettingConfig | None = None,
    line_policy: str = LINE_POLICY_LATEST_CURRENT,
    require_odds_before_prediction: bool = True,
    detail_mode: str = BACKTEST_DETAIL_DECISIONS,
    max_one_bet_per_fight: bool = False,
) -> BacktestReportRows:
    """Return CSV-ready fight, event, and summary report rows."""
    if detail_mode not in BACKTEST_DETAIL_MODES:
        raise ValueError(f"detail_mode must be one of: {', '.join(BACKTEST_DETAIL_MODES)}")
    if detail_mode == BACKTEST_DETAIL_EVALUATED and dataset is None:
        raise ValueError("dataset is required when detail_mode='evaluated'")

    config = config or default_config()
    metadata = _report_metadata(
        config=config,
        line_policy=line_policy,
        require_odds_before_prediction=require_odds_before_prediction,
        max_one_bet_per_fight=max_one_bet_per_fight,
        starting_bankroll=simulation.starting_bankroll,
        ending_bankroll=simulation.ending_bankroll,
    )
    if detail_mode == BACKTEST_DETAIL_EVALUATED:
        fight_rows = tuple(_historical_dataset_report_row(row, metadata) for row in dataset.rows)
    else:
        fight_rows = tuple(_backtest_fight_report_row(row, metadata) for row in simulation.bets)
    event_rows = tuple(_backtest_event_report_row(row, metadata) for row in simulation.events)
    summary_rows = tuple(_backtest_summary_rows(simulation, metadata))
    return BacktestReportRows(
        fight_rows=fight_rows,
        event_rows=event_rows,
        summary_rows=summary_rows,
    )


def build_backtest_bet_decisions(
    rows: list[HistoricalBettingRow] | tuple[HistoricalBettingRow, ...],
    *,
    config: BettingConfig | None = None,
    max_one_bet_per_fight: bool = False,
) -> tuple[BetDecision, ...]:
    """Convert historical dataset rows into pre-staking bet/pass decisions."""
    config = config or default_config()
    decisions: list[BetDecision] = []
    grouped: dict[tuple, list[HistoricalBettingRow]] = {}

    for row in rows:
        if row.decision != "evaluate":
            decisions.append(_pass_decision_from_historical_row(row))
            continue
        grouped.setdefault(_historical_market_key(row), []).append(row)

    for group in grouped.values():
        group_decisions = [
            _decision_from_historical_row(row, config=config)
            for row in group
        ]
        decisions.extend(_suppress_opposing_historical_bets(group_decisions))

    if max_one_bet_per_fight:
        decisions = _suppress_extra_fight_bets(decisions)

    return tuple(decisions)


def calculate_drawdown(bankroll: Decimal | int | float | str, peak_bankroll: Decimal | int | float | str) -> Decimal:
    """Return fractional drawdown from peak bankroll."""
    bankroll_decimal = _decimal_required_value(bankroll, "bankroll")
    peak_decimal = _positive_decimal(peak_bankroll, "peak_bankroll")
    if bankroll_decimal >= peak_decimal:
        return Decimal("0")
    return (peak_decimal - bankroll_decimal) / peak_decimal


def load_historical_betting_dataset(
    conn,
    *,
    line_policy: str = LINE_POLICY_LATEST_CURRENT,
    require_odds_before_prediction: bool = True,
) -> HistoricalBettingDataset:
    """Fetch historical prediction and odds rows from the warehouse."""
    return build_historical_betting_dataset(
        fetch_pre_event_prediction_rows(conn),
        fetch_historical_no_vig_odds_rows(conn),
        line_policy=line_policy,
        require_odds_before_prediction=require_odds_before_prediction,
    )


def generate_backtest_reports_from_connection(
    conn,
    *,
    config: BettingConfig | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    bookmaker: str | None = None,
    odds_policy: str = CLI_ODDS_POLICY_LATEST_BEFORE_PREDICTION,
    line_type: str | None = None,
    initial_bankroll: Decimal | int | float | str = Decimal("1000"),
    detail_mode: str = BACKTEST_DETAIL_DECISIONS,
    max_one_bet_per_fight: bool = False,
) -> BacktestReportResult:
    """Fetch warehouse rows, build a leakage-safe dataset, and write reports."""
    config = config or default_config()
    line_policy, require_odds_before_prediction, selected_line_type = _cli_policy_settings(
        odds_policy,
        line_type=line_type,
    )
    prediction_rows = filter_historical_prediction_rows(
        fetch_pre_event_prediction_rows(conn),
        start_date=start_date,
        end_date=end_date,
    )
    odds_rows = filter_historical_odds_rows(
        fetch_historical_no_vig_odds_rows(conn),
        bookmaker=bookmaker,
        line_type=selected_line_type,
    )
    dataset = build_historical_betting_dataset(
        prediction_rows,
        odds_rows,
        line_policy=line_policy,
        require_odds_before_prediction=require_odds_before_prediction,
    )
    return generate_backtest_reports(
        dataset,
        starting_bankroll=initial_bankroll,
        config=config,
        detail_mode=detail_mode,
        max_one_bet_per_fight=max_one_bet_per_fight,
    )


def filter_historical_prediction_rows(
    prediction_rows: list[Mapping[str, object]],
    *,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> list[Mapping[str, object]]:
    """Filter historical prediction rows by event date bounds."""
    start = _date_optional(start_date)
    end = _date_optional(end_date)
    rows = []
    for row in prediction_rows:
        event_date = _date_optional(row.get("event_date"))
        if event_date is None:
            if start is None and end is None:
                rows.append(row)
            continue
        if start is not None and event_date < start:
            continue
        if end is not None and event_date > end:
            continue
        rows.append(row)
    return rows


def filter_historical_odds_rows(
    odds_rows: list[Mapping[str, object]],
    *,
    bookmaker: str | None = None,
    line_type: str | None = None,
) -> list[Mapping[str, object]]:
    """Filter historical no-vig odds rows by bookmaker and line type."""
    bookmaker_key = bookmaker.casefold() if bookmaker else None
    return [
        row for row in odds_rows
        if (bookmaker_key is None or (_text(row.get("bookmaker")) or "").casefold() == bookmaker_key)
        and (line_type is None or _text(row.get("line_type")) == line_type)
    ]


def fetch_pre_event_prediction_rows(conn) -> list[dict[str, object]]:
    """Fetch the leakage-safe pre-event prediction view for backtests."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pre_event_prediction_fights")
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_historical_no_vig_odds_rows(conn) -> list[dict[str, object]]:
    """Fetch no-vig moneyline odds rows for historical backtest joining."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM fight_odds_no_vig")
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the historical betting backtest CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate leakage-safe historical betting backtest reports from "
            "pre-event predictions and timestamped no-vig odds."
        )
    )
    parser.add_argument("--start-date", help="First event date to include, in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Last event date to include, in YYYY-MM-DD format.")
    parser.add_argument("--bookmaker", help="Restrict odds to one bookmaker/source.")
    parser.add_argument(
        "--line-type",
        choices=("current", "opening", "closing"),
        help="Restrict odds line type. Defaults to the selected odds policy.",
    )
    parser.add_argument(
        "--odds-policy",
        choices=CLI_ODDS_POLICIES,
        default=CLI_ODDS_POLICY_LATEST_BEFORE_PREDICTION,
        help=(
            "Historical odds selection policy. The default requires current lines "
            "to be observed before the prediction timestamp."
        ),
    )
    parser.add_argument("--initial-bankroll", type=_decimal_arg, default=Decimal("1000"), help="Starting bankroll for event-by-event simulation.")
    parser.add_argument("--detail-mode", choices=BACKTEST_DETAIL_MODES, default=BACKTEST_DETAIL_DECISIONS, help="Fight report detail level.")
    parser.add_argument(
        "--max-one-bet-per-fight",
        action="store_true",
        help="Conservatively keep only the highest-EV bet candidate per fight.",
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
    parser.add_argument("--drawdown-protection-threshold", type=float, help="Enable drawdown protection at this bankroll drawdown fraction.")
    return parser


def main(argv: list[str] | None = None, *, conn=None) -> int:
    """CLI entry point for historical betting backtest reports."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = load_config_file(args.config) if args.config else default_config()
    config = apply_cli_overrides(config, args)

    close_conn = False
    if conn is None:
        from warehouse.db import get_connection

        conn = get_connection()
        close_conn = True

    try:
        try:
            result = generate_backtest_reports_from_connection(
                conn,
                config=config,
                start_date=args.start_date,
                end_date=args.end_date,
                bookmaker=args.bookmaker,
                odds_policy=args.odds_policy,
                line_type=args.line_type,
                initial_bankroll=args.initial_bankroll,
                detail_mode=args.detail_mode,
                max_one_bet_per_fight=args.max_one_bet_per_fight,
            )
        except ValueError as exc:
            parser.error(str(exc))
    finally:
        if close_conn:
            conn.close()

    print_backtest_summary(result)
    return 0


def print_backtest_summary(result: BacktestReportResult) -> None:
    """Print concise backtest metrics to stdout."""
    overall = next(
        (
            row for row in result.rows.summary_rows
            if row["summary_type"] == "overall" and row["group"] == "all"
        ),
        None,
    )
    if overall is None:
        print("Total bets: 0")
        print("Total staked: 0")
        print("Profit/Loss: 0")
        print("ROI: ")
        print("Hit rate: ")
        print("Max drawdown: 0")
    else:
        print(f"Total bets: {overall['total_bets']}")
        print(f"Total staked: {overall['total_staked']}")
        print(f"Profit/Loss: {overall['profit_loss']}")
        print(f"ROI: {overall['roi']}")
        print(f"Hit rate: {overall['hit_rate']}")
        print(f"Max drawdown: {overall['max_drawdown']}")
        print(f"Ending bankroll: {overall['ending_bankroll']}")
    print(f"Wrote: {result.fights_path}")
    print(f"Wrote: {result.events_path}")
    print(f"Wrote: {result.summary_path}")


BACKTEST_FIGHT_COLUMNS = [
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
    "scored_at",
    "model_probability",
    "market_implied_probability",
    "no_vig_market_probability",
    "edge",
    "edge_bucket",
    "ev_per_unit",
    "offered_decimal_odds",
    "decision",
    "recommended_fighter_id",
    "recommended_fighter_name",
    "confidence_tier",
    "reason_codes",
    "full_kelly_fraction",
    "fractional_kelly_fraction",
    "final_stake_fraction",
    "stake_amount",
    "bet_result",
    "profit_loss_amount",
    "bankroll_before_event",
    "bankroll_after_event",
    "peak_bankroll",
    "drawdown",
    "max_drawdown",
    "actual_winner_fighter_id",
    "actual_winner_name",
    "result_type",
    "resolved",
    "detail_mode",
    "odds_policy",
    "require_odds_before_prediction",
    "max_one_bet_per_fight",
    "starting_bankroll",
    "ending_bankroll",
    "kelly_fraction",
    "min_edge",
    "min_ev",
    "max_single_bet_fraction",
    "max_event_fraction",
    "medium_tier_cap",
    "high_tier_cap",
    "toss_up_tier_cap",
    "drawdown_protection_threshold",
]


BACKTEST_EVENT_COLUMNS = [
    "event_id",
    "event_name",
    "event_date",
    "bets",
    "wins",
    "losses",
    "pushes",
    "passes",
    "staked",
    "profit_loss",
    "roi",
    "ending_bankroll",
    "drawdown_after_event",
    "bankroll_before_event",
    "peak_bankroll",
    "max_drawdown",
    "odds_policy",
    "require_odds_before_prediction",
    "max_one_bet_per_fight",
    "starting_bankroll",
    "kelly_fraction",
    "min_edge",
    "min_ev",
    "max_single_bet_fraction",
    "max_event_fraction",
]


BACKTEST_SUMMARY_COLUMNS = [
    "summary_type",
    "group",
    "total_bets",
    "wins",
    "losses",
    "pushes",
    "total_staked",
    "profit_loss",
    "roi",
    "hit_rate",
    "average_odds",
    "max_drawdown",
    "starting_bankroll",
    "ending_bankroll",
    "odds_policy",
    "require_odds_before_prediction",
    "max_one_bet_per_fight",
    "kelly_fraction",
    "min_edge",
    "min_ev",
    "max_single_bet_fraction",
    "max_event_fraction",
    "medium_tier_cap",
    "high_tier_cap",
    "toss_up_tier_cap",
    "drawdown_protection_threshold",
]


def _backtest_fight_report_row(
    row: BacktestBetResult,
    metadata: Mapping[str, str],
) -> dict[str, str]:
    return {
        "event_id": _format_value(row.event_id),
        "event_name": _format_value(row.event_name),
        "event_date": _format_value(row.event_date),
        "fight_id": _format_value(row.fight_id),
        "fighter_id": _format_value(row.fighter_id),
        "fighter_name": _format_value(row.fighter_name),
        "opponent_fighter_id": _format_value(row.opponent_fighter_id),
        "opponent_fighter_name": _format_value(row.opponent_fighter_name),
        "bookmaker": _format_value(row.bookmaker),
        "market": _format_value(row.market),
        "line_type": _format_value(row.line_type),
        "odds_timestamp": _format_value(row.odds_timestamp),
        "scored_at": _format_value(row.scored_at),
        "model_probability": _format_decimal(row.model_probability),
        "market_implied_probability": _format_decimal(row.market_implied_probability),
        "no_vig_market_probability": _format_decimal(row.no_vig_market_probability),
        "edge": _format_decimal(row.edge),
        "edge_bucket": edge_bucket(row.edge),
        "ev_per_unit": _format_decimal(row.ev_per_unit),
        "offered_decimal_odds": _format_decimal(row.offered_decimal_odds),
        "decision": row.decision,
        "recommended_fighter_id": _format_value(row.recommended_fighter_id),
        "recommended_fighter_name": _format_value(row.recommended_fighter_name),
        "confidence_tier": _format_value(row.confidence_tier),
        "reason_codes": format_reason_codes(row.reason_codes),
        "full_kelly_fraction": _format_decimal(row.uncapped_kelly_fraction),
        "fractional_kelly_fraction": _format_decimal(row.fractional_kelly_fraction),
        "final_stake_fraction": _format_decimal(row.final_stake_fraction),
        "stake_amount": _format_decimal(row.stake_amount),
        "bet_result": row.bet_result,
        "profit_loss_amount": _format_decimal(row.profit_loss_amount),
        "bankroll_before_event": _format_decimal(row.bankroll_before_event),
        "bankroll_after_event": _format_decimal(row.bankroll_after_event),
        "peak_bankroll": _format_decimal(row.peak_bankroll),
        "drawdown": _format_decimal(row.drawdown),
        "max_drawdown": _format_decimal(row.max_drawdown),
        "actual_winner_fighter_id": _format_value(row.actual_winner_fighter_id),
        "actual_winner_name": _format_value(row.actual_winner_name),
        "result_type": _format_value(row.result_type),
        "resolved": str(row.resolved).lower(),
        "detail_mode": BACKTEST_DETAIL_DECISIONS,
        **metadata,
    }


def _historical_dataset_report_row(
    row: HistoricalBettingRow,
    metadata: Mapping[str, str],
) -> dict[str, str]:
    return {
        "event_id": _format_value(row.event_id),
        "event_name": _format_value(row.event_name),
        "event_date": _format_value(row.event_date),
        "fight_id": _format_value(row.fight_id),
        "fighter_id": _format_value(row.fighter_id),
        "fighter_name": _format_value(row.fighter_name),
        "opponent_fighter_id": _format_value(row.opponent_fighter_id),
        "opponent_fighter_name": _format_value(row.opponent_fighter_name),
        "bookmaker": _format_value(row.bookmaker),
        "market": _format_value(row.market),
        "line_type": _format_value(row.line_type),
        "odds_timestamp": _format_value(row.odds_timestamp),
        "scored_at": _format_value(row.scored_at),
        "model_probability": _format_decimal(row.model_probability),
        "market_implied_probability": _format_decimal(row.market_implied_probability),
        "no_vig_market_probability": _format_decimal(row.no_vig_market_probability),
        "edge": "",
        "edge_bucket": "",
        "ev_per_unit": "",
        "offered_decimal_odds": _format_decimal(row.normalized_decimal_odds),
        "decision": row.decision,
        "recommended_fighter_id": "",
        "recommended_fighter_name": "",
        "confidence_tier": _format_value(row.confidence_tier),
        "reason_codes": format_reason_codes(row.reason_codes),
        "full_kelly_fraction": "",
        "fractional_kelly_fraction": "",
        "final_stake_fraction": "",
        "stake_amount": "",
        "bet_result": "",
        "profit_loss_amount": "",
        "bankroll_before_event": "",
        "bankroll_after_event": "",
        "peak_bankroll": "",
        "drawdown": "",
        "max_drawdown": "",
        "actual_winner_fighter_id": _format_value(row.actual_winner_fighter_id),
        "actual_winner_name": _format_value(row.actual_winner_name),
        "result_type": _format_value(row.result_type),
        "resolved": str(row.resolved).lower(),
        "detail_mode": BACKTEST_DETAIL_EVALUATED,
        **metadata,
    }


def _backtest_event_report_row(
    row: BacktestEventResult,
    metadata: Mapping[str, str],
) -> dict[str, str]:
    return {
        "event_id": _format_value(row.event_id),
        "event_name": _format_value(row.event_name),
        "event_date": _format_value(row.event_date),
        "bets": str(row.bets),
        "wins": str(row.wins),
        "losses": str(row.losses),
        "pushes": str(row.pushes),
        "passes": str(row.passes),
        "staked": _format_decimal(row.total_staked),
        "profit_loss": _format_decimal(row.profit_loss_amount),
        "roi": _format_decimal(_roi(row.profit_loss_amount, row.total_staked)),
        "ending_bankroll": _format_decimal(row.bankroll_after_event),
        "drawdown_after_event": _format_decimal(row.drawdown),
        "bankroll_before_event": _format_decimal(row.bankroll_before_event),
        "peak_bankroll": _format_decimal(row.peak_bankroll),
        "max_drawdown": _format_decimal(row.max_drawdown),
        "odds_policy": metadata["odds_policy"],
        "require_odds_before_prediction": metadata["require_odds_before_prediction"],
        "max_one_bet_per_fight": metadata["max_one_bet_per_fight"],
        "starting_bankroll": metadata["starting_bankroll"],
        "kelly_fraction": metadata["kelly_fraction"],
        "min_edge": metadata["min_edge"],
        "min_ev": metadata["min_ev"],
        "max_single_bet_fraction": metadata["max_single_bet_fraction"],
        "max_event_fraction": metadata["max_event_fraction"],
    }


def _backtest_summary_rows(
    simulation: BacktestSimulationResult,
    metadata: Mapping[str, str],
) -> list[dict[str, str]]:
    rows = [_summary_row("overall", "all", simulation.bets, simulation.max_drawdown, metadata)]

    for tier in sorted({row.confidence_tier or "unknown" for row in _bet_rows(simulation.bets)}):
        tier_rows = tuple(row for row in simulation.bets if (row.confidence_tier or "unknown") == tier)
        rows.append(_summary_row("confidence_tier", tier, tier_rows, simulation.max_drawdown, metadata))

    for label, _, _ in EDGE_BUCKETS:
        bucket_rows = tuple(row for row in simulation.bets if edge_bucket(row.edge) == label)
        rows.append(_summary_row("edge_bucket", label, bucket_rows, simulation.max_drawdown, metadata))

    for event in simulation.events:
        event_rows = tuple(row for row in simulation.bets if _bet_event_key(row) == _event_result_key(event))
        group = event.event_name or event.event_id or "unknown_event"
        rows.append(_summary_row("event", group, event_rows, event.max_drawdown, metadata))

    return rows


def _summary_row(
    summary_type: str,
    group: str,
    rows: tuple[BacktestBetResult, ...],
    max_drawdown: Decimal,
    metadata: Mapping[str, str],
) -> dict[str, str]:
    bet_rows = _bet_rows(rows)
    wins = sum(1 for row in bet_rows if row.bet_result == "win")
    losses = sum(1 for row in bet_rows if row.bet_result == "loss")
    pushes = sum(1 for row in bet_rows if row.bet_result == "push")
    total_staked = sum((row.stake_amount for row in bet_rows), Decimal("0"))
    profit_loss = sum((row.profit_loss_amount for row in bet_rows), Decimal("0"))
    odds_rows = [row for row in bet_rows if row.offered_decimal_odds is not None]
    average_odds = (
        sum((row.offered_decimal_odds for row in odds_rows), Decimal("0")) / Decimal(len(odds_rows))
        if odds_rows
        else None
    )
    return {
        "summary_type": summary_type,
        "group": group,
        "total_bets": str(len(bet_rows)),
        "wins": str(wins),
        "losses": str(losses),
        "pushes": str(pushes),
        "total_staked": _format_decimal(total_staked),
        "profit_loss": _format_decimal(profit_loss),
        "roi": _format_decimal(_roi(profit_loss, total_staked)),
        "hit_rate": _format_decimal(_hit_rate(wins, losses)),
        "average_odds": _format_decimal(average_odds),
        "max_drawdown": _format_decimal(max_drawdown),
        **metadata,
    }


def edge_bucket(edge: Decimal | None) -> str:
    """Return the default edge bucket label for a model edge."""
    if edge is None:
        return ""
    if edge < Decimal("0"):
        return "<0%"
    for label, lower, upper in EDGE_BUCKETS:
        if edge >= lower and (upper is None or edge < upper):
            return label
    return ""


def _bet_rows(rows: tuple[BacktestBetResult, ...]) -> tuple[BacktestBetResult, ...]:
    return tuple(row for row in rows if row.decision == "bet")


def _roi(profit_loss: Decimal, total_staked: Decimal) -> Decimal | None:
    if total_staked <= Decimal("0"):
        return None
    return profit_loss / total_staked


def _hit_rate(wins: int, losses: int) -> Decimal | None:
    settled = wins + losses
    if settled == 0:
        return None
    return Decimal(wins) / Decimal(settled)


def _bet_event_key(row: BacktestBetResult) -> tuple:
    return (row.event_date, row.event_name or "", row.event_id or "")


def _event_result_key(row: BacktestEventResult) -> tuple:
    return (row.event_date, row.event_name or "", row.event_id or "")


def _report_metadata(
    *,
    config: BettingConfig,
    line_policy: str,
    require_odds_before_prediction: bool,
    max_one_bet_per_fight: bool,
    starting_bankroll: Decimal,
    ending_bankroll: Decimal,
) -> dict[str, str]:
    risk = config.risk
    return {
        "odds_policy": line_policy,
        "require_odds_before_prediction": str(require_odds_before_prediction).lower(),
        "max_one_bet_per_fight": str(max_one_bet_per_fight).lower(),
        "starting_bankroll": _format_decimal(starting_bankroll),
        "ending_bankroll": _format_decimal(ending_bankroll),
        "kelly_fraction": str(risk.kelly_fraction),
        "min_edge": str(risk.min_edge),
        "min_ev": str(risk.min_ev),
        "max_single_bet_fraction": str(risk.max_single_bet_fraction),
        "max_event_fraction": str(risk.max_event_fraction),
        "medium_tier_cap": str(risk.medium_tier_cap),
        "high_tier_cap": str(risk.high_tier_cap),
        "toss_up_tier_cap": str(risk.toss_up_tier_cap),
        "drawdown_protection_threshold": (
            "" if risk.drawdown_protection_threshold is None else str(risk.drawdown_protection_threshold)
        ),
    }


def _write_csv(path: Path, columns: list[str], rows: tuple[dict[str, str], ...]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _report_dir(config: BettingConfig) -> Path:
    path = Path(config.report_dir)
    return path if path.is_absolute() else REPO_ROOT / path


def _format_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def _format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _group_rows_by_event(
    rows: tuple[HistoricalBettingRow, ...],
) -> dict[tuple, list[HistoricalBettingRow]]:
    grouped: dict[tuple, list[HistoricalBettingRow]] = {}
    for row in rows:
        grouped.setdefault(_historical_event_key(row), []).append(row)
    return grouped


def _historical_event_key(row: HistoricalBettingRow) -> tuple:
    return (
        row.event_date,
        row.event_name or "",
        row.event_id or "",
    )


def _historical_market_key(row: HistoricalBettingRow) -> tuple:
    return (
        row.fight_id,
        row.bookmaker,
        row.market,
        row.line_type,
        row.odds_timestamp,
    )


def _decision_from_historical_row(
    row: HistoricalBettingRow,
    *,
    config: BettingConfig,
) -> BetDecision:
    value = evaluate_value(row, config=config)
    decision = value.decision
    reason_codes = value.reason_codes
    recommended_fighter_id = row.fighter_id
    recommended_fighter_name = row.fighter_name

    if row.confidence_tier == "toss-up":
        decision = "pass"
        reason_codes = (ReasonCode.TOSS_UP_TIER,)
        recommended_fighter_id = None
        recommended_fighter_name = None
    elif value.decision != "bet":
        recommended_fighter_id = None
        recommended_fighter_name = None

    return BetDecision(
        decision=decision,
        event_id=row.event_id,
        fight_id=row.fight_id,
        recommended_fighter_id=recommended_fighter_id,
        recommended_fighter_name=recommended_fighter_name,
        reason_codes=reason_codes,
        event_name=row.event_name,
        event_date=row.event_date,
        bookmaker=row.bookmaker,
        market=row.market,
        line_type=row.line_type,
        odds_timestamp=row.odds_timestamp,
        scored_at=row.scored_at,
        evaluated_fighter_id=row.fighter_id,
        evaluated_fighter_name=row.fighter_name,
        opponent_fighter_id=row.opponent_fighter_id,
        opponent_fighter_name=row.opponent_fighter_name,
        confidence_tier=row.confidence_tier,
        model_probability=value.model_probability,
        market_implied_probability=value.market_implied_probability,
        no_vig_market_probability=value.no_vig_market_probability,
        offered_decimal_odds=value.offered_decimal_odds,
        edge=value.edge,
        ev_per_unit=value.ev_per_unit,
        actual_winner_fighter_id=row.actual_winner_fighter_id,
        actual_winner_name=row.actual_winner_name,
        result_type=row.result_type,
        resolved=row.resolved,
    )


def _pass_decision_from_historical_row(row: HistoricalBettingRow) -> BetDecision:
    return BetDecision(
        decision="pass",
        event_id=row.event_id,
        fight_id=row.fight_id,
        recommended_fighter_id=None,
        recommended_fighter_name=None,
        reason_codes=row.reason_codes,
        event_name=row.event_name,
        event_date=row.event_date,
        bookmaker=row.bookmaker,
        market=row.market,
        line_type=row.line_type,
        odds_timestamp=row.odds_timestamp,
        scored_at=row.scored_at,
        evaluated_fighter_id=row.fighter_id,
        evaluated_fighter_name=row.fighter_name,
        opponent_fighter_id=row.opponent_fighter_id,
        opponent_fighter_name=row.opponent_fighter_name,
        confidence_tier=row.confidence_tier,
        model_probability=row.model_probability,
        market_implied_probability=row.market_implied_probability,
        no_vig_market_probability=row.no_vig_market_probability,
        offered_decimal_odds=row.normalized_decimal_odds,
        actual_winner_fighter_id=row.actual_winner_fighter_id,
        actual_winner_name=row.actual_winner_name,
        result_type=row.result_type,
        resolved=row.resolved,
    )


def _suppress_opposing_historical_bets(decisions: list[BetDecision]) -> list[BetDecision]:
    bets = [decision for decision in decisions if decision.decision == "bet"]
    if len(bets) <= 1:
        return decisions

    winner = max(
        bets,
        key=lambda decision: (
            decision.ev_per_unit or Decimal("-Infinity"),
            decision.edge or Decimal("-Infinity"),
            decision.recommended_fighter_id or "",
        ),
    )
    output: list[BetDecision] = []
    for decision in decisions:
        if decision.decision != "bet" or decision is winner:
            output.append(decision)
            continue
        output.append(_capped_pass_decision(decision, (ReasonCode.AMBIGUOUS_ODDS,)))
    return output


def _suppress_extra_fight_bets(decisions: list[BetDecision]) -> list[BetDecision]:
    bets_by_fight: dict[str, list[BetDecision]] = {}
    for decision in decisions:
        if decision.decision == "bet" and decision.fight_id is not None:
            bets_by_fight.setdefault(decision.fight_id, []).append(decision)

    winners = {
        fight_id: max(bets, key=_best_fight_bet_key)
        for fight_id, bets in bets_by_fight.items()
        if len(bets) > 1
    }
    if not winners:
        return decisions

    output: list[BetDecision] = []
    for decision in decisions:
        winner = winners.get(decision.fight_id or "")
        if decision.decision != "bet" or winner is None or decision is winner:
            output.append(decision)
            continue
        output.append(_capped_pass_decision(decision, (ReasonCode.FIGHT_BET_CAP_APPLIED,)))
    return output


def _best_fight_bet_key(decision: BetDecision) -> tuple:
    return (
        decision.ev_per_unit or Decimal("-Infinity"),
        decision.edge or Decimal("-Infinity"),
        decision.offered_decimal_odds or Decimal("-Infinity"),
        decision.odds_timestamp or datetime.min.replace(tzinfo=timezone.utc),
        decision.bookmaker or "",
        decision.recommended_fighter_id or "",
    )


def _capped_pass_decision(
    decision: BetDecision,
    reason_codes: tuple[ReasonCode, ...],
) -> BetDecision:
    return BetDecision(
        decision="pass",
        event_id=decision.event_id,
        fight_id=decision.fight_id,
        recommended_fighter_id=None,
        recommended_fighter_name=None,
        reason_codes=reason_codes,
        event_name=decision.event_name,
        event_date=decision.event_date,
        bookmaker=decision.bookmaker,
        market=decision.market,
        line_type=decision.line_type,
        odds_timestamp=decision.odds_timestamp,
        scored_at=decision.scored_at,
        evaluated_fighter_id=decision.evaluated_fighter_id,
        evaluated_fighter_name=decision.evaluated_fighter_name,
        opponent_fighter_id=decision.opponent_fighter_id,
        opponent_fighter_name=decision.opponent_fighter_name,
        confidence_tier=decision.confidence_tier,
        model_probability=decision.model_probability,
        market_implied_probability=decision.market_implied_probability,
        no_vig_market_probability=decision.no_vig_market_probability,
        offered_decimal_odds=decision.offered_decimal_odds,
        edge=decision.edge,
        ev_per_unit=decision.ev_per_unit,
        actual_winner_fighter_id=decision.actual_winner_fighter_id,
        actual_winner_name=decision.actual_winner_name,
        result_type=decision.result_type,
        resolved=decision.resolved,
    )


def _settle_staked_decision(decision: StakedBetDecision) -> dict[str, Decimal | str]:
    stake_amount = decision.stake_amount or Decimal("0")
    if decision.decision != "bet":
        return {
            "bet_result": "no_bet",
            "stake_amount": Decimal("0"),
            "profit_loss_amount": Decimal("0"),
        }
    if (
        decision.result_type != "win"
        or not decision.resolved
        or decision.actual_winner_fighter_id is None
        or decision.offered_decimal_odds is None
    ):
        return {
            "bet_result": "push",
            "stake_amount": stake_amount,
            "profit_loss_amount": Decimal("0"),
        }
    if decision.recommended_fighter_id == decision.actual_winner_fighter_id:
        return {
            "bet_result": "win",
            "stake_amount": stake_amount,
            "profit_loss_amount": stake_amount * (decision.offered_decimal_odds - Decimal("1")),
        }
    return {
        "bet_result": "loss",
        "stake_amount": stake_amount,
        "profit_loss_amount": -stake_amount,
    }


def _backtest_bet_result(
    decision: StakedBetDecision,
    settlement: Mapping[str, Decimal | str],
    *,
    bankroll_before_event: Decimal,
    bankroll_after_event: Decimal,
    peak_bankroll: Decimal,
    drawdown: Decimal,
    max_drawdown: Decimal,
) -> BacktestBetResult:
    return BacktestBetResult(
        event_id=decision.event_id,
        event_name=decision.event_name,
        event_date=_date_required(decision.event_date, "event_date"),
        fight_id=decision.fight_id,
        fighter_id=decision.evaluated_fighter_id,
        fighter_name=decision.evaluated_fighter_name,
        opponent_fighter_id=decision.opponent_fighter_id,
        opponent_fighter_name=decision.opponent_fighter_name,
        bookmaker=decision.bookmaker,
        market=decision.market,
        line_type=decision.line_type,
        odds_timestamp=decision.odds_timestamp,
        scored_at=decision.scored_at,
        decision=decision.decision,
        recommended_fighter_id=decision.recommended_fighter_id,
        recommended_fighter_name=decision.recommended_fighter_name,
        reason_codes=decision.reason_codes,
        confidence_tier=decision.confidence_tier,
        model_probability=decision.model_probability,
        market_implied_probability=decision.market_implied_probability,
        no_vig_market_probability=decision.no_vig_market_probability,
        edge=decision.edge,
        ev_per_unit=decision.ev_per_unit,
        offered_decimal_odds=decision.offered_decimal_odds,
        uncapped_kelly_fraction=decision.uncapped_kelly_fraction,
        fractional_kelly_fraction=decision.fractional_kelly_fraction,
        final_stake_fraction=decision.final_stake_fraction,
        stake_amount=decision.stake_amount or Decimal("0"),
        bet_result=str(settlement["bet_result"]),
        profit_loss_amount=settlement["profit_loss_amount"],
        bankroll_before_event=bankroll_before_event,
        bankroll_after_event=bankroll_after_event,
        peak_bankroll=peak_bankroll,
        drawdown=drawdown,
        max_drawdown=max_drawdown,
        actual_winner_fighter_id=decision.actual_winner_fighter_id,
        actual_winner_name=decision.actual_winner_name,
        result_type=decision.result_type,
        resolved=decision.resolved,
    )


def _backtest_event_result(
    event_rows: list[HistoricalBettingRow],
    settlements: list[dict[str, Decimal | str]],
    *,
    bankroll_before_event: Decimal,
    bankroll_after_event: Decimal,
    peak_bankroll: Decimal,
    drawdown: Decimal,
    max_drawdown: Decimal,
) -> BacktestEventResult:
    first = sorted(event_rows, key=lambda row: (row.event_name or "", row.event_id or "", row.fight_id))[0]
    return BacktestEventResult(
        event_id=first.event_id,
        event_name=first.event_name,
        event_date=first.event_date,
        bankroll_before_event=bankroll_before_event,
        total_staked=sum(
            (settlement["stake_amount"] for settlement in settlements),
            Decimal("0"),
        ),
        profit_loss_amount=sum(
            (settlement["profit_loss_amount"] for settlement in settlements),
            Decimal("0"),
        ),
        bankroll_after_event=bankroll_after_event,
        peak_bankroll=peak_bankroll,
        drawdown=drawdown,
        max_drawdown=max_drawdown,
        bets=sum(1 for settlement in settlements if settlement["bet_result"] in {"win", "loss", "push"}),
        wins=sum(1 for settlement in settlements if settlement["bet_result"] == "win"),
        losses=sum(1 for settlement in settlements if settlement["bet_result"] == "loss"),
        pushes=sum(1 for settlement in settlements if settlement["bet_result"] == "push"),
        passes=sum(1 for settlement in settlements if settlement["bet_result"] == "no_bet"),
    )


def _prediction_context(row: Mapping[str, object]) -> dict[str, object]:
    event_date = _date_required(row.get("event_date"), "event_date")
    scored_at = _datetime_required(row.get("scored_at"), "scored_at")
    fighter_1_id = _required_text(row, "fighter_1_id")
    fighter_2_id = _required_text(row, "fighter_2_id")
    calibrated_prob_f1 = _decimal_required(row, "calibrated_prob_f1")
    if not Decimal("0") <= calibrated_prob_f1 <= Decimal("1"):
        raise ValueError("calibrated_prob_f1 must be between 0 and 1")

    return {
        "event_id": _text(row.get("event_id")),
        "event_name": _text(row.get("event_name")),
        "event_date": event_date,
        "fight_id": _required_text(row, "fight_id"),
        "fighter_1_id": fighter_1_id,
        "fighter_2_id": fighter_2_id,
        "fighter_1_name": _text(row.get("fighter_1_name")),
        "fighter_2_name": _text(row.get("fighter_2_name")),
        "fighter_ids": {fighter_1_id, fighter_2_id},
        "predicted_prob_f1": _decimal_optional(row.get("predicted_prob_f1")),
        "calibrated_prob_f1": calibrated_prob_f1,
        "confidence_tier": _text(row.get("confidence_tier")),
        "is_uncertain": row.get("is_uncertain"),
        "model_name": _text(row.get("model_name")),
        "model_artifact": _text(row.get("model_artifact")),
        "scored_at": scored_at,
        "actual_label": _int_optional(row.get("actual_label")),
        "actual_winner_name": _text(row.get("actual_winner_name")),
        "result_type": _text(row.get("result_type")),
        "resolved": _bool(row.get("resolved")),
    }


def _outcome_pass_reason(
    prediction: Mapping[str, object],
    context: Mapping[str, object],
) -> ReasonCode | None:
    result_type = context["result_type"]
    actual_label = context["actual_label"]
    actual_winner_name = context["actual_winner_name"] or ""

    if actual_winner_name.startswith("Fighter changed:"):
        return ReasonCode.FIGHTER_REPLACEMENT
    if result_type in {"draw", "no_contest"}:
        return ReasonCode.NON_WIN_OUTCOME
    if result_type != "win" or not context["resolved"] or actual_label is None:
        return ReasonCode.UNRESOLVED_OUTCOME
    if _actual_winner_fighter_id(prediction, context) is None:
        return ReasonCode.FIGHTER_REPLACEMENT
    return None


def _actual_winner_fighter_id(
    prediction: Mapping[str, object],
    context: Mapping[str, object],
) -> str | None:
    actual_label = context["actual_label"]
    if actual_label == 1:
        return context["fighter_1_id"]
    if actual_label == 0:
        return context["fighter_2_id"]
    winner = _text(prediction.get("winner_fighter_id"))
    if winner in context["fighter_ids"]:
        return winner
    return None


def _group_odds_by_fight(
    odds_rows: list[Mapping[str, object]],
) -> dict[str, list[Mapping[str, object]]]:
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for row in odds_rows:
        fight_id = _text(row.get("fight_id"))
        if fight_id is None:
            continue
        grouped.setdefault(fight_id, []).append(row)
    return grouped


def _eligible_market_groups(
    odds_rows: list[Mapping[str, object]],
    context: Mapping[str, object],
    *,
    line_policy: str,
    require_odds_before_prediction: bool,
) -> tuple[list[list[Mapping[str, object]]], bool]:
    groups: dict[tuple, list[Mapping[str, object]]] = {}
    ambiguous_seen = False
    wanted_line_type = _line_type_for_policy(line_policy)
    event_cutoff = datetime.combine(
        context["event_date"],
        time.min,
        tzinfo=timezone.utc,
    )

    for row in odds_rows:
        odds_timestamp = _datetime_optional(row.get("odds_timestamp"))
        if odds_timestamp is None:
            ambiguous_seen = True
            continue
        if _text(row.get("line_type")) != wanted_line_type:
            continue
        if odds_timestamp >= event_cutoff:
            continue
        if require_odds_before_prediction and odds_timestamp > context["scored_at"]:
            continue
        key = (
            _text(row.get("bookmaker")),
            _text(row.get("market")) or "moneyline",
            _text(row.get("line_type")),
            odds_timestamp,
        )
        groups.setdefault(key, []).append(row)

    eligible_groups = []
    for group in groups.values():
        if _valid_market_group(group, context):
            eligible_groups.append(group)
        else:
            ambiguous_seen = True
    return eligible_groups, ambiguous_seen


def _select_market_groups(
    groups: list[list[Mapping[str, object]]],
    line_policy: str,
) -> list[list[Mapping[str, object]]]:
    by_bookmaker_market: dict[tuple, list[list[Mapping[str, object]]]] = {}
    for group in groups:
        first = group[0]
        key = (
            _text(first.get("bookmaker")),
            _text(first.get("market")) or "moneyline",
        )
        by_bookmaker_market.setdefault(key, []).append(group)

    selected = []
    for key in sorted(by_bookmaker_market):
        candidates = by_bookmaker_market[key]
        reverse = line_policy in {LINE_POLICY_LATEST_CURRENT, LINE_POLICY_CLOSING}
        selected.append(sorted(
            candidates,
            key=lambda group: _datetime_required(group[0].get("odds_timestamp"), "odds_timestamp"),
            reverse=reverse,
        )[0])
    return selected


def _valid_market_group(
    group: list[Mapping[str, object]],
    context: Mapping[str, object],
) -> bool:
    if len(group) != 2:
        return False
    fighter_ids = {_text(row.get("fighter_id")) for row in group}
    if None in fighter_ids or fighter_ids != context["fighter_ids"]:
        return False
    for row in group:
        fighter_id = _text(row.get("fighter_id"))
        opponent_id = _text(row.get("opponent_fighter_id"))
        if fighter_id is None or opponent_id is None:
            return False
        if opponent_id not in context["fighter_ids"] or opponent_id == fighter_id:
            return False
        if _decimal_optional(row.get("normalized_decimal_odds")) is None:
            return False
        if _decimal_optional(row.get("implied_probability")) is None:
            return False
        if _decimal_optional(row.get("no_vig_implied_probability")) is None:
            return False
    return True


def _historical_side_row(
    context: Mapping[str, object],
    odds_row: Mapping[str, object],
) -> HistoricalBettingRow:
    fighter_id = _required_text(odds_row, "fighter_id")
    fighter_1_id = context["fighter_1_id"]
    fighter_2_id = context["fighter_2_id"]
    calibrated_prob_f1 = context["calibrated_prob_f1"]

    if fighter_id == fighter_1_id:
        fighter_slot = "fighter_1"
        fighter_name = context["fighter_1_name"]
        opponent_fighter_id = fighter_2_id
        opponent_fighter_name = context["fighter_2_name"]
        model_probability = calibrated_prob_f1
    elif fighter_id == fighter_2_id:
        fighter_slot = "fighter_2"
        fighter_name = context["fighter_2_name"]
        opponent_fighter_id = fighter_1_id
        opponent_fighter_name = context["fighter_1_name"]
        model_probability = Decimal("1") - calibrated_prob_f1
    else:
        raise ValueError("odds row fighter_id does not match prediction")

    return HistoricalBettingRow(
        decision="evaluate",
        reason_codes=(),
        event_id=context["event_id"],
        event_name=context["event_name"],
        event_date=context["event_date"],
        fight_id=context["fight_id"],
        scored_at=context["scored_at"],
        prediction_source="pre_event_prediction_fights",
        fighter_id=fighter_id,
        fighter_name=fighter_name,
        opponent_fighter_id=opponent_fighter_id,
        opponent_fighter_name=opponent_fighter_name,
        fighter_slot=fighter_slot,
        model_probability=model_probability,
        predicted_prob_f1=context["predicted_prob_f1"],
        calibrated_prob_f1=calibrated_prob_f1,
        confidence_tier=context["confidence_tier"],
        is_uncertain=context["is_uncertain"],
        model_name=context["model_name"],
        model_artifact=context["model_artifact"],
        bookmaker=_text(odds_row.get("bookmaker")),
        market=_text(odds_row.get("market")) or "moneyline",
        line_type=_text(odds_row.get("line_type")),
        odds_timestamp=_datetime_required(odds_row.get("odds_timestamp"), "odds_timestamp"),
        american_odds=odds_row.get("american_odds"),
        decimal_odds=_decimal_optional(odds_row.get("decimal_odds")),
        normalized_decimal_odds=_decimal_required(odds_row, "normalized_decimal_odds"),
        market_implied_probability=_decimal_required(odds_row, "implied_probability"),
        no_vig_market_probability=_decimal_required(odds_row, "no_vig_implied_probability"),
        overround=_decimal_optional(odds_row.get("overround")),
        actual_winner_fighter_id=_actual_winner_fighter_id(odds_row, context),
        actual_winner_name=context["actual_winner_name"],
        actual_label=context["actual_label"],
        result_type=context["result_type"],
        resolved=context["resolved"],
    )


def _pass_row(
    context: Mapping[str, object],
    *,
    reason_code: ReasonCode,
    detail: str,
) -> HistoricalBettingRow:
    return HistoricalBettingRow(
        decision="pass",
        reason_codes=(reason_code,),
        event_id=context["event_id"],
        event_name=context["event_name"],
        event_date=context["event_date"],
        fight_id=context["fight_id"],
        scored_at=context["scored_at"],
        prediction_source=f"pre_event_prediction_fights: {detail}",
        predicted_prob_f1=context["predicted_prob_f1"],
        calibrated_prob_f1=context["calibrated_prob_f1"],
        confidence_tier=context["confidence_tier"],
        is_uncertain=context["is_uncertain"],
        model_name=context["model_name"],
        model_artifact=context["model_artifact"],
        actual_winner_fighter_id=_actual_winner_fighter_id({}, context),
        actual_winner_name=context["actual_winner_name"],
        actual_label=context["actual_label"],
        result_type=context["result_type"],
        resolved=context["resolved"],
    )


def _line_type_for_policy(line_policy: str) -> str:
    if line_policy == LINE_POLICY_LATEST_CURRENT:
        return "current"
    return line_policy


def _cli_policy_settings(
    odds_policy: str,
    *,
    line_type: str | None = None,
) -> tuple[str, bool, str]:
    if odds_policy not in CLI_ODDS_POLICIES:
        raise ValueError(f"odds_policy must be one of: {', '.join(CLI_ODDS_POLICIES)}")

    if odds_policy in {CLI_ODDS_POLICY_OPENING, CLI_ODDS_POLICY_CLOSING}:
        if line_type is not None and line_type != odds_policy:
            raise ValueError(
                f"--line-type {line_type} conflicts with --odds-policy {odds_policy}"
            )
        return _line_policy_for_line_type(odds_policy), False, odds_policy

    selected_line_type = line_type or "current"
    return (
        _line_policy_for_line_type(selected_line_type),
        odds_policy == CLI_ODDS_POLICY_LATEST_BEFORE_PREDICTION,
        selected_line_type,
    )


def _line_policy_for_line_type(line_type: str) -> str:
    if line_type == "current":
        return LINE_POLICY_LATEST_CURRENT
    if line_type == "opening":
        return LINE_POLICY_OPENING
    if line_type == "closing":
        return LINE_POLICY_CLOSING
    raise ValueError("line_type must be one of: current, opening, closing")


def _prediction_sort_key(row: Mapping[str, object]) -> tuple:
    return (
        _date_optional(row.get("event_date")) or date.max,
        _text(row.get("event_name")) or "",
        _text(row.get("fight_id")) or "",
    )


def _required_text(row: Mapping[str, object], key: str) -> str:
    value = _text(row.get(key))
    if value is None:
        raise ValueError(f"missing required value for {key}")
    return value


def _text(value: object) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _decimal_required(row: Mapping[str, object], key: str) -> Decimal:
    value = _decimal_optional(row.get(key))
    if value is None:
        raise ValueError(f"missing required value for {key}")
    return value


def _positive_decimal(value: object, label: str) -> Decimal:
    decimal_value = _decimal_required_value(value, label)
    if decimal_value <= Decimal("0"):
        raise ValueError(f"{label} must be positive")
    return decimal_value


def _decimal_required_value(value: object, label: str) -> Decimal:
    decimal_value = _decimal_optional(value)
    if decimal_value is None:
        raise ValueError(f"{label} is required")
    return decimal_value


def _decimal_optional(value: object) -> Decimal | None:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    try:
        return Decimal(text_value)
    except InvalidOperation:
        return None


def _decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}") from exc


def _int_optional(value: object) -> int | None:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    return int(text_value)


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _date_required(value: object, label: str) -> date:
    date_value = _date_optional(value)
    if date_value is None:
        raise ValueError(f"{label} is required")
    return date_value


def _date_optional(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text_value = _text(value)
    if text_value is None:
        return None
    return date.fromisoformat(text_value[:10])


def _datetime_required(value: object, label: str) -> datetime:
    datetime_value = _datetime_optional(value)
    if datetime_value is None:
        raise ValueError(f"{label} is required")
    return datetime_value


def _datetime_optional(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text_value = _text(value)
    if text_value is None:
        return None
    return _ensure_aware(datetime.fromisoformat(text_value.replace("Z", "+00:00")))


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


__all__ = [
    "HistoricalBettingDataset",
    "HistoricalBettingRow",
    "HistoricalDatasetIssue",
    "BACKTEST_DETAIL_DECISIONS",
    "BACKTEST_DETAIL_EVALUATED",
    "BACKTEST_DETAIL_MODES",
    "BACKTEST_EVENT_COLUMNS",
    "BACKTEST_FIGHT_COLUMNS",
    "BACKTEST_SUMMARY_COLUMNS",
    "BacktestReportResult",
    "BacktestReportRows",
    "LINE_POLICIES",
    "LINE_POLICY_CLOSING",
    "LINE_POLICY_LATEST_CURRENT",
    "LINE_POLICY_OPENING",
    "BacktestBetResult",
    "BacktestEventResult",
    "BacktestSimulationResult",
    "CLI_ODDS_POLICIES",
    "CLI_ODDS_POLICY_CLOSING",
    "CLI_ODDS_POLICY_LATEST_BEFORE_EVENT",
    "CLI_ODDS_POLICY_LATEST_BEFORE_PREDICTION",
    "CLI_ODDS_POLICY_OPENING",
    "EDGE_BUCKETS",
    "build_backtest_report_rows",
    "build_arg_parser",
    "build_historical_betting_dataset",
    "build_backtest_bet_decisions",
    "calculate_drawdown",
    "edge_bucket",
    "fetch_historical_no_vig_odds_rows",
    "fetch_pre_event_prediction_rows",
    "filter_historical_odds_rows",
    "filter_historical_prediction_rows",
    "generate_backtest_reports",
    "generate_backtest_reports_from_connection",
    "load_historical_betting_dataset",
    "main",
    "print_backtest_summary",
    "simulate_betting_backtest",
    "write_backtest_reports",
]


if __name__ == "__main__":
    raise SystemExit(main())
