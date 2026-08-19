"""Recommendation input joins, policy, and current-card betting report CLI."""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
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
from betting.value import ValueEvaluation, evaluate_value

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class RecommendationInputRow:
    """One fighter-side prediction plus matching market odds."""

    event_id: str | None
    event_name: str | None
    event_date: object | None
    fight_id: str
    fighter_id: str
    fighter_name: str | None
    opponent_fighter_id: str
    opponent_fighter_name: str | None
    fighter_slot: str
    model_probability: Decimal
    predicted_prob_f1: Decimal | None
    calibrated_prob_f1: Decimal
    confidence_tier: str | None
    is_uncertain: object | None
    model_name: str | None
    model_artifact: str | None
    scored_at: object | None
    bookmaker: str
    market: str
    line_type: str
    odds_timestamp: datetime
    american_odds: object | None
    decimal_odds: Decimal | None
    normalized_decimal_odds: Decimal
    market_implied_probability: Decimal
    no_vig_market_probability: Decimal
    overround: Decimal


@dataclass(frozen=True)
class JoinIssue:
    """Prediction/odds join issue for a fight or market group."""

    fight_id: str | None
    reason_code: ReasonCode
    detail: str
    event_id: str | None = None
    event_name: str | None = None
    event_date: object | None = None
    bookmaker: str | None = None
    line_type: str | None = None
    odds_timestamp: datetime | None = None


@dataclass(frozen=True)
class RecommendationInputResult:
    """Joined recommendation inputs plus rejected groups."""

    rows: tuple[RecommendationInputRow, ...]
    issues: tuple[JoinIssue, ...]


@dataclass(frozen=True)
class BetDecision:
    """Final pre-staking bet/pass decision for one candidate or issue."""

    decision: str
    event_id: str | None
    fight_id: str | None
    recommended_fighter_id: str | None
    recommended_fighter_name: str | None
    reason_codes: tuple[ReasonCode, ...]
    event_name: str | None = None
    event_date: object | None = None
    bookmaker: str | None = None
    market: str | None = None
    line_type: str | None = None
    odds_timestamp: datetime | None = None
    scored_at: datetime | None = None
    evaluated_fighter_id: str | None = None
    evaluated_fighter_name: str | None = None
    opponent_fighter_id: str | None = None
    opponent_fighter_name: str | None = None
    confidence_tier: str | None = None
    model_probability: Decimal | None = None
    market_implied_probability: Decimal | None = None
    no_vig_market_probability: Decimal | None = None
    offered_decimal_odds: Decimal | None = None
    edge: Decimal | None = None
    ev_per_unit: Decimal | None = None
    actual_winner_fighter_id: str | None = None
    actual_winner_name: str | None = None
    result_type: str | None = None
    resolved: bool = False


@dataclass(frozen=True)
class BetPolicyResult:
    """Final decisions after conservative bet/pass policy."""

    decisions: tuple[BetDecision, ...]


@dataclass(frozen=True)
class CurrentBettingReportResult:
    """Paths and rows produced by the current-card recommendation CLI."""

    recommendations_path: Path
    event_summary_path: Path
    recommendation_rows: tuple[dict[str, str], ...]
    event_summary_rows: tuple[dict[str, str], ...]
    bets_recommended: int
    total_stake_fraction: Decimal
    total_stake_amount: Decimal | None
    top_pass_reasons: tuple[tuple[ReasonCode, int], ...]


def build_recommendation_inputs(
    prediction_rows: list[Mapping[str, object]],
    odds_rows: list[Mapping[str, object]],
    *,
    config: BettingConfig | None = None,
    as_of: datetime | None = None,
    bookmaker: str | None = None,
    line_type: str | None = None,
) -> RecommendationInputResult:
    """Join current prediction rows to valid no-vig odds rows."""
    config = config or default_config()
    as_of = _ensure_aware(as_of or datetime.now(timezone.utc))
    prediction_by_fight = {_text(row.get("fight_id")): row for row in prediction_rows}
    odds_by_fight = _group_odds_rows(odds_rows, bookmaker=bookmaker, line_type=line_type)

    output_rows: list[RecommendationInputRow] = []
    issues: list[JoinIssue] = []

    for fight_id, prediction in prediction_by_fight.items():
        if fight_id is None:
            issues.append(_join_issue_with_prediction_context(
                JoinIssue(None, ReasonCode.MISSING_PREDICTION, "prediction missing fight_id"),
                prediction,
            ))
            continue

        try:
            prediction_context = _prediction_context(prediction)
        except ValueError as exc:
            issues.append(_join_issue_with_prediction_context(
                JoinIssue(fight_id, ReasonCode.MISSING_PREDICTION, str(exc)),
                prediction,
            ))
            continue

        groups = odds_by_fight.get(fight_id, [])
        if not groups:
            issues.append(_join_issue_with_prediction_context(
                JoinIssue(fight_id, ReasonCode.MISSING_ODDS, "no matching odds rows"),
                prediction,
            ))
            continue

        for group_key, group_rows in groups:
            group_issue = _validate_odds_group(
                fight_id=fight_id,
                prediction_context=prediction_context,
                group_key=group_key,
                group_rows=group_rows,
                as_of=as_of,
                max_age_hours=config.risk.max_odds_age_hours_current,
            )
            if group_issue is not None:
                issues.append(_join_issue_with_prediction_context(group_issue, prediction))
                continue

            for odds_row in group_rows:
                output_rows.append(_build_row(prediction, prediction_context, odds_row))

    return RecommendationInputResult(rows=tuple(output_rows), issues=tuple(issues))


def apply_bet_pass_policy(
    recommendation_inputs: RecommendationInputResult,
    *,
    config: BettingConfig | None = None,
) -> BetPolicyResult:
    """Apply conservative pre-staking bet/pass policy."""
    config = config or default_config()
    decisions: list[BetDecision] = [
        _decision_from_join_issue(issue)
        for issue in recommendation_inputs.issues
    ]

    grouped_rows: dict[tuple, list[RecommendationInputRow]] = {}
    for row in recommendation_inputs.rows:
        grouped_rows.setdefault(_market_key(row), []).append(row)

    for rows in grouped_rows.values():
        group_decisions = [
            _decision_from_input_row(row, evaluate_value(row, config=config))
            for row in rows
        ]
        decisions.extend(_suppress_opposing_bets(group_decisions))

    return BetPolicyResult(decisions=tuple(decisions))


def load_current_recommendation_inputs(
    conn,
    *,
    config: BettingConfig | None = None,
    bookmaker: str | None = None,
    line_type: str = "current",
    as_of: datetime | None = None,
) -> RecommendationInputResult:
    """Read current predictions and no-vig odds from the warehouse, then join them."""
    prediction_rows = fetch_current_prediction_rows(conn)
    odds_rows = fetch_no_vig_odds_rows(conn, bookmaker=bookmaker, line_type=line_type)
    return build_recommendation_inputs(
        prediction_rows,
        odds_rows,
        config=config,
        bookmaker=bookmaker,
        line_type=line_type,
        as_of=as_of,
    )


def load_current_bet_decisions(
    conn,
    *,
    config: BettingConfig | None = None,
    bookmaker: str | None = None,
    line_type: str = "current",
    as_of: datetime | None = None,
) -> BetPolicyResult:
    """Read current inputs and apply conservative bet/pass policy."""
    inputs = load_current_recommendation_inputs(
        conn,
        config=config,
        bookmaker=bookmaker,
        line_type=line_type,
        as_of=as_of,
    )
    return apply_bet_pass_policy(inputs, config=config)


def generate_current_betting_reports(
    conn,
    *,
    config: BettingConfig | None = None,
    event: str | None = None,
    next_event: bool = False,
    bookmaker: str | None = None,
    line_type: str = "current",
    bankroll: Decimal | int | float | str | None = None,
    current_drawdown: Decimal | int | float | str | None = None,
    as_of: datetime | None = None,
) -> CurrentBettingReportResult:
    """Generate current-card betting recommendation and event summary CSVs."""
    from betting.risk import apply_staking_caps

    config = config or default_config()
    prediction_rows = filter_prediction_rows(
        fetch_current_prediction_rows(conn),
        event=event,
        next_event=next_event,
    )
    odds_rows = fetch_no_vig_odds_rows(conn, bookmaker=bookmaker, line_type=line_type)
    inputs = build_recommendation_inputs(
        prediction_rows,
        odds_rows,
        config=config,
        as_of=as_of,
        bookmaker=bookmaker,
        line_type=line_type,
    )
    policy = apply_bet_pass_policy(inputs, config=config)
    staking = apply_staking_caps(
        policy.decisions,
        config=config,
        bankroll=bankroll,
        current_drawdown=current_drawdown,
    )

    recommendation_rows = tuple(_recommendation_report_row(decision) for decision in staking.decisions)
    event_summary_rows = tuple(_event_summary_rows(staking.decisions))
    report_dir = _report_dir(config)
    report_dir.mkdir(parents=True, exist_ok=True)
    recommendations_path = report_dir / config.recommendations_report
    event_summary_path = report_dir / config.event_summary_report
    _write_csv(recommendations_path, RECOMMENDATION_REPORT_COLUMNS, recommendation_rows)
    _write_csv(event_summary_path, EVENT_SUMMARY_COLUMNS, event_summary_rows)

    bet_decisions = [decision for decision in staking.decisions if decision.decision == "bet"]
    total_stake_fraction = sum(
        (decision.final_stake_fraction for decision in bet_decisions),
        Decimal("0"),
    )
    stake_amounts = [decision.stake_amount for decision in bet_decisions if decision.stake_amount is not None]
    total_stake_amount = sum(stake_amounts, Decimal("0")) if stake_amounts else None
    return CurrentBettingReportResult(
        recommendations_path=recommendations_path,
        event_summary_path=event_summary_path,
        recommendation_rows=recommendation_rows,
        event_summary_rows=event_summary_rows,
        bets_recommended=len(bet_decisions),
        total_stake_fraction=total_stake_fraction,
        total_stake_amount=total_stake_amount,
        top_pass_reasons=_top_pass_reasons(staking.decisions),
    )


def filter_prediction_rows(
    prediction_rows: list[Mapping[str, object]],
    *,
    event: str | None = None,
    next_event: bool = False,
) -> list[Mapping[str, object]]:
    """Filter current prediction rows by event name/id or the next card."""
    rows = list(prediction_rows)
    if event:
        target = event.strip().casefold()
        rows = [
            row for row in rows
            if _event_filter_value(row.get("event_name")) == target
            or _event_filter_value(row.get("event_id")) == target
        ]

    if next_event and rows:
        next_key = min(_event_sort_key(row) for row in rows)
        next_identity = _event_identity(next(row for row in rows if _event_sort_key(row) == next_key))
        rows = [row for row in rows if _event_identity(row) == next_identity]

    return rows


def fetch_current_prediction_rows(conn) -> list[dict[str, object]]:
    """Fetch rows from current_event_predictions for current-card recommendations."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM current_event_predictions")
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_no_vig_odds_rows(
    conn,
    *,
    bookmaker: str | None = None,
    line_type: str | None = "current",
) -> list[dict[str, object]]:
    """Fetch no-vig odds rows, optionally filtered by bookmaker and line type."""
    clauses = []
    params = []
    if bookmaker:
        clauses.append("bookmaker = %s")
        params.append(bookmaker)
    if line_type:
        clauses.append("line_type = %s")
        params.append(line_type)

    sql = "SELECT * FROM fight_odds_no_vig"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


RECOMMENDATION_REPORT_COLUMNS = [
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
    "model_probability",
    "market_implied_probability",
    "no_vig_market_probability",
    "edge",
    "ev_per_unit",
    "ev_percent",
    "full_kelly_fraction",
    "fractional_kelly_fraction",
    "final_stake_fraction",
    "stake_amount",
    "decision",
    "recommended_fighter_id",
    "recommended_fighter_name",
    "confidence_tier",
    "drawdown_protection_enabled",
    "drawdown_protection_fired",
    "current_drawdown",
    "reason_codes",
]


EVENT_SUMMARY_COLUMNS = [
    "event_id",
    "event_name",
    "event_date",
    "bets_recommended",
    "total_stake_fraction",
    "total_stake_amount",
    "event_exposure_fraction",
    "pass_count",
    "top_pass_reasons",
    "drawdown_protection_enabled",
    "drawdown_protection_fired",
]


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the current-card betting recommendation CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate betting recommendations from existing current predictions "
            "and warehouse odds. Run model scoring separately before this command."
        )
    )
    event_group = parser.add_mutually_exclusive_group()
    event_group.add_argument("--event", help="Current/upcoming event name or event_id to evaluate.")
    event_group.add_argument("--next", action="store_true", dest="next_event", help="Evaluate only the next upcoming/current event.")
    parser.add_argument("--bookmaker", help="Restrict odds to one bookmaker/source.")
    parser.add_argument(
        "--line-type",
        choices=("current", "opening", "closing"),
        default="current",
        help="Odds line type to evaluate.",
    )
    parser.add_argument("--bankroll", type=_decimal_arg, help="Bankroll amount used to calculate stake_amount.")
    parser.add_argument("--current-drawdown", type=_decimal_arg, help="Optional current drawdown fraction for drawdown protection.")
    parser.add_argument("--as-of", help="UTC timestamp used for odds freshness checks; defaults to now.")
    parser.add_argument("--config", help="Optional .json or .toml betting config file.")
    parser.add_argument("--report-dir", help="Override report output directory.")
    parser.add_argument("--min-edge", type=float, help="Override minimum model-vs-no-vig edge.")
    parser.add_argument("--min-ev", type=float, help="Override minimum expected value per unit.")
    parser.add_argument("--max-odds-age-hours-current", type=int, help="Override current odds freshness cap.")
    return parser


def main(argv: list[str] | None = None, *, conn=None) -> int:
    """CLI entry point for current-card betting recommendation reports."""
    args = build_arg_parser().parse_args(argv)
    config = load_config_file(args.config) if args.config else default_config()
    config = apply_cli_overrides(config, args)
    as_of = _parse_datetime(args.as_of) if args.as_of else None

    close_conn = False
    if conn is None:
        from warehouse.db import get_connection

        conn = get_connection()
        close_conn = True

    try:
        result = generate_current_betting_reports(
            conn,
            config=config,
            event=args.event,
            next_event=args.next_event,
            bookmaker=args.bookmaker,
            line_type=args.line_type,
            bankroll=args.bankroll,
            current_drawdown=args.current_drawdown,
            as_of=as_of,
        )
    finally:
        if close_conn:
            conn.close()

    print_current_card_summary(result)
    return 0


def print_current_card_summary(result: CurrentBettingReportResult) -> None:
    """Print a concise card-level summary to stdout."""
    if result.total_stake_amount is None:
        total_stake = f"{_format_decimal(result.total_stake_fraction)} bankroll fraction"
    else:
        total_stake = (
            f"{_format_decimal(result.total_stake_amount)} "
            f"({_format_decimal(result.total_stake_fraction)} bankroll fraction)"
        )

    print(f"Bets recommended: {result.bets_recommended}")
    print(f"Total stake: {total_stake}")
    print(f"Event exposure: {_summary_exposure_text(result.event_summary_rows)}")
    print(f"Top pass reasons: {_pass_reason_text(result.top_pass_reasons)}")
    print(f"Wrote: {result.recommendations_path}")
    print(f"Wrote: {result.event_summary_path}")


def _recommendation_report_row(decision) -> dict[str, str]:
    return {
        "event_id": _format_value(decision.event_id),
        "event_name": _format_value(decision.event_name),
        "event_date": _format_value(decision.event_date),
        "fight_id": _format_value(decision.fight_id),
        "fighter_id": _format_value(decision.evaluated_fighter_id),
        "fighter_name": _format_value(decision.evaluated_fighter_name),
        "opponent_fighter_id": _format_value(decision.opponent_fighter_id),
        "opponent_fighter_name": _format_value(decision.opponent_fighter_name),
        "bookmaker": _format_value(decision.bookmaker),
        "market": _format_value(decision.market),
        "line_type": _format_value(decision.line_type),
        "odds_timestamp": _format_value(decision.odds_timestamp),
        "model_probability": _format_decimal(decision.model_probability),
        "market_implied_probability": _format_decimal(decision.market_implied_probability),
        "no_vig_market_probability": _format_decimal(decision.no_vig_market_probability),
        "edge": _format_decimal(decision.edge),
        "ev_per_unit": _format_decimal(decision.ev_per_unit),
        "ev_percent": _format_decimal(decision.ev_per_unit),
        "full_kelly_fraction": _format_decimal(decision.uncapped_kelly_fraction),
        "fractional_kelly_fraction": _format_decimal(decision.fractional_kelly_fraction),
        "final_stake_fraction": _format_decimal(decision.final_stake_fraction),
        "stake_amount": _format_decimal(decision.stake_amount),
        "decision": decision.decision,
        "recommended_fighter_id": _format_value(decision.recommended_fighter_id),
        "recommended_fighter_name": _format_value(decision.recommended_fighter_name),
        "confidence_tier": _format_value(decision.confidence_tier),
        "drawdown_protection_enabled": str(decision.drawdown_protection_enabled).lower(),
        "drawdown_protection_fired": str(decision.drawdown_protection_fired).lower(),
        "current_drawdown": _format_decimal(decision.current_drawdown),
        "reason_codes": format_reason_codes(decision.reason_codes),
    }


def _event_summary_rows(decisions) -> list[dict[str, str]]:
    grouped: dict[tuple, list] = {}
    for decision in decisions:
        grouped.setdefault(_event_summary_key(decision), []).append(decision)

    rows = []
    for key in sorted(grouped):
        group = grouped[key]
        bet_decisions = [decision for decision in group if decision.decision == "bet"]
        total_stake_fraction = sum(
            (decision.final_stake_fraction for decision in bet_decisions),
            Decimal("0"),
        )
        stake_amounts = [decision.stake_amount for decision in bet_decisions if decision.stake_amount is not None]
        total_stake_amount = sum(stake_amounts, Decimal("0")) if stake_amounts else None
        pass_reasons = _top_pass_reasons(group)
        drawdown_enabled = any(decision.drawdown_protection_enabled for decision in group)
        drawdown_fired = any(decision.drawdown_protection_fired for decision in group)
        event_id, event_name, event_date = key
        rows.append({
            "event_id": _format_value(event_id),
            "event_name": _format_value(event_name),
            "event_date": _format_value(event_date),
            "bets_recommended": str(len(bet_decisions)),
            "total_stake_fraction": _format_decimal(total_stake_fraction),
            "total_stake_amount": _format_decimal(total_stake_amount),
            "event_exposure_fraction": _format_decimal(total_stake_fraction),
            "pass_count": str(len(group) - len(bet_decisions)),
            "top_pass_reasons": _pass_reason_text(pass_reasons),
            "drawdown_protection_enabled": str(drawdown_enabled).lower(),
            "drawdown_protection_fired": str(drawdown_fired).lower(),
        })
    return rows


def _top_pass_reasons(decisions) -> tuple[tuple[ReasonCode, int], ...]:
    counts: Counter[ReasonCode] = Counter()
    for decision in decisions:
        if decision.decision == "bet":
            continue
        counts.update(decision.reason_codes)
    return tuple(counts.most_common(5))


def _write_csv(path: Path, columns: list[str], rows: tuple[dict[str, str], ...]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _report_dir(config: BettingConfig) -> Path:
    path = Path(config.report_dir)
    return path if path.is_absolute() else REPO_ROOT / path


def _event_filter_value(value: object) -> str | None:
    text = _text(value)
    return text.casefold() if text is not None else None


def _event_sort_key(row: Mapping[str, object]) -> tuple:
    return (
        _format_value(row.get("event_date")),
        _format_value(row.get("event_name")),
        _format_value(row.get("event_id")),
    )


def _event_identity(row: Mapping[str, object]) -> tuple:
    return (
        _format_value(row.get("event_id")),
        _format_value(row.get("event_name")),
        _format_value(row.get("event_date")),
    )


def _event_summary_key(decision) -> tuple:
    return (
        decision.event_id or "",
        decision.event_name or "",
        _format_value(decision.event_date),
    )


def _summary_exposure_text(rows: tuple[dict[str, str], ...]) -> str:
    if not rows:
        return "none"
    parts = []
    for row in rows:
        label = row["event_name"] or row["event_id"] or "unknown_event"
        parts.append(f"{label}={row['event_exposure_fraction']}")
    return ", ".join(parts)


def _pass_reason_text(reasons: tuple[tuple[ReasonCode, int], ...]) -> str:
    if not reasons:
        return "none"
    return ", ".join(f"{reason.value}={count}" for reason, count in reasons)


def _format_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def _format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}") from exc


def _decision_from_join_issue(issue: JoinIssue) -> BetDecision:
    return BetDecision(
        decision="pass",
        event_id=issue.event_id,
        fight_id=issue.fight_id,
        recommended_fighter_id=None,
        recommended_fighter_name=None,
        reason_codes=(issue.reason_code,),
        event_name=issue.event_name,
        event_date=issue.event_date,
        bookmaker=issue.bookmaker,
        line_type=issue.line_type,
        odds_timestamp=issue.odds_timestamp,
    )


def _join_issue_with_prediction_context(
    issue: JoinIssue,
    prediction: Mapping[str, object],
) -> JoinIssue:
    return JoinIssue(
        fight_id=issue.fight_id,
        reason_code=issue.reason_code,
        detail=issue.detail,
        event_id=_text(prediction.get("event_id")),
        event_name=_text(prediction.get("event_name")),
        event_date=prediction.get("event_date"),
        bookmaker=issue.bookmaker,
        line_type=issue.line_type,
        odds_timestamp=issue.odds_timestamp,
    )


def _decision_from_input_row(
    row: RecommendationInputRow,
    value: ValueEvaluation,
) -> BetDecision:
    reason_codes = value.reason_codes
    decision = value.decision
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
        scored_at=_parse_datetime(row.scored_at),
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
    )


def _suppress_opposing_bets(decisions: list[BetDecision]) -> list[BetDecision]:
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
        output.append(
            BetDecision(
                decision="pass",
                event_id=decision.event_id,
                fight_id=decision.fight_id,
                recommended_fighter_id=None,
                recommended_fighter_name=None,
                reason_codes=(ReasonCode.AMBIGUOUS_ODDS,),
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
        )
    return output


def _market_key(row: RecommendationInputRow) -> tuple:
    return (
        row.fight_id,
        row.bookmaker,
        row.market,
        row.line_type,
        row.odds_timestamp,
    )


def _group_odds_rows(
    odds_rows: list[Mapping[str, object]],
    *,
    bookmaker: str | None,
    line_type: str | None,
) -> dict[str, list[tuple[tuple, list[Mapping[str, object]]]]]:
    grouped: dict[str, dict[tuple, list[Mapping[str, object]]]] = {}
    for row in odds_rows:
        if bookmaker is not None and row.get("bookmaker") != bookmaker:
            continue
        if line_type is not None and row.get("line_type") != line_type:
            continue
        fight_id = _text(row.get("fight_id"))
        if fight_id is None:
            continue
        key = (
            row.get("bookmaker"),
            row.get("market", "moneyline"),
            row.get("line_type"),
            _parse_datetime(row.get("odds_timestamp")),
        )
        grouped.setdefault(fight_id, {}).setdefault(key, []).append(row)

    return {
        fight_id: list(groups.items())
        for fight_id, groups in grouped.items()
    }


def _prediction_context(prediction: Mapping[str, object]) -> dict[str, object]:
    fighter_1_id = _required_text(prediction, "fighter_1_id")
    fighter_2_id = _required_text(prediction, "fighter_2_id")
    calibrated_prob_f1 = _decimal_required(prediction, "calibrated_prob_f1")
    if not Decimal("0") <= calibrated_prob_f1 <= Decimal("1"):
        raise ValueError("calibrated_prob_f1 must be between 0 and 1")

    return {
        "fighter_ids": {fighter_1_id, fighter_2_id},
        "fighter_1_id": fighter_1_id,
        "fighter_2_id": fighter_2_id,
        "calibrated_prob_f1": calibrated_prob_f1,
    }


def _validate_odds_group(
    *,
    fight_id: str,
    prediction_context: Mapping[str, object],
    group_key: tuple,
    group_rows: list[Mapping[str, object]],
    as_of: datetime,
    max_age_hours: int,
) -> JoinIssue | None:
    bookmaker, _, line_type, odds_timestamp = group_key
    if odds_timestamp is None:
        return JoinIssue(
            fight_id,
            ReasonCode.INVALID_ODDS,
            "odds group missing odds_timestamp",
            bookmaker=bookmaker,
            line_type=line_type,
        )

    age_hours = (as_of - odds_timestamp).total_seconds() / 3600
    if age_hours > max_age_hours:
        return JoinIssue(
            fight_id,
            ReasonCode.STALE_ODDS,
            f"odds age {age_hours:.2f}h exceeds {max_age_hours}h",
            bookmaker=bookmaker,
            line_type=line_type,
            odds_timestamp=odds_timestamp,
        )

    if len(group_rows) != 2:
        return JoinIssue(
            fight_id,
            ReasonCode.AMBIGUOUS_ODDS,
            f"expected 2 odds sides, got {len(group_rows)}",
            bookmaker=bookmaker,
            line_type=line_type,
            odds_timestamp=odds_timestamp,
        )

    odds_fighter_ids = {_text(row.get("fighter_id")) for row in group_rows}
    if None in odds_fighter_ids or odds_fighter_ids != prediction_context["fighter_ids"]:
        return JoinIssue(
            fight_id,
            ReasonCode.AMBIGUOUS_ODDS,
            "odds fighter IDs do not match prediction fighter IDs",
            bookmaker=bookmaker,
            line_type=line_type,
            odds_timestamp=odds_timestamp,
        )

    if len(odds_fighter_ids) != 2:
        return JoinIssue(
            fight_id,
            ReasonCode.AMBIGUOUS_ODDS,
            "duplicate odds side in market group",
            bookmaker=bookmaker,
            line_type=line_type,
            odds_timestamp=odds_timestamp,
        )
    return None


def _build_row(
    prediction: Mapping[str, object],
    prediction_context: Mapping[str, object],
    odds_row: Mapping[str, object],
) -> RecommendationInputRow:
    fighter_id = _required_text(odds_row, "fighter_id")
    fighter_1_id = prediction_context["fighter_1_id"]
    fighter_2_id = prediction_context["fighter_2_id"]
    calibrated_prob_f1 = prediction_context["calibrated_prob_f1"]

    if fighter_id == fighter_1_id:
        fighter_slot = "fighter_1"
        model_probability = calibrated_prob_f1
        fighter_name = _text(prediction.get("fighter_1_name"))
        opponent_fighter_id = fighter_2_id
        opponent_fighter_name = _text(prediction.get("fighter_2_name"))
    elif fighter_id == fighter_2_id:
        fighter_slot = "fighter_2"
        model_probability = Decimal("1") - calibrated_prob_f1
        fighter_name = _text(prediction.get("fighter_2_name"))
        opponent_fighter_id = fighter_1_id
        opponent_fighter_name = _text(prediction.get("fighter_1_name"))
    else:
        raise ValueError("odds row fighter_id does not match prediction")

    return RecommendationInputRow(
        event_id=_text(prediction.get("event_id")),
        event_name=_text(prediction.get("event_name")),
        event_date=prediction.get("event_date"),
        fight_id=_required_text(prediction, "fight_id"),
        fighter_id=fighter_id,
        fighter_name=fighter_name,
        opponent_fighter_id=opponent_fighter_id,
        opponent_fighter_name=opponent_fighter_name,
        fighter_slot=fighter_slot,
        model_probability=model_probability,
        predicted_prob_f1=_decimal_optional(prediction.get("predicted_prob_f1")),
        calibrated_prob_f1=calibrated_prob_f1,
        confidence_tier=_text(prediction.get("confidence_tier")),
        is_uncertain=prediction.get("is_uncertain"),
        model_name=_text(prediction.get("model_name")),
        model_artifact=_text(prediction.get("model_artifact")),
        scored_at=prediction.get("scored_at"),
        bookmaker=_required_text(odds_row, "bookmaker"),
        market=_text(odds_row.get("market")) or "moneyline",
        line_type=_required_text(odds_row, "line_type"),
        odds_timestamp=_parse_datetime(odds_row.get("odds_timestamp")),
        american_odds=odds_row.get("american_odds"),
        decimal_odds=_decimal_optional(odds_row.get("decimal_odds")),
        normalized_decimal_odds=_decimal_required(odds_row, "normalized_decimal_odds"),
        market_implied_probability=_decimal_required(odds_row, "implied_probability"),
        no_vig_market_probability=_decimal_required(odds_row, "no_vig_implied_probability"),
        overround=_decimal_required(odds_row, "overround"),
    )


def _required_text(row: Mapping[str, object], key: str) -> str:
    value = _text(row.get(key))
    if value is None:
        raise ValueError(f"missing required value for {key}")
    return value


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _decimal_required(row: Mapping[str, object], key: str) -> Decimal:
    value = _decimal_optional(row.get(key))
    if value is None:
        raise ValueError(f"missing required value for {key}")
    return value


def _decimal_optional(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Decimal(text)


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = _text(value)
    if text is None:
        return None
    return _ensure_aware(datetime.fromisoformat(text.replace("Z", "+00:00")))


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


__all__ = [
    "BetDecision",
    "BetPolicyResult",
    "CurrentBettingReportResult",
    "JoinIssue",
    "RecommendationInputResult",
    "RecommendationInputRow",
    "apply_bet_pass_policy",
    "build_arg_parser",
    "build_recommendation_inputs",
    "fetch_current_prediction_rows",
    "fetch_no_vig_odds_rows",
    "filter_prediction_rows",
    "generate_current_betting_reports",
    "load_current_bet_decisions",
    "load_current_recommendation_inputs",
    "main",
    "print_current_card_summary",
]


if __name__ == "__main__":
    raise SystemExit(main())
