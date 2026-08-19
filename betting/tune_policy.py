"""Walk-forward betting calibration and policy tuning.

This is an evaluation layer for turning model probabilities into betting
decisions. It tunes on a validation window and reports the chosen candidate on
a later holdout window, using the research-only Kaggle odds adapter.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from betting.backtest import (
    BacktestSimulationResult,
    LINE_POLICY_LATEST_CURRENT,
    build_historical_betting_dataset,
    generate_backtest_reports,
    simulate_betting_backtest,
)
from betting.config import BettingConfig, default_config
from betting.kaggle_research_backtest import (
    DEFAULT_ODDS,
    DEFAULT_PREDICTIONS,
    _filter_odds_rows,
    _prepare_no_vig_odds_rows,
    _prepare_prediction_rows,
    _read_csv,
)
from modeling.calibrate import calibrate_isotonic, calibrate_platt
from modeling.uncertainty import confidence_tier

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_DIR = REPO_ROOT / "data" / "reports"


@dataclass(frozen=True)
class SimulationMetrics:
    """Compact comparable metrics for one simulation."""

    total_bets: int
    wins: int
    losses: int
    pushes: int
    total_staked: Decimal
    profit_loss: Decimal
    roi: Decimal | None
    hit_rate: Decimal | None
    max_drawdown: Decimal
    ending_bankroll: Decimal


def run_policy_tuning(
    *,
    predictions_path: Path = DEFAULT_PREDICTIONS,
    odds_path: Path = DEFAULT_ODDS,
    report_dir: Path = DEFAULT_REPORT_DIR,
    train_end: date = date(2021, 12, 31),
    validation_start: date = date(2022, 1, 1),
    validation_end: date = date(2024, 12, 31),
    holdout_start: date = date(2025, 1, 1),
    holdout_end: date | None = None,
    min_validation_bets: int = 50,
    max_validation_drawdown: Decimal = Decimal("0.50"),
    initial_bankroll: Decimal = Decimal("1000"),
) -> dict[str, Path | dict[str, object]]:
    """Tune policy candidates and write reports."""
    report_dir.mkdir(parents=True, exist_ok=True)

    base_predictions = pd.read_csv(predictions_path)
    base_predictions["event_date"] = pd.to_datetime(base_predictions["event_date"]).dt.date
    base_predictions["actual_label"] = pd.to_numeric(base_predictions["actual_label"], errors="coerce")
    base_predictions = base_predictions.dropna(subset=["actual_label", "calibrated_prob_f1"]).copy()
    base_predictions["actual_label"] = base_predictions["actual_label"].astype(int)

    odds_rows, odds_counters = _prepare_no_vig_odds_rows(_read_csv(odds_path))
    calibration_rows: list[dict[str, object]] = []
    tuning_rows: list[dict[str, object]] = []

    method_frames = _calibrated_prediction_frames(
        base_predictions,
        train_end=train_end,
    )
    policy_grid = list(_policy_grid())

    best_record: dict[str, object] | None = None
    best_validation_metrics: SimulationMetrics | None = None
    best_holdout_metrics: SimulationMetrics | None = None
    best_holdout_dataset = None
    best_config: BettingConfig | None = None
    fallback_record: dict[str, object] | None = None
    fallback_validation_metrics: SimulationMetrics | None = None
    fallback_holdout_metrics: SimulationMetrics | None = None
    fallback_holdout_dataset = None
    fallback_config: BettingConfig | None = None

    for method, predictions in method_frames.items():
        calibration_rows.extend(_calibration_report_rows(
            method,
            predictions,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            holdout_start=holdout_start,
            holdout_end=holdout_end,
        ))

        validation_dataset = _dataset_for_window(
            predictions,
            odds_rows,
            start_date=validation_start,
            end_date=validation_end,
        )
        holdout_dataset = _dataset_for_window(
            predictions,
            odds_rows,
            start_date=holdout_start,
            end_date=holdout_end,
        )

        for policy in policy_grid:
            config = _config_for_policy(policy)
            validation_sim = simulate_betting_backtest(
                validation_dataset,
                starting_bankroll=initial_bankroll,
                config=config,
                max_one_bet_per_fight=True,
            )
            holdout_sim = simulate_betting_backtest(
                holdout_dataset,
                starting_bankroll=initial_bankroll,
                config=config,
                max_one_bet_per_fight=True,
            )
            validation_metrics = _simulation_metrics(validation_sim)
            holdout_metrics = _simulation_metrics(holdout_sim)

            tuning_rows.append(_tuning_row(
                "validation",
                method,
                policy,
                validation_metrics,
            ))
            tuning_rows.append(_tuning_row(
                "holdout",
                method,
                policy,
                holdout_metrics,
            ))

            record = {
                "calibration_method": method,
                **policy,
            }
            if (
                fallback_record is None
                or _candidate_sort_key(validation_metrics) > _candidate_sort_key(fallback_validation_metrics)
            ):
                fallback_record = record
                fallback_validation_metrics = validation_metrics
                fallback_holdout_metrics = holdout_metrics
                fallback_holdout_dataset = holdout_dataset
                fallback_config = config

            if not _eligible_candidate(
                validation_metrics,
                min_validation_bets=min_validation_bets,
                max_validation_drawdown=max_validation_drawdown,
            ):
                continue
            if (
                best_record is None
                or _candidate_sort_key(validation_metrics) > _candidate_sort_key(best_validation_metrics)
            ):
                best_record = record
                best_validation_metrics = validation_metrics
                best_holdout_metrics = holdout_metrics
                best_holdout_dataset = holdout_dataset
                best_config = config

    if best_record is None:
        best_record = fallback_record
        best_validation_metrics = fallback_validation_metrics
        best_holdout_metrics = fallback_holdout_metrics
        best_holdout_dataset = fallback_holdout_dataset
        best_config = fallback_config
        selection_passed_filters = False
    else:
        selection_passed_filters = True

    if best_record is None:
        raise RuntimeError("no policy candidates were evaluated")

    calibration_path = report_dir / "betting_calibration_walkforward.csv"
    tuning_path = report_dir / "betting_policy_tuning.csv"
    best_path = report_dir / "betting_policy_tuning_best.json"
    _write_csv(calibration_path, calibration_rows)
    _write_csv(tuning_path, tuning_rows)

    holdout_config = best_config.with_overrides({
        "report_dir": str(report_dir / "betting_policy_tuned_holdout"),
        "backtest_fights_report": "fights.csv",
        "backtest_events_report": "events.csv",
        "backtest_summary_report": "summary.csv",
    })
    holdout_report = generate_backtest_reports(
        best_holdout_dataset,
        starting_bankroll=initial_bankroll,
        config=holdout_config,
        max_one_bet_per_fight=True,
    )

    best_payload = {
        "research_only": True,
        "promotion_recommendation": (
            "candidate_for_manual_review"
            if selection_passed_filters
            else "do_not_promote_validation_filter_failed"
        ),
        "reason": (
            "Uses retro model scores and Kaggle odds with synthetic pre-event "
            "timestamps. Promote only after honest pre-event validation grows."
        ),
        "split": {
            "train_end": train_end.isoformat(),
            "validation_start": validation_start.isoformat(),
            "validation_end": validation_end.isoformat(),
            "holdout_start": holdout_start.isoformat(),
            "holdout_end": holdout_end.isoformat() if holdout_end else None,
        },
        "odds_counters": dict(odds_counters),
        "selection_filters": {
            "min_validation_bets": min_validation_bets,
            "max_validation_drawdown": str(max_validation_drawdown),
            "passed": selection_passed_filters,
        },
        "selected_policy": best_record,
        "validation_metrics": _json_metrics(best_validation_metrics),
        "holdout_metrics": _json_metrics(best_holdout_metrics),
        "holdout_reports": {
            "fights": str(holdout_report.fights_path),
            "events": str(holdout_report.events_path),
            "summary": str(holdout_report.summary_path),
        },
    }
    best_path.write_text(json.dumps(best_payload, indent=2), encoding="utf-8")

    return {
        "calibration_path": calibration_path,
        "tuning_path": tuning_path,
        "best_path": best_path,
        "best": best_payload,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    """Return CLI parser."""
    parser = argparse.ArgumentParser(description="Tune betting calibration and policy on research odds.")
    parser.add_argument("--predictions", default=str(DEFAULT_PREDICTIONS))
    parser.add_argument("--odds", default=str(DEFAULT_ODDS))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--train-end", default="2021-12-31")
    parser.add_argument("--validation-start", default="2022-01-01")
    parser.add_argument("--validation-end", default="2024-12-31")
    parser.add_argument("--holdout-start", default="2025-01-01")
    parser.add_argument("--holdout-end")
    parser.add_argument("--min-validation-bets", type=int, default=50)
    parser.add_argument("--max-validation-drawdown", type=_decimal_arg, default=Decimal("0.50"))
    parser.add_argument("--initial-bankroll", type=_decimal_arg, default=Decimal("1000"))
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_arg_parser().parse_args(argv)
    result = run_policy_tuning(
        predictions_path=Path(args.predictions),
        odds_path=Path(args.odds),
        report_dir=Path(args.report_dir),
        train_end=_date_arg(args.train_end),
        validation_start=_date_arg(args.validation_start),
        validation_end=_date_arg(args.validation_end),
        holdout_start=_date_arg(args.holdout_start),
        holdout_end=_date_arg(args.holdout_end) if args.holdout_end else None,
        min_validation_bets=args.min_validation_bets,
        max_validation_drawdown=args.max_validation_drawdown,
        initial_bankroll=args.initial_bankroll,
    )
    best = result["best"]
    selected = best["selected_policy"]
    validation = best["validation_metrics"]
    holdout = best["holdout_metrics"]
    print("Research-only betting calibration/policy tuning complete.")
    print(f"Promotion recommendation: {best['promotion_recommendation']}")
    print(f"Selected calibration: {selected['calibration_method']}")
    print(
        "Selected policy: "
        f"min_edge={selected['min_edge']} min_ev={selected['min_ev']} "
        f"kelly={selected['kelly_fraction']} medium_cap={selected['medium_tier_cap']} "
        f"high_cap={selected['high_tier_cap']}"
    )
    print(
        "Validation: "
        f"bets={validation['total_bets']} roi={validation['roi']} "
        f"pl={validation['profit_loss']} max_dd={validation['max_drawdown']}"
    )
    print(
        "Holdout: "
        f"bets={holdout['total_bets']} roi={holdout['roi']} "
        f"pl={holdout['profit_loss']} max_dd={holdout['max_drawdown']}"
    )
    print(f"Wrote: {result['calibration_path']}")
    print(f"Wrote: {result['tuning_path']}")
    print(f"Wrote: {result['best_path']}")
    return 0


def _calibrated_prediction_frames(
    df: pd.DataFrame,
    *,
    train_end: date,
) -> dict[str, pd.DataFrame]:
    train_mask = df["event_date"] <= train_end
    y_train = df.loc[train_mask, "actual_label"].to_numpy(dtype=int)
    p_train = df.loc[train_mask, "calibrated_prob_f1"].to_numpy(dtype=float)

    frames = {"current": _with_probability(df, df["calibrated_prob_f1"].to_numpy(dtype=float))}
    if len(np.unique(y_train)) < 2:
        return frames

    all_prob = df["calibrated_prob_f1"].to_numpy(dtype=float)
    frames["platt"] = _with_probability(df, calibrate_platt(p_train, y_train, all_prob))
    frames["isotonic"] = _with_probability(df, calibrate_isotonic(p_train, y_train, all_prob))
    return frames


def _with_probability(df: pd.DataFrame, probabilities: np.ndarray) -> pd.DataFrame:
    output = df.copy()
    clipped = np.clip(np.asarray(probabilities, dtype=float), 0.001, 0.999)
    output["calibrated_prob_f1"] = clipped
    output["predicted_label"] = (clipped >= 0.5).astype(int)
    output["predicted_prob_winner"] = np.where(output["predicted_label"] == 1, clipped, 1.0 - clipped)
    output["predicted_correct"] = output["predicted_label"] == output["actual_label"].astype(int)
    output["confidence_tier"] = confidence_tier(clipped)
    output["is_uncertain"] = output["confidence_tier"] == "toss-up"
    output["predicted_winner_id"] = np.where(
        output["predicted_label"] == 1,
        output["fighter_1_id"],
        output["fighter_2_id"],
    )
    output["predicted_winner_name"] = np.where(
        output["predicted_label"] == 1,
        output["fighter_1_name"],
        output["fighter_2_name"],
    )
    return output


def _dataset_for_window(
    predictions: pd.DataFrame,
    odds_rows: list[dict[str, str]],
    *,
    start_date: date,
    end_date: date | None,
) -> object:
    rows = _prepare_prediction_rows(predictions.to_dict("records"))
    odds = _filter_odds_rows(
        odds_rows,
        start_date=start_date,
        end_date=end_date,
    )
    rows = [
        row for row in rows
        if _within_window(_date_arg(str(row["event_date"])[:10]), start_date, end_date)
    ]
    return build_historical_betting_dataset(
        rows,
        odds,
        line_policy=LINE_POLICY_LATEST_CURRENT,
        require_odds_before_prediction=True,
    )


def _policy_grid() -> Iterable[dict[str, float]]:
    for min_edge in (0.03, 0.05, 0.07, 0.10):
        for min_ev in (0.01, 0.03, 0.05, 0.10):
            for kelly_fraction in (0.125, 0.25):
                for medium_tier_cap in (0.005, 0.01):
                    for high_tier_cap in (0.01, 0.02):
                        yield {
                            "min_edge": min_edge,
                            "min_ev": min_ev,
                            "kelly_fraction": kelly_fraction,
                            "medium_tier_cap": medium_tier_cap,
                            "high_tier_cap": high_tier_cap,
                            "max_single_bet_fraction": high_tier_cap,
                            "max_event_fraction": 0.06,
                            "toss_up_tier_cap": 0.0,
                        }


def _config_for_policy(policy: Mapping[str, float]) -> BettingConfig:
    return default_config().with_overrides(policy)


def _simulation_metrics(simulation: BacktestSimulationResult) -> SimulationMetrics:
    bets = tuple(row for row in simulation.bets if row.decision == "bet")
    wins = sum(1 for row in bets if row.bet_result == "win")
    losses = sum(1 for row in bets if row.bet_result == "loss")
    pushes = sum(1 for row in bets if row.bet_result == "push")
    total_staked = sum((row.stake_amount for row in bets), Decimal("0"))
    profit_loss = sum((row.profit_loss_amount for row in bets), Decimal("0"))
    return SimulationMetrics(
        total_bets=len(bets),
        wins=wins,
        losses=losses,
        pushes=pushes,
        total_staked=total_staked,
        profit_loss=profit_loss,
        roi=(profit_loss / total_staked) if total_staked > 0 else None,
        hit_rate=(Decimal(wins) / Decimal(wins + losses)) if wins + losses > 0 else None,
        max_drawdown=simulation.max_drawdown,
        ending_bankroll=simulation.ending_bankroll,
    )


def _eligible_candidate(
    metrics: SimulationMetrics,
    *,
    min_validation_bets: int,
    max_validation_drawdown: Decimal,
) -> bool:
    return (
        metrics.total_bets >= min_validation_bets
        and metrics.roi is not None
        and metrics.roi > Decimal("0")
        and metrics.max_drawdown <= max_validation_drawdown
    )


def _candidate_sort_key(metrics: SimulationMetrics | None) -> tuple[Decimal, Decimal, int]:
    if metrics is None:
        return (Decimal("-Infinity"), Decimal("-Infinity"), 0)
    return (
        metrics.roi or Decimal("-Infinity"),
        metrics.profit_loss,
        metrics.total_bets,
    )


def _tuning_row(
    split: str,
    method: str,
    policy: Mapping[str, float],
    metrics: SimulationMetrics,
) -> dict[str, object]:
    return {
        "split": split,
        "calibration_method": method,
        **policy,
        **asdict(metrics),
    }


def _calibration_report_rows(
    method: str,
    predictions: pd.DataFrame,
    *,
    train_end: date,
    validation_start: date,
    validation_end: date,
    holdout_start: date,
    holdout_end: date | None,
) -> list[dict[str, object]]:
    windows = {
        "train": (None, train_end),
        "validation": (validation_start, validation_end),
        "holdout": (holdout_start, holdout_end),
    }
    rows = []
    for split, (start, end) in windows.items():
        subset = predictions.copy()
        if start is not None:
            subset = subset[subset["event_date"] >= start]
        if end is not None:
            subset = subset[subset["event_date"] <= end]
        if subset.empty:
            continue
        y_true = subset["actual_label"].to_numpy(dtype=int)
        y_prob = subset["calibrated_prob_f1"].to_numpy(dtype=float)
        rows.append({
            "split": split,
            "calibration_method": method,
            "rows": len(subset),
            **_probability_metrics(y_true, y_prob),
        })
    return rows


def _probability_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict[str, float | None]:
    y_pred = (y_prob >= 0.5).astype(int)
    metrics: dict[str, float | None] = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "log_loss": float(log_loss(y_true, y_prob, labels=[0, 1])),
        "brier_score": float(brier_score_loss(y_true, y_prob)),
        "ece": float(_ece(y_true, y_prob)),
    }
    metrics["roc_auc"] = (
        float(roc_auc_score(y_true, y_prob))
        if len(np.unique(y_true)) > 1
        else None
    )
    return metrics


def _ece(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> float:
    bins = np.linspace(0, 1, n_bins + 1)
    total = len(y_true)
    value = 0.0
    for lower, upper in zip(bins[:-1], bins[1:]):
        mask = (y_prob > lower) & (y_prob <= upper) if lower > 0 else (y_prob >= lower) & (y_prob <= upper)
        if not mask.any():
            continue
        value += (mask.sum() / total) * abs(float(y_prob[mask].mean()) - float(y_true[mask].mean()))
    return value


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _format_value(value) for key, value in row.items()})


def _json_metrics(metrics: SimulationMetrics | None) -> dict[str, str | int | None]:
    if metrics is None:
        return {}
    return {key: _format_value(value) for key, value in asdict(metrics).items()}


def _format_value(value: object) -> str | int | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return f"{value:.12g}"
    return value


def _within_window(value: date, start: date, end: date | None) -> bool:
    if value < start:
        return False
    return end is None or value <= end


def _date_arg(value: str) -> date:
    return date.fromisoformat(value[:10])


def _decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
