"""Load selected betting backtest report CSVs into Postgres.

This makes dashboard deployment independent from local CSV paths. Re-running the
loader replaces each configured report snapshot transactionally.

Usage:
    python warehouse/load_betting_reports.py
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping

from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_ROOT = REPO_ROOT / "data" / "reports"


@dataclass(frozen=True)
class BettingReportSource:
    report_key: str
    label: str
    source: str
    summary_path: Path
    fights_path: Path


DEFAULT_REPORTS = (
    BettingReportSource(
        report_key="default_policy",
        label="Default Policy",
        source="honest",
        summary_path=REPORT_ROOT / "betting_default" / "betting_backtest_summary.csv",
        fights_path=REPORT_ROOT / "betting_default" / "betting_backtest_fights.csv",
    ),
    BettingReportSource(
        report_key="conservative_candidate",
        label="Conservative Candidate",
        source="honest",
        summary_path=REPORT_ROOT / "betting_conservative_candidate" / "betting_backtest_summary.csv",
        fights_path=REPORT_ROOT / "betting_conservative_candidate" / "betting_backtest_fights.csv",
    ),
    BettingReportSource(
        report_key="kaggle_research",
        label="Kaggle Research",
        source="research",
        summary_path=REPORT_ROOT / "kaggle_research_backtest_summary.csv",
        fights_path=REPORT_ROOT / "kaggle_research_backtest_fights.csv",
    ),
)

SUMMARY_COLUMNS = [
    "report_key",
    "label",
    "source",
    "summary_type",
    "group_name",
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
    "report_generated_at",
    "imported_at",
]

FIGHT_COLUMNS = [
    "report_key",
    "label",
    "source",
    "row_number",
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
    "report_generated_at",
    "imported_at",
]


def load_betting_reports(conn, reports: Iterable[BettingReportSource] = DEFAULT_REPORTS) -> None:
    imported_at = datetime.now(timezone.utc)

    with conn:
        with conn.cursor() as cur:
            for report in reports:
                report_generated_at = _report_timestamp(report)
                summary_rows = [
                    _summary_row(report, row, report_generated_at, imported_at)
                    for row in _read_csv(report.summary_path)
                ]
                fight_rows = [
                    _fight_row(report, row_number, row, report_generated_at, imported_at)
                    for row_number, row in enumerate(_read_csv(report.fights_path), start=1)
                ]

                cur.execute("DELETE FROM betting_report_summaries WHERE report_key = %s", (report.report_key,))
                cur.execute("DELETE FROM betting_report_fights WHERE report_key = %s", (report.report_key,))

                _insert_rows(cur, "betting_report_summaries", SUMMARY_COLUMNS, summary_rows)
                _insert_rows(cur, "betting_report_fights", FIGHT_COLUMNS, fight_rows)

                bet_count = sum(1 for row in fight_rows if row["decision"] == "bet")
                print(
                    f"  loaded {report.label}: "
                    f"{len(summary_rows)} summary rows, {len(fight_rows)} fight rows, {bet_count} bets"
                )


def _insert_rows(cur, table: str, columns: list[str], rows: list[dict]) -> None:
    if not rows:
        return

    column_sql = ", ".join(f'"{column}"' for column in columns)
    values = [tuple(row[column] for column in columns) for row in rows]
    execute_values(cur, f"INSERT INTO {table} ({column_sql}) VALUES %s", values)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing report CSV: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _report_timestamp(report: BettingReportSource) -> datetime:
    latest_mtime = max(report.summary_path.stat().st_mtime, report.fights_path.stat().st_mtime)
    return datetime.fromtimestamp(latest_mtime, timezone.utc)


def _summary_row(
    report: BettingReportSource,
    row: Mapping[str, str],
    report_generated_at: datetime,
    imported_at: datetime,
) -> dict:
    return {
        "report_key": report.report_key,
        "label": report.label,
        "source": report.source,
        "summary_type": _text(row, "summary_type"),
        "group_name": _text(row, "group"),
        "total_bets": _int(row, "total_bets"),
        "wins": _int(row, "wins"),
        "losses": _int(row, "losses"),
        "pushes": _int(row, "pushes"),
        "total_staked": _decimal(row, "total_staked"),
        "profit_loss": _decimal(row, "profit_loss"),
        "roi": _optional_decimal(row, "roi"),
        "hit_rate": _optional_decimal(row, "hit_rate"),
        "average_odds": _optional_decimal(row, "average_odds"),
        "max_drawdown": _decimal(row, "max_drawdown"),
        "starting_bankroll": _decimal(row, "starting_bankroll"),
        "ending_bankroll": _decimal(row, "ending_bankroll"),
        "odds_policy": _text(row, "odds_policy"),
        "require_odds_before_prediction": _bool(row, "require_odds_before_prediction", default=True),
        "max_one_bet_per_fight": _bool(row, "max_one_bet_per_fight", default=True),
        "kelly_fraction": _decimal(row, "kelly_fraction"),
        "min_edge": _decimal(row, "min_edge"),
        "min_ev": _decimal(row, "min_ev"),
        "max_single_bet_fraction": _decimal(row, "max_single_bet_fraction"),
        "max_event_fraction": _decimal(row, "max_event_fraction"),
        "medium_tier_cap": _optional_decimal(row, "medium_tier_cap"),
        "high_tier_cap": _optional_decimal(row, "high_tier_cap"),
        "toss_up_tier_cap": _optional_decimal(row, "toss_up_tier_cap"),
        "drawdown_protection_threshold": _optional_decimal(row, "drawdown_protection_threshold"),
        "report_generated_at": report_generated_at,
        "imported_at": imported_at,
    }


def _fight_row(
    report: BettingReportSource,
    row_number: int,
    row: Mapping[str, str],
    report_generated_at: datetime,
    imported_at: datetime,
) -> dict:
    return {
        "report_key": report.report_key,
        "label": report.label,
        "source": report.source,
        "row_number": row_number,
        "event_id": _text(row, "event_id"),
        "event_name": _text(row, "event_name"),
        "event_date": _optional_text(row, "event_date"),
        "fight_id": _text(row, "fight_id"),
        "fighter_id": _text(row, "fighter_id"),
        "fighter_name": _text(row, "fighter_name"),
        "opponent_fighter_id": _text(row, "opponent_fighter_id"),
        "opponent_fighter_name": _text(row, "opponent_fighter_name"),
        "bookmaker": _text(row, "bookmaker"),
        "market": _text(row, "market"),
        "line_type": _text(row, "line_type"),
        "odds_timestamp": _optional_text(row, "odds_timestamp"),
        "scored_at": _optional_text(row, "scored_at"),
        "model_probability": _decimal(row, "model_probability"),
        "market_implied_probability": _decimal(row, "market_implied_probability"),
        "no_vig_market_probability": _decimal(row, "no_vig_market_probability"),
        "edge": _decimal(row, "edge"),
        "edge_bucket": _text(row, "edge_bucket"),
        "ev_per_unit": _decimal(row, "ev_per_unit"),
        "offered_decimal_odds": _decimal(row, "offered_decimal_odds"),
        "decision": _text(row, "decision"),
        "recommended_fighter_id": _text(row, "recommended_fighter_id"),
        "recommended_fighter_name": _text(row, "recommended_fighter_name"),
        "confidence_tier": _text(row, "confidence_tier"),
        "reason_codes": _text(row, "reason_codes"),
        "full_kelly_fraction": _decimal(row, "full_kelly_fraction"),
        "fractional_kelly_fraction": _decimal(row, "fractional_kelly_fraction"),
        "final_stake_fraction": _decimal(row, "final_stake_fraction"),
        "stake_amount": _decimal(row, "stake_amount"),
        "bet_result": _text(row, "bet_result"),
        "profit_loss_amount": _decimal(row, "profit_loss_amount"),
        "bankroll_before_event": _decimal(row, "bankroll_before_event"),
        "bankroll_after_event": _decimal(row, "bankroll_after_event"),
        "peak_bankroll": _decimal(row, "peak_bankroll"),
        "drawdown": _decimal(row, "drawdown"),
        "max_drawdown": _decimal(row, "max_drawdown"),
        "actual_winner_fighter_id": _text(row, "actual_winner_fighter_id"),
        "actual_winner_name": _text(row, "actual_winner_name"),
        "result_type": _text(row, "result_type"),
        "resolved": _bool(row, "resolved", default=False),
        "detail_mode": _text(row, "detail_mode"),
        "odds_policy": _text(row, "odds_policy"),
        "require_odds_before_prediction": _bool(row, "require_odds_before_prediction", default=True),
        "max_one_bet_per_fight": _bool(row, "max_one_bet_per_fight", default=True),
        "starting_bankroll": _decimal(row, "starting_bankroll"),
        "ending_bankroll": _decimal(row, "ending_bankroll"),
        "kelly_fraction": _decimal(row, "kelly_fraction"),
        "min_edge": _decimal(row, "min_edge"),
        "min_ev": _decimal(row, "min_ev"),
        "max_single_bet_fraction": _decimal(row, "max_single_bet_fraction"),
        "max_event_fraction": _decimal(row, "max_event_fraction"),
        "medium_tier_cap": _optional_decimal(row, "medium_tier_cap"),
        "high_tier_cap": _optional_decimal(row, "high_tier_cap"),
        "toss_up_tier_cap": _optional_decimal(row, "toss_up_tier_cap"),
        "drawdown_protection_threshold": _optional_decimal(row, "drawdown_protection_threshold"),
        "report_generated_at": report_generated_at,
        "imported_at": imported_at,
    }


def _text(row: Mapping[str, str], column: str) -> str:
    return (row.get(column) or "").strip()


def _optional_text(row: Mapping[str, str], column: str) -> str | None:
    return _text(row, column) or None


def _int(row: Mapping[str, str], column: str) -> int:
    text = _text(row, column)
    return int(text) if text else 0


def _decimal(row: Mapping[str, str], column: str) -> Decimal:
    text = _text(row, column)
    return Decimal(text) if text else Decimal("0")


def _optional_decimal(row: Mapping[str, str], column: str) -> Decimal | None:
    text = _text(row, column)
    return Decimal(text) if text else None


def _bool(row: Mapping[str, str], column: str, *, default: bool) -> bool:
    text = _text(row, column).lower()
    if not text:
        return default
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    raise ValueError(f"invalid boolean for {column}: {text}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load betting report CSVs into the warehouse.")
    parser.parse_args(argv)

    conn = get_connection()
    try:
        load_betting_reports(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
