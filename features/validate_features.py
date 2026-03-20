"""Feature quality and distribution checks.

Validates feature distributions, missingness rates, and correlations
against the homelab DB. This is an informational report, not a hard gate.

Usage:
    python features/validate_features.py
    # or: make validate_features
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_numeric_columns(cur, table: str) -> list[str]:
    """Return nullable numeric/smallint column names for a table."""
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s
          AND data_type IN ('numeric', 'smallint')
          AND is_nullable = 'YES'
        ORDER BY ordinal_position
    """, (table,))
    return [r[0] for r in cur.fetchall()]


def _print_header(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


# ── Checks ───────────────────────────────────────────────────────────────────

def check_row_counts(cur) -> None:
    _print_header("Row counts")

    cur.execute("SELECT count(*) FROM fights")
    n_fights = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM fighter_snapshots")
    n_snap = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM bout_features")
    n_bout = cur.fetchone()[0]

    expected_snap = 2 * n_fights
    snap_ok = "OK" if n_snap == expected_snap else "MISMATCH"
    bout_ok = "OK" if n_bout == n_fights else "MISMATCH"

    print(f"  fights:            {n_fights:>8,}")
    print(f"  fighter_snapshots: {n_snap:>8,}  (expected {expected_snap:,}) [{snap_ok}]")
    print(f"  bout_features:     {n_bout:>8,}  (expected {n_fights:,}) [{bout_ok}]")


def check_missingness(cur, table: str, columns: list[str]) -> None:
    _print_header(f"Missingness — {table}")

    cur.execute(f"SELECT count(*) FROM {table}")
    total = cur.fetchone()[0]
    if total == 0:
        print("  (no rows)")
        return

    warnings = []
    print(f"  {'column':<45s} {'null%':>7s}  {'nulls':>8s} / {total:,}")
    print(f"  {'-'*45} {'-'*7}  {'-'*8}")

    for col in columns:
        cur.execute(f"SELECT count(*) FROM {table} WHERE {col} IS NULL")
        n_null = cur.fetchone()[0]
        pct = 100.0 * n_null / total
        flag = " ⚠ >50%" if pct > 50 else ""
        print(f"  {col:<45s} {pct:6.1f}%  {n_null:>8,}{flag}")
        if pct > 50:
            warnings.append(col)

    if warnings:
        print(f"\n  ⚠ High missingness (>50%): {', '.join(warnings)}")


def check_distributions(cur, table: str, columns: list[str]) -> None:
    _print_header(f"Distribution — {table}")

    zero_var = []
    print(f"  {'column':<40s} {'mean':>10s} {'std':>10s} {'min':>10s} "
          f"{'p5':>10s} {'p95':>10s} {'max':>10s}")
    print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    for col in columns:
        cur.execute(f"""
            SELECT
                avg({col})::numeric(12,4),
                stddev({col})::numeric(12,4),
                min({col})::numeric(12,4),
                max({col})::numeric(12,4),
                percentile_cont(0.05) WITHIN GROUP (ORDER BY {col})::numeric(12,4),
                percentile_cont(0.95) WITHIN GROUP (ORDER BY {col})::numeric(12,4)
            FROM {table}
            WHERE {col} IS NOT NULL
        """)
        row = cur.fetchone()
        if row[0] is None:
            print(f"  {col:<40s}  (all NULL)")
            continue

        mean, std, mn, mx, p5, p95 = row
        std = std or 0
        flag = ""
        if float(std) == 0:
            flag = " ⚠ zero-var"
            zero_var.append(col)

        print(f"  {col:<40s} {float(mean):10.4f} {float(std):10.4f} "
              f"{float(mn):10.4f} {float(p5):10.4f} {float(p95):10.4f} "
              f"{float(mx):10.4f}{flag}")

    if zero_var:
        print(f"\n  ⚠ Zero variance: {', '.join(zero_var)}")


def check_label_correlation(cur) -> None:
    _print_header("Correlation with label — bout_features")

    columns = _get_numeric_columns(cur, "bout_features")
    columns = [c for c in columns if c != "label"]

    suspicious = []
    print(f"  {'column':<45s} {'pearson_r':>10s}")
    print(f"  {'-'*45} {'-'*10}")

    for col in columns:
        cur.execute(f"""
            SELECT corr({col}::float, label::float)::numeric(8,4)
            FROM bout_features
            WHERE {col} IS NOT NULL AND label IS NOT NULL
        """)
        r = cur.fetchone()[0]
        if r is None:
            print(f"  {col:<45s}       (n/a)")
            continue
        flag = ""
        if abs(float(r)) > 0.5:
            flag = " ⚠ suspicious"
            suspicious.append((col, float(r)))
        print(f"  {col:<45s} {float(r):10.4f}{flag}")

    if suspicious:
        print(f"\n  ⚠ |r| > 0.5 (potential leakage?):")
        for col, r in suspicious:
            print(f"    {col}: r={r:.4f}")
    else:
        print(f"\n  No features with |r| > 0.5")


def check_feature_completeness(cur) -> None:
    _print_header("Feature completeness — bout_features core diffs")

    core_diffs = [
        "diff_elo", "diff_career_wins", "diff_career_fights",
        "diff_career_win_rate", "diff_career_finish_rate",
        "diff_career_sig_strikes_landed_pm", "diff_career_sig_strike_accuracy",
        "diff_career_takedown_accuracy", "diff_career_control_rate",
    ]

    cond = " AND ".join(f"{c} IS NOT NULL" for c in core_diffs)
    cur.execute(f"""
        SELECT
            count(*) FILTER (WHERE {cond}) AS complete,
            count(*) AS total
        FROM bout_features
    """)
    complete, total = cur.fetchone()
    pct = 100.0 * complete / total if total else 0
    print(f"  Rows with all core diffs non-NULL: {complete:,} / {total:,} ({pct:.1f}%)")

    # Breakdown: debut vs experienced
    cur.execute(f"""
        SELECT
            count(*) FILTER (WHERE {cond}) AS complete,
            count(*) AS total
        FROM bout_features
        WHERE both_debuting = false
    """)
    c2, t2 = cur.fetchone()
    pct2 = 100.0 * c2 / t2 if t2 else 0
    print(f"  Non-debut bouts:                   {c2:,} / {t2:,} ({pct2:.1f}%)")

    cur.execute(f"""
        SELECT
            count(*) FILTER (WHERE {cond}) AS complete,
            count(*) AS total
        FROM bout_features
        WHERE both_debuting = true
    """)
    c3, t3 = cur.fetchone()
    pct3 = 100.0 * c3 / t3 if t3 else 0
    print(f"  Both-debuting bouts:               {c3:,} / {t3:,} ({pct3:.1f}%)")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        check_row_counts(cur)

        snap_cols = _get_numeric_columns(cur, "fighter_snapshots")
        bout_cols = _get_numeric_columns(cur, "bout_features")

        check_missingness(cur, "fighter_snapshots", snap_cols)
        check_missingness(cur, "bout_features", bout_cols)

        check_distributions(cur, "fighter_snapshots", snap_cols)
        check_distributions(cur, "bout_features", bout_cols)

        check_label_correlation(cur)
        check_feature_completeness(cur)

        print(f"\n{'=' * 70}")
        print("  Feature validation complete.")
        print(f"{'=' * 70}\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
