"""Fixed dashboard refresh orchestration with durable audit and local receipts."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4
from urllib.parse import unquote, urlsplit

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.automation.prepare_runtime import prepare_runtime

COMMAND = ("make", "-j1", "update_dashboard_data")
COMMAND_VERSION = "v1"
DISABLED_SCRAPERS = [
    {"command": "make refresh_scrape", "reason": "headless_source_readiness_not_approved"},
    {"command": "make post_event", "reason": "includes_unapproved_results_acquisition"},
    {"command": "make refresh_current_odds", "reason": "headless_bfo_readiness_not_approved"},
]
TABLES = ("events", "fights", "fight_odds", "betting_report_summaries", "betting_report_fights")


class RefreshFailure(RuntimeError):
    """Failure codes are fixed labels, never raw database/input error messages."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def receipt(record: dict, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / f"{record['run_id']}.json"
    temporary = path.with_suffix(".json.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        os.chmod(temporary, 0o600)
        json.dump(record, stream, indent=2, sort_keys=True)
        stream.write("\n")
    temporary.replace(path)


def persist(conn, record: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO automation_refresh_runs (
                run_id, started_at, finished_at, status, image_revision,
                git_revision, command_version, exit_code, counts, errors, disabled_scrapers
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at = EXCLUDED.finished_at, status = EXCLUDED.status,
                exit_code = EXCLUDED.exit_code, counts = EXCLUDED.counts,
                errors = EXCLUDED.errors
        """, (record["run_id"], record["started_at"], record["finished_at"], record["status"],
              record["image_revision"], record["git_revision"], record["command_version"],
              record["exit_code"], json.dumps(record["counts"]), json.dumps(record["errors"]),
              json.dumps(record["disabled_scrapers"])))
    conn.commit()


def table_counts(conn) -> dict[str, int]:
    counts = {}
    with conn.cursor() as cur:
        for table in TABLES:  # Fixed internal identifiers, never CLI-provided SQL.
            cur.execute(f"SELECT count(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
    return counts


def precheck(conn) -> dict[str, int]:
    counts = table_counts(conn)
    with conn.cursor() as cur:
        for table in ("fighters", "fight_stats_aggregate", "bout_features"):
            cur.execute(f"SELECT count(*) FROM {table}")
            if cur.fetchone()[0] == 0:
                raise RefreshFailure("warehouse_prerequisites_empty")
        # Fail before make when required migrated views are absent.
        cur.execute("SELECT fight_id FROM current_event_predictions LIMIT 0")
        cur.execute("SELECT fight_id FROM pre_event_prediction_fights LIMIT 0")
        cur.execute("SELECT fight_id FROM pre_event_prediction_fight_decisions LIMIT 0")
    conn.commit()
    return counts


def postcheck(conn, started_at: str, measured: dict | None = None) -> dict:
    measured = measured if measured is not None else {}
    counts = table_counts(conn)
    measured["table_counts_after"] = counts
    if any(counts[name] == 0 for name in ("events", "fights", "fight_odds")):
        raise RefreshFailure("warehouse_output_empty")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT report_key, count(*) FROM betting_report_summaries
            WHERE report_key IN ('default_policy', 'conservative_candidate')
              AND imported_at >= %s GROUP BY report_key
        """, (started_at,))
        summaries = dict(cur.fetchall())
        measured["fresh_report_summary_rows"] = summaries
        if not all(summaries.get(name, 0) > 0 for name in ("default_policy", "conservative_candidate")):
            raise RefreshFailure("required_report_summaries_missing_or_stale")
        cur.execute("""
            SELECT count(*) FROM betting_report_summaries WHERE imported_at >= %s
        """, (started_at,))
        imported_summaries = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM betting_report_fights WHERE imported_at >= %s", (started_at,))
        imported_fights = cur.fetchone()[0]
        measured["betting_report_rows_imported_since_start"] = imported_summaries + imported_fights
        cur.execute("""
            SELECT e.event_id FROM events e
            WHERE e.event_status = 'upcoming' AND e.event_date >= CURRENT_DATE
              AND EXISTS (SELECT 1 FROM fights f WHERE f.event_id = e.event_id AND f.result_type = 'upcoming')
            ORDER BY e.event_date, e.event_id LIMIT 1
        """)
        next_event = cur.fetchone()
        card = {"status": "no_future_card", "event_id": None, "expected_fights": 0}
        measured["current_card"] = card
        if next_event:
            event_id = str(next_event[0])
            cur.execute("""
                SELECT count(*),
                    count(*) FILTER (WHERE EXISTS (
                        SELECT 1 FROM current_event_predictions p
                        WHERE p.fight_id = f.fight_id AND p.scored_at >= %s)),
                    count(*) FILTER (WHERE EXISTS (
                        SELECT 1 FROM betting_report_fights r
                        WHERE r.report_key = 'current_bfo_mean' AND r.fight_id = f.fight_id::text
                          AND r.event_id = f.event_id::text AND r.imported_at >= %s))
                FROM fights f WHERE f.event_id = %s AND f.result_type = 'upcoming'
            """, (started_at, started_at, event_id))
            expected, predictions, recommendations = cur.fetchone()
            card = {"status": "present", "event_id": event_id, "expected_fights": expected,
                    "prediction_fights": predictions, "recommendation_fights": recommendations}
            measured["current_card"] = card
            if expected == 0 or predictions != expected or recommendations != expected:
                raise RefreshFailure("future_card_coverage_missing_or_stale")
            cur.execute("""
                SELECT count(*) FROM betting_report_summaries
                WHERE report_key = 'current_bfo_mean' AND imported_at >= %s
            """, (started_at,))
            if cur.fetchone()[0] == 0:
                raise RefreshFailure("current_report_summary_missing_or_stale")
    conn.commit()
    return measured


def run_command(command, cwd: Path) -> int:
    # Stream progress while suppressing secret values in any downstream output.
    secrets = [os.environ.get(name, "") for name in
               ("DATABASE_URL", "POSTGRES_PASSWORD", "PGPASSWORD", "POSTGRES_USER", "PGUSER")]
    try:
        connection_uri = urlsplit(os.environ.get("DATABASE_URL", ""))
        for value in (connection_uri.username, connection_uri.password):
            if value:
                secrets.extend((value, unquote(value)))
    except ValueError:
        pass
    secrets = sorted(set(secrets), key=len, reverse=True)
    with subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True) as process:
        for line in process.stdout:
            for secret in secrets:
                if secret:
                    line = line.replace(secret, "[REDACTED]")
            print(line, end="", flush=True)
        code = process.wait()
    return 128 - code if code < 0 else code


def run_refresh(repo: Path, source_data: Path, source_models: Path, audit_dir: Path,
                *, connect=None, runner=None) -> int:
    record = {"run_id": str(uuid4()), "started_at": utc_now(), "finished_at": None,
              "status": "running", "exit_code": None,
              "image_revision": os.environ.get("AUTOMATION_IMAGE_REVISION", os.environ.get("IMAGE_REVISION", "unknown")),
              "git_revision": os.environ.get("AUTOMATION_GIT_REVISION", "unknown"),
              "command_version": COMMAND_VERSION, "counts": {}, "errors": [],
              "disabled_scrapers": DISABLED_SCRAPERS, "durable_audit": False}
    print(f"automation run_id={record['run_id']} command_version={COMMAND_VERSION}", flush=True)
    for scraper in DISABLED_SCRAPERS:
        print(f"automation scraper=disabled command={scraper['command']!r} reason={scraper['reason']}", flush=True)
    conn = None
    code = 0
    stage = "local_receipt"
    try:
        receipt(record, audit_dir)
        stage = "database_connect"
        if connect is None:
            from warehouse.db import get_connection
            connect = get_connection
        conn = connect()
        stage = "audit_start"
        persist(conn, record)
        record["durable_audit"] = True
        stage = "input_preflight"
        record["counts"]["input_rows"] = prepare_runtime(repo, source_data, source_models)
        # Existing loaders do not report exact affected-row counts to this process.
        record["counts"].update(fights_updated=None, odds_rows_loaded=None)
        stage = "warehouse_precheck"
        record["counts"]["table_counts_before"] = precheck(conn)
        receipt(record, audit_dir)
        stage = "make_update_dashboard_data"
        print("automation step=make_update_dashboard_data jobs=1", flush=True)
        code = (runner or run_command)(COMMAND, cwd=repo)
        if code:
            raise RefreshFailure("refresh_command_failed")
        stage = "warehouse_postcheck"
        postcheck(conn, record["started_at"], record["counts"])
        before = record["counts"]["table_counts_before"]
        after = record["counts"]["table_counts_after"]
        record["counts"]["table_count_net_changes"] = {name: after[name] - before[name] for name in TABLES}
    except Exception as exc:
        code = code or 1
        error = {"stage": stage, "category": type(exc).__name__}
        if isinstance(exc, RefreshFailure):
            error["code"] = str(exc)
        record["errors"].append(error)
        print(f"automation failure_stage={stage} error_category={type(exc).__name__}", flush=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    record.update(status="failed" if code else "succeeded", exit_code=code, finished_at=utc_now())
    if conn:
        try:
            persist(conn, record)
            record["durable_audit"] = True
        except Exception as exc:
            code = code or 1
            record.update(status="failed", exit_code=code, durable_audit=False)
            record["errors"].append({"stage": "audit_finish", "category": type(exc).__name__})
            print(f"automation failure_stage=audit_finish error_category={type(exc).__name__}", flush=True)
    try:
        receipt(record, audit_dir)
    except Exception as exc:
        code = code or 1
        record.update(status="failed", exit_code=code, finished_at=utc_now())
        record["errors"].append({"stage": "local_receipt", "category": type(exc).__name__})
        print(f"automation failure_stage=local_receipt error_category={type(exc).__name__}", flush=True)
        # A failed final receipt must not leave the durable audit claiming success.
        if conn:
            try:
                conn.rollback()
                persist(conn, record)
                record["durable_audit"] = True
            except Exception as audit_exc:
                record["durable_audit"] = False
                print(f"automation failure_stage=audit_correction error_category={type(audit_exc).__name__}", flush=True)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    print(f"automation run_id={record['run_id']} status={record['status']} exit_code={code}", flush=True)
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-only", action="store_true", help="Copy and validate inputs without DB access or make")
    parser.add_argument("--data-source", type=Path, default=Path(os.environ.get("AUTOMATION_DATA_SOURCE", "/runtime-inputs/data")))
    parser.add_argument("--models-source", type=Path, default=Path(os.environ.get("AUTOMATION_MODELS_SOURCE", "/runtime-inputs/models")))
    parser.add_argument("--audit-dir", type=Path, default=None)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    if args.preflight_only:
        try:
            counts = prepare_runtime(repo, args.data_source, args.models_source)
        except Exception as exc:
            print(f"automation preflight=failed error_category={type(exc).__name__}", flush=True)
            return 1
        print("automation preflight=succeeded input_rows=" + json.dumps(counts, sort_keys=True), flush=True)
        return 0
    audit_dir = args.audit_dir or Path(os.environ.get("AUTOMATION_AUDIT_DIR", "/tmp/automation-audit"))
    return run_refresh(repo, args.data_source, args.models_source, audit_dir)


if __name__ == "__main__":
    raise SystemExit(main())
