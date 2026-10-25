"""Offline checks: synthetic bundles, fake connections and stubbed make only."""
import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
import re

import pytest

from scripts.automation.prepare_runtime import DATA_FILES, InputError, prepare_runtime
from scripts.automation.run_refresh import COMMAND, run_command, run_refresh

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_TABLES = set(re.findall(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
    "\n".join(path.read_text() for path in (ROOT / "warehouse/sql").glob("*.sql")),
    flags=re.IGNORECASE,
))


def bundle(tmp_path):
    source_data = tmp_path / "inputs/data"
    source_models = tmp_path / "inputs/models"
    for name, columns in DATA_FILES.items():
        path = source_data / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=sorted(columns))
            writer.writeheader()
            writer.writerow({name: "synthetic-value" for name in columns})
    artifact = source_models / "xgb/selected"
    artifact.mkdir(parents=True)
    (artifact / "model.joblib").write_bytes(b"synthetic-model-never-deserialize")
    (artifact / "metadata.json").write_text(json.dumps({"feature_cols": ["synthetic_feature"]}))
    (artifact / "supporting.json").write_text('{"preserve": true}')
    (source_models / "production_model.json").write_text(json.dumps({
        "selected_model": "Synthetic", "artifact_path": "/old/checkout/models/xgb/selected"}))
    repo = tmp_path / "repo"
    repo.mkdir()
    return repo, source_data, source_models


def wrapper_workspace(tmp_path):
    runtime = tmp_path / "repo"
    scripts = runtime / "scripts/automation"
    scripts.mkdir(parents=True)
    for source in (ROOT / "scripts/automation").glob("*.py"):
        shutil.copy2(source, scripts)
    shutil.copy2(ROOT / "scripts/automation/refresh.sh", scripts)
    binary = tmp_path / "bin"
    binary.mkdir()
    env = dict(os.environ, PATH=f"{binary}:{os.environ['PATH']}",
               MAKE_CALLS=str(tmp_path / "calls"), AUTOMATION_IMAGE_REVISION="test-revision")
    return scripts / "refresh.sh", binary, env


@pytest.mark.parametrize("exit_code", [0, 37])
def test_strict_wrapper_logs_and_preserves_child_status(tmp_path, exit_code):
    script, binary, env = wrapper_workspace(tmp_path)
    python = binary / "python3"
    python.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$MAKE_CALLS"\nexit "${CHILD_EXIT_CODE}"\n')
    python.chmod(0o755)
    env["CHILD_EXIT_CODE"] = str(exit_code)
    result = subprocess.run([str(script)], env=env, text=True, capture_output=True)
    assert result.returncode == exit_code
    assert Path(env["MAKE_CALLS"]).read_text() == "scripts/automation/run_refresh.py\n"
    assert "revision=test-revision command_version=v1" in result.stdout
    assert "automation start=" in result.stdout
    assert f"exit_code={exit_code}" in result.stdout
    assert f"status={'succeeded' if exit_code == 0 else 'failed'}" in result.stdout


def test_preparation_preserves_source_and_remaps_only_copy(tmp_path):
    repo, data, models = bundle(tmp_path)
    before = {p.relative_to(models): p.read_bytes() for p in models.rglob("*") if p.is_file()}
    (models / "production_model.json").chmod(0o444)
    counts = prepare_runtime(repo, data, models)
    assert counts == {name: 1 for name in DATA_FILES}
    assert json.loads((repo / "models/production_model.json").read_text())["artifact_path"] == str(repo / "models/xgb/selected")
    assert before == {p.relative_to(models): p.read_bytes() for p in models.rglob("*") if p.is_file()}
    for name in ("model.joblib", "metadata.json", "supporting.json"):
        assert (repo / "models/xgb/selected" / name).read_bytes() == (models / "xgb/selected" / name).read_bytes()
    # A repeat run reuses private copies; no source pointer changes or training.
    assert prepare_runtime(repo, data, models) == counts


@pytest.mark.parametrize("missing", ["events.csv", "odds/fight_odds.csv", "model.joblib", "metadata.json"])
def test_preparation_rejects_missing_files(tmp_path, missing):
    repo, data, models = bundle(tmp_path)
    path = models / "xgb/selected" / missing if missing.endswith(("joblib", "json")) else data / missing
    path.unlink()
    with pytest.raises((InputError, FileNotFoundError)):
        prepare_runtime(repo, data, models)
    assert not (repo / "models/production_model.json").exists()


@pytest.mark.parametrize("artifact_path", ["/old/models/../outside", "/old/not-models/run", "../models/run"])
def test_preparation_rejects_escaping_model_pointer(tmp_path, artifact_path):
    repo, data, models = bundle(tmp_path)
    (models / "production_model.json").write_text(json.dumps({"selected_model": "test", "artifact_path": artifact_path}))
    with pytest.raises(InputError):
        prepare_runtime(repo, data, models)


def test_preparation_rejects_source_overlap_and_links(tmp_path):
    repo, data, models = bundle(tmp_path)
    with pytest.raises(InputError):
        prepare_runtime(data.parent, data, models)
    (data / "link").symlink_to(models / "production_model.json")
    with pytest.raises(InputError):
        prepare_runtime(repo, data, models)


def test_preflight_cli_uses_no_database_or_make(tmp_path):
    script, binary, env = wrapper_workspace(tmp_path)
    _, data, models = bundle(tmp_path / "bundle")
    # Both shims fail loudly if a forbidden operation is attempted.
    (binary / "make").write_text('#!/bin/sh\necho forbidden-make >&2\nexit 98\n')
    (binary / "make").chmod(0o755)
    env.update(DATABASE_URL="invalid://must-not-connect", POSTGRES_HOST="must-not-connect")
    result = subprocess.run([str(script), "--preflight-only", "--data-source", str(data),
                             "--models-source", str(models)], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "preflight=succeeded" in result.stdout
    assert "forbidden-make" not in result.stderr
    assert not (script.parents[2] / "data/reports/automation").exists()


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, args=()):
        sql = " ".join(query.split())
        db = self.db
        if "INSERT INTO automation_refresh_runs" in sql:
            if db.fail_start or (db.finished and db.fail_finish):
                raise ConnectionError("postgresql://user:password-must-not-log@secret-host/db")
            db.audits.append({"run_id": args[0], "status": args[3], "exit_code": args[7],
                              "counts": json.loads(args[8]), "errors": json.loads(args[9])})
        elif "SELECT e.event_id FROM events e" in sql:
            self.rows = [("synthetic-event",)] if db.future else []
        elif "count(*) FILTER" in sql:
            assert len(args) == 3
            self.rows = [(2, db.predictions, db.recommendations)]
        elif "GROUP BY report_key" in sql:
            assert "imported_at >= %s" in sql
            self.rows = [(key, 1) for key in db.reports]
        elif "report_key = 'current_bfo_mean'" in sql:
            self.rows = [(db.current_summary,)]
        elif "WHERE imported_at >= %s" in sql:
            self.rows = [(3,)]
        elif sql.startswith("SELECT count(*) FROM "):
            table = sql.split()[-1]
            assert table in WAREHOUSE_TABLES, "Precheck must query a declared warehouse table"
            self.rows = [(0 if table == db.empty_table else (11 if db.finished else 10),)]
        elif "LIMIT 0" in sql:
            self.rows = []
        else:
            raise AssertionError(f"Unexpected fake SQL: {sql}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeDB:
    def __init__(self, *, future=True):
        self.audits = []
        self.future = future
        self.finished = False
        self.empty_table = None
        self.fail_start = self.fail_finish = False
        self.predictions = self.recommendations = 2
        self.current_summary = 1
        self.reports = ["default_policy", "conservative_candidate"]
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def invoke(tmp_path, db, *, make_code=0):
    repo, data, models = bundle(tmp_path)
    calls = []
    def runner(command, cwd):
        calls.append((command, cwd))
        db.finished = True
        return make_code
    code = run_refresh(repo, data, models, tmp_path / "audit", connect=lambda: db, runner=runner)
    records = list((tmp_path / "audit").glob("*.json"))
    assert len(records) == 1
    return code, json.loads(records[0].read_text()), calls


@pytest.mark.parametrize("future", [True, False])
def test_success_audits_counts_future_card_and_unique_repeat_runs(tmp_path, future):
    db = FakeDB(future=future)
    code, record, calls = invoke(tmp_path / "first", db)
    assert code == 0
    assert record["status"] == "succeeded" and record["durable_audit"]
    assert calls == [(COMMAND, tmp_path / "first/repo")]
    assert [row["status"] for row in db.audits] == ["running", "succeeded"]
    counts = record["counts"]
    assert counts["input_rows"] == {name: 1 for name in DATA_FILES}
    assert counts["fights_updated"] is None and counts["odds_rows_loaded"] is None
    assert counts["table_count_net_changes"]["events"] == 1
    assert counts["betting_report_rows_imported_since_start"] == 6
    assert counts["current_card"]["status"] == ("present" if future else "no_future_card")
    # No bet count is required: complete recommendation/pass rows satisfy coverage.
    assert record["finished_at"] >= record["started_at"]
    assert len(record["disabled_scrapers"]) == 3
    assert db.closed
    _, repeated, _ = invoke(tmp_path / "second", FakeDB(future=future))
    assert repeated["run_id"] != record["run_id"]


def test_make_failure_preserves_original_code_and_failure_audit(tmp_path):
    code, record, _ = invoke(tmp_path, FakeDB(), make_code=37)
    assert code == 37 and record["exit_code"] == 37
    assert record["status"] == "failed"
    assert record["errors"][0]["stage"] == "make_update_dashboard_data"
    assert "table_counts_after" not in record["counts"]


@pytest.mark.parametrize("fault", ["missing_report", "missing_prediction", "missing_recommendation", "missing_current_summary", "empty_output"])
def test_postcheck_failures_are_failed_runs(tmp_path, fault):
    db = FakeDB()
    if fault == "missing_report":
        db.reports = ["default_policy"]
    elif fault == "missing_prediction":
        db.predictions = 1
    elif fault == "missing_recommendation":
        db.recommendations = 1
    elif fault == "missing_current_summary":
        db.current_summary = 0
    else:
        db.empty_table = "fight_odds"
    code, record, calls = invoke(tmp_path, db)
    assert code != 0 and calls
    assert record["status"] == "failed"
    assert record["errors"][0]["stage"] == "warehouse_postcheck"
    assert "table_counts_after" in record["counts"]
    assert db.audits[-1]["status"] == "failed"


def test_precheck_failure_prevents_make(tmp_path):
    db = FakeDB()
    db.empty_table = "bout_features"
    code, record, calls = invoke(tmp_path, db)
    assert code != 0 and calls == []
    assert record["errors"][0]["stage"] == "warehouse_precheck"


def test_missing_input_records_failure_before_make(tmp_path):
    db = FakeDB()
    repo = tmp_path / "repo"
    code = run_refresh(repo, tmp_path / "missing-data", tmp_path / "missing-models",
                       tmp_path / "audit", connect=lambda: db,
                       runner=lambda *args, **kwargs: pytest.fail("make must not run"))
    assert code != 0
    assert [r["status"] for r in db.audits] == ["running", "failed"]
    assert db.audits[-1]["errors"][0]["stage"] == "input_preflight"


def test_database_outage_creates_sanitized_local_receipt(tmp_path, capsys):
    def unavailable():
        raise ConnectionError("postgresql://user:password-must-not-log@secret-host/db")
    code = run_refresh(tmp_path, tmp_path / "data", tmp_path / "models", tmp_path / "audit",
                       connect=unavailable, runner=lambda *args, **kwargs: pytest.fail("make must not run"))
    record = json.loads(next((tmp_path / "audit").glob("*.json")).read_text())
    assert code != 0 and record["status"] == "failed" and not record["durable_audit"]
    assert record["errors"] == [{"stage": "database_connect", "category": "ConnectionError"}]
    assert "password-must-not-log" not in capsys.readouterr().out + json.dumps(record)


@pytest.mark.parametrize("failure", ["start", "finish"])
def test_audit_write_failure_is_not_success(tmp_path, failure, capsys):
    db = FakeDB()
    setattr(db, f"fail_{failure}", True)
    code, record, calls = invoke(tmp_path, db)
    assert code != 0 and record["status"] == "failed" and not record["durable_audit"]
    assert bool(calls) == (failure == "finish")
    assert "password-must-not-log" not in capsys.readouterr().out + json.dumps(record)


@pytest.mark.parametrize("exit_code", [0, 37])
def test_real_command_runner_uses_only_fixed_serial_make(tmp_path, monkeypatch, capsys, exit_code):
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "make"
    stub.write_text('#!/bin/sh\nprintf "%s\\n" "$*" > "$CALLS"\nprintf "%s\\n" "$DATABASE_URL"\nexit "$STUB_CODE"\n')
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", str(binary) + ":" + os.environ["PATH"])
    monkeypatch.setenv("CALLS", str(tmp_path / "calls"))
    monkeypatch.setenv("STUB_CODE", str(exit_code))
    monkeypatch.setenv("DATABASE_URL", "postgresql://synthetic-secret")
    assert run_command(COMMAND, tmp_path) == exit_code
    assert (tmp_path / "calls").read_text() == "-j1 update_dashboard_data\n"
    output = capsys.readouterr().out
    assert "[REDACTED]" in output and "synthetic-secret" not in output


def test_repreparation_removes_stale_generated_files_in_private_copy(tmp_path):
    repo, data, models = bundle(tmp_path)
    prepare_runtime(repo, data, models)
    stale = repo / "data/reports/obsolete.csv"
    stale.parent.mkdir()
    stale.write_text("stale report must not survive a fresh run")
    prepare_runtime(repo, data, models)
    assert not stale.exists()
    assert (repo / "data/events.csv").is_file()


def test_preparation_refuses_to_overwrite_existing_host_data(tmp_path):
    repo, data, models = bundle(tmp_path)
    existing = repo / "data/events.csv"
    existing.parent.mkdir()
    existing.write_text("operator-owned-data")
    with pytest.raises(InputError, match="nonempty_runtime_destination"):
        prepare_runtime(repo, data, models)
    assert existing.read_text() == "operator-owned-data"
    assert not (repo / "models").exists()


def test_final_receipt_failure_corrects_durable_status(tmp_path, monkeypatch, capsys):
    import scripts.automation.run_refresh as runtime
    db = FakeDB()
    original = runtime.receipt
    attempts = []
    def fail_final(record, directory):
        attempts.append(record["status"])
        if record["status"] == "succeeded":
            raise PermissionError("sensitive-error-details-must-not-log")
        original(record, directory)
    monkeypatch.setattr(runtime, "receipt", fail_final)
    code, _, _ = invoke(tmp_path, db)
    assert code != 0
    assert attempts[-1] == "succeeded"
    assert db.audits[-1]["status"] == "failed"
    assert db.audits[-1]["errors"][-1]["stage"] == "local_receipt"
    output = capsys.readouterr().out
    assert "status=failed exit_code=1" in output
    assert "sensitive-error-details-must-not-log" not in output


def test_audit_migration_is_new_and_structurally_constrained():
    sql = (ROOT / "warehouse/sql/021_automation_refresh_runs.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS automation_refresh_runs" in sql
    for field in ("run_id uuid PRIMARY KEY", "started_at timestamptz NOT NULL", "finished_at timestamptz",
                  "counts jsonb", "errors jsonb", "disabled_scrapers jsonb", "command_version text NOT NULL"):
        assert field in sql
    assert "status IN ('running', 'succeeded', 'failed')" in sql
    assert "status = 'running' AND finished_at IS NULL AND exit_code IS NULL" in sql


def test_command_runner_redacts_uri_decoded_credentials(tmp_path, monkeypatch, capsys):
    stub = tmp_path / "make"
    stub.write_text('#!/bin/sh\nprintf "%s\\n" "connection user=example-user password=escaped@password"\n')
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("DATABASE_URL", "postgresql://example-user:escaped%40password@host/db")
    assert run_command(COMMAND, tmp_path) == 0
    output = capsys.readouterr().out
    assert "example-user" not in output and "escaped@password" not in output
