"""Synthetic fixtures only: no real DB, sockets, subprocesses, or model imports."""

import builtins
from copy import deepcopy
import csv
import io
import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import apply_phase5c12_manual_fight_correction as correction  # noqa: E402


@pytest.fixture(autouse=True)
def forbid_real_access(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("real network/database/model access forbidden")
    for name in ("create_connection", "getaddrinfo", "gethostbyname"):
        monkeypatch.setattr(socket, name, denied)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"psycopg", "psycopg2", "sqlite3", "sqlalchemy", "warehouse", "sklearn", "torch", "xgboost", "ufc_scraper"}:
            denied()
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded)


def csv_bytes(rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode()


@pytest.fixture
def csv_case():
    fight = {"fight_id": correction.FIGHT_ID, "event_id": correction.EVENT_ID,
             "fighter_1_id": correction.FIGHTER_IDS[0], "fighter_2_id": correction.FIGHTER_IDS[1],
             "url": correction.FIGHT_URL, "scraped_at": "2026-08-08 23:38:48 UTC",
             "bout_type": "Middleweight Bout", "judge_1": "Unchanged, quoted", "referee": "Line1\nLine2",
             **{key: "" for key in correction.CSV_CHANGES}, "event_status": "upcoming"}
    event = {"event_id": correction.EVENT_ID, "url": correction.EVENT_URL, "event_status": "upcoming"}
    other = {**fight, "fight_id": "other", "url": "manual://other", "judge_1": 'A "quoted" judge'}
    fights, events = csv_bytes([other, fight, other]), csv_bytes([event])
    base = {"fights": {"row": fight, "input_csv_lineage": {"sha256": correction.sha256(fights)}},
            "events": {"row": event, "input_csv_lineage": {"sha256": correction.sha256(events)}}}
    proposal = {"proposed_fight_row": {**fight, **correction.CSV_CHANGES},
                "allowed_changed_fields": sorted(correction.CSV_CHANGES),
                "field_diff": [{"field": key, "before": fight[key], "after": value} for key, value in correction.CSV_CHANGES.items()],
                "identity_orientation": {"card_to_stored_permutation": [2, 1]},
                "proposed_parent_event_changes": [],
                "source_provenance": {"detail": {"capture_utc": None, "final_browser_url": None,
                    "http_status": None, "request_cache_provenance": None, "original_http_wire_bytes_verified": False}}}
    return fights, events, {"base": base, "proposal": proposal}


@pytest.fixture
def snapshot():
    row = {key: None for key in correction.FIGHT_COLUMNS}
    row.update({"fight_id": correction.FIGHT_ID, "event_id": correction.EVENT_ID,
                "fighter_1_id": correction.FIGHTER_IDS[0], "fighter_2_id": correction.FIGHTER_IDS[1],
                "source_url": correction.FIGHT_URL, "result_type": "upcoming", "weight_class": "middleweight",
                "is_title_fight": False, "is_interim_title": False, "scheduled_rounds": 3,
                "scraped_at": "2026-08-08T23:38:48+00:00"})
    types = {key: "text" for key in correction.FIGHT_COLUMNS}
    types.update({key: "uuid" for key in correction.FIGHT_COLUMNS if key.endswith("_id")})
    types.update({key: "boolean" for key in ("is_title_fight", "is_interim_title")})
    types.update({key: "smallint" for key in ("scheduled_rounds", "finish_round", "finish_time_seconds")})
    types["scraped_at"] = "timestamp with time zone"
    return {"database_identity_sha256": correction.sha256(json.dumps(["synthetic", "public", "127.0.0.1", 5432], separators=(",", ":")).encode()),
            "schema": [{"name": key, "type": types[key], "generated": "NEVER", "default": None, "nullable": "YES"} for key in correction.FIGHT_COLUMNS],
            "tables": [{"name": name, "kind": "r", "rls": False, "force_rls": False} for name in sorted(("fights", "events", "fighters", "fight_stats_aggregate", "fight_stats_by_round"))],
            "triggers": [{"internal": True, "name": "FK", "enabled": "O", "definition": "internal NO ACTION"}], "rules": [],
            "fights": [row], "events": [{"event_id": correction.EVENT_ID, "source_url": correction.EVENT_URL,
                "event_name": "UFC Fight Night: Du Plessis vs. Usman", "event_date": "2026-07-18", "event_status": "completed"}],
            "fighters": [{"fighter_id": fid, "source_url": url, "full_name": name} for fid, url, name in zip(correction.FIGHTER_IDS, correction.PROFILE_URLS, ("Jared Cannonier", "Christian Leroy Duncan"))],
            "statistics_aggregate": [], "statistics_by_round": [{"round": 1, "fighter_id": fid, "fight_id": correction.FIGHT_ID, "sig_strikes_landed": n} for n, fid in enumerate(correction.FIGHTER_IDS)]}


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.closed_cursors += 1

    def execute(self, sql, params=()):
        connection = self.connection
        connection.queries.append((sql, params))
        snapshot = connection.local
        if "SELECT current_database()" in sql:
            self.rows = [("synthetic", "public", "127.0.0.1", 5432)]
            return
        metadata = [("information_schema.columns", "schema"), ("pg_catalog.pg_class", "tables"),
                    ("pg_catalog.pg_trigger", "triggers"), ("pg_catalog.pg_rewrite", "rules"),
                    ("FROM public.fights f", "fights"), ("FROM public.events e", "events"),
                    ("FROM public.fighters p", "fighters"), ("FROM public.fight_stats_aggregate s", "statistics_aggregate"),
                    ("FROM public.fight_stats_by_round s", "statistics_by_round")]
        for marker, key in metadata:
            if marker in sql:
                self.rows = [(deepcopy(row),) for row in snapshot[key]]
                self.rowcount = len(self.rows)
                return
        if sql.startswith("UPDATE"):
            expected = tuple(snapshot["fights"][0][key] for key in correction.FIGHT_COLUMNS)
            if params[len(correction.DB_CHANGES):] != expected:
                self.rows, self.rowcount = [], 0
                return
            snapshot["fights"][0].update(correction.DB_CHANGES)
            returned = deepcopy(snapshot["fights"][0])
            if connection.store.returning_mutation:
                returned.update(connection.store.returning_mutation)
            if connection.store.after_mutation:
                snapshot["events"][0].update(connection.store.after_mutation)
            self.rows = [(returned,)]
            self.rowcount = connection.store.update_rowcount
            return
        self.rows, self.rowcount = [], 0

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, store):
        self.store = store
        self.local = deepcopy(store.committed)
        self.queries = []
        self.commits = self.rollbacks = self.closed_cursors = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def set_session(self, **settings):
        self.settings = settings

    def commit(self):
        self.commits += 1
        if self.store.commit_mode == "fail_before":
            raise RuntimeError("synthetic commit uncertainty")
        self.store.committed = deepcopy(self.local)
        if self.store.commit_mode == "fail_after":
            raise RuntimeError("synthetic lost acknowledgement")

    def rollback(self):
        self.rollbacks += 1
        self.local = deepcopy(self.store.committed)

    def close(self):
        self.closed = True
        if self.store.close_failure and self.commits:
            raise RuntimeError("synthetic close uncertainty")


class FakeStore:
    def __init__(self, snapshot):
        self.committed = deepcopy(snapshot)
        self.connections = []
        self.commit_mode = "success"
        self.returning_mutation = self.after_mutation = None
        self.update_rowcount = 1
        self.close_failure = self.reread_failure = False

    def connect(self):
        if self.reread_failure and self.connections:
            raise RuntimeError("synthetic reread unavailable")
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection


@pytest.fixture
def apply_case(tmp_path, snapshot):
    # Fixture creation itself uses the same exclusive writer, never a live CSV.
    correction.write_artifact(tmp_path, "synthetic.csv", b"synthetic corrected csv\r\n")
    return FakeStore(snapshot), tmp_path / "synthetic.csv", correction.sha256(b"synthetic corrected csv\r\n")


def apply(case, expected, **kwargs):
    store, path, pin = case
    return correction.apply_db_case(store.connect, expected, path, pin, **kwargs)


def test_pins_and_projections_are_case_scoped():
    assert correction.PROPOSAL_CHECKSUM_SHA == "e65b01dddfae1b4af42a63a1a2d4fe541b5b683bbc8e8cea6e6411819b9b40b2"
    assert correction.PROPOSAL_TOOL_SHA == "8c489087a4f2c7db326ea248da82eed26071f3065934a61890fea5a22abf9af4"
    assert len(correction.CSV_CHANGES) == 9 and len(correction.DB_CHANGES) == 6
    assert correction.DB_CHANGES["winner_fighter_id"] == correction.FIGHTER_IDS[1]
    assert correction.BOUNDARIES["source_capture_utc"] is None
    assert correction.BOUNDARIES["affirmative_non_title_evidence"] == "UNKNOWN"
    assert correction.BOUNDARIES["training_scoring_admission"] == "NOT_ADMISSIBLE"


@pytest.mark.parametrize("member", ["checksums.json", "review_tool.py"])
def test_bad_pins_refused_before_stored_code_execution(tmp_path, monkeypatch, member):
    correction.write_artifact(tmp_path, "checksums.json", b"index")
    correction.write_artifact(tmp_path, "review_tool.py", b"must never execute")
    monkeypatch.setattr(correction, "PROPOSAL_CHECKSUM_SHA", correction.sha256(b"index"))
    monkeypatch.setattr(correction, "PROPOSAL_TOOL_SHA", correction.sha256(b"must never execute"))
    monkeypatch.setattr(correction, "PROPOSAL_CHECKSUM_SHA" if member == "checksums.json" else "PROPOSAL_TOOL_SHA", "0" * 64)
    monkeypatch.setattr(correction.importlib.util, "spec_from_file_location", lambda *args: pytest.fail("executed before pins"))
    with pytest.raises(correction.Refusal, match="proposal_pin_mismatch"):
        correction.load_pinned_proposal(tmp_path)


def test_csv_exact_nine_changes_preserves_other_bytes_crlf_quotes_and_multiline(csv_case):
    fights, events, package = csv_case
    correction.check_proposal(package["base"], package["proposal"])
    plan = correction.plan_csv(fights, events, package)
    assert plan.before_bytes == fights
    assert plan.after_bytes[:plan.start] == fights[:plan.start]
    delta = len(plan.after_bytes) - len(fights)
    assert plan.after_bytes[plan.end + delta:] == fights[plan.end:]
    assert plan.after_bytes.count(b"\r\n") == fights.count(b"\r\n")
    assert {key for key in plan.before_row if plan.before_row[key] != plan.after_row[key]} == set(correction.CSV_CHANGES)
    for key in ("scraped_at", "referee", "judge_1", "fighter_1_id", "fighter_2_id"):
        assert plan.before_row[key] == plan.after_row[key]
    assert correction.plan_csv(plan.after_bytes, events, package).status == "VERIFIED_ALREADY_CORRECT"


@pytest.mark.parametrize("mutation", ["diff", "extra", "orientation", "parent", "capture"])
def test_proposal_drift_refused(csv_case, mutation):
    package = deepcopy(csv_case[2])
    proposal = package["proposal"]
    if mutation == "diff":
        proposal["field_diff"][0]["after"] = "W"
    elif mutation == "extra":
        proposal["proposed_fight_row"]["judge_1"] = "new"
    elif mutation == "orientation":
        proposal["identity_orientation"]["card_to_stored_permutation"] = [1, 2]
    elif mutation == "parent":
        proposal["proposed_parent_event_changes"] = ["completed"]
    else:
        proposal["source_provenance"]["detail"]["capture_utc"] = "2026-07-18"
    with pytest.raises(correction.Refusal):
        correction.check_proposal(package["base"], proposal)


@pytest.mark.parametrize("mutation", ["duplicate", "conflict", "non_target", "parent"])
def test_csv_changed_duplicate_and_conflicting_states_refused(csv_case, mutation):
    fights, events, package = csv_case
    if mutation == "duplicate":
        target = correction.selected_csv_record(fights, "fight_id", correction.FIGHT_ID, correction.FIGHT_URL)
        fights += fights[target["start"]:target["end"]]
    elif mutation == "conflict":
        fights = fights.replace(b",upcoming,", b",draw,")
    elif mutation == "non_target":
        fights = fights.replace(b'manual://other', b'manual://changed', 1)
    else:
        events = events.replace(b"upcoming", b"completed")
    with pytest.raises(correction.Refusal):
        correction.plan_csv(fights, events, package)


def test_success_commits_once_six_columns_and_full_cas_then_rereads(apply_case, snapshot):
    before = deepcopy(snapshot)
    result = apply(apply_case, snapshot)
    store = apply_case[0]
    assert result["status"] == "VERIFIED_CORRECTION_COMMITTED" and result["commit_attempts"] == 1
    assert result["independent_after"] == correction.corrected_snapshot(before)
    assert snapshot == before
    writer, reader = store.connections
    assert writer.commits == 1 and writer.rollbacks == 0 and reader.rollbacks == 1
    assert all(connection.closed for connection in store.connections)
    assert reader.settings["readonly"] is True
    updates = [(sql, params) for sql, params in writer.queries if sql.startswith("UPDATE")]
    assert len(updates) == 1
    sql, params = updates[0]
    set_clause = sql.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
    assert set_clause.count("= %s") == 6 and sql.count("IS NOT DISTINCT FROM") == 17
    assert params[:6] == tuple(correction.DB_CHANGES.values())
    assert params[6:] == tuple(before["fights"][0][key] for key in correction.FIGHT_COLUMNS)
    assert "RETURNING to_jsonb(f)" in sql
    lock_index = next(i for i, (query, _) in enumerate(writer.queries) if query.startswith("LOCK TABLE"))
    schema_index = next(i for i, (query, _) in enumerate(writer.queries) if "information_schema.columns" in query)
    assert lock_index < schema_index
    for connection in store.connections:
        assert any("statement_timeout" in query for query, _ in connection.queries)


def test_existing_parent_profiles_stats_title_and_scrape_are_unchanged(apply_case, snapshot):
    snapshot["fights"][0]["is_title_fight"] = True  # Preserve actual value, never infer/default it.
    apply_case[0].committed = deepcopy(snapshot)
    result = apply(apply_case, snapshot)
    after = result["independent_after"]
    for key in ("events", "fighters", "statistics_aggregate", "statistics_by_round"):
        assert after[key] == snapshot[key]
    for key in set(correction.FIGHT_COLUMNS) - set(correction.DB_CHANGES):
        assert after["fights"][0][key] == snapshot["fights"][0][key]
    assert after["events"][0]["event_status"] == "completed"
    assert "database_identity" not in after


def test_already_correct_is_verified_noop(apply_case, snapshot):
    expected = correction.corrected_snapshot(snapshot)
    apply_case[0].committed = deepcopy(expected)
    result = apply(apply_case, expected)
    assert result["status"] == "VERIFIED_ALREADY_CORRECT"
    assert result["changed_columns"] == [] and result["commit_attempts"] == 0
    assert all(connection.commits == 0 for connection in apply_case[0].connections)
    assert not any(query.startswith("UPDATE") for query, _ in apply_case[0].connections[0].queries)


@pytest.mark.parametrize("column,value", [("winner_fighter_id", correction.FIGHTER_IDS[0]), ("result_type", "draw"), ("finish_method", "ko_tko")])
def test_conflicting_resolved_before_refused_without_connection(apply_case, snapshot, column, value):
    snapshot["fights"][0][column] = value
    with pytest.raises(correction.Refusal, match="conflicting_or_already_resolved"):
        apply(apply_case, snapshot)
    assert apply_case[0].connections == []


@pytest.mark.parametrize("drift", ["fight", "parent", "profile", "statistics", "duplicate", "trigger", "schema"])
def test_live_drift_rolls_back_closes_and_does_not_write(apply_case, snapshot, drift):
    current = apply_case[0].committed
    if drift == "fight":
        current["fights"][0]["scraped_at"] = "later"
    elif drift == "parent":
        current["events"][0]["event_status"] = "upcoming"
    elif drift == "profile":
        current["fighters"][0]["full_name"] = "different"
    elif drift == "statistics":
        current["statistics_by_round"][0]["sig_strikes_landed"] = 99
    elif drift == "duplicate":
        current["fights"].append(deepcopy(current["fights"][0]))
    elif drift == "trigger":
        current["triggers"].append({"internal": False})
    else:
        current["schema"][0]["name"] = "changed"
    with pytest.raises(correction.Refusal):
        apply(apply_case, snapshot)
    connection = apply_case[0].connections[0]
    assert connection.rollbacks == 1 and connection.commits == 0 and connection.closed
    assert not any(query.startswith("UPDATE") for query, _ in connection.queries)


@pytest.mark.parametrize("rowcount", [0, 2])
def test_cas_rowcount_mismatch_aborts(apply_case, snapshot, rowcount):
    apply_case[0].update_rowcount = rowcount
    with pytest.raises(correction.Refusal, match="rowcount"):
        apply(apply_case, snapshot)
    assert apply_case[0].connections[0].rollbacks == 1
    assert apply_case[0].committed == snapshot


@pytest.mark.parametrize("mutation", [{"is_title_fight": True}, {"scraped_at": "later"}, {"fighter_1_id": correction.FIGHTER_IDS[1]}])
def test_returning_non_allowlisted_changes_abort(apply_case, snapshot, mutation):
    apply_case[0].returning_mutation = mutation
    with pytest.raises(correction.Refusal, match="returning_non_allowlisted"):
        apply(apply_case, snapshot)
    assert apply_case[0].committed == snapshot and apply_case[0].connections[0].closed


def test_after_parent_drift_aborts_before_commit(apply_case, snapshot):
    apply_case[0].after_mutation = {"event_status": "upcoming"}
    with pytest.raises(correction.Refusal, match="after_state"):
        apply(apply_case, snapshot)
    assert apply_case[0].committed == snapshot


@pytest.mark.parametrize("mode,status", [("fail_after", "VERIFIED_COMMIT_BY_REREAD"), ("fail_before", "VERIFIED_NOT_COMMITTED")])
def test_ambiguous_commit_resolved_only_by_independent_reread(apply_case, snapshot, mode, status):
    store = apply_case[0]
    store.commit_mode = mode
    result = apply(apply_case, snapshot)
    assert result["status"] == status and len(store.connections) == 2
    assert store.connections[0].commits == 1 and store.connections[0].rollbacks == 0
    assert all(connection.closed for connection in store.connections)
    assert not any(query.startswith("UPDATE") for query, _ in store.connections[1].queries)


def test_close_error_after_commit_is_resolved_by_reread(apply_case, snapshot):
    apply_case[0].close_failure = True
    result = apply(apply_case, snapshot)
    assert result["status"] == "VERIFIED_COMMIT_BY_REREAD"
    assert result["events"][-1]["close_error_type"] == "RuntimeError"


def test_commit_and_reread_uncertainty_never_claims_success_or_compensates(apply_case, snapshot):
    store = apply_case[0]
    store.commit_mode, store.reread_failure = "fail_after", True
    result = apply(apply_case, snapshot)
    assert result["status"] == "COMMIT_OUTCOME_UNRESOLVED" and result["independent_after"] is None
    assert store.connections[0].rollbacks == 0
    assert apply_case[1].read_bytes() == b"synthetic corrected csv\r\n"


def test_file_hash_refuses_before_connect_and_again_before_commit(apply_case, snapshot, monkeypatch):
    store, path, pin = apply_case
    with pytest.raises(correction.Refusal, match="file_expected_hash"):
        correction.apply_db_case(store.connect, snapshot, path, "0" * 64)
    assert store.connections == []
    original = correction.require_file_hash
    calls = []

    def changing_guard(*args):
        calls.append(args)
        if len(calls) == 3:
            raise correction.Refusal("file_expected_hash_mismatch")
        return original(*args)
    monkeypatch.setattr(correction, "require_file_hash", changing_guard)
    with pytest.raises(correction.Refusal, match="file_expected_hash"):
        apply(apply_case, snapshot)
    assert store.connections[0].rollbacks == 1 and store.connections[0].commits == 0
    assert store.committed == snapshot


def test_exclusive_durable_artifacts_and_journal_before_commit(tmp_path, apply_case, snapshot):
    receipt = correction.write_artifact(tmp_path, "before.json", snapshot)
    assert receipt["sha256"] == correction.sha256(correction.json_bytes(snapshot))
    with pytest.raises(FileExistsError):
        correction.write_artifact(tmp_path, "before.json", {"replacement": True})
    with pytest.raises(correction.Refusal):
        correction.write_artifact(tmp_path, "../escape.json", {})
    journal = []

    def record(event):
        receipt = correction.write_artifact(tmp_path, f"state-{len(journal):02d}.json", event)
        journal.append(receipt)
    result = apply(apply_case, snapshot, journal=record)
    assert len(journal) == 4 and result["events"][-1]["state"] == "VERIFIED_CORRECTION_COMMITTED"
    assert all(event["audit_utc"].endswith("Z") for event in result["events"])
    assert result["source_capture_utc"] is None and result["v1"] == "ENFORCED"
    assert result["production_models"] == "UNCHANGED" and result["historical_comparison"] == "STILL_BLOCKED"


def test_journal_failure_before_commit_aborts_without_commit(apply_case, snapshot):
    def fail(event):
        if event["state"] == "COMMIT_ATTEMPT":
            raise OSError("synthetic disk failure")
    with pytest.raises(OSError):
        apply(apply_case, snapshot, journal=fail)
    assert apply_case[0].committed == snapshot
    assert apply_case[0].connections[0].commits == 0 and apply_case[0].connections[0].rollbacks == 1
