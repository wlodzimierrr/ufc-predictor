"""Migrations run in a disposable local PostgreSQL cluster, never the warehouse."""

from datetime import date, timedelta
from pathlib import Path
import os
import shutil
import subprocess
import uuid

import numpy as np
import pandas as pd
import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from modeling.decisions import DECISION_FIELDS, decide_prediction, decision_metrics
from predict import _database_rows
from warehouse.db import upsert

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
OLD_VIEWS = (
    "latest_predictions", "current_event_predictions", "pre_event_prediction_fights",
    "pre_event_prediction_events",
)


def signatures(cur):
    cur.execute("""
        SELECT table_name, ordinal_position, column_name, data_type, udt_name
        FROM information_schema.columns WHERE table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
    """, (list(OLD_VIEWS),))
    return cur.fetchall()


@pytest.fixture(scope="module")
def disposable_database(tmp_path_factory):
    binaries = sorted(Path("/usr/lib/postgresql").glob("*/bin/initdb"))
    initdb = os.environ.get("PHASE2_TEST_INITDB") or shutil.which("initdb") or (str(binaries[-1]) if binaries else None)
    if initdb is None:
        pytest.skip("Disposable PostgreSQL unavailable: initdb not installed")
    pg_ctl = str(Path(initdb).with_name("pg_ctl"))
    root = tmp_path_factory.mktemp("phase2_postgres")
    data, socket = root / "data", root / "socket"
    socket.mkdir()
    subprocess.run([initdb, "-D", str(data), "-A", "trust", "-U", "phase2_test"],
                   check=True, capture_output=True, timeout=30)
    started = False
    try:
        subprocess.run([pg_ctl, "-D", str(data), "-l", str(root / "server.log"),
                        "-o", f"-F -h '' -k {socket} -p 55439", "-w", "start"],
                       check=True, capture_output=True, timeout=30)
        started = True
        params = {"host": str(socket), "port": 55439, "dbname": "postgres", "user": "phase2_test"}
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                paths = sorted(SQL_DIR.glob("*.sql"))
                # Existing FK creation needs fighters before fights on a fresh DB.
                ordered = [SQL_DIR / "001_events.sql", SQL_DIR / "003_fighters.sql", SQL_DIR / "002_fights.sql"]
                ordered += [p for p in paths if 4 <= int(p.name[:3]) <= 18]
                for path in ordered:
                    cur.execute(path.read_text())
                before = signatures(cur)
                _seed_legacy_predictions(cur)
                cur.execute("SELECT row_to_json(p) FROM predictions p ORDER BY fight_id")
                prediction_before = cur.fetchall()
                cur.execute((SQL_DIR / "019_prediction_decisions.sql").read_text())
                assert signatures(cur) == before, "Old view column names/types/order changed"
                cur.execute("""SELECT row_to_json(p)::jsonb - ARRAY[
                    'predicted_prob_f1_full', 'calibrated_prob_f1_full', 'decision_status',
                    'is_actionable', 'pick_label', 'pick_winner_name', 'uncertainty_reasons',
                    'decision_policy_version', 'decision_origin'] FROM predictions p ORDER BY fight_id""")
                assert cur.fetchall() == prediction_before, "Migration rewrote historical rows"
        yield params
    finally:
        if started:
            subprocess.run([pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop"],
                           check=True, capture_output=True, timeout=30)


@pytest.fixture()
def db(disposable_database):
    conn = psycopg2.connect(**disposable_database)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def _seed_legacy_predictions(cur):
    event_id = str(uuid.UUID(int=1))
    f1, f2 = str(uuid.UUID(int=2)), str(uuid.UUID(int=3))
    tomorrow = date.today() + timedelta(days=1)
    cur.execute("INSERT INTO events (event_id,event_name,event_date,event_status,source_url) VALUES (%s,'Test Card',%s,'upcoming','fixture')", (event_id, tomorrow))
    for fid, name in ((f1, "One"), (f2, "Two")):
        cur.execute("INSERT INTO fighters (fighter_id, full_name, source_url) VALUES (%s,%s,'fixture')", (fid, name))
    for i, (p, label) in enumerate(zip((.2, .4, .5, .6, .8), (0, 1, 0, None, 1))):
        fight_id = str(uuid.UUID(int=10+i))
        winner = None if label is None else (f1 if label == 1 else f2)
        cur.execute("""INSERT INTO fights (fight_id,event_id,fighter_1_id,fighter_2_id,result_type,winner_fighter_id,source_url)
            VALUES (%s,%s,%s,%s,%s,%s,'fixture')""", (fight_id,event_id,f1,f2,'upcoming' if label is None else 'win',winner))
        cur.execute("""INSERT INTO predictions (fight_id,event_date,fighter_1_id,fighter_2_id,fighter_1_name,fighter_2_name,
            predicted_prob_f1,calibrated_prob_f1,confidence_tier,is_uncertain,model_name,scored_at)
            VALUES (%s,%s,%s,%s,'One','Two',%s,%s,%s,%s,'fixture',now())""",
            (fight_id,tomorrow,f1,f2,p,p,'high' if p in (.2,.8) else 'toss-up',.4 <= p <= .6))


@pytest.mark.parametrize("p", [
    0,.3,.4,.5,.6,.7,1,
    np.nextafter(.4,0),np.nextafter(.4,1),np.nextafter(.6,0),np.nextafter(.6,1),
    .39999,.40001,.59999,.60001,
])
def test_python_sql_policy_matches_including_rounding(db, p):
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM probability_band_v1(%s,'One','Two')", (float(p),))
        assert dict(cur.fetchone()) == decide_prediction(p, "One", "Two")


@pytest.mark.parametrize("p", [None,float('nan'),float('inf'),float('-inf'),-.01,1.01])
def test_sql_invalid_probabilities_raise(db, p):
    with db.cursor() as cur:
        with pytest.raises(psycopg2.Error, match="finite and within"):
            cur.execute("SELECT * FROM probability_band_v1(%s,'One','Two')", (p,))


def test_legacy_derived_metadata_and_sql_denominators(db):
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM pre_event_prediction_fight_decisions ORDER BY fight_id")
        rows = [dict(row) for row in cur.fetchall()]
        assert all(row["decision_origin"] == "derived_from_legacy_probability" for row in rows)
        assert all(row["pick_correct"] is None for row in rows if not row["is_actionable"])
        cur.execute("SELECT * FROM pre_event_prediction_event_decisions")
        aggregate = dict(cur.fetchone())
        expected = decision_metrics(pd.DataFrame(rows).assign(calibrated_prob_f1=[r["calibrated_prob_f1_full"] for r in rows]))
        for key, value in expected.items():
            assert (aggregate[key] is None) if value is None else float(aggregate[key]) == pytest.approx(value)
        cur.execute("SELECT decision_policy_version FROM predictions")
        assert all(row["decision_policy_version"] is None for row in cur.fetchall())


@pytest.mark.parametrize("p", [np.nextafter(.4,0),.5,np.nextafter(.6,1)])
def test_future_writes_preserve_precise_decisions_and_nullable_fields(db, p):
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM predictions ORDER BY fight_id LIMIT 1")
        source = dict(cur.fetchone())
    source["scored_at"] = "2098-01-01T12:00:00+00:00"
    source["calibrated_prob_f1"] = p
    source["decision_origin"] = "recorded_at_scoring"
    source["decision_policy_version"] = "probability_band_v1"
    record = _database_rows(pd.DataFrame([source]))[0]
    upsert(db, "predictions", [record], pk_columns=["fight_id", "scored_at"])
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM latest_prediction_decisions WHERE fight_id=%s", (source["fight_id"],))
        row = dict(cur.fetchone())
        assert row["calibrated_prob_f1_full"] == p
        assert {key: row[key] for key in DECISION_FIELDS} == decide_prediction(p, "One", "Two")
        if p != .5:
            assert float(row["calibrated_prob_f1"]) in (.4,.6)  # rounded legacy projection


def test_inconsistent_metadata_rejected(db):
    with db.cursor() as cur:
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute("UPDATE predictions SET calibrated_prob_f1_full=.5,decision_policy_version='probability_band_v1' WHERE calibrated_prob_f1=.5")


def test_sql_all_no_pick_and_pending_accuracy_null(db):
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("UPDATE predictions SET calibrated_prob_f1=.5")
        cur.execute("SELECT * FROM pre_event_prediction_event_decisions")
        row = cur.fetchone()
        assert row["actionable_count"] == 0 and row["actionable_accuracy"] is None
        assert row["log_loss"] is not None and row["brier_score"] is not None
        cur.execute("UPDATE fights SET winner_fighter_id=NULL,result_type='upcoming'")
        cur.execute("SELECT * FROM pre_event_prediction_event_decisions")
        row = cur.fetchone()
        assert row["resolved_count"] == 0 and row["latent_accuracy"] is None
        assert row["log_loss"] is row["brier_score"] is None


def test_review_persistence_and_reporting_use_precise_values(db, monkeypatch):
    import modeling.post_event_review as review
    import modeling.build_pre_event_prediction_log as report

    class BorrowedConnection:
        # Keep the test transaction isolated despite production helpers closing
        # connections; all writes are rolled back by the disposable-db fixture.
        def cursor(self, *args, **kwargs):
            return db.cursor(*args, **kwargs)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def close(self):
            pass

    monkeypatch.setattr(review, "get_connection", BorrowedConnection)
    monkeypatch.setattr(report, "get_connection", BorrowedConnection)
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM predictions ORDER BY fight_id LIMIT 1")
        source = dict(cur.fetchone())
    p = np.nextafter(.6,1)
    reviewed = pd.DataFrame([{
        **source, "fight_id": str(uuid.UUID(int=999)), "actual_fight_id": None,
        "fighter_1": "One", "fighter_2": "Two", "actual_label": 1,
        "calibrated_prob_f1": p, "predicted_label": 1, "predicted_winner_name": "One",
        "predicted_correct": True, "resolved": True, "decision_policy_version": None,
        "decision_origin": None, "scored_at": None,
    }])
    summary = {
        "event_name": "Catchup Card", "event_date": str(source["event_date"]),
        "review_type": "catchup_scored_before_result_load", "model_name": "fixture",
        "n_fights": 1, "accuracy": 1, "decision_policy_version": "probability_band_v1",
        **decision_metrics(reviewed),
    }
    review._upsert_review_fights(summary, reviewed, "")
    review._upsert_review_summary(summary)
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM reviewed_prediction_fights WHERE fight_id=%s", (str(uuid.UUID(int=999)),))
        row = cur.fetchone()
        assert row["calibrated_prob_f1_full"] == p and row["pick_label"] == 1
        assert row["decision_origin"] == "derived_from_legacy_probability"
        cur.execute("SELECT * FROM reviewed_prediction_event_decisions WHERE event_name='Catchup Card'")
        assert cur.fetchone()["actionable_accuracy"] == 1
    fights = report._build_fight_log_from_database()
    catchup = fights[fights["fight_id"] == str(uuid.UUID(int=999))].iloc[0]
    assert catchup["calibrated_prob_f1"] == p and catchup["pick_label"] == 1
    assert catchup["pre_event_evidence"] == "catchup_scored_before_result_load"
    assert catchup["decision_origin"] == "derived_from_legacy_probability"
