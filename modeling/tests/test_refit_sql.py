"""Verify SQL label filtering in a disposable server, never the warehouse."""

from datetime import date
import os
from pathlib import Path
import shutil
import subprocess
import uuid

import psycopg2
import pytest

from modeling.data import load_bout_data
from modeling.holdout import load_holdout_fight_ids


def test_sql_cutoff_excludes_original_alias_and_future_before_label_fetch(tmp_path):
    candidates = sorted(Path("/usr/lib/postgresql").glob("*/bin/initdb"))
    prior_binary = Path("/tmp/ufc-phase2-postgres-bin/extracted/usr/lib/postgresql/15/bin/initdb")
    initdb = (os.environ.get("PHASE3A_TEST_INITDB") or shutil.which("initdb") or
              (str(candidates[-1]) if candidates else None) or
              (str(prior_binary) if prior_binary.is_file() else None))
    if initdb is None:
        pytest.skip("Disposable PostgreSQL unavailable")
    pg_ctl = str(Path(initdb).with_name("pg_ctl"))
    cluster, socket = tmp_path / "cluster", tmp_path / "socket"
    socket.mkdir()
    subprocess.run([initdb, "-D", str(cluster), "-A", "trust", "-U", "phase3a_test"],
                   check=True, capture_output=True, timeout=30)
    started = False
    try:
        subprocess.run([pg_ctl, "-D", str(cluster), "-l", str(tmp_path / "server.log"),
                        "-o", f"-F -h '' -k {socket} -p 55440", "-w", "start"],
                       check=True, capture_output=True, timeout=30)
        started = True
        conn = psycopg2.connect(host=str(socket), port=55440, dbname="postgres", user="phase3a_test")
        try:
            exclusions = load_holdout_fight_ids()
            held = sorted(exclusions)[:2]
            eligible, same_day, future = [str(uuid.UUID(int=i)) for i in (10, 11, 12)]
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE bout_features (
                    fight_id uuid, fighter_1_id uuid, fighter_2_id uuid,
                    event_date date, weight_class text, label double precision,
                    both_debuting boolean, feature_version smallint, computed_at timestamptz)""")
                for fid, day, label in [(eligible, date(2026, 3, 30), 1),
                                         (same_day, date(2026, 3, 31), 0.5),
                                         (future, date(2026, 4, 1), 0.5),
                                         (held[0], date(2026, 3, 1), 0), (held[1], date(2026, 3, 1), 0)]:
                    cur.execute("INSERT INTO bout_features VALUES (%s,%s,%s,%s,'lightweight',%s,NULL,2,now())",
                                (fid, str(uuid.UUID(int=1)), str(uuid.UUID(int=2)), day, label))
            conn.commit()
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
            df = load_bout_data(conn, ["both_debuting"], event_cutoff="2026-03-31",
                                excluded_fight_ids=exclusions, include_metadata=True)
            assert list(df.fight_id.map(str)) == [eligible]
            assert list(df.label) == [1]
            assert df.both_debuting.isna().all()
        finally:
            conn.rollback()
            conn.close()
    finally:
        if started:
            subprocess.run([pg_ctl, "-D", str(cluster), "-m", "immediate", "-w", "stop"],
                           check=True, capture_output=True, timeout=30)
