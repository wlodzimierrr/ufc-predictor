"""Mock/disposable Phase5A resources only. Never use the live integration test."""

from copy import deepcopy
from datetime import date, datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.tests.test_replay import synthetic_source
from modeling import phase5_current_data as current
from modeling import prospective_registry as registry
from modeling.holdout import load_holdout_fight_ids
from modeling.refit_preflight import DEBUT_COLS, FEATURE_ORDER, PreflightError, ROOT, json_bytes, sha256
from tools import prepare_phase5a_prospective_shadow as command

CUTOFF = "2026-10-02"
COMPLETED = "2026-10-02T10:00:00+00:00"


def source():
    data = synthetic_source()
    days = ["2020-01-01", "2025-09-01", "2025-10-03", "2026-01-03", "2026-04-03", "2026-07-03", "2026-09-03", "2026-10-02"]
    for e, day in zip(data.events, days):
        e["event_date"] = date.fromisoformat(day)
        e["scraped_at"] = datetime(2026, 10, 1, tzinfo=timezone.utc)
    for f in data.fights:
        if f["event_id"] == data.events[-1]["event_id"]:
            f.update(result_type="upcoming", winner_fighter_id=None)
    for s in data.fight_stats:
        s["sig_strikes_attempted"] = 1000
    return data


def metadata():
    return {"feature_cols": FEATURE_ORDER, "feature_version": 2, "debut_priors_applied": True,
            "val_date": "2022-03-12", "test_date": "2024-03-09", "train_rows": 10, "val_rows": 2}


def sources():
    data = source()
    schemas = []
    tables = {"events": data.events, "fighters": data.fighters, "fights": data.fights,
              "fight_stats_aggregate": data.fight_stats}
    for name, rows in tables.items():
        for key in rows[0]:
            typ = "date" if key in {"event_date", "dob"} else "timestamp with time zone" if key == "scraped_at" else "numeric" if key in {"height_cm", "reach_cm"} else "text"
            schemas.append({"table_name": name, "column_name": key, "data_type": typ})
    p = {f"sources/{name}.json": current.table_bytes(rows) for name,rows in tables.items()}
    p["sources/schemas.json"] = current.table_bytes(schemas)
    base = current.prepare_current(data, CUTOFF)[0].iloc[0].to_dict()
    base = {k: None if pd.isna(v) else v for k,v in base.items()}
    base.pop("event_id")
    base["event_date"] = "2020-01-01"
    base.update(source_event_id="event-0", source_fighter_1_id=base["fighter_1_id"],
                source_fighter_2_id=base["fighter_2_id"], source_event_date=base["event_date"],
                source_result_type="win", source_winner_fighter_id=base["fighter_2_id"],
                computed_at="2026-10-01T10:00:00+00:00")
    ref = [base, {**base, "fight_id": "cal-0", "event_date": "2023-01-01", "source_event_date": "2023-01-01"},
           {**base, "fight_id": "cal-1", "event_date": "2023-02-01", "source_event_date": "2023-02-01", "label": 1,
            "source_winner_fighter_id": base["fighter_1_id"]}]
    p["reference_bootstrap/stored_features.json"] = current.table_bytes(ref)
    return p


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def execute(self, sql, params=None):
        self.conn.calls.append((sql,params))
        if sql == "SHOW transaction_read_only":
            rows = [{"transaction_read_only": self.conn.readonly}]
        elif sql == "SHOW transaction_isolation":
            rows = [{"transaction_isolation": "repeatable read"}]
        elif "transaction_timestamp" in sql:
            rows = [{"transaction_started_at": COMPLETED, "database_capture_start": COMPLETED,
                     "database_timezone": "UTC", "transaction_snapshot": "1:2:"}]
        elif sql == current.SCHEMA_QUERY:
            rows = json.loads(self.conn.payloads["sources/schemas.json"])
        elif sql == current.REFERENCE_QUERY:
            assert params == (date(2024,3,9), sorted(load_holdout_fight_ids()))
            rows = json.loads(self.conn.payloads["reference_bootstrap/stored_features.json"])
        elif "database_capture_end" in sql:
            rows = [{"database_capture_end": COMPLETED}]
        elif sql.startswith("SELECT * FROM public."):
            table = sql.split('"')[1]
            rows = json.loads(self.conn.payloads[f"sources/{table}.json"])
        else:
            raise AssertionError("Unexpected/write query: " + sql)
        self.description = [(k,) for k in rows[0]]
        self.rows = [tuple(r[k] for k in rows[0]) for r in rows]
    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, readonly="on"):
        self.readonly = readonly
        self.payloads = sources()
        self.calls = []
        self.rolled_back = self.closed = False
    def cursor(self):
        return FakeCursor(self)
    def set_session(self, **kwargs):
        assert kwargs == {"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False}
    def rollback(self):
        self.rolled_back = True
    def close(self):
        self.closed = True
    def commit(self):
        raise AssertionError("No commit allowed")


def test_one_readonly_transaction_restricted_reference_labels_and_no_write():
    conn = FakeConnection()
    payload, receipt = current.capture_warehouse(lambda: conn, metadata(), load_holdout_fight_ids())
    assert conn.calls[0][0] == "SHOW transaction_read_only"
    assert conn.rolled_back and conn.closed and receipt["transaction_read_only"] == "on"
    assert "fighter_snapshots" not in " ".join(c[0] for c in conn.calls)
    assert len([c for c in conn.calls if c[0] == current.REFERENCE_QUERY]) == 1
    assert "WHERE bf.event_date < %s" in current.REFERENCE_QUERY and "NOT (bf.fight_id::text = ANY(%s))" in current.REFERENCE_QUERY
    assert all(c[0].startswith(("SELECT", "SHOW")) for c in conn.calls)
    assert receipt["queries"] and "sources/fights.json" in payload


def test_readonly_failure_before_source_reads_or_publication(tmp_path):
    conn = FakeConnection(readonly="off")
    with pytest.raises(PreflightError, match="before any source read"):
        current.capture_warehouse(lambda: conn, metadata(), load_holdout_fight_ids())
    assert len(conn.calls) == 1 and conn.rolled_back and conn.closed
    assert not list(tmp_path.iterdir())


def test_query_failure_always_rolls_back_closes(monkeypatch):
    conn = FakeConnection()
    original = FakeCursor.execute
    def fail(self, sql, params=None):
        if sql == current.SCHEMA_QUERY:
            raise RuntimeError("synthetic failure")
        original(self, sql, params)
    monkeypatch.setattr(FakeCursor, "execute", fail)
    with pytest.raises(RuntimeError):
        current.capture_warehouse(lambda: conn, metadata(), load_holdout_fight_ids())
    assert conn.rolled_back and conn.closed


def test_cutoff_feature_order_schedules_deferred_and_missingness():
    frame, report, ledger = current.prepare_current(source(), CUTOFF)
    assert report["ready"] and len(ledger) == len(source().fights)
    assert (frame.event_date < pd.Timestamp(CUTOFF)).all()
    assert "fight-7-0" not in set(frame.fight_id)
    assert list(frame.columns[-50:]) == FEATURE_ORDER
    assert frame[DEBUT_COLS + ["scheduled_rounds"]].isna().all().all()
    assert frame[FEATURE_ORDER].isna().any().any()
    assert frame.loc[frame.both_debuting == 0, "diff_five_round_fights"].isna().all()


def test_all_166_exclusions_applied_before_return_and_earlier_history_retained():
    data = source()
    ids = sorted(load_holdout_fight_ids())
    originals = deepcopy(data.fights)
    data.fight_stats = []
    data.fights = [{**originals[0], "fight_id": fid,
                    "event_id": f"excluded-event-{i}"} for i,fid in enumerate(ids)] + [originals[-2]]
    data.events += [{"event_id": f"excluded-event-{i}", "event_date": date(2020,1,1)} for i in range(166)]
    frame, summary, ledger = current.prepare_current(data, CUTOFF)
    assert summary["ready"] and not set(frame.fight_id) & set(ids)
    assert summary["independent_excluded_results_in_history"] == ids
    assert len([r for r in ledger if "166_id_fitting_exclusion" in r["reasons"]]) == 166
    assert abs(frame.diff_career_wins.iloc[0]) == 166


@pytest.mark.parametrize("change", ["target", "same_date", "future"])
def test_target_same_date_future_feature_invariance(change):
    data = source()
    before = current.prepare_current(data, CUTOFF)[0].set_index("fight_id")
    changed = deepcopy(data)
    day = date(2026,7,3)
    for f in changed.fights:
        e = next(e for e in changed.events if e["event_id"] == f["event_id"])
        selected = f["fight_id"] == "fight-5-0" if change == "target" else e["event_date"] == day if change == "same_date" else e["event_date"] > day
        if selected and f["result_type"] == "win":
            f.update(winner_fighter_id=f["fighter_2_id"], finish_round=1, finish_time_seconds=2)
            for s in changed.fight_stats:
                if s["fight_id"] == f["fight_id"]:
                    s.update(sig_strikes_landed=999, sig_strikes_attempted=1000)
    after = current.prepare_current(changed, CUTOFF)[0].set_index("fight_id")
    pd.testing.assert_series_equal(before.loc["fight-5-0", FEATURE_ORDER], after.loc["fight-5-0", FEATURE_ORDER])


@pytest.mark.parametrize("defect", ["winner", "join", "stat", "profile", "duplicate", "date"])
def test_invalid_structure_blocks_no_silent_deletion(defect):
    data = source()
    if defect == "winner": data.fights[0]["winner_fighter_id"] = "unknown"
    if defect == "join": data.fights[0]["event_id"] = "missing"
    if defect == "stat": data.fight_stats[0]["sig_strikes_landed"] = -1
    if defect == "profile": data.fighters.pop()
    if defect == "duplicate": data.fights.append(deepcopy(data.fights[0]))
    if defect == "date": data.events[0]["event_date"] = None
    frame, report, ledger = current.prepare_current(data, CUTOFF)
    assert frame.empty and not report["ready"] and report["structural_issues"]
    assert len(ledger) == len(data.fights)
    assert all("structural_defects_block_entire_preparation" in r["reasons"] for r in ledger)


def test_draws_nc_only_prior_histories():
    data = source()
    data.fights[0].update(result_type="draw", winner_fighter_id=None)
    data.fights[1].update(result_type="nc", winner_fighter_id=None)
    frame, summary, ledger = current.prepare_current(data, CUTOFF)
    assert summary["ready"] and summary["draw_history_rows"] == summary["nc_history_rows"] == 1
    assert {data.fights[0]["fight_id"],data.fights[1]["fight_id"]}.isdisjoint(frame.fight_id)


def test_fixed_calendar_oof_exact_membership_hashes_no_selection():
    frame = current.prepare_current(source(), CUTOFF)[0]
    folds = current.current_folds(frame, CUTOFF)
    assert folds["boundaries"] == ["2025-10-02", "2026-01-02", "2026-04-02", "2026-07-02", "2026-10-02"]
    expected = frame[frame.event_date >= pd.Timestamp("2025-10-02")]
    assert folds["oof_rows"] == len(expected)
    assert all(f["boosting_rounds"] == 310 and not f["early_stopping"] and not f["tuning"] for f in folds["folds"])
    assert folds == current.current_folds(frame.sample(frac=1,random_state=1), CUTOFF)
    assert folds["oof_ids_sha256"] == sha256(json_bytes(sorted(expected.fight_id)))


def test_challenger_independent_of_reference_stored_values():
    p = sources()
    config = json.loads((ROOT / "configs/phase5_prospective_shadow_v1.json").read_bytes())
    first = current.build_payloads(p, metadata(), config, COMPLETED)
    refs = json.loads(p["reference_bootstrap/stored_features.json"])
    refs[0]["diff_elo"] = "999999"
    p["reference_bootstrap/stored_features.json"] = current.table_bytes(refs)
    second = current.build_payloads(p, metadata(), config, COMPLETED)
    assert first["training.csv"] == second["training.csv"] and first["folds.json"] == second["folds.json"]
    assert first["reference_bootstrap/preprocessing_rows.json"] != second["reference_bootstrap/preprocessing_rows.json"]


def test_reference_invalid_labels_preserved_block_readiness():
    p = sources()
    refs = json.loads(p["reference_bootstrap/stored_features.json"])
    refs[0]["label"] = 0.5
    p["reference_bootstrap/stored_features.json"] = current.table_bytes(refs)
    prepared = current.reference_preparation(p, metadata())
    assert not json.loads(prepared["reference_bootstrap/manifest.json"])["ready"]
    assert json.loads(prepared["reference_bootstrap/preprocessing_rows.json"])[0]["label"] == 0.5


def valid_bout():
    return {"fight_id":"bout", "event_id":"event", "fighter_1_id":"a", "fighter_2_id":"b",
        "announced_event_date":"2026-10-10", "weight_class":"lightweight", "is_title_fight":False,
        "title_evidence":"source-body-hash-and-assertion", "fighter_1_profile_present":True,
        "fighter_2_profile_present":True, "fighter_1_experience_status":"verified_debut",
        "fighter_2_experience_status":"verified_history", "fighter_1_experience_evidence":"profile-hash",
        "fighter_2_experience_evidence":"profile-history-hash"}


def pair_capture(reg):
    return {"kind":"capture", "registry_key":reg["registry_key"], "identity":{k:reg["bout"][k] for k in registry.IDENTITY},
        "announced_event_date":reg["bout"]["announced_event_date"], "observation_cutoff":COMPLETED,
        "forecast_at":"2026-10-02T11:00:00+00:00", "pipelines":{role:{"observation_cutoff":COMPLETED,
        "input_capture_end":COMPLETED, "components_frozen_at":"2026-10-01T10:00:00+00:00",
        "input_sha256":"a"*64, "component_sha256":"b"*64, "recipe_version":version,"complete":True}
        for role,version in registry.RECIPES.items()}}


@pytest.mark.parametrize("time,reason", [("2026-09-26T00:00:00Z", None), ("2026-10-09T00:00:00Z",None),
    ("2026-09-25T23:59:59Z","outside_14_day_window"), ("2026-10-09T00:00:01Z","less_than_24_hours_before_utc_event_boundary")])
def test_forecast_lead_inclusive_boundaries(time,reason):
    assert registry.lead_time_reasons("2026-10-10",time) == ([] if reason is None else [reason])


def test_first_complete_pair_and_common_observation_cutoff():
    reg = registry.registration_records([valid_bout()], captured_at=COMPLETED, source_sha256="a"*64)[0]
    capture = pair_capture(reg)
    result = registry.validate_pair_capture(reg,capture,[])
    assert result["primary_capture"]
    later = {**capture,"observation_cutoff":"2026-10-03T10:00:00Z","forecast_at":"2026-10-03T11:00:00Z"}
    for v in later["pipelines"].values(): v["observation_cutoff"] = later["observation_cutoff"]
    assert "primary_already_frozen_supplemental_only" in registry.validate_pair_capture(reg,later,[result])["blocking_reasons"]
    capture["pipelines"]["frozen_reference"]["observation_cutoff"] = "2026-10-01T10:00:00Z"
    assert "unmatched_observation_cutoffs" in registry.validate_pair_capture(reg,capture,[])["blocking_reasons"]


@pytest.mark.parametrize("field,value,reason", [("is_title_fight",None,"unknown_title_status"),
    ("title_evidence",None,"unknown_title_status"), ("fighter_1_profile_present",False,"missing_fighter_1_profile"),
    ("fighter_1_experience_status","unverified","unresolved_fighter_1_experience"),
    ("fighter_1_experience_evidence",None,"missing_fighter_1_experience_evidence")])
def test_unknown_metadata_never_defaults(field,value,reason):
    bout=valid_bout(); bout[field]=value
    assert reason in registry.eligibility(bout,observation_cutoff=COMPLETED)


def test_full_considered_registry_coverage_revisions_cancellation_append_only(tmp_path):
    bouts=[valid_bout(),{**valid_bout(),"fight_id":"blocked","is_title_fight":None}]
    regs=registry.registration_records(bouts,captured_at=COMPLETED,source_sha256="a"*64)
    directory=tmp_path/'records'
    for reg in regs: registry.append_record(directory,reg)
    originals={p.name:p.read_bytes() for p in directory.glob('*.json')}
    registry.verify_coverage(directory,["bout","blocked"])
    with pytest.raises(PreflightError,match="cover every"):
        registry.verify_coverage(directory,["bout"])
    registry.append_record(directory,{"kind":"revision","registry_key":"bout","recorded_at":COMPLETED,
        "source_sha256":"b"*64,"changes":{"announced_event_date":"2026-10-11"}})
    registry.append_record(directory,{"kind":"cancellation","registry_key":"bout","recorded_at":COMPLETED,
        "source_sha256":"c"*64,"reason":"announced cancellation"})
    assert all((directory/n).read_bytes()==b for n,b in originals.items())
    with pytest.raises(PreflightError,match="Cancelled"):
        registry.append_record(directory,pair_capture(regs[0]))
    with pytest.raises(PreflightError,match="already registered"):
        registry.append_record(directory,regs[0])


def test_registry_checksum_tampering(tmp_path):
    directory=tmp_path/'records'
    regs=registry.registration_records([valid_bout(),{**valid_bout(),"fight_id":"b2"}],captured_at=COMPLETED,source_sha256="a"*64)
    for r in regs: registry.append_record(directory,r)
    path=directory/'00000001.json'; path.chmod(0o644)
    path.write_bytes(path.read_bytes().replace(b'"bout"',b'"changed"'))
    with pytest.raises(PreflightError,match="checksum chain"):
        registry.read_records(directory)


def test_registry_rejects_predictions_outcomes_and_unregistered_rows(tmp_path):
    with pytest.raises(PreflightError,match="predictions/outcomes"):
        registry.registration_records([{**valid_bout(),"calibrated_prob_f1":0.7}],captured_at=COMPLETED,source_sha256="a"*64)
    with pytest.raises(PreflightError,match="Register every"):
        registry.append_record(tmp_path,{"kind":"capture","registry_key":"unknown"})


def test_deterministic_rebuild_exclusive_publication_and_tamper(tmp_path):
    config=json.loads((ROOT/'configs/phase5_prospective_shadow_v1.json').read_bytes())
    p=sources(); one=current.build_payloads(p,metadata(),config,COMPLETED)
    assert one==current.build_payloads(p,metadata(),config,COMPLETED)
    payload={**p,**one,"reference_bootstrap/production_metadata.json":json_bytes(metadata()),
             "run_receipt.json":json_bytes({"coherent_capture_completed_at":COMPLETED}),
             "package_versions.json":json_bytes(current.versions()),
             "code_versions.json":json_bytes({p:sha256((ROOT/p).read_bytes()) for p in current.CODE_PATHS})}
    directory=tmp_path/'run'; digest=current.publish(directory,payload)
    assert command.rebuild(directory,digest)["status"]=="VERIFIED"
    current.load_current_preparation(directory,expected_checksums_sha256=digest,
        expected_training_manifest_sha256=sha256(one["training_manifest.json"]))
    with pytest.raises(PreflightError,match="Never overwrite"):
        current.publish(directory,payload)
    path=directory/'training.csv'; path.chmod(0o644); path.write_bytes(b'corrupt')
    with pytest.raises(PreflightError,match="tampering"):
        current.verify_checksums(directory,expected_checksums_sha256=digest)


def test_no_frozen_outcomes_model_fits_real_prediction_calls(monkeypatch):
    import builtins
    from sklearn.linear_model import LogisticRegression
    from xgboost import XGBClassifier
    from features import debut_prior
    from modeling import refit_preflight
    def forbidden(*a,**kw): raise AssertionError("Forbidden real fitting/scoring")
    for cls in (LogisticRegression,XGBClassifier):
        monkeypatch.setattr(cls,"fit",forbidden)
        monkeypatch.setattr(cls,"predict_proba",forbidden)
    monkeypatch.setattr(debut_prior,"compute_debut_priors",forbidden)
    monkeypatch.setattr(refit_preflight,"compute_debut_priors",forbidden)
    original_open=builtins.open; original_path_open=Path.open
    def guard(path):
        name=str(path)
        if any(x in name for x in ('outcomes.csv','phase1-holdout-evidence','pre_event_prediction_fights.csv','joined')):
            raise AssertionError("Forbidden frozen outcome/evidence access: "+name)
    def checked(path,*a,**kw): guard(path); return original_open(path,*a,**kw)
    def checked_path(path,*a,**kw): guard(path); return original_path_open(path,*a,**kw)
    monkeypatch.setattr(builtins,'open',checked); monkeypatch.setattr(Path,'open',checked_path)
    config=json.loads((ROOT/'configs/phase5_prospective_shadow_v1.json').read_bytes())
    current.build_payloads(sources(),metadata(),config,COMPLETED)
    conn=FakeConnection(); current.capture_warehouse(lambda:conn,metadata(),load_holdout_fight_ids())


def test_bounded_official_read_exact_response_and_stop_on_block(monkeypatch):
    import io
    from urllib.error import HTTPError
    p=sources(); ev=json.loads(p['sources/events.json']); ev[-1]['source_url']='http://ufcstats.com/event-details/target'
    p['sources/events.json']=current.table_bytes(ev)
    fights=json.loads(p['sources/fights.json']); fights[-1]['source_url']='http://ufcstats.com/fight-details/target'
    p['sources/fights.json']=current.table_bytes(fights)
    monkeypatch.setattr(command,'now',lambda:COMPLETED)
    calls=[]
    def blocked(req,timeout):
        calls.append(req.full_url)
        raise HTTPError(req.full_url,403,'Forbidden',{},io.BytesIO(b'exact blocked body'))
    monkeypatch.setattr(command,'urlopen',blocked)
    out=command.targeted_official_reads(p)
    assert len(calls)==1 and out['official_sources/01.response']==b'exact blocked body'
    rec=json.loads(out['official_sources/manifest.json'])['records'][0]
    assert rec['status']==403 and rec['body_sha256']==sha256(b'exact blocked body') and rec['stopped_on_access_or_response_problem']


def test_successor_registry_publication_preserves_original_run(tmp_path):
    from tools.append_prospective_registry import successor
    reg=registry.registration_records([valid_bout()],captured_at=COMPLETED,source_sha256='a'*64)[0]
    original=json_bytes({'sequence':1,'previous_sha256':None,'record':reg})
    parent=tmp_path/'parent'; root=current.publish(parent,{'registry/records/00000001.json':original})
    output=tmp_path/'successor'
    digest=successor(parent,root,[{'kind':'revision','registry_key':'bout','changes':{'announced_event_date':'2026-10-11'},
        'source_sha256':'b'*64,'recorded_at':COMPLETED}],output)
    assert (parent/'registry/records/00000001.json').read_bytes()==original
    assert (output/'registry/records/00000001.json').read_bytes()==original
    current.verify_checksums(parent,expected_checksums_sha256=root)
    current.verify_checksums(output,expected_checksums_sha256=digest)
    assert len(registry.read_records(output/'registry/records'))==2


def test_future_or_unfrozen_component_inputs_block_pair():
    reg=registry.registration_records([valid_bout()],captured_at=COMPLETED,source_sha256='a'*64)[0]
    cap=pair_capture(reg)
    cap['pipelines']['current_challenger']['input_capture_end']='2026-10-03T00:00:00Z'
    cap['pipelines']['frozen_reference']['components_frozen_at']='2026-10-03T00:00:00Z'
    result=registry.validate_pair_capture(reg,cap,[])
    assert not result['primary_capture']
    assert 'input_captured_after_observation_cutoff' in result['blocking_reasons']
    assert 'components_not_frozen_before_capture' in result['blocking_reasons']
    cap['forecast_at']='2026-10-01T00:00:00Z'
    with pytest.raises(PreflightError,match='Register bout before|Forecast precedes'):
        registry.validate_pair_capture(reg,cap,[])


def test_incomplete_publication_and_protected_destination_fail(tmp_path):
    directory=tmp_path/'incomplete'; root=current.publish(directory,{'test.json':b'{}'})
    (directory/'INCOMPLETE').write_text('failed')
    with pytest.raises(PreflightError,match='Incomplete'):
        current.verify_checksums(directory,expected_checksums_sha256=root)
    with pytest.raises(PreflightError,match='separate Phase5A root'):
        current.publish(ROOT/'models/synthetic_phase5a_forbidden',{'test.json':b'{}'})
    assert not (ROOT/'models/synthetic_phase5a_forbidden').exists()
