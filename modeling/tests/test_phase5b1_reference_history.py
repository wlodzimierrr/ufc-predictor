"""Scoped offline Phase5B1 checks. Real probabilities are bootstrap parity only."""
from copy import deepcopy
from datetime import date
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from features.data_loader import WarehouseData
from features.history import build_fighter_index, get_history
from features.elo import compute_all_elos
from features.replay import reconstruct_bouts
from features.tests.test_replay import synthetic_source
from modeling import phase5_frozen_reference as reference
from modeling import phase5_history_identity as identity
from modeling.phase5_reference_adapter import build_reference_features
from modeling.phase5b1_artifacts import PARENT, PARENT_ROOT, OUTPUT, publish
from modeling.phase5_current_data import source_data, TABLES, table_bytes, load_current_preparation
from modeling.holdout import load_holdout_fight_ids
from modeling.refit_preflight import ROOT, PreflightError, FEATURE_ORDER, json_bytes, sha256
from tools.freeze_phase5b1_reference_and_history import reconcile

REFERENCE_RUN = OUTPUT/'20261002_phase5b1_frozen_reference_v3_identity_guarded'


@pytest.fixture(scope='module')
def evidence():
    return identity.evidence_payloads()


def real_rows():
    return {n:json.loads((PARENT/'sources'/f'{n}.json').read_bytes()) for n in
            ('events','fighters','fights','fight_stats_aggregate')}


def one_duplicate(rows,ledger):
    d = deepcopy(next(d for d in ledger['groups'] if d['disposition']=='proven_duplicate'))
    ids = set(d['original_ids']); e=d['event_id']; ps=set(d['participant_ids'])
    subset = {'events':[x for x in rows['events'] if x['event_id']==e],
              'fighters':[x for x in rows['fighters'] if x['fighter_id'] in ps],
              'fights':[x for x in rows['fights'] if x['fight_id'] in ids],
              'fight_stats_aggregate':[]}
    return subset,{'groups':[d]}


def test_exact_bootstrap_populations_exclusions_orientation_and_numeric_contracts():
    frames,manifest,checks = reference.validate_bootstrap()
    exclusions = load_holdout_fight_ids()
    assert len(exclusions)==166
    assert manifest['preprocessing_rows']==6391 and manifest['calibration_rows']==1009
    for name,frame in frames.items():
        assert set(frame.fight_id).isdisjoint(exclusions)
        assert sha256(json_bytes(sorted(frame.fight_id)))==reference.MEMBERSHIPS[name][1]
        assert not np.isinf(frame[FEATURE_ORDER[:-3]].to_numpy(dtype=float)).any()
        assert frame.fighter_1_id.equals(frame.source_fighter_1_id)
        assert frame.fighter_2_id.equals(frame.source_fighter_2_id)
    assert (frames['preprocessing'].event_date<'2022-03-12').all()
    assert (frames['calibration'].event_date>='2022-03-12').all()
    assert (frames['calibration'].event_date<'2024-03-09').all()
    assert set(frames['calibration'].label)=={0,1}


def test_all_fourteen_dispositions_and_complete_lineage(evidence):
    ledger=json.loads(evidence['identity_ledger.json']);lineage=json.loads(evidence['source_to_derived_lineage.json'])
    assert ledger['disposition_counts']=={'proven_distinct':1,'proven_duplicate':12,'unresolved':1}
    assert ledger['group_count']==14 and not ledger['ready']
    assert lineage['original_fights']==8992 and lineage['derived_fights']==8980
    assert len(lineage['lineage']['fights'])==8992
    assert len(lineage['lineage']['statistics'])==17386
    assert len(lineage['aliases'])==12 and len(lineage['expanded_exclusion_ids'])==166
    assert len({f['canonical_id'] for f in lineage['lineage']['fights']})==8980


def test_unresolved_never_dropped_or_quarantined_before_history(evidence,monkeypatch):
    ledger=json.loads(evidence['identity_ledger.json']);rows=real_rows()
    before=deepcopy(rows)
    normalized,_,_,_=identity.normalize_rows(rows,ledger,load_holdout_fight_ids())
    unresolved=next(d for d in ledger['groups'] if d['disposition']=='unresolved')
    assert set(unresolved['original_ids']) <= {r['fight_id'] for r in normalized['fights']}
    monkeypatch.setattr(identity,'validate_source',lambda *a,**k:pytest.fail('Indexing unresolved source'))
    with pytest.raises(PreflightError,match='block all history'):
        identity.history_source(rows,ledger,load_holdout_fight_ids())
    assert rows==before


@pytest.mark.parametrize('excluded_side',['canonical','alias'])
def test_exclusion_propagates_over_alias_class(evidence,excluded_side):
    rows,ledger=one_duplicate(real_rows(),json.loads(evidence['identity_ledger.json']))
    d=ledger['groups'][0];can=d['canonical_id'];other=next(i for i in d['original_ids'] if i!=can)
    _,aliases,_,expanded=identity.normalize_rows(rows,ledger,{can if excluded_side=='canonical' else other})
    assert expanded==frozenset(d['original_ids']) and aliases[other]==can
    # One expanded set is returned for fitting, preprocessing, OOF and calibration.
    assert {r['fight_id'] for r in rows['fights']}-expanded==set()


def test_proven_duplicate_history_elo_opponent_stats_count_once(evidence):
    rows,ledger=one_duplicate(real_rows(),json.loads(evidence['identity_ledger.json']))
    d=ledger['groups'][0];can=d['canonical_id'];p1,p2=d['participant_ids']
    # Synthetic completed versions of this identity fixture, never a real ingestion.
    for f in rows['fights']:
        f.update(result_type='win',winner_fighter_id=p1,finish_method='decision',finish_round=3,finish_time_seconds=300)
        rows['fight_stats_aggregate'] += [{'fight_stat_id':f['fight_id']+pid,'fight_id':f['fight_id'],
            'fighter_id':pid,'sig_strikes_landed':10,'sig_strikes_attempted':20} for pid in (p1,p2)]
    d.pop('source_rows')
    data,_=identity.history_source(rows,ledger,set())
    index=build_fighter_index(data)
    assert len(data.fights)==1 and len(index[p1])==len(index[p2])==1
    assert index[p1][0].fighter_stats['fighter_id']==p1 and index[p1][0].opponent_stats['fighter_id']==p2
    probe={**data.fights[0],'fight_id':'synthetic-next','event_date':date(2026,9,1),'result_type':'upcoming','winner_fighter_id':None}
    elo=compute_all_elos(data.fights+[probe]);assert elo['synthetic-next'][p1]==1516
    assert elo['synthetic-next'][p2]==1484
    assert index[p1][0].fight_id==can


def test_reversed_alias_orientation_keeps_per_fighter_stats(evidence):
    rows,ledger=one_duplicate(real_rows(),json.loads(evidence['identity_ledger.json']))
    d=ledger['groups'][0];can=d['canonical_id'];d.pop('source_rows')
    other=next(f for f in rows['fights'] if f['fight_id']!=can)
    other['fighter_1_id'],other['fighter_2_id']=other['fighter_2_id'],other['fighter_1_id']
    result,_,lineage,_=identity.normalize_rows(rows,ledger,set())
    canonical=next(f for f in rows['fights'] if f['fight_id']==can)
    assert result['fights']==[canonical]
    assert next(r for r in lineage['fights'] if r['original_id']==other['fight_id'])['original_orientation']==[other['fighter_1_id'],other['fighter_2_id']]


@pytest.mark.parametrize('conflict',['winner','weight_class','statistics','missing_statistics'])
def test_conflicts_never_coalesced_by_priority(evidence,conflict):
    rows,ledger=one_duplicate(real_rows(),json.loads(evidence['identity_ledger.json']))
    ledger['groups'][0].pop('source_rows');f=rows['fights'][0]
    if conflict=='winner': f['winner_fighter_id']=f['fighter_1_id']
    elif conflict=='weight_class': f['weight_class']='heavyweight'
    else:
        for i,r in enumerate(rows['fights']):
            if conflict=='missing_statistics' and i==1:continue
            rows['fight_stats_aggregate'].append({'fight_stat_id':r['fight_id']+'s','fight_id':r['fight_id'],
                'fighter_id':r['fighter_1_id'],'sig_strikes_landed':i})
    with pytest.raises(PreflightError,match='Conflicting'):
        identity.normalize_rows(rows,ledger,set())


@pytest.mark.parametrize('bad',['exclusion_only','same_participants_only','multiple_occurrences'])
def test_unsupported_duplicate_claims_block(evidence,bad):
    rows,ledger=one_duplicate(real_rows(),json.loads(evidence['identity_ledger.json']));d=ledger['groups'][0]
    if bad=='exclusion_only':d['proof_kind']='verified_same_bout_from_exclusions'
    elif bad=='same_participants_only':d['evidence']=[]
    else:d['card_occurrences']*=2
    with pytest.raises(PreflightError,match='Unsupported duplicate'):
        identity.normalize_rows(rows,ledger,set(d['original_ids']))


def test_exact_narrow_rematch_contract_and_no_synthetic_event_ids(evidence):
    rows=real_rows();ledger=json.loads(evidence['identity_ledger.json']);d=ledger['groups'][0]
    ids=set(d['original_ids']);ps=set(d['participant_ids'])
    rs={'fights':[f for f in rows['fights'] if f['fight_id'] in ids],
        'fighters':[f for f in rows['fighters'] if f['fighter_id'] in ps],
        'events':[e for e in rows['events'] if e['event_id']==d['event_id']],
        'fight_stats_aggregate':[s for s in rows['fight_stats_aggregate'] if s['fight_id'] in ids]}
    rs['events'][0]['event_date']=date(1997,12,21)
    for f in rs['fighters']:
        if f.get('dob'): f['dob']=date.fromisoformat(f['dob'])
    data,_=identity.history_source(rs,{'groups':[d]},set())
    assert {f['fight_id'] for f in data.fights}==identity.REMATCH
    assert len({f['event_id'] for f in data.fights})==1
    index=build_fighter_index(data)
    assert len(index[d['participant_ids'][0]])==2
    assert get_history(index,d['participant_ids'][0],date(1997,12,21))==[]
    elo=compute_all_elos(data.fights)
    assert elo[d['original_ids'][0]]==elo[d['original_ids'][1]]
    assert len(data.stats_by_fight_fighter)==4
    d['card_occurrences'].pop()
    with pytest.raises(PreflightError,match='Unsupported distinct'):
        identity.normalize_rows(rs,{'groups':[d]},set())


def test_rematch_contract_does_not_admit_other_event(evidence):
    rows,ledger=one_duplicate(real_rows(),json.loads(evidence['identity_ledger.json']))
    ledger['groups'][0]['disposition']='proven_distinct'
    with pytest.raises(PreflightError,match='Outside narrow'):
        identity.normalize_rows(rows,ledger,set())


def test_event_projection_is_outcome_free_and_result_invariant():
    raw=b'<tr><td>WIN 99 12:00</td><a href="http://ufcstats.com/fight-details/x">X</a><a href="http://ufcstats.com/fighter-details/a">A</a><a href="http://ufcstats.com/fighter-details/b">B</a></tr>'
    assert identity.event_link_projection(raw)==identity.event_link_projection(raw.replace(b'WIN 99 12:00',b'DRAW 0 1:00'))
    assert set(identity.event_link_projection(raw)[0])=={'occurrence','participant_links'}


def saved_priors():
    return {'base_prior':.5,'height_stats':{},'reach_stats':{},'global_height_std':5.,'global_reach_std':6.,'training_debut_win_rate':.5}


def synthetic_future():
    data=synthetic_source();data.fights[-1].update(result_type='upcoming',winner_fighter_id=None)
    return data


def test_future_adapter_legacy_snapshot_date_defaults_schedules_and_no_fits(monkeypatch):
    def forbidden(*a,**k):pytest.fail('Forbidden fit/query/write')
    from features import debut_prior
    from warehouse import db
    monkeypatch.setattr(debut_prior,'compute_debut_priors',forbidden)
    monkeypatch.setattr(db,'get_connection',forbidden);monkeypatch.setattr(db,'upsert',forbidden)
    monkeypatch.setattr(XGBClassifier,'fit',forbidden);monkeypatch.setattr(LogisticRegression,'fit',forbidden)
    data=synthetic_future();before=deepcopy(data)
    frame=build_reference_features(data,['fight-7-0'],observation_cutoff='2020-07-15T12:00:00Z',saved_preprocessing=saved_priors())
    assert frame.reference_snapshot_date.iloc[0]=='2020-07-15'
    assert frame.event_date.iloc[0]==date(2020,8,1) and frame.scheduled_rounds.iloc[0]==3
    assert frame.label.isna().all()
    assert all(c in frame for c in FEATURE_ORDER)
    assert data==before
    # Label-free same-observation-day and future rows cannot enter histories.
    later=build_reference_features(data,['fight-7-0'],observation_cutoff='2020-09-01T12:00:00Z',saved_preprocessing=saved_priors())
    assert later.reference_snapshot_date.iloc[0]=='2020-08-01'


def test_future_adapter_preserves_legacy_within_date_elo_difference():
    from modeling.reference_legacy_v1.elo import compute_all_elos as legacy
    data=synthetic_future()
    assert legacy(data.fights)['fight-5-0']['b']!=legacy(data.fights)['fight-5-1']['b']
    assert compute_all_elos(data.fights)['fight-5-0']['b']==compute_all_elos(data.fights)['fight-5-1']['b']


def test_future_adapter_uses_saved_debut_components_only():
    data=synthetic_future();data.fights=[data.fights[-1]];data.fight_stats=[]
    frame=build_reference_features(data,['fight-7-0'],observation_cutoff='2020-07-15T12:00:00Z',saved_preprocessing=saved_priors())
    assert frame.both_debuting.iloc[0]==1
    assert frame.debut_prior_win_prob_f1.iloc[0]==.5
    assert frame.debut_height_adv.iloc[0]==-1
    assert frame.debut_reach_adv.iloc[0]==pytest.approx(-5/6)


def test_reference_adapter_rejects_alias_history_before_indexing(monkeypatch):
    from modeling import phase5_reference_adapter as adapter
    data=synthetic_future()
    data.fights.append({**data.fights[0],'fight_id':'synthetic-unresolved-alias'})
    monkeypatch.setattr(adapter,'build_fighter_index',lambda *a,**k:pytest.fail('Indexed alias history'))
    monkeypatch.setattr(adapter,'validate_source',lambda *a,**k:pytest.fail('Built lookup/statistic indexes for alias history'))
    with pytest.raises(PreflightError,match='normalized, resolved history identities'):
        build_reference_features(data,['fight-7-0'],observation_cutoff='2020-07-15T12:00:00Z',saved_preprocessing=saved_priors())


def test_reference_adapter_narrow_occurrence_guard():
    from modeling.phase5_reference_adapter import validate_history_identities
    ids=['2c4d505e-c625-5e27-89f8-e36c2b8224b4','383d786b-c425-517e-a2af-d4cb19081135']
    urls=['ec1bda9a4c2aab42','2750ac5854e8b28b']
    fights=[{'fight_id':fid,'event_id':'fe881b94-92d3-5dbe-97e7-28014b17202d',
             'event_date':date(1997,12,21),'fighter_1_id':'0ce3676f-6d84-5bcf-b635-af666b7a7f19',
             'fighter_2_id':'97c74ad5-628e-5da5-bd90-b595e3f60047',
             'source_url':'http://ufcstats.com/fight-details/'+url} for fid,url in zip(ids,urls)]
    validate_history_identities(WarehouseData(fights=fights))
    for key in ['fighter_1_id','event_id','event_date']:
        changed=deepcopy(fights)
        for f in changed:f[key]=date(1998,1,1) if key=='event_date' else 'unsupported'
        with pytest.raises(PreflightError,match='normalized, resolved history identities'):
            validate_history_identities(WarehouseData(fights=changed))


def test_future_adapter_inherits_all_capture_elo_limitation():
    data=synthetic_future();target=data.fights[-1]
    early=build_reference_features(data,[target['fight_id']],observation_cutoff='2020-05-15T12:00:00Z',saved_preprocessing=saved_priors())
    changed=deepcopy(data)
    for f in changed.fights:
        if f['event_date']>date(2020,5,15) and f['result_type']=='win':
            f['winner_fighter_id']=f['fighter_2_id']
    after=build_reference_features(changed,[target['fight_id']],observation_cutoff='2020-05-15T12:00:00Z',saved_preprocessing=saved_priors())
    assert early.diff_elo.iloc[0]!=after.diff_elo.iloc[0]
    assert early.diff_career_fights.iloc[0]==after.diff_career_fights.iloc[0]


def test_original_registry_and_source_freshness_limits_preserved():
    coverage=json.loads((PARENT/'registry/coverage.json').read_bytes())
    assert coverage['considered_count']==coverage['registered_count']==35
    assert coverage['real_predictions']==coverage['complete_paired_forecasts']==0
    assert len(list((PARENT/'registry/records').glob('*.json')))==35
    rows=real_rows();events={e['event_id']:e for e in rows['events']}
    # The recorded August boundary is the resolved binary fitting population.
    resolved=[f for f in rows['fights'] if f['result_type']=='win']
    assert max(events[f['event_id']]['event_date'] for f in resolved)=='2026-08-29'
    assert sum(f['result_type']=='upcoming' and events[f['event_id']]['event_date']<'2026-10-02' for f in rows['fights'])==128
    assert max(s['scraped_at'] for s in rows['fight_stats_aggregate']).startswith('2026-08-09')


@pytest.mark.parametrize('change',['same_day','future'])
def test_date_frozen_challenger_invariance_after_normalization(change):
    data=synthetic_source();target=data.fight_by_id['fight-4-0']
    def normalized(source):
        rows={'events':source.events,'fighters':source.fighters,'fights':source.fights,
              'fight_stats_aggregate':source.fight_stats}
        return identity.history_source(rows,{'groups':[]},set())[0]
    before=reconstruct_bouts(normalized(data),[target])[0]
    changed=deepcopy(data)
    for f in changed.fights:
        if f['event_date']==target['event_date'] if change=='same_day' else f['event_date']>target['event_date']:
            f['winner_fighter_id']=f['fighter_2_id'];f['finish_round']=1
            for s in changed.fight_stats:
                if s['fight_id']==f['fight_id']:s['sig_strikes_landed']=999
    after=reconstruct_bouts(normalized(changed),[target])[0]
    assert {c:before[c] for c in FEATURE_ORDER[:-3]}=={c:after[c] for c in FEATURE_ORDER[:-3]}


def test_deterministic_reconciliation_overwrite_and_guarded_refusal(evidence,tmp_path):
    assert evidence==identity.evidence_payloads()
    path=tmp_path/'reconciliation';receipt=reconcile(path)
    assert receipt['deterministic_payloads']==11 and receipt['guarded_loader_refusal']
    with pytest.raises(PreflightError,match='Never overwrite'):reconcile(path)
    with pytest.raises(PreflightError,match='remains blocked'):
        identity.load_reconciliation(path,expected_checksums_sha256=receipt['checksums_sha256'])


def test_old_diagnostic_preparation_stays_unloadable():
    with pytest.raises(PreflightError,match='Blocked or incompatible'):
        load_current_preparation(PARENT,expected_checksums_sha256=PARENT_ROOT,
            expected_training_manifest_sha256='e23f7635c096e54090ffd229ab547ceb9e5f26fa7b71b2eac876be70f14ebe79')


def test_no_outcome_reads_in_identity_and_bootstrap_validation(monkeypatch):
    original=Path.open
    def checked(p,*a,**k):
        if any(s in str(p) for s in ('outcomes.csv','phase1-holdout-evidence','joined','phase4a')):
            pytest.fail('Forbidden historical outcome/recovery access')
        return original(p,*a,**k)
    monkeypatch.setattr(Path,'open',checked)
    identity.evidence_payloads();reference.validate_bootstrap()


@pytest.fixture
def bundle():
    if not REFERENCE_RUN.exists():pytest.skip('Run after the one authorized production-derived reference freeze')
    root=sha256((REFERENCE_RUN/'checksums.json').read_bytes());m=sha256((REFERENCE_RUN/'manifest.json').read_bytes())
    return REFERENCE_RUN,root,m


def test_persisted_components_exact_bootstrap_parity_no_load_fit_or_live_calls(bundle,monkeypatch):
    path,root,m=bundle
    def forbidden(*a,**k):pytest.fail('Forbidden load-time fit/live operation')
    from features import debut_prior
    from warehouse import db
    import socket
    import psycopg2
    monkeypatch.setattr(reference,'compute_debut_priors',forbidden);monkeypatch.setattr(debut_prior,'compute_debut_priors',forbidden)
    monkeypatch.setattr(XGBClassifier,'fit',forbidden);monkeypatch.setattr(LogisticRegression,'fit',forbidden)
    monkeypatch.setattr(db,'get_connection',forbidden);monkeypatch.setattr(db,'upsert',forbidden)
    monkeypatch.setattr(socket,'create_connection',forbidden)
    monkeypatch.setattr(psycopg2,'connect',forbidden)
    loaded=reference.load_reference(path,expected_checksums_sha256=root,expected_manifest_sha256=m)
    frames,_,_=reference.validate_bootstrap()
    assert reference.bootstrap_results(loaded.base,loaded.calibrator,loaded.preprocessing,frames)==json.loads((path/'bootstrap_parity.json').read_bytes())
    assert loaded.manifest['membership_difference']=={'preprocessing':15,'calibration':2}
    assert loaded.manifest['new_capture_after_freeze_required']


@pytest.mark.parametrize('component',['base_model.joblib','preprocessing.json','calibrator.joblib','feature_order.json'])
@pytest.mark.parametrize('change',['tamper','missing'])
def test_loader_rejects_tampered_or_missing_component_before_deserialization(bundle,tmp_path,monkeypatch,component,change):
    source,root,m=bundle;dest=tmp_path/'bad';shutil.copytree(source,dest)
    target=dest/component;target.chmod(0o644)
    if change=='missing':target.unlink()
    else:target.write_bytes(b'tampered')
    monkeypatch.setattr(reference.joblib,'load',lambda *a,**k:pytest.fail('Deserialized invalid component'))
    with pytest.raises(PreflightError):reference.load_reference(dest,expected_checksums_sha256=root,expected_manifest_sha256=m)


@pytest.mark.parametrize('component,change',[
    ('package_versions.json',lambda d:{**d,'numpy':'incompatible'}),
    ('code_versions.json',lambda d:{k:'0'*64 for k in d}),
    ('manifest.json',lambda d:{**d,'parent_checksums_sha256':'0'*64}),
    ('manifest.json',lambda d:{**d,'platt':{**d['platt'],'C':1.0}}),
    ('manifest.json',lambda d:{**d,'memberships':{}}),
    ('manifest.json',lambda d:{**d,'component_freeze_times':{}}),
])
def test_loader_rejects_incompatible_provenance_even_with_recomputed_root(bundle,tmp_path,monkeypatch,component,change):
    source,_,_=bundle
    payload={str(p.relative_to(source)):p.read_bytes() for p in source.rglob('*') if p.is_file() and p.name!='checksums.json'}
    payload[component]=json_bytes(change(json.loads(payload[component])))
    dest=tmp_path/'bad';root=publish(dest,payload);m=sha256(payload['manifest.json'])
    monkeypatch.setattr(reference.joblib,'load',lambda *a,**k:pytest.fail('Deserialized incompatible provenance'))
    with pytest.raises(PreflightError):reference.load_reference(dest,expected_checksums_sha256=root,expected_manifest_sha256=m)


def test_reference_refuses_overwrite_before_any_fit(bundle):
    with pytest.raises(PreflightError,match='Never overwrite'):
        reference.freeze_reference(bundle[0])


def test_protected_publication_and_incomplete_refusal(tmp_path):
    with pytest.raises(PreflightError,match='isolated artifact root'):
        publish(ROOT/'models/forbidden_phase5b1',{'test':b'{}'})
    p=tmp_path/'incomplete';root=publish(p,{'test':b'{}'});(p/'INCOMPLETE').write_text('failed')
    with pytest.raises(PreflightError,match='Incomplete'):
        reference.load_reference(p,expected_checksums_sha256=root,expected_manifest_sha256='0'*64)


def test_preservation_detects_existing_changes_missing_and_new_protected_files(tmp_path,monkeypatch):
    from modeling import phase5b1_artifacts as artifacts
    (tmp_path/'data/experiments').mkdir(parents=True)
    (tmp_path/'models').mkdir()
    p=tmp_path/'models/existing';p.write_bytes(b'original')
    baseline={'files':{'models/existing':sha256(b'original')}}
    monkeypatch.setattr(artifacts,'ROOT',tmp_path)
    assert artifacts.preservation(baseline)['status']=='VERIFIED'
    p.write_bytes(b'changed')
    with pytest.raises(PreflightError,match='Preservation failed'):artifacts.preservation(baseline)
    p.unlink()
    with pytest.raises(PreflightError,match='Preservation failed'):artifacts.preservation(baseline)
    p.write_bytes(b'original');(tmp_path/'models/new').write_bytes(b'new')
    with pytest.raises(PreflightError,match='Preservation failed'):artifacts.preservation(baseline)
