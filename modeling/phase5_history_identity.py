"""Bounded Phase 5 identity evidence and normalization before any indexes.

Supplementary HTML is projected to links only. No outcomes, statistics or
historical joined evidence are imported from it. Original rows never change.
"""
import ast
from collections import Counter, defaultdict
from copy import deepcopy
import json
from pathlib import Path
import re
from urllib.parse import urlparse
from uuid import uuid5, NAMESPACE_URL

from modeling.holdout import load_holdout_fight_ids
from modeling.phase5_current_data import TABLES, table_bytes, source_data, verify_checksums, versions
from modeling.phase5b1_artifacts import PARENT, PARENT_ROOT, PARENT_MANIFEST, parent_inputs, code_hashes, verify_code_and_packages
from modeling.refit_preflight import ROOT, PreflightError, json_bytes, sha256, validate_source

VERSION = 'phase5_history_identity_v1'
POLICY = 'docs/phase5-history-identity-policy-v1.md'
CODE = ['tools/freeze_phase5b1_reference_and_history.py',
        'modeling/phase5_history_identity.py','modeling/phase5b1_artifacts.py',
        'modeling/phase5_current_data.py','modeling/refit_preflight.py','modeling/holdout.py',
        'features/data_loader.py','features/replay.py',POLICY]
REMATCH = {'2c4d505e-c625-5e27-89f8-e36c2b8224b4','383d786b-c425-517e-a2af-d4cb19081135'}


def link(url):
    p = urlparse(url)
    return p.netloc.lower().removeprefix('www.')+p.path.rstrip('/')


def event_link_projection(raw):
    """Only href attributes from bout table rows, never result/finish cells."""
    result = []
    for row in re.findall(r'<tr\b.*?</tr>',raw.decode(),re.S):
        hrefs = re.findall(r'href\s*=\s*[\"\']([^\"\']+)[\"\']',row)
        bouts = list(dict.fromkeys(link(h) for h in hrefs if '/fight-details/' in h))
        fighters = list(dict.fromkeys(link(h) for h in hrefs if '/fighter-details/' in h))
        if bouts and len(fighters)==2:
            if len(bouts)!=1: raise PreflightError('Ambiguous source card row')
            result.append({'occurrence':bouts[0],'participant_links':fighters})
    return result


def groups(rows):
    result = defaultdict(list)
    for f in rows:
        result[(f['event_id'],*sorted((f['fighter_1_id'],f['fighter_2_id'])))].append(f)
    return {k:sorted(v,key=lambda f:f['fight_id']) for k,v in result.items() if len(v)>1}


def semantic_fight(row):
    return {k:v for k,v in row.items() if k not in {'fight_id','source_url','scraped_at','fighter_1_id','fighter_2_id'}}


def statistic_values(row):
    return {k:v for k,v in row.items() if k not in {'fight_id','fight_stat_id','source_url','scraped_at'}}


def alias_conflicts(rows, stats):
    conflicts = []
    if any(semantic_fight(r)!=semantic_fight(rows[0]) for r in rows[1:]):
        conflicts.append('conflicting_nonprovenance_bout_fields')
    indexed = {r['fight_id']:{s['fighter_id']:statistic_values(s) for s in stats if s['fight_id']==r['fight_id']} for r in rows}
    if any(s!=indexed[rows[0]['fight_id']] for s in indexed.values()):
        conflicts.append('conflicting_or_differently_missing_participant_statistics')
    return conflicts


def manual_entries(raw):
    tree = ast.parse(raw.decode())
    constants = {}
    for n in tree.body:
        if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name):
            name = n.targets[0].id
            if name in ('JULY_18_EVENT_ID','AUG_01_EVENT_ID'): constants[name]=ast.literal_eval(n.value)
            if name=='ACTIVE_BOUTS':
                return [tuple(constants[x.id] if isinstance(x,ast.Name) else ast.literal_eval(x)
                              for x in el.elts) for el in n.value.elts]
    raise PreflightError('Missing preserved manual creation entries')


def evidence_payloads():
    checks = parent_inputs()
    names = ['sources/'+n+'.json' for n in ('events','fighters','fights','fight_stats_aggregate','schemas')]
    payloads = {n:(PARENT/n).read_bytes() for n in names}
    rows = {n:json.loads(payloads['sources/'+n+'.json']) for n in ('events','fighters','fights','fight_stats_aggregate')}
    ev = {r['event_id']:r for r in rows['events']}
    fighters = {r['fighter_id']:r for r in rows['fighters']}
    grouped = groups(rows['fights'])
    if len(grouped)!=14 or sum(map(len,grouped.values()))!=28:
        raise PreflightError('Investigation scope differs from frozen fourteen groups')
    script = 'tools/apply_manual_catchup_cards.py'
    script_raw = (ROOT/script).read_bytes()
    entries = manual_entries(script_raw)
    support = {script:sha256(script_raw),POLICY:sha256((ROOT/POLICY).read_bytes())}
    projections = {}
    decisions = []
    exclusions = load_holdout_fight_ids()
    for number,(key,fs) in enumerate(sorted(grouped.items(),key=lambda kv:(ev[kv[0][0]]['event_date'],kv[0])),1):
        event_id,*participants = key
        ids = [f['fight_id'] for f in fs]
        path = f'data/raw/ufcstats/events/{event_id}.html'
        raw = (ROOT/path).read_bytes()
        support[path] = sha256(raw)
        projection = event_link_projection(raw)
        projections[path] = projection
        participant_links = {link(fighters[fid]['source_url']) for fid in participants}
        occurrences = [r for r in projection if set(r['participant_links'])==participant_links]
        d = {'group':number,'event_id':event_id,'event_date':ev[event_id]['event_date'],
             'participant_ids':participants,'participant_names':[fighters[fid]['full_name'] for fid in participants],
             'original_ids':ids,'source_rows':[{'row':f,'row_sha256':sha256(table_bytes([f])),
                 'orientation':[f['fighter_1_id'],f['fighter_2_id']],
                 'statistics':[{'row':s,'row_sha256':sha256(table_bytes([s]))} for s in rows['fight_stats_aggregate'] if s['fight_id']==f['fight_id']]} for f in fs],
             'evidence':[{'path':path,'sha256':support[path],'meaning':'identity-only event href projection'}],
             'card_occurrences':occurrences,'excluded_original_ids':sorted(set(ids)&exclusions),
             'disposition':'unresolved','canonical_id':None,'occurrence_by_id':{f['fight_id']:link(f['source_url']) for f in fs},
             'chosen_source_lineage':None,'unresolved_conflicts':[]}
        if set(ids)==REMATCH:
            if len(occurrences)!=2 or {o['occurrence'] for o in occurrences}!={link(f['source_url']) for f in fs}:
                raise PreflightError('Same-event rematch occurrence evidence changed')
            if len({(f['finish_method'],f['finish_time_seconds']) for f in fs})!=2 or any(len(x['statistics'])!=2 for x in d['source_rows']):
                raise PreflightError('Same-event rematch support changed')
            d.update(disposition='proven_distinct',proof_kind='explicit_two_source_occurrences',
                     chosen_source_lineage={f['fight_id']:f['fight_id'] for f in fs},
                     rationale='Two separate card/detail occurrences and independently keyed statistics; no within-date ordering claimed')
        elif any(f['source_url'].startswith('manual://') for f in fs):
            manual = next(f for f in fs if f['source_url'].startswith('manual://'))
            official = next(f for f in fs if not f['source_url'].startswith('manual://'))
            matching = [r for r in entries if r[0]==event_id and {r[1],r[2]}=={fighters[fid]['full_name'] for fid in participants}]
            if len(matching)!=1 or len(occurrences)!=1 or occurrences[0]['occurrence']!=link(official['source_url']):
                raise PreflightError('Missing unique manual-announcement/source-occurrence linkage')
            entry = matching[0]
            by_name = {fighters[fid]['full_name']:fid for fid in participants}
            f1,f2 = by_name[entry[1]],by_name[entry[2]]
            expected_id = str(uuid5(NAMESPACE_URL,f'manual:fight:{event_id}:{f1}:{f2}'))
            slug = lambda n:re.sub(r'[^a-z0-9]+','-',n.lower()).strip('-')
            expected_url = f'manual://fight/{event_id}/{slug(entry[1])}-vs-{slug(entry[2])}'
            if manual['fight_id']!=expected_id or manual['source_url']!=expected_url:
                raise PreflightError('Manual creation identity not reproduced exactly')
            conflicts = alias_conflicts(fs,rows['fight_stats_aggregate'])
            d['evidence'].append({'path':script,'sha256':support[script],'meaning':'unique ACTIVE_BOUTS announcement and deterministic manual identity construction'})
            d.update(proof_kind='manual_announcement_to_unique_source_occurrence',manual_active_entry=list(entry),
                     manual_original_orientation=[f1,f2],unresolved_conflicts=conflicts)
            if not conflicts:
                d.update(disposition='proven_duplicate',canonical_id=official['fight_id'],
                    occurrence_by_id={fid:link(official['source_url']) for fid in ids},
                    chosen_source_lineage={fid:official['fight_id'] for fid in ids},
                    rationale='Manual substitute construction reproduces the unique linked source announcement; compatible complete rows, no statistic merging')
        else:
            d['unresolved_conflicts'] = ['Different old/new official URLs; no explicit URL transition, cancellation/replacement or occurrence mapping',
                                         'upcoming versus draw and bantamweight versus catch_weight fields cannot be coalesced by priority']
            d['required_evidence'] = 'Preserved source ID transition/redirect or announcement revision explicitly linking a842a365f408bb4a to 552f7cdaf93e1055; otherwise evidence of separate/cancelled occurrences'
            for f in fs:
                path = f'data/raw/ufcstats/fights/{f["fight_id"]}.html'
                support[path] = sha256((ROOT/path).read_bytes())
                d['evidence'].append({'path':path,'sha256':support[path],'meaning':'preserved page bytes; event/participant hrefs only, no result ingestion'})
        decisions.append(d)
    ledger = {'contract_version':VERSION,'policy_sha256':support[POLICY],
              'parent_checksums_sha256':PARENT_ROOT,'parent_manifest_sha256':PARENT_MANIFEST,
              'source_hashes':{n:checks[n] for n in names},'supporting_evidence_hashes':support,
              'group_count':14,'groups':decisions,'disposition_counts':dict(Counter(d['disposition'] for d in decisions)),
              'ready':all(d['disposition']!='unresolved' for d in decisions)}
    normalized,alias_map,lineage,excluded = normalize_rows(rows,ledger,exclusions)
    source_lineage = {'original_fights':len(rows['fights']),'derived_fights':len(normalized['fights']),
                     'original_statistics':len(rows['fight_stats_aggregate']),'derived_statistics':len(normalized['fight_stats_aggregate']),
                     'aliases':alias_map,'expanded_exclusion_ids':sorted(excluded),'lineage':lineage}
    manifest = {'contract_version':VERSION,'artifact_role':'blocked_history_identity_derived_view',
                'ready':ledger['ready'],'fitting_ready':False,'fitting_rows':0,'new_preparation_published':False,
                'event_cutoff_exclusive':'2026-10-02','source_capture_moved':False,
                'unresolved_groups':[d['group'] for d in decisions if d['disposition']=='unresolved'],
                'source_lineage':{k:v for k,v in source_lineage.items() if k!='lineage'},
                'source_hashes':ledger['source_hashes'],'policy_sha256':support[POLICY],
                'historical_comparison':'STILL_BLOCKED','real_predictions':0,
                'note':'No histories indexed or features recomputed while any group remains unresolved'}
    out = {'identity_ledger.json':json_bytes(ledger),'manifest.json':json_bytes(manifest),
           'source_to_derived_lineage.json':json_bytes(source_lineage),'source_identity_projections.json':json_bytes(projections),
           'policy.md':(ROOT/POLICY).read_bytes(),'package_versions.json':json_bytes(versions()),
           'code_versions.json':json_bytes(code_hashes(CODE))}
    for n,rs in normalized.items(): out['derived/'+n+'.json'] = table_bytes(rs)
    return out


def normalize_rows(rows,ledger,exclusions):
    """Create a view without indexing; unresolved rows are retained with blockers."""
    source = {f['fight_id']:f for f in rows['fights']}
    if len(source)!=len(rows['fights']): raise PreflightError('Duplicate original source bout IDs')
    stat_pairs = {(s['fight_id'],s['fighter_id']):s for s in rows['fight_stats_aggregate']}
    if len(stat_pairs)!=len(rows['fight_stats_aggregate']): raise PreflightError('Duplicate original statistic pairs')
    grouped = groups(rows['fights'])
    if len(ledger['groups'])!=len(grouped): raise PreflightError('Incomplete identity decisions')
    alias = {fid:fid for fid in source}
    visited = set()
    for d in ledger['groups']:
        ids = d['original_ids']
        key = (d['event_id'],*sorted(d['participant_ids']))
        if key in visited or key not in grouped or set(ids)!={f['fight_id'] for f in grouped[key]}:
            raise PreflightError('Identity decision scope mismatch')
        visited.add(key)
        if any(x['row_sha256']!=sha256(table_bytes([source[x['row']['fight_id']]]))
               for x in d.get('source_rows',[])):
            raise PreflightError('Identity source row changed')
        if d['disposition']=='proven_duplicate':
            if d.get('proof_kind')!='manual_announcement_to_unique_source_occurrence' or len(d.get('card_occurrences',[]))!=1 or len(d.get('evidence',[]))<2:
                raise PreflightError('Unsupported duplicate claim')
            fs = [source[fid] for fid in ids]
            if alias_conflicts(fs,rows['fight_stats_aggregate']):
                raise PreflightError('Conflicting duplicate orientation/statistics')
            canonical = d['canonical_id']
            if canonical not in ids or link(source[canonical]['source_url'])!=d['card_occurrences'][0]['occurrence']:
                raise PreflightError('Uncorroborated chosen occurrence lineage')
            for fid in ids: alias[fid]=canonical
        elif d['disposition']=='proven_distinct':
            if set(ids)!=REMATCH or d['event_id']!='fe881b94-92d3-5dbe-97e7-28014b17202d' or d['event_date']!='1997-12-21':
                raise PreflightError('Outside narrow Phase5 same-event occurrence contract')
            if d.get('proof_kind')!='explicit_two_source_occurrences' or len(d.get('card_occurrences',[]))!=len(ids) or len(set(d['occurrence_by_id'].values()))!=len(ids):
                raise PreflightError('Unsupported distinct occurrence contract')
            if set(d['occurrence_by_id'].values())!={o['occurrence'] for o in d['card_occurrences']}:
                raise PreflightError('Distinct occurrence links mismatch')
        elif d['disposition']!='unresolved': raise PreflightError('Unknown identity disposition')
    expanded = set(exclusions)
    for d in ledger['groups']:
        if d['disposition']=='proven_duplicate' and set(d['original_ids'])&expanded:
            expanded.update(d['original_ids'])
            expanded.add(d['canonical_id'])
    result = deepcopy(rows)
    result['fights'] = [deepcopy(f) for f in rows['fights'] if alias[f['fight_id']]==f['fight_id']]
    result['fight_stats_aggregate'] = [deepcopy(s) for s in rows['fight_stats_aggregate'] if alias[s['fight_id']]==s['fight_id']]
    lineage = {'fights':[{'original_id':fid,'canonical_id':alias[fid],'original_row_sha256':sha256(table_bytes([f])),
                         'chosen_row_sha256':sha256(table_bytes([source[alias[fid]]])),
                         'original_orientation':[f['fighter_1_id'],f['fighter_2_id']],
                         'canonical_orientation':[source[alias[fid]]['fighter_1_id'],source[alias[fid]]['fighter_2_id']],
                         'fitting_excluded':alias[fid] in expanded} for fid,f in sorted(source.items())],
               'statistics':[{'original_id':s['fight_stat_id'],'original_fight_id':s['fight_id'],
                    'canonical_fight_id':alias[s['fight_id']],'fighter_id':s['fighter_id'],
                    'chosen_statistic_id':stat_pairs[(alias[s['fight_id']],s['fighter_id'])]['fight_stat_id'],
                    'chosen_row_sha256':sha256(table_bytes([stat_pairs[(alias[s['fight_id']],s['fighter_id'])]])),
                    'retained_original_row':alias[s['fight_id']]==s['fight_id'],
                    'original_row_sha256':sha256(table_bytes([s]))} for s in rows['fight_stats_aggregate']],
               'unchanged_tables':{n:sha256(table_bytes(rows[n])) for n in ('events','fighters')}}
    return result,{fid:can for fid,can in sorted(alias.items()) if fid!=can},lineage,frozenset(expanded)


def history_source(rows,ledger,exclusions):
    """The only indexing entry point: fail before indexing any unresolved group."""
    if any(d['disposition']=='unresolved' for d in ledger['groups']):
        raise PreflightError('Unresolved identity groups block all history/preparation indexing')
    normalized,alias,lineage,expanded = normalize_rows(rows,ledger,exclusions)
    # Decode the captured SQL scalar types only after identity normalization.
    # Reuse the frozen schema, never a live warehouse schema/query.
    parent_inputs()
    payloads = {'sources/'+n+'.json':table_bytes(normalized[n]) for n in TABLES}
    payloads['sources/schemas.json'] = (PARENT/'sources/schemas.json').read_bytes()
    data = source_data(payloads)
    validate_source(data)
    return data,expanded


def load_reconciliation(directory: Path, *, expected_checksums_sha256: str):
    verify_checksums(directory,expected_checksums_sha256=expected_checksums_sha256)
    verify_code_and_packages(directory,CODE)
    rebuilt = evidence_payloads()
    if any((directory/n).read_bytes()!=b for n,b in rebuilt.items()):
        raise PreflightError('Reconciliation rebuild/integrity mismatch')
    ledger = json.loads((directory/'identity_ledger.json').read_bytes())
    if not ledger['ready']:
        raise PreflightError('Reconciliation remains blocked by unresolved identity evidence')
    return ledger
