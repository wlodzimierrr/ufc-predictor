"""Bounded Phase 4A.2 archive reconciliation; evidence only, no real vectors.

The v1 preparation, replay algorithm and loader remain untouched. A rejected
tree loses no historical rows: it is rejected in full, including global Elo
and opponent dependencies. Hashes identify evidence, never rank revisions.
"""

from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
import csv
import io
import json
import re
import subprocess

from modeling.scoring_inputs import (
    ROOT, TARGET_COLUMNS, SCHEMA_REQUIRED, MetadataHTML, Snapshot, PreflightError,
    assess_inputs, csv_bytes, decode_snapshot, frozen_evidence, git, json_bytes,
    read_csv, resolve_blob, sha256, utc_instant, verify_run_checksums,
    verify_training_contract, url_id,
)
from warehouse.transform import _extract_bout_flags

V1_RUN = ROOT / "data/experiments/phase4a_pre_event_2026_scoring_inputs/20261002_phase4a_archived_asof_v1_blocked"
V1_PIN = "ac762f369aa132fc3e83694071056cda217f436f6e4315b665c454963d4a59d0"
OUTPUT_ROOT = ROOT / "data/experiments/phase4a2_pre_event_2026_scoring_inputs"
POLICY_PATH = "docs/phase4a-source-policy-v2.md"
COMPONENTS = ("events", "fights", "fighters", "fight_stats")
PRODUCERS = (
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/parsers/fight_info_parser.py",
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/parsers/event_info_parser.py",
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/parsers/fighter_info_parser.py",
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/parsers/fight_stat_parser.py",
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/pipelines.py",
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/exporters.py",
    "scraper/UFC-Web-Scraping-main/Makefile",
    "tools/recover_cached_upcoming_fights.py",
    "tools/apply_manual_catchup_cards.py",
    "warehouse/load_upcoming_fights.py",
    "features/bout.py",
    "features/build_upcoming.py",
    "modeling/score_upcoming.py",
)


def load_frozen_targets(run=V1_RUN):
    if sha256((run / "checksums.json").read_bytes()) != V1_PIN:
        raise PreflightError("Frozen blocked-run pin differs")
    verify_run_checksums(run)
    columns, targets = read_csv((run / "targets.csv").read_bytes())
    if columns != TARGET_COLUMNS or len(targets) != 108 or len({t['fight_id'] for t in targets}) != 108:
        raise PreflightError("Require exact original 108 targets")
    for t in targets:
        if utc_instant(t['scored_at']) != utc_instant(t['scored_at_original']):
            raise PreflightError("Original scoring instant differs")
    return targets


def scoped_paths(targets):
    return ([f"{prefix}{name}.csv" for prefix in ("data/", "scraper/data/") for name in COMPONENTS]
            + ["data/manifests/events_manifest.csv", "models/upcoming/upcoming_features.csv"]
            + sorted({f"models/predictions/{t['event_date']}/predictions.csv" for t in targets}))


def observation(value):
    return utc_instant(value.replace(" UTC", "+00:00"))


def reconcile_rows(rows, keys, available, *, clock="scraped_at"):
    """Return selected rows and complete decisions; reject every ambiguous tie."""
    groups = defaultdict(list)
    for line, row in enumerate(rows, 2):
        groups[tuple(row[k] for k in keys)].append((line, row))
    selected, ledger, unresolved = [], [], []
    for key, revisions in sorted(groups.items()):
        unique = {}
        for line, row in revisions:
            unique.setdefault(json_bytes(row), []).append((line, row))
        distinct = [values[0][1] for values in unique.values()]
        reason, choice = "unique_record", distinct[0]
        try:
            times = [observation(r[clock]) for r in distinct]
            if any(t > utc_instant(available) for t in times):
                reason, choice = "observation_after_version_availability", None
            elif len(distinct) > 1:
                if len(set(times)) != len(times):
                    reason, choice = "ambiguous_tied_observations", None
                else:
                    choice = distinct[times.index(max(times))]
                    reason = "latest_documented_observation"
            elif len(revisions) > 1:
                reason = "identical_records_collapsed"
        except (ValueError, KeyError, TypeError):
            reason, choice = "missing_or_invalid_observation", None
        if choice is None:
            unresolved.append({"identity": list(key), "reason": reason})
        else:
            selected.append(choice)
        if len(revisions) > 1 or choice is None:
            ledger.append({"identity": list(key), "reason": reason, "records": [
                {"line": line, "raw_record": row, "record_sha256": sha256(json_bytes(row)),
                 "decision": "selected" if row == choice else "rejected",
                 "reason": reason if row == choice or choice is None else "superseded_observation"}
                for line, row in revisions]})
    return selected, ledger, unresolved


def inspect_version(commit, path, times, repo=ROOT):
    author, committer = times
    entry = {"commit": commit, "git_path": path, "author_time": author,
             "committer_time": committer,
             "availability_utc": max(utc_instant(author), utc_instant(committer)).isoformat(),
             "independently_trusted": False}
    try:
        blob = git("rev-parse", f"{commit}:{path}", repo=repo).decode().strip()
    except subprocess.CalledProcessError:
        return {**entry, "exists": False}, None
    raw = git("cat-file", "blob", blob, repo=repo)
    entry.update(exists=True, blob=blob, sha256=sha256(raw))
    if resolve_blob(entry, repo=repo) != raw:
        raise PreflightError("Resolver parity differs")
    return entry, raw


def discover_catalogue(targets, repo=ROOT):
    paths = scoped_paths(targets)
    commits = sorted(set(git("log", "--all", "--format=%H", "--", *paths, repo=repo).decode().splitlines()))
    if not commits or len(commits) > 256:
        raise PreflightError("Empty source search or 256-tree bound exceeded")
    inspected, producers, tables = [], [], {}
    for commit in commits:
        times = git("show", "-s", "--format=%aI%n%cI", commit, repo=repo).decode().splitlines()
        for path in paths:
            entry, raw = inspect_version(commit, path, times, repo)
            inspected.append(entry)
            if raw is None:
                continue
            if path.startswith("models/predictions/"):
                # Header only: probabilities and decision columns never parsed.
                entry["columns"] = next(csv.reader(io.StringIO(raw.decode())))
                entry["use"] = "schema/hash only; no title/profile/experience evidence"
                continue
            if path == "models/upcoming/upcoming_features.csv":
                entry["use"] = "metadata only; old feature vectors prohibited"
                continue
            try:
                columns, rows = read_csv(raw)
                name = Path(path).stem
                if name in COMPONENTS and not SCHEMA_REQUIRED[name] <= set(columns):
                    raise PreflightError("Required component schema missing")
                keys = ("fight_id", "fighter_id") if name == "fight_stats" else (
                    "event_id",) if name == "events_manifest" else (name[:-1] + "_id",)
                selected, decisions, unresolved = reconcile_rows(rows, keys, entry['availability_utc'],
                    clock="last_seen_at" if name == "events_manifest" else "scraped_at")
                entry.update(columns=columns, raw_rows=len(rows), selected_rows=len(selected),
                             revision_decisions=decisions, unresolved=unresolved)
                tables[commit, path] = (columns, selected, entry)
            except (ValueError, KeyError, TypeError) as exc:
                entry['unusable_reason'] = str(exc)
        for path in PRODUCERS:
            entry, _ = inspect_version(commit, path, times, repo)
            producers.append(entry)
    return {"policy_version": "phase4a_source_v2", "bound": 256, "trees_inspected": len(commits),
            "commits": commits, "data_paths": paths, "inspected_path_versions": inspected,
            "producer_path_versions": producers}, tables


def select_component_reference(tables, paths, identity, key, cap):
    """Fixed path-class priority, then latest eligible observation/version."""
    inspected = []
    for path in paths:
        candidates = []
        for (_, p), (_, rows, entry) in sorted(tables.items()):
            if p != path:
                continue
            matches = [r for r in rows if r[key] == identity]
            if not matches:
                continue
            if utc_instant(entry['availability_utc']) > utc_instant(cap):
                inspected.append({"component": entry_ref(entry), "reason": "version_after_component_cap"})
                continue
            if entry.get('unresolved'):
                inspected.append({"component": entry_ref(entry), "reason": "unresolved_component"})
                continue
            candidates.append((entry, matches[0]))
        if candidates:
            latest = max(utc_instant(e['availability_utc']) for e, _ in candidates)
            tied = [(e, r) for e, r in candidates if utc_instant(e['availability_utc']) == latest]
            if any(r != tied[0][1] for _, r in tied):
                return None, {"identity": identity, "reason": "ambiguous_tied_component_versions",
                              "candidates": [{"component": entry_ref(e), "raw_record": r} for e, r in tied]}
            e, r = tied[0]  # identical records only; commit order cannot decide content
            inspected.extend({'component': entry_ref(old), 'reason': 'superseded_component_version'}
                             for old, _ in candidates if utc_instant(old['availability_utc']) < latest)
            return r, {"identity": identity, "component": entry_ref(e), "raw_record": r,
                       "reason": "fixed_component_priority_latest_eligible", "rejected": inspected}
    return None, {"identity": identity, "reason": "no_eligible_component_reference", "rejected": inspected}


def entry_ref(entry):
    return {k: entry[k] for k in ('commit', 'git_path', 'blob', 'sha256', 'availability_utc')}


def reconcile_primary(commit, tables):
    chosen, references, rejection = {}, [], []
    for name in COMPONENTS:
        table = tables.get((commit, f"data/{name}.csv"))
        if table is None:
            rejection.append({"component": name, "reason": "missing_or_unusable_component"})
            continue
        columns, rows, entry = table
        chosen[name] = (columns, list(rows), entry)
        if entry['unresolved']:
            rejection.append({"component": name, "reason": "unresolved_revisions", "identities": entry['unresolved']})
    if rejection:
        return None, {"commit": commit, "coherent": False, "rejections": rejection}
    available = chosen['fights'][2]['availability_utc']
    for name, key, participant_keys, paths in (
        ('events', 'event_id', ('event_id',), ['data/events.csv', 'scraper/data/events.csv', 'data/manifests/events_manifest.csv']),
        ('fighters', 'fighter_id', ('fighter_1_id', 'fighter_2_id'), ['data/fighters.csv', 'scraper/data/fighters.csv']),
    ):
        columns, rows, _ = chosen[name]
        missing = sorted({f[k] for f in chosen['fights'][1] for k in participant_keys} - {r[key] for r in rows})
        for identity in missing:
            row, receipt = select_component_reference(tables, paths, identity, key, available)
            receipt['destination_component'] = name
            references.append(receipt)
            if row is None:
                rejection.append(receipt)
                continue
            if name == 'fighters' and not any(row.get(k) for k in ('height_cm', 'reach_cm', 'stance', 'dob_formatted')):
                rejection.append({**receipt, "reason": "empty_profile_stub"})
                continue
            if receipt['component']['git_path'].endswith('events_manifest.csv'):
                row = {**dict.fromkeys(columns, ''), 'event_id': row['event_id'], 'name': row['event_name'],
                       'date_formatted': row['event_date'], 'url': row['event_url'],
                       'event_status': row['event_status'], 'scraped_at': row['last_seen_at']}
            rows.append(row)
    record = {"commit": commit, "availability_utc": available, "files": {
        n: entry_ref(e) for n, (_, _, e) in chosen.items()}, "supplementary_references": references,
        "historical_fights_dropped": 0, "dependency_rule": "reject whole tree; preserve global Elo/opponent dependencies"}
    if rejection:
        return None, {**record, "coherent": False, "rejections": rejection}
    try:
        data, rows, summary = decode_snapshot({n: csv_bytes(rs, cols) for n, (cols, rs, _) in chosen.items()})
    except (ValueError, KeyError, TypeError) as exc:
        return None, {**record, "coherent": False, "rejections": [{"reason": str(exc)}]}
    record.update(coherent=True, summary=summary)
    return Snapshot(record, data, rows), record


def select_primary(snapshots, scored_at):
    eligible = [s for s in snapshots if utc_instant(s.record['availability_utc']) <= utc_instant(scored_at)]
    if not eligible:
        return None
    latest = max(utc_instant(s.record['availability_utc']) for s in eligible)
    tied = [s for s in eligible if utc_instant(s.record['availability_utc']) == latest]
    signature = lambda s: {n: e['sha256'] for n, e in s.record['files'].items()}
    if any(signature(s) != signature(tied[0]) for s in tied):
        raise PreflightError("Ambiguous tied primary versions")
    return tied[0]


def classify_csv_title(row, target):
    """No generic CSV Bout or original feature default becomes verified false."""
    if row is None:
        return None, "no_selected_target_record"
    if row['event_id'] != target['event_id'] or {row['fighter_1_id'], row['fighter_2_id']} != {
            target['fighter_1_id'], target['fighter_2_id']}:
        return None, "identity_conflict"
    wc, title, _ = _extract_bout_flags(row.get('bout_type'))
    if wc != target['weight_class']:
        return None, "weight_class_conflict"
    if title and 'Title Bout' in row.get('bout_type', ''):
        return True, "explicit_archived_title_metadata"
    return None, "generic_bout_text_without_row_producer_attestation"


def assess_recovery(targets, snapshots, v1_assignments, v1_supplements):
    if (len(targets) != 108 or len({t['fight_id'] for t in targets}) != 108 or
            [r['fight_id'] for r in v1_assignments] != [t['fight_id'] for t in targets]):
        raise PreflightError("Require exact ordered 108-row coverage")
    assignments = []
    for t, old in zip(targets, v1_assignments):
        s = select_primary(snapshots, t['scored_at'])
        if s is None:
            assignments.append({'fight_id': t['fight_id'], 'gaps': [{'kind': 'no_eligible_coherent_source'}]})
            continue
        cutoff = min(date.fromisoformat(t['event_date']), utc_instant(t['scored_at']).date())
        selected = next((r for r in s.rows['fights'] if r['fight_id'] == t['fight_id']), None)
        title, classification = classify_csv_title(selected, t)
        original_title = v1_supplements[t['fight_id']]['title']
        title_conflict = title is not None and original_title is not None and title != original_title
        if original_title is not None and not title_conflict:
            title, classification = original_title, 'verified_v1_exact_raw_body'
        gaps = []
        if title_conflict:
            title, classification = None, 'conflicting_eligible_title_metadata'
            gaps.append({'kind': 'conflicting_target_title_metadata'})
        if classification in ('identity_conflict', 'weight_class_conflict'):
            gaps.append({'kind': 'archived_target_identity_conflict'})
        if selected is not None:
            event = s.data.event_by_id.get(t['event_id'])
            if event is None or event['event_date'].isoformat() != t['event_date']:
                gaps.append({'kind': 'archived_target_date_conflict'})
        if title is None:
            gaps.append({'kind': 'unknown_target_title_status'})
        history = [f for f in s.data.fights if f['event_date'] < cutoff and f['fight_id'] != t['fight_id']
                   and f['result_type'] in {'win', 'draw', 'nc'}]
        profiles = []
        for fid in (t['fighter_1_id'], t['fighter_2_id']):
            profile = s.data.fighter_by_id.get(fid)
            prior_ids = sorted(f['fight_id'] for f in history if fid in (f['fighter_1_id'], f['fighter_2_id']))
            origin = 'selected_archive_profile'
            if profile is not None and not any(profile.get(k) is not None for k in ('height_cm', 'reach_cm', 'stance', 'dob')):
                profile, origin = None, 'empty_profile_not_certified'
            if profile is None:
                profile = v1_supplements[t['fight_id']]['profiles'].get(fid)
                origin = 'verified_v1_exact_raw_profile' if profile else origin
            if profile is None:
                gaps.append({'kind': 'missing_target_profile', 'identity': fid})
            elif origin == 'verified_v1_exact_raw_profile' and not prior_ids:
                gaps.append({'kind': 'unresolved_experience_for_supplemented_profile', 'identity': fid})
            profiles.append({'fighter_id': fid, 'profile_classification': origin, 'prior_fight_ids': prior_ids,
                             'prior_count': len(prior_ids), 'zero_experience_certified': False})
        assignments.append({'fight_id': t['fight_id'], 'scored_at': t['scored_at'],
            'source_commit': s.record['commit'], 'source_components': s.record['files'],
            'supplementary_references': s.record['supplementary_references'],
            'history_date_cutoff_exclusive': cutoff.isoformat(), 'feature_reference_date': t['event_date'],
            'title_status': title, 'title_classification': classification,
            'selected_target_metadata': {k: selected[k] for k in ('fight_id', 'event_id', 'fighter_1_id',
                'fighter_2_id', 'scraped_at', 'bout_type', 'weight_class')} if selected else None,
            'v1_raw_supplements': old['supplements'], 'history_profiles': profiles,
            'previous_gaps': old['gaps'], 'gaps': gaps, 'essential_inputs_ready': not gaps})
    return assignments


def inspect_local_metadata(targets, repo=ROOT):
    paths = scoped_paths(targets)[9:]
    evidence = []
    allowed = ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id', 'event_date', 'scored_at',
               'computed_at', 'is_title_fight')
    identities = {t['fight_id'] for t in targets}
    for path in paths:
        p = repo / path
        entry = {'path': path, 'exists': p.is_file(), 'historical_availability_certified': False,
                 'mtime_used_as_availability': False}
        if p.is_file():
            raw = p.read_bytes()
            reader = csv.DictReader(io.StringIO(raw.decode()))
            metadata = [{k: r[k] for k in allowed if k in r} for r in reader]
            entry.update(sha256=sha256(raw), rows=len(metadata), metadata_columns=[k for k in allowed if k in reader.fieldnames],
                         target_metadata=[r for r in metadata if r['fight_id'] in identities],
                         event_dates=sorted({r['event_date'] for r in metadata}),
                         use='metadata inspection only; no historical feature-vector reuse or default certification')
        evidence.append(entry)
    return evidence


def profile_history_references(bodies):
    """Inspect preserved profile bodies for identity/date links only.

    A positive prior reference disproves a safe zero assumption. It does not
    supply complete chronological result/statistic history. An empty prior
    listing likewise does not certify debut.
    """
    evidence = []
    for path, raw in sorted(bodies.items()):
        if not path.startswith('sources/raw/fighter/'):
            continue
        references = []
        for tr in re.findall(r'<tr\b[^>]*>.*?</tr>', raw.decode(), re.S):
            parser = MetadataHTML()
            parser.feed(tr)
            fights = sorted({url_id(u) for u in parser.links if '/fight-details/' in u})
            events = sorted({url_id(u) for u in parser.links if '/event-details/' in u})
            if not fights or not events:
                continue
            text = re.sub(r'<[^>]+>', ' ', tr)
            match = re.search(r'\b([A-Z][a-z]{2})\.?\s+(\d{1,2}),\s+(\d{4})\b', text)
            explicit_date = datetime.strptime(' '.join(match.groups()), '%b %d %Y').date().isoformat() if match else None
            references.append({'fight_ids': fights, 'event_ids': events, 'explicit_event_date': explicit_date})
        evidence.append({'body_path': path, 'sha256': sha256(raw), 'references': references,
                         'complete_experience_certified': False, 'results_or_statistics_used': False})
    return evidence


def prepare_recovery(repo=ROOT):
    targets = load_frozen_targets()
    contract = verify_training_contract()
    catalogue, tables = discover_catalogue(targets, repo)
    snapshots, reconciliations = [], []
    for commit in catalogue['commits']:
        s, record = reconcile_primary(commit, tables)
        reconciliations.append(record)
        if s:
            snapshots.append(s)
    original_snapshots, relevant, raw_evidence = frozen_evidence(V1_RUN)
    old, bodies, supplements = assess_inputs(targets, original_snapshots, relevant, raw_evidence, frozen_run=V1_RUN)
    frozen_old = json.loads((V1_RUN / 'source-selection.json').read_bytes())['assignments']
    if old != frozen_old:
        raise PreflightError("Original frozen assessment parity differs")
    assignments = assess_recovery(targets, snapshots, old, supplements)
    for assignment in assignments:
        assignment['rejected_primary_versions'] = [
            {'commit': r['commit'], 'reason': 'incoherent_tree' if not r['coherent'] else
             'version_after_scoring' if utc_instant(r['availability_utc']) > utc_instant(assignment['scored_at']) else
             'superseded_eligible_tree'}
            for r in reconciliations if r['commit'] != assignment['source_commit']]
    counts = dict(Counter(g['kind'] for r in assignments for g in r['gaps']))
    affected_before = {r['fight_id'] for r in old if r['gaps']}
    affected_now = {r['fight_id'] for r in assignments if r['gaps']}
    if not affected_now:
        raise PreflightError("Evidence-only publisher requires blocked inputs; complete reconstruction needs validated v2 adapter")
    status = 'STILL_BLOCKED'
    manifest = {**catalogue, 'status': status, 'policy_document_sha256': sha256((ROOT / POLICY_PATH).read_bytes()),
                'source_availability_qualification': 'repository/capture evidence only; not independently trusted',
                'archive_completeness_certified': False, 'reconciled_primary_versions': reconciliations,
                'original_run_checksums_sha256': V1_PIN,
                'v1_raw_evidence_sha256': sha256((V1_RUN / 'source-fetch-evidence.json').read_bytes()),
                'code_sha256': {p: sha256((ROOT / p).read_bytes()) for p in (
                    'modeling/source_reconciliation.py', 'tools/prepare_phase4a2_source_recovery.py',
                    'modeling/scoring_inputs.py', 'features/forecast_replay.py')}}
    validation = {'status': status, 'target_rows': 108, 'original_affected_forecasts': len(affected_before),
        'resolved_original_forecasts': len(affected_before - affected_now),
        'remaining_affected_forecasts': len(affected_now), 'new_affected_forecasts': len(affected_now - affected_before),
        'gap_counts': counts, 'resolved_gap_counts': {
            kind: dict(Counter(g['kind'] for r in old for g in r['gaps']))[kind] - counts.get(kind, 0)
            for kind in sorted({g['kind'] for r in old for g in r['gaps']})},
        'features_constructed': False, 'real_feature_rows_constructed': 0,
        'candidate_forecasts_scored': False, 'frozen_outcomes_parsed': False, 'outcome_metrics_calculated': False,
        'historical_source_records_structurally_validated': True,
        'model_fitted': False, 'warehouse_accessed': False, 'historical_fights_dropped': 0,
        'phase4b_available': False, 'bounded_attempt_complete': True}
    payload = {'targets.csv': (V1_RUN / 'targets.csv').read_bytes(),
        'comparison-protocol.json': (V1_RUN / 'comparison-protocol.json').read_bytes(),
        'source-manifest.json': json_bytes(manifest),
        'source-selection.json': json_bytes({'status': status, 'assignments': assignments}),
        'raw-profile-history-references.json': json_bytes(profile_history_references(bodies)),
        'local-metadata-evidence.json': json_bytes(inspect_local_metadata(targets, repo)),
        'compatibility.json': json_bytes({**contract, 'status': 'BLOCKED_NO_COMPLETE_SCORING_INPUT',
            'real_vectors_validated': False, 'loader_reference_supplied': False,
            'actual_recovery_manifest_sha256': sha256(json_bytes(manifest)),
            'policy_version': 'phase4a_source_v2', 'training_contract_is_scoring_source': False}),
        'validation_results.json': json_bytes(validation),
        'INCOMPLETE.json': json_bytes({'status': status, 'meaning': 'recovery evidence only; no completed scoring inputs; do not score',
                                     'gap_counts': counts}), **bodies}
    return payload, validation


def publish_recovery(destination, payload):
    if destination.parent.resolve() != OUTPUT_ROOT.resolve() or destination.exists() or destination.is_symlink():
        raise PreflightError("Require new isolated Phase 4A.2 run; refusing overwrite")
    if 'features.csv' in payload or 'INCOMPLETE.json' not in payload:
        raise PreflightError("Incomplete recovery cannot publish features")
    for name in payload:
        if Path(name).is_absolute() or '..' in Path(name).parts:
            raise PreflightError("Invalid artifact path")
    destination.mkdir(parents=True, exist_ok=False)
    for name, raw in sorted(payload.items()):
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as f:
            f.write(raw)
    checksums = {'schema_version': 1, 'self_excluded': 'checksums.json',
                 'files': {k: sha256(v) for k, v in sorted(payload.items())}}
    with (destination / 'checksums.json').open('xb') as f:
        f.write(json_bytes(checksums))
    for p in destination.rglob('*'):
        if p.is_file():
            p.chmod(0o444)
    verify_run_checksums(destination)
    return checksums
