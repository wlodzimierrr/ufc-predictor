"""Fresh receipt validation and role projections, independent of preparation loading."""
from copy import deepcopy
from datetime import timedelta
import json
from modeling.phase5c1_contract import decoded
from modeling.phase5c1_contract import (Blocked, IDENTITY, LEGACY, LEGACY_PIN, SOURCE_VERSION,
    TABLES, digest, encoded, instant, inventory, local_bytes, require, sha, uuid)
from modeling.phase5c1_evidence import validate_evidence


def validate_package(directory, *, receipt_pin, contract):
    raw = local_bytes(directory, 'capture_receipt.json')
    require(sha(raw) == receipt_pin, 'capture_receipt_pin_mismatch')
    r = decoded(raw)
    require(r.get('version') == SOURCE_VERSION, 'source_contract_mismatch')
    uuid(r['capture_id'])
    require(r.get('synthetic') in (True, False) and type(r.get('synthetic')) is bool, 'capture_kind_required')
    require(r.get('mode') in {'authoritative_export', 'readonly_repeatable_read'}, 'source_mode')
    require(r.get('provider') and r.get('source_scope'), 'source_scope_required')
    require(set(r['tables']) == set(TABLES) | {'schemas'}, 'source_inventory')
    start, end = instant(r['started_at']), instant(r['completed_at'])
    cutoff = instant(r['observation_cutoff'])
    require(start <= end == cutoff, 'common_capture_cutoff')
    freezes = [instant(contract['frozen_at']), *[instant(t) for t in contract['component_freezes']]]
    require(start > max(freezes), 'capture_before_component_or_contract_freeze')
    if not r['synthetic']:
        from modeling.phase5c1_contract import now
        require(end <= instant(now()), 'future_observation_timestamp')
    training_receipt = local_bytes(LEGACY, 'source_capture_receipt.json')
    require(sha(raw) != sha(training_receipt), 'original_training_capture_replay')
    if r['mode'] == 'readonly_repeatable_read':
        tx = r['transaction']
        require(tx['read_only'] == 'on' and tx['isolation'] == 'repeatable read' and tx['rolled_back'] is True and tx['closed'] is True, 'unverified_readonly_transaction')
        require(tx.get('database_timezone') and tx.get('snapshot') and tx.get('queries'), 'transaction_receipt_incomplete')
    payloads, rows = {}, {}
    for table, entry in r['tables'].items():
        body = local_bytes(directory, entry['body'])
        require(sha(body) == entry['sha256'] and len(body) == entry['bytes'], 'source_hash_or_size_mismatch:' + table)
        require(start <= instant(entry['requested_at']) <= instant(entry['observed_at']) <= cutoff, 'source_observation_chronology')
        require(entry.get('provider') == r['provider'] and entry.get('url'), 'source_provider_identity')
        payloads[entry['body']] = body
        rows[table] = decoded(body)
    inventory(LEGACY, LEGACY_PIN)
    accepted_schema = decoded(local_bytes(LEGACY, 'sources/schemas.json'))
    require([s for s in rows['schemas'] if s['table_name'] in TABLES] ==
            [s for s in accepted_schema if s['table_name'] in TABLES], 'incompatible_source_schema')
    require(all(s in accepted_schema for s in rows['schemas']), 'unexpected_schema_component')
    from modeling.phase5_role_aware import validate_raw
    # Same UTC date results may be structurally observed, but never historical
    # inputs. Tomorrow's resolved records are inconsistent with this observation.
    validate_raw({n: rows[n] for n in TABLES}, rows['schemas'], (cutoff.date() + timedelta(days=1)).isoformat())
    for table in TABLES:
        for row in rows[table]:
            if row.get('scraped_at'):
                require(instant(row['scraped_at']) <= cutoff, 'source_row_observed_after_cutoff')
            if table == 'fighters' and row.get('dob'):
                require(row['dob'] <= cutoff.date().isoformat(), 'profile_chronology')
    evidence_raw = local_bytes(directory, r['evidence_body'])
    require(sha(evidence_raw) == r['evidence_sha256'], 'evidence_package_hash_mismatch')
    evidence = validate_evidence(directory, decoded(evidence_raw), r['observation_cutoff'])
    considered = r['considered']
    require(isinstance(considered, list), 'considered_scope_required')
    require(len({c['source_row_id'] for c in considered}) == len(considered), 'duplicate_considered_row')
    future = {f['fight_id'] for f in rows['fights'] if f['result_type'] == 'upcoming' and
              next(e['event_date'] for e in rows['events'] if e['event_id'] == f['event_id']) >= cutoff.date().isoformat()}
    require(future <= {c.get('fight_id') for c in considered}, 'incomplete_considered_bout_coverage')
    for c in considered:
        if c.get('fight_id'):
            matches = [f for f in rows['fights'] if f['fight_id'] == c['fight_id']]
            require(len(matches) == 1 and matches[0]['result_type'] == 'upcoming', 'considered_target_result_leakage')
            require(all(c.get(k) == matches[0][k] for k in IDENTITY), 'considered_identity_mismatch')
            event = next(e for e in rows['events'] if e['event_id'] == matches[0]['event_id'])
            require(c['event_date'] == event['event_date'], 'considered_announced_date_mismatch')
    return {'version': SOURCE_VERSION, 'receipt': r, 'receipt_sha256': receipt_pin,
            'rows': {n: rows[n] for n in TABLES}, 'schemas': rows['schemas'], 'evidence': evidence,
            'source_sha256': {n: r['tables'][n]['sha256'] for n in TABLES},
            'evidence_sha256': r['evidence_sha256'], 'directory': str(directory)}


def project(package):
    """Normalize whole rows before any computational indexing; no ledger copying."""
    rows = deepcopy(package['rows'])
    events = {e['event_id']: e['event_date'] for e in rows['events']}
    fs = {f['fight_id']: f for f in rows['fights']}
    aliases, decisions = {}, []
    from modeling.phase5_history_identity import alias_conflicts
    from modeling.phase5_role_aware import normalize_proven, validate_occurrences
    # Previously accepted mappings are reused only after exact contemporary
    # row/statistic validation. Changed observations need a fresh explicit claim.
    ledger_root = LEGACY.parent.parent / 'phase5b1_reference_and_history/20261002_phase5b1_history_reconciliation_v1_blocked'
    inventory(ledger_root, '8c28425abf011bcce88cff2fa8d3f4d866156396a84a59f9a609050aa16344cb')
    ledger = decoded(local_bytes(ledger_root, 'identity_ledger.json'))['groups']
    claims = [a['claim'] for a in package['evidence']['assertions'] if a['kind'] == 'identity']
    for d in ledger:
        if d['disposition'] == 'unresolved' or not set(d['original_ids']) <= set(fs):
            continue
        if any(set(c['fight_ids']) == set(d['original_ids']) for c in claims):
            continue
        # This pure guard checks exact preserved whole-row/stat hashes anew.
        normalize_proven(rows, [d], set())
        decisions.append(d)
    for c in claims:
        ids = c['fight_ids']
        require(len(ids) == 2 and len(set(ids)) == 2 and set(ids) <= set(fs), 'identity_relationship_scope')
        actual = [fs[fid] for fid in ids]
        require(c['event_id'] == actual[0]['event_id'] == actual[1]['event_id'] and
                c['event_date'] == events[c['event_id']] and
                all(set(c['participant_ids']) == {f['fighter_1_id'], f['fighter_2_id']} for f in actual), 'identity_relationship_mismatch')
        from modeling.phase5_current_data import table_bytes
        require(c['row_sha256'] == {fid: sha(table_bytes([fs[fid]])) for fid in ids}, 'changed_identity_row_lineage')
        require(c['statistic_sha256'] == {fid: sorted(sha(table_bytes([s])) for s in rows['fight_stats_aggregate'] if s['fight_id'] == fid) for fid in ids}, 'changed_identity_statistic_lineage')
        if c['relationship'] == 'distinct_occurrences':
            # The unchanged legacy reference gate supports only its exact rematch.
            matches = [d for d in ledger if d['disposition'] == 'proven_distinct' and set(d['original_ids']) == set(ids)]
            require(len(matches) == 1, 'distinct_occurrence_requires_versioned_adapter_contract')
            normalize_proven(rows, matches, set())
            decisions.extend(matches)
        else:
            require(not alias_conflicts(actual, rows['fight_stats_aggregate']), 'conflicting_source_rows_or_statistics')
            require(c['canonical_id'] in ids and c.get('explicit_transition') is True, 'unsupported_alias')
            require(actual[0]['fighter_1_id'] == actual[1]['fighter_1_id'], 'alias_orientation_conflict')
            for fid in ids:
                require(fid not in aliases, 'overlapping_identity_relationship')
                aliases[fid] = c['canonical_id']
    normalized, accepted_aliases, _ = normalize_proven(rows, decisions, set())
    for a, b in accepted_aliases.items():
        if a != b:
            require(a not in aliases, 'overlapping_identity_relationship')
            aliases[a] = b
    normalized['fights'] = [f for f in normalized['fights'] if aliases.get(f['fight_id'], f['fight_id']) == f['fight_id']]
    normalized['fight_stats_aggregate'] = [s for s in normalized['fight_stats_aggregate'] if aliases.get(s['fight_id'], s['fight_id']) == s['fight_id']]
    cutoff = instant(package['receipt']['observation_cutoff']).date().isoformat()
    history = [dict(f, event_date=events[f['event_id']]) for f in normalized['fights']
               if f['result_type'] in {'win', 'draw', 'nc'} and events[f['event_id']] < cutoff]
    validate_occurrences(history, decisions, 'prospective_history')
    ids = {f['fight_id'] for f in history}
    targets = []
    for c in package['receipt']['considered']:
        if c.get('fight_id'):
            f = fs[c['fight_id']]
            targets.append({k: f[k] for k in (*IDENTITY, 'weight_class', 'source_url')} |
                           {'event_date': events[f['event_id']], 'source_row_id': c['source_row_id']})
        else:
            targets.append(deepcopy(c))
    # Target conflicts are explicit blockers; stale non-target announcements do
    # not enter history or any legacy/corrected index.
    known = [t for t in targets if t.get('fight_id')]
    validate_occurrences([t for t in known if aliases.get(t['fight_id'], t['fight_id']) == t['fight_id']], decisions, 'prospective_targets')
    lineage = [{'fight_id': f['fight_id'], 'canonical_id': aliases.get(f['fight_id'], f['fight_id']),
                'row_sha256': digest(f), 'history_admitted': f['fight_id'] in ids,
                'reason': 'earlier_resolved' if f['fight_id'] in ids else 'not_earlier_resolved_history'} for f in rows['fights']]
    stats = [s for s in normalized['fight_stats_aggregate'] if s['fight_id'] in ids]
    return {'version': SOURCE_VERSION, 'history': history, 'targets': targets,
            'events': rows['events'], 'profiles': rows['fighters'], 'statistics': stats,
            'aliases': aliases, 'decisions': decisions, 'fight_lineage': lineage,
            'statistic_lineage': [{'fight_stat_id': s['fight_stat_id'], 'fight_id': s['fight_id'],
                'fighter_id': s['fighter_id'], 'row_sha256': digest(s),
                'admitted': s in stats} for s in rows['fight_stats_aggregate']],
            'source_order_preserved': True}


def _readonly_capture(connection_factory, *, clock, mock_required):
    """Future capture boundary; this version permits marked mocks only.

    A later real invocation requires separate authorization and activation of
    this boundary. No connection factory is hidden in preparation/prediction.
    """
    started_at = clock()
    conn = connection_factory()
    queries, payloads = [], {}
    try:
        if mock_required:
            require(getattr(conn, 'phase5c1_mock', False) is True, 'real_capture_not_authorized')
        conn.set_session(isolation_level='REPEATABLE READ', readonly=True, autocommit=False)
        with conn.cursor() as cur:
            def query(sql):
                begun = clock()
                cur.execute(sql)
                result = cur.fetchall()
                queries.append({'sql': sql, 'started_at': begun, 'finished_at': clock(),
                                'columns': [d[0] for d in cur.description], 'rows': len(result)})
                return [dict(zip([d[0] for d in cur.description], row)) for row in result]
            require(query('SHOW transaction_read_only')[0]['transaction_read_only'] == 'on', 'read_only_verification_failed')
            require(query('SHOW transaction_isolation')[0]['transaction_isolation'] == 'repeatable read', 'repeatable_read_verification_failed')
            tx = query("SELECT transaction_timestamp() AS transaction_started_at, current_setting('TimeZone') AS database_timezone, pg_current_snapshot()::text AS snapshot")[0]
            from modeling.phase5_current_data import SCHEMA_QUERY, table_bytes
            payloads['schemas'] = table_bytes(query(SCHEMA_QUERY.replace('= ANY(%s)', "IN ('events','fighters','fights','fight_stats_aggregate')")))
            for table, key in zip(TABLES, ('event_id', 'fighter_id', 'fight_id', 'fight_stat_id')):
                payloads[table] = table_bytes(query(f'SELECT * FROM public."{table}" ORDER BY "{key}"'))
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()
    return payloads, {'queries': queries, 'started_at': started_at, 'completed_at': clock(),
                      'transaction': tx | {'read_only': 'on', 'isolation': 'repeatable read',
                         'rolled_back': True, 'closed': True, 'queries': queries},
                      'rolled_back': True, 'closed': True,
                      'source_sha256': {n: sha(b) for n, b in payloads.items()}, 'synthetic': mock_required}


def mocked_readonly_capture(connection_factory, *, synthetic_authorization, clock):
    require(synthetic_authorization == 'phase5c1_mock_capture_only', 'separate_capture_authorization_required')
    return _readonly_capture(connection_factory, clock=clock, mock_required=True)


def authorized_readonly_capture(connection_factory, *, authorization):
    """Explicit future boundary; never invoked in Phase5C.1 or by model loading."""
    require(isinstance(authorization, dict) and authorization.get('scope') == 'separately_authorized_phase5c2_readonly_capture'
            and authorization.get('authorization_text_sha256') and authorization.get('source_identity'),
            'separate_capture_authorization_required')
    from modeling.phase5c1_contract import now
    return _readonly_capture(connection_factory, clock=now, mock_required=False)
