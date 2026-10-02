"""Immutable metadata successors. Forecast probabilities live only in run bundles."""
from contextlib import contextmanager
import fcntl
import json
from modeling.phase5c1_contract import decoded
from pathlib import Path
from modeling.phase5c1_contract import (IDENTITY, LEGACY, LEGACY_PIN, PINS, VERSION,
    digest, encoded, instant, inventory, local_bytes, now, publish, require)


@contextmanager
def exclusive(root):
    root = Path(root)
    from modeling.phase5c1_contract import ROOT, OUTPUT
    require(not root.resolve().is_relative_to(ROOT) or root.resolve().is_relative_to(OUTPUT), 'isolated_phase5c_root_required')
    root.mkdir(parents=True, exist_ok=True)
    with (root / '.publication.lock').open('a+b') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            from modeling.phase5c1_contract import Blocked
            raise Blocked('concurrent_publication') from exc
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def namespace(root, *, synthetic):
    """A synthetic namespace can never become a real trial journal."""
    p = Path(root) / 'namespace.json'
    expected = {'version': VERSION, 'synthetic': synthetic, 'purpose': 'SYNTHETIC_IMPLEMENTATION_TEST_ONLY' if synthetic else 'AUTHORIZED_PROSPECTIVE_TRIAL'}
    if p.exists():
        require(local_bytes(root, 'namespace.json') == encoded(expected), 'synthetic_real_journal_mixing_forbidden')
    else:
        with p.open('xb') as f:
            f.write(encoded(expected))
        p.chmod(0o444)


def latest(root):
    directories = sorted((Path(root) / 'registry').glob('*'))
    if not directories:
        return None, None, []
    parent, tail, entries = None, None, []
    for d in directories:
        require(not (d / 'INCOMPLETE').exists(), 'incomplete_registry_requires_review')
        from modeling.phase5c1_contract import sha
        pin = sha(local_bytes(d, 'checksums.json'))
        inventory(d, pin)
        m = decoded(local_bytes(d, 'successor.json'))
        require(m['parent_checksums'] == tail and m['sequence'] == len(entries) + 1, 'registry_chain_conflict')
        require(m['previous_entry_sha256'] == (digest(entries[-1]) if entries else None), 'registry_entry_chain')
        current = decoded(local_bytes(d, 'entries.json'))
        require(current[:-1] == entries and len(current) == len(entries) + 1, 'registry_rewrite')
        entries = current
        parent, tail = d, pin
    return parent, tail, entries


def append(root, entry):
    """Caller must hold exclusive(). No probabilities/outcomes enter this journal."""
    from modeling.prospective_registry import no_predictions_or_outcomes
    no_predictions_or_outcomes(entry)
    parent, pin, entries = latest(root)
    inventory(LEGACY, LEGACY_PIN)
    legacy = {str(p.relative_to(LEGACY)): p.read_bytes() for p in (LEGACY / 'registry/records').glob('*.json')}
    require(len(legacy) == 35, 'original_registry_population_changed')
    payload = {'legacy/' + n: b for n, b in legacy.items()}
    if parent:
        for n, b in payload.items():
            require(local_bytes(parent, n) == b, 'original_registry_bytes_changed')
    e = dict(entry, components=PINS, contract=VERSION)
    payload['entries.json'] = encoded(entries + [e])
    payload['successor.json'] = encoded({'sequence': len(entries) + 1, 'parent_checksums': pin,
        'previous_entry_sha256': digest(entries[-1]) if entries else None,
        'legacy_parent_checksums': LEGACY_PIN, 'legacy_records': 35, 'created_at': now()})
    d = Path(root) / 'registry' / f'{len(entries)+1:08d}'
    return publish(d, payload), e


def state(entries):
    bouts = {}
    for e in entries:
        action = e['action']
        if action in {'register', 'revise', 'replace'}:
            key = e['bout']['source_row_id']
            if action == 'replace':
                bouts[e['replaces']]['disposition'] = 'replaced'
                bouts[e['replaces']]['primary_valid'] = False
            previous = bouts.get(key, {})
            bouts[key] = {**previous, 'bout': e['bout'], 'registered_at': e['recorded_at'],
                          'revision': previous.get('revision', 0) + (action == 'revise'),
                          'disposition': 'active', 'primary_valid': action != 'revise',
                          'selected': previous.get('selected'), 'attempts': previous.get('attempts', [])}
        elif action == 'cancel':
            bouts[e['source_row_id']]['disposition'] = 'cancelled'
            bouts[e['source_row_id']]['primary_valid'] = False
        elif action in {'attempt_reserved', 'attempt_completed', 'attempt_failed'}:
            for key in e['source_row_ids']:
                b = bouts[key]
                if action == 'attempt_reserved':
                    b['attempts'].append(e)
                elif action == 'attempt_completed':
                    require(b.get('selected') is None, 'first_pair_already_selected')
                    b['selected'] = e['run']
                    b['primary_valid'] = True
    return bouts


def register(root, considered, *, receipt_pin, recorded_at, action='register', replaces=None):
    _, _, entries = latest(root)
    current = state(entries)
    for bout in considered:
        key = bout['source_row_id']
        old = current.get(key)
        if action == 'register' and old:
            require(all(old['bout'].get(k) == bout.get(k) for k in (*IDENTITY, 'event_date')), 'explicit_revision_required')
            continue
        require(action in {'register', 'revise', 'replace'}, 'registration_action')
        if action == 'revise':
            require(old is not None and old['disposition'] == 'active', 'revision_of_unregistered_or_inactive_bout')
            require(any(old['bout'].get(k) != bout.get(k) for k in (*IDENTITY, 'event_date')), 'revision_without_change')
        if action == 'replace':
            require(old is None and replaces in current and current[replaces]['disposition'] == 'active', 'invalid_replacement')
        pin, _ = append(root, {'action': action, 'bout': bout, 'recorded_at': recorded_at,
                             'capture_receipt_sha256': receipt_pin, 'replaces': replaces})
    return latest(root)[1]


def cancel(root, key, *, recorded_at, reason):
    current = state(latest(root)[2])
    require(key in current and current[key]['disposition'] == 'active' and bool(reason), 'invalid_cancellation')
    return append(root, {'action': 'cancel', 'source_row_id': key, 'recorded_at': recorded_at, 'reason': reason})[0]
