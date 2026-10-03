"""Scoped mocks only; no live connection, learner inference, fit or outcome read."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from modeling.phase5c1_contract import Blocked, ROOT, TABLES, decoded, digest, encoded, instant, inventory, now, sha
from modeling.phase5c1_safety import implementation_guards
from modeling import phase5c2_activation as a


@pytest.fixture(autouse=True)
def safe():
    with implementation_guards():
        yield


@pytest.fixture
def frozen(tmp_path):
    auth = tmp_path / 'authorization.txt'
    auth.write_bytes(b'SYNTHETIC TEST AUTHORIZATION: mock capture and mock inference only.\n')
    d = tmp_path / 'activation'
    f = a.freeze(d, auth)
    return d, f['activation_checksums_sha256']


class Connection:
    """Pure in-memory SQL fixture, including contemporary mock transaction clocks."""
    def __init__(self, *, readonly='on', isolation='repeatable read', fail_index=None):
        from modeling.phase5b3_non_influence import synthetic_rows
        self.rows, self.schemas, _ = synthetic_rows()
        for e in self.rows['events']:
            if (any(f['result_type'] == 'upcoming' and f['event_id'] == e['event_id'] for f in self.rows['fights'])
                    and not any(f['result_type'] != 'upcoming' and f['event_id'] == e['event_id'] for f in self.rows['fights'])):
                e['event_date'] = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
        self.readonly, self.isolation, self.fail_index = readonly, isolation, fail_index
        self.sql, self.rolled_back, self.closed = [], False, False
        self.description, self.result = [], []
    def set_session(self, **kwargs):
        assert kwargs == {'isolation_level': 'REPEATABLE READ', 'readonly': True, 'autocommit': False}
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql):
        assert sql == a._queries()[len(self.sql)]
        self.sql.append(sql)
        if len(self.sql) - 1 == self.fail_index:
            raise RuntimeError('postgresql://SECRET_USER:SECRET_PASSWORD@SECRET_HOST/SECRET_DB')
        if sql.startswith('SHOW transaction_read_only'): rows = [{'transaction_read_only': self.readonly}]
        elif sql.startswith('SHOW transaction_isolation'): rows = [{'transaction_isolation': self.isolation}]
        elif sql.startswith('SELECT transaction_timestamp'):
            rows = [{'transaction_started_at': now(), 'database_timezone': 'UTC', 'snapshot': 'SYNTHETIC_SNAPSHOT'}]
        elif 'information_schema' in sql: rows = [s for s in self.schemas if s['table_name'] in TABLES]
        else: rows = self.rows[next(t for t in TABLES if '"' + t + '"' in sql)]
        self.description = [(k,) for k in rows[0]] if rows else []
        self.result = [tuple(r[k] for k, in self.description) for r in rows]
    def fetchall(self): return self.result
    def rollback(self): self.rolled_back = True
    def close(self): self.closed = True


def captured(frozen, conn=None):
    d, pin = frozen
    conn = conn or Connection()
    f = a.capture(d.parent / 'capture_attempt', activation_directory=d, activation_pin=pin, connection_factory=lambda: conn)
    return d, pin, f, conn


def test_exact_activation_code_config_authorization_and_freeze(frozen):
    d, pin = frozen
    m, c = a.activation(d, pin)
    assert m['authorization_text_sha256'] == sha((d / 'authorization.txt').read_bytes())
    assert instant(m['frozen_at']) > instant(c['frozen_at'])
    assert set(m['code_sha256']) == set(a.CODE)
    assert all((d / 'code' / p).read_bytes() == (ROOT / p).read_bytes() for p in a.CODE)
    with pytest.raises(FileExistsError): a.freeze(d, d / 'authorization.txt')


def test_capture_scope_exact_bytes_receipts_cleanup_and_single_attempt(frozen):
    d, pin, f, conn = captured(frozen)
    assert f['status'] == 'READY' and len(conn.sql) == 8
    assert conn.rolled_back and conn.closed and not any('bout_features' in q for q in conn.sql)
    obs = d.parent / 'capture_attempt/observation'
    inventory(obs, f['observation_checksums_sha256'])
    r = decoded((obs / 'capture_receipt.json').read_bytes())
    assert instant(r['started_at']) > instant(a.activation(d, pin)[0]['frozen_at'])
    assert r['observation_cutoff'] == r['completed_at'] and r['transaction']['snapshot'] == 'SYNTHETIC_SNAPSHOT'
    assert set(r['tables']) == set(TABLES) | {'schemas'}
    assert decoded((obs / 'evidence_package.json').read_bytes()) == {'version': 'phase5c1_evidence_package_v1', 'sources': {}, 'assertions': []}
    assert len(r['considered']) == sum(f['result_type'] == 'upcoming' for f in conn.rows['fights'])
    for t in r['tables']:
        api = decoded((obs / 'capture_api_receipt.json').read_bytes())
        audit = decoded((d.parent / 'capture_attempt/result/query_audit.json').read_bytes())
        i = ('schemas', *TABLES).index(t) + 3
        assert (obs / r['tables'][t]['body']).read_bytes() == (d.parent / 'capture_attempt/acquisition_log' / audit[i]['body']).read_bytes()
        assert r['tables'][t]['observed_at'] == api['queries'][i]['finished_at']
    factory = Mock(side_effect=AssertionError('Second connection forbidden'))
    with pytest.raises(FileExistsError): a.capture(d.parent / 'capture_attempt', activation_directory=d, activation_pin=pin, connection_factory=factory)
    with pytest.raises(Blocked, match='fixed_capture'): a.capture(d.parent / 'other_capture', activation_directory=d, activation_pin=pin, connection_factory=factory)
    factory.assert_not_called()


@pytest.mark.parametrize('kwargs,query_count', [({'readonly': 'off'}, 1), ({'isolation': 'read committed'}, 2), ({'fail_index': 4}, 5)])
def test_capture_failure_preserves_partial_receipts_suppresses_secrets_and_no_retry(frozen, kwargs, query_count):
    d, pin, f, conn = captured(frozen, Connection(**kwargs))
    assert f['status'] == 'BLOCKED' and f['acquisition_stopped'] and f['prediction_calls'] == 0
    assert conn.rolled_back and conn.closed and len(conn.sql) == query_count
    result_dir = d.parent / 'capture_attempt/result'
    inventory(result_dir, f['capture_result_checksums_sha256'])
    logs = d.parent / 'capture_attempt/acquisition_log'
    inventory(logs, f['acquisition_log_checksums_sha256'])
    assert (logs / f'query_receipts/{query_count:02d}_started.json').is_file()
    if kwargs.get('fail_index') is not None:
        assert not (logs / f'query_receipts/{query_count:02d}_complete.json').exists()
    assert all(b'SECRET_' not in p.read_bytes() for p in result_dir.rglob('*') if p.is_file())
    factory = Mock()
    with pytest.raises(FileExistsError): a.capture(d.parent / 'capture_attempt', activation_directory=d, activation_pin=pin, connection_factory=factory)
    factory.assert_not_called()
    with pytest.raises(Blocked, match='failed_capture'): a.intake(activation_directory=d, activation_pin=pin, capture_result_pin=f['capture_result_checksums_sha256'])


def test_factory_failure_is_reserved_and_no_diagnostics(frozen):
    d, pin = frozen
    factory = Mock(side_effect=RuntimeError('SECRET_PASSWORD'))
    f = a.capture(d.parent / 'capture_attempt', activation_directory=d, activation_pin=pin, connection_factory=factory)
    assert f['status'] == 'BLOCKED' and not f['connected'] and factory.call_count == 1
    assert 'SECRET' not in json.dumps(f)
    assert (d.parent / 'capture_attempt/reservation/checksums.json').is_file()


def test_missing_evidence_real_intake_zero_feature_prediction_calls_and_preservation(frozen, monkeypatch):
    d, pin, f, _ = captured(frozen)
    from modeling.phase5c1_contract import LEGACY
    before = {p.name: p.read_bytes() for p in (LEGACY / 'registry/records').glob('*.json')}
    from modeling import phase5c1_adapters as adapters
    loader = Mock(side_effect=AssertionError('Blocked input loaded models'))
    build = Mock(side_effect=AssertionError('Blocked input built features'))
    monkeypatch.setattr(adapters, 'SavedPipelines', loader)
    monkeypatch.setattr('modeling.phase5c1_runner.build_features', build)
    result = a.intake(activation_directory=d, activation_pin=pin, capture_result_pin=f['capture_result_checksums_sha256'])
    assert result['status'] == 'BLOCKED' and result['prediction_calls'] == 0
    loader.assert_not_called(); build.assert_not_called()
    run = d.parent / 'journal/runs/real_intake_v1'
    assert not (run / 'inputs').exists() and not (run / 'outputs').exists()
    from modeling.phase5c1_journal import latest, state
    parent, _, entries = latest(d.parent / 'journal')
    assert not any(e['action'].startswith('attempt_') for e in entries)
    assert all((parent / 'legacy/registry/records' / name).read_bytes() == raw for name, raw in before.items())
    assert len(before) == 35 and len(state(entries)) == result['readiness']['considered_count']
    assert a.verify(activation_directory=d, activation_pin=pin, intake_result_pin=result['intake_result_checksums_sha256'])['verification'] == 'VERIFIED'
    with pytest.raises(Blocked, match='overwrite'): a.intake(activation_directory=d, activation_pin=pin, capture_result_pin=f['capture_result_checksums_sha256'])


def test_capture_external_pin_tampering_rejected_before_intake(frozen):
    d, pin, f, _ = captured(frozen)
    with pytest.raises(Blocked, match='checksum_root'):
        a.intake(activation_directory=d, activation_pin=pin, capture_result_pin='0' * 64)
    assert not (d.parent / 'journal').exists()


def test_activation_pin_required_before_connection(frozen):
    d, pin = frozen
    factory = Mock()
    with pytest.raises(Blocked, match='checksum_root'):
        a.capture(d.parent / 'capture_attempt', activation_directory=d, activation_pin='0' * 64, connection_factory=factory)
    factory.assert_not_called()


def test_guarded_offline_work_forbids_source_fit_outcome_and_production(tmp_path):
    with a.offline_guards():
        import psycopg2
        with pytest.raises(RuntimeError): psycopg2.connect()
        with pytest.raises(RuntimeError): (ROOT / 'data/holdouts/never-read.json').read_bytes()
        with pytest.raises(RuntimeError): (ROOT / 'models/never-written.json').write_bytes(b'forbidden')
        from sklearn.linear_model import LogisticRegression
        with pytest.raises(RuntimeError): LogisticRegression().fit([[0], [1]], [0, 1])


def test_actual_clock_freeze_gate_before_reservation(frozen, monkeypatch):
    d, pin = frozen
    frozen_at = a.activation(d, pin)[0]['frozen_at']
    monkeypatch.setattr(a, 'now', lambda: frozen_at)
    factory = Mock()
    with pytest.raises(Blocked, match='capture_before_freeze'):
        a.capture(d.parent / 'capture_attempt', activation_directory=d, activation_pin=pin, connection_factory=factory)
    factory.assert_not_called()
    assert not (d.parent / 'capture_attempt').exists()


@pytest.mark.parametrize('authorization', [None, {}, {'scope': 'wrong'},
    {'scope': 'separately_authorized_phase5c2_readonly_capture', 'authorization_text_sha256': 'pin'}])
def test_existing_capture_api_authorization_before_connection(authorization):
    from modeling.phase5c1_sources import authorized_readonly_capture
    factory = Mock()
    with pytest.raises(Blocked, match='authorization'):
        authorized_readonly_capture(factory, authorization=authorization)
    factory.assert_not_called()
