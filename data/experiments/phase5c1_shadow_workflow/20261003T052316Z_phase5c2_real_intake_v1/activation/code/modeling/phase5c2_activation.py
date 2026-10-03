"""Bound one real capture and conditional inference to an immutable authorization.

The accepted Phase5C.1 implementations, evidence and inference contracts remain
unchanged. No source access occurs outside capture(); all later work is offline.
"""
from contextlib import ExitStack, contextmanager
import builtins
import io
import os
from pathlib import Path
import socket
from unittest.mock import patch
from uuid import uuid4

from modeling.phase5c1_contract import (Blocked, IDENTITY, OUTPUT, ROOT, SOURCE_VERSION,
    TABLES, decoded, digest, encoded, instant, inventory, load_contract, local_bytes,
    now, publish, require, sha)

CONFIG = 'configs/phase5c2_activation_v1.json'
CODE = ('modeling/phase5c2_activation.py', 'tools/run_phase5c2_activation.py',
        'modeling/tests/test_phase5c2_activation.py', 'warehouse/db.py', CONFIG)


def freeze(destination, authorization_text):
    config = decoded(local_bytes(ROOT, CONFIG))
    contract = load_contract(ROOT / config['workflow_directory'], config['workflow_checksums_sha256'])
    raw = Path(authorization_text).read_bytes()
    require(raw and raw.decode('utf-8').strip(), 'authorization_text_required')
    code = {p: local_bytes(ROOT, p) for p in CODE}
    frozen_at = now()
    manifest = {'version': config['version'], 'frozen_at': frozen_at,
        'config': config, 'config_sha256': sha(code[CONFIG]),
        'authorization_text_sha256': sha(raw), 'code_sha256': {p: sha(b) for p, b in code.items()},
        'workflow_contract_sha256': digest(contract),
        'workflow_frozen_at': contract['frozen_at'], 'component_freezes': contract['component_freezes']}
    pin = publish(destination, {'activation_manifest.json': encoded(manifest),
        'authorization.txt': raw, 'configuration.json': code[CONFIG],
        'freeze_receipt.json': encoded({'frozen_at': frozen_at, 'capture_attempts': 0, 'prediction_calls': 0}),
        **{'code/' + p: b for p, b in code.items()}})
    return {'status': 'READY', 'activation_checksums_sha256': pin,
            'activation_manifest_sha256': digest(manifest), 'frozen_at': frozen_at}


def activation(directory, pin):
    inventory(directory, pin)
    m = decoded(local_bytes(directory, 'activation_manifest.json'))
    require(m['code_sha256'] == {p: sha(local_bytes(ROOT, p)) for p in CODE}, 'activation_code_changed')
    require(m['config'] == decoded(local_bytes(ROOT, CONFIG)), 'activation_configuration_changed')
    require(m['config_sha256'] == sha(local_bytes(directory, 'configuration.json')), 'activation_config_pin')
    require(m['authorization_text_sha256'] == sha(local_bytes(directory, 'authorization.txt')), 'authorization_text_pin')
    require(m['frozen_at'] == decoded(local_bytes(directory, 'freeze_receipt.json'))['frozen_at'], 'activation_freeze_pin')
    c = load_contract(ROOT / m['config']['workflow_directory'], m['config']['workflow_checksums_sha256'])
    require(digest(c) == m['workflow_contract_sha256'], 'workflow_contract_pin')
    require(m['config']['capture_attempt_budget'] == 1 and m['config']['tables'] == list(TABLES), 'bounded_scope_required')
    return m, c


def _queries():
    from modeling.phase5_current_data import SCHEMA_QUERY
    return ['SHOW transaction_read_only', 'SHOW transaction_isolation',
        "SELECT transaction_timestamp() AS transaction_started_at, current_setting('TimeZone') AS database_timezone, pg_current_snapshot()::text AS snapshot",
        SCHEMA_QUERY.replace('= ANY(%s)', "IN ('events','fighters','fights','fight_stats_aggregate')"),
        *[f'SELECT * FROM public."{t}" ORDER BY "{k}"' for t, k in
          zip(TABLES, ('event_id', 'fighter_id', 'fight_id', 'fight_stat_id'))]]


class CaptureAudit:
    """Transparent receipt observer; never adds a query or logs driver diagnostics."""
    def __init__(self, directory):
        self.queries, self.bodies = [], {}
        self.directory = Path(directory)
        self.directory.mkdir(exist_ok=False)
        self.connected = self.rolled_back = self.closed = False

    def save(self, name, raw):
        p = self.directory / name
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        p.chmod(0o444)

    def seal(self):
        # Streaming receipts already exist, so seal them without overwriting or
        # duplicating their source bodies. A crash leaves exclusive partial files.
        files = {str(p.relative_to(self.directory)): sha(p.read_bytes())
                 for p in self.directory.rglob('*') if p.is_file()}
        raw = encoded({'files': files})
        self.save('checksums.json', raw)
        inventory(self.directory, sha(raw))
        return sha(raw)

    def wrap(self, conn):
        audit = self
        audit.connected = True

        class Cursor:
            def __init__(self, cur): self.cur = cur
            def __enter__(self): self.cur.__enter__(); return self
            def __exit__(self, *args): return self.cur.__exit__(*args)
            @property
            def description(self): return self.cur.description
            def execute(self, sql):
                index = len(audit.queries)
                require(index < len(_queries()) and sql == _queries()[index], 'capture_query_outside_scope')
                self.receipt = {'sql': sql, 'requested_at': now(), 'status': 'STARTED'}
                audit.queries.append(self.receipt)
                audit.save(f'query_receipts/{index + 1:02d}_started.json', encoded(self.receipt))
                self.cur.execute(sql)
            def fetchall(self):
                rows = self.cur.fetchall()
                from modeling.phase5_current_data import table_bytes
                body = table_bytes([dict(zip([d[0] for d in self.description], r)) for r in rows])
                name = f'query_bodies/{len(audit.queries):02d}.json'
                audit.bodies[name] = body
                audit.save(name, body)
                self.receipt.update(observed_at=now(), status='COMPLETE', body=name,
                                    sha256=sha(body), bytes=len(body), rows=len(rows))
                audit.save(f'query_receipts/{len(audit.queries):02d}_complete.json', encoded(self.receipt))
                return rows

        class Connection:
            def set_session(self, **kwargs): return conn.set_session(**kwargs)
            def cursor(self): return Cursor(conn.cursor())
            def rollback(self):
                conn.rollback(); audit.rolled_back = True
                audit.save('rollback.json', encoded({'rolled_back': True, 'completed_at': now()}))
            def close(self):
                conn.close(); audit.closed = True
                audit.save('close.json', encoded({'closed': True, 'completed_at': now()}))
        return Connection()


def capture(destination, *, activation_directory, activation_pin, connection_factory):
    m, contract = activation(activation_directory, activation_pin)
    require(instant(now()) > max(map(instant, [m['frozen_at'], contract['frozen_at'], *contract['component_freezes']])), 'capture_before_freeze')
    d = Path(destination)
    # Reserve before opening a connection. Even a crash/failure consumes this
    # activation's sole capture attempt; no replacement path is selectable.
    require(d == Path(activation_directory).parent / 'capture_attempt', 'fixed_capture_destination_required')
    d.mkdir(exist_ok=False)
    reservation_pin = publish(d / 'reservation', {'reservation.json': encoded({
        'activation_checksums_sha256': activation_pin, 'activation_manifest_sha256': digest(m),
        'reserved_at': now(), 'capture_attempt_number': 1, 'attempt_budget': 1})})
    audit = CaptureAudit(d / 'acquisition_log')
    authorization = {'scope': m['config']['capture_scope'],
        'authorization_text_sha256': m['authorization_text_sha256'], 'source_identity': m['config']['source_identity']}
    try:
        from modeling.phase5c1_sources import authorized_readonly_capture
        bodies, api_receipt = authorized_readonly_capture(lambda: audit.wrap(connection_factory()), authorization=authorization)
        from modeling.phase5_current_data import table_bytes
        api_receipt = decoded(table_bytes(api_receipt))  # lossless SQL date/scalar export
        require(instant(api_receipt['started_at']) > instant(m['frozen_at']), 'capture_before_activation_freeze')
        config = m['config']
        events = decoded(bodies['events'])
        fights = decoded(bodies['fights'])
        considered = []
        for f in fights:
            if f['result_type'] == 'upcoming':
                e = next(e for e in events if e['event_id'] == f['event_id'])
                considered.append({k: f[k] for k in IDENTITY} | {
                    'source_row_id': 'warehouse:' + f['fight_id'], 'event_date': e['event_date'],
                    'source_url': f.get('source_url')})
        evidence = encoded({'version': 'phase5c1_evidence_package_v1', 'sources': {}, 'assertions': []})
        table_receipts = {}
        for i, t in enumerate(('schemas', *TABLES), start=3):
            q = api_receipt['queries'][i]
            table_receipts[t] = {'body': f'sources/{t}.json', 'sha256': sha(bodies[t]), 'bytes': len(bodies[t]),
                'provider': config['source_identity'], 'url': config['source_url_prefix'] + t,
                'requested_at': q['started_at'], 'observed_at': q['finished_at']}
        receipt = {'version': SOURCE_VERSION, 'capture_id': str(uuid4()), 'synthetic': False,
            'mode': 'readonly_repeatable_read', 'provider': config['source_identity'],
            'source_scope': config['consideration_scope'], 'started_at': api_receipt['started_at'],
            'completed_at': api_receipt['completed_at'], 'observation_cutoff': api_receipt['completed_at'],
            'transaction': api_receipt['transaction'], 'tables': table_receipts, 'considered': considered,
            'evidence_body': 'evidence_package.json', 'evidence_sha256': sha(evidence),
            'authorization': authorization, 'activation_checksums_sha256': activation_pin,
            'activation_manifest_sha256': digest(m), 'reservation_checksums_sha256': reservation_pin}
        observation_pin = publish(d / 'observation', {
            **{f'sources/{t}.json': b for t, b in bodies.items()},
            'capture_receipt.json': encoded(receipt), 'evidence_package.json': evidence,
            'capture_api_receipt.json': encoded(api_receipt)})
        result = {'status': 'READY', 'capture_attempts': 1, 'capture_receipt_sha256': digest(receipt),
            'observation_checksums_sha256': observation_pin, 'considered_count': len(considered),
            'started_at': receipt['started_at'], 'observation_cutoff': receipt['observation_cutoff']}
    except Exception as exc:
        # Never serialize str(exc), repr(conn), DSNs, environment or tracebacks.
        result = {'status': 'BLOCKED', 'capture_attempts': 1, 'failure_class': type(exc).__name__,
                  'acquisition_stopped': True, 'prediction_calls': 0}
    result.update(completed_at=now(), connected=audit.connected, rolled_back=audit.rolled_back, closed=audit.closed,
                  reservation_checksums_sha256=reservation_pin, activation_checksums_sha256=activation_pin,
                  acquisition_log_checksums_sha256=audit.seal())
    result_pin = publish(d / 'result', {'capture_result.json': encoded(result),
        'query_audit.json': encoded(audit.queries)})
    return result | {'capture_result_checksums_sha256': result_pin}


@contextmanager
def offline_guards():
    """Allow separately gated saved inference; forbid acquisition, fits and outcomes."""
    def forbidden(*args, **kwargs): raise RuntimeError('Phase5C2 offline operation forbidden')
    def checked(original):
        def opened(file, mode='r', *args, **kwargs):
            if isinstance(file, (str, bytes, Path)):
                p = Path(file).resolve()
                if p == ROOT / '.env' or p.is_relative_to(ROOT / 'data/audits') or p.is_relative_to(ROOT / 'data/holdouts'):
                    forbidden()
                if any(c in mode for c in 'wax+') and p.is_relative_to(ROOT) and not p.is_relative_to(OUTPUT):
                    forbidden()
            return original(file, mode, *args, **kwargs)
        return opened
    with ExitStack() as stack:
        stack.enter_context(patch('dotenv.load_dotenv', lambda *a, **k: False))
        for name in ('psycopg2.connect', 'warehouse.db.get_connection', 'warehouse.db.upsert',
            'socket.create_connection', 'socket.getaddrinfo', 'urllib.request.urlopen',
            'requests.sessions.Session.request', 'features.data_loader.load_all_data',
            'features.debut_prior.compute_debut_priors', 'xgboost.train',
            'xgboost.XGBClassifier.fit', 'sklearn.linear_model.LogisticRegression.fit'):
            stack.enter_context(patch(name, forbidden))
        stack.enter_context(patch.object(socket.socket, 'connect', forbidden))
        stack.enter_context(patch('builtins.open', checked(builtins.open)))
        stack.enter_context(patch('io.open', checked(io.open)))
        yield


class LazySavedPipelines:
    synthetic = False
    def __init__(self): self.saved = None
    @property
    def reference_preprocessing(self):
        # The existing runner reaches this only AFTER readiness, registration
        # and reservation of the first eligible attempt.
        from modeling.phase5c1_adapters import SavedPipelines
        if self.saved is None: self.saved = SavedPipelines()
        return self.saved.reference_preprocessing
    def predict(self, *args, **kwargs):
        require(self.saved is not None, 'saved_components_not_loaded')
        return self.saved.predict(*args, **kwargs)


def intake(*, activation_directory, activation_pin, capture_result_pin):
    m, contract = activation(activation_directory, activation_pin)
    root = Path(activation_directory).parent
    inventory(root / 'capture_attempt/result', capture_result_pin)
    result = decoded(local_bytes(root / 'capture_attempt/result', 'capture_result.json'))
    require(result['status'] == 'READY', 'failed_capture_acquisition_stopped')
    inventory(root / 'capture_attempt/observation', result['observation_checksums_sha256'])
    r = decoded(local_bytes(root / 'capture_attempt/observation', 'capture_receipt.json'))
    require(r['activation_checksums_sha256'] == activation_pin and not r['synthetic'], 'capture_activation_binding')
    require(instant(r['started_at']) > instant(m['frozen_at']), 'capture_before_activation_freeze')
    authorization = {'scope': m['config']['forecast_scope'], 'authorization_text_sha256': m['authorization_text_sha256'],
        'capture_receipt_sha256': result['capture_receipt_sha256'], 'contract_sha256': digest(contract)}
    with offline_guards():
        from modeling.phase5c1_runner import forecast
        f = forecast(root / 'journal', 'real_intake_v1', {
            'directory': str(root / 'capture_attempt/observation'), 'receipt_sha256': result['capture_receipt_sha256']},
            contract, LazySavedPipelines(), include_march=m['config']['include_march'], authorization=authorization)
        pin = publish(root / 'intake_result', {'intake_result.json': encoded(f),
            'authorization_binding.json': encoded(authorization), 'activation_manifest.json': encoded(m),
            'capture_result_pin.json': encoded({'checksums_sha256': capture_result_pin}),
            'march_control.json': encoded({'predeclared': True, 'included_if_ready': True,
                'absence_when_blocked': 'NO_ELIGIBLE_BOUT_NO_CONTROL_PREDICTION'}),
            'execution_completed_at.json': encoded({'completed_at': now()})})
    return f | {'intake_result_checksums_sha256': pin}


def verify(*, activation_directory, activation_pin, intake_result_pin):
    """Offline reconstruction only; never supply pipelines for probability replay."""
    with offline_guards():
        m, c = activation(activation_directory, activation_pin)
        root = Path(activation_directory).parent
        inventory(root / 'intake_result', intake_result_pin)
        capture_pin = decoded(local_bytes(root / 'intake_result', 'capture_result_pin.json'))['checksums_sha256']
        inventory(root / 'capture_attempt/result', capture_pin)
        result = decoded(local_bytes(root / 'capture_attempt/result', 'capture_result.json'))
        inventory(root / 'capture_attempt/acquisition_log', result['acquisition_log_checksums_sha256'])
        obs = root / 'capture_attempt/observation'
        inventory(obs, result['observation_checksums_sha256'])
        require(result['activation_checksums_sha256'] == activation_pin and result['rolled_back'] and result['closed'], 'capture_cleanup_or_binding')
        receipt = decoded(local_bytes(obs, 'capture_receipt.json'))
        require(digest(receipt) == result['capture_receipt_sha256'], 'capture_receipt_pin_mismatch')
        api = decoded(local_bytes(obs, 'capture_api_receipt.json'))
        audit = decoded(local_bytes(root / 'capture_attempt/result', 'query_audit.json'))
        require([q['sql'] for q in api['queries']] == [q['sql'] for q in audit] == _queries(), 'capture_query_receipts')
        require(receipt['transaction'] == api['transaction'] and receipt['observation_cutoff'] == api['completed_at'], 'transaction_receipt_binding')
        require(instant(api['started_at']) > instant(m['frozen_at']), 'capture_before_activation_freeze')
        for i, t in enumerate(('schemas', *TABLES), start=3):
            body = local_bytes(obs, receipt['tables'][t]['body'])
            require(body == local_bytes(root / 'capture_attempt/acquisition_log', audit[i]['body']), 'capture_export_bytes_changed')
            require(sha(body) == api['source_sha256'][t] == receipt['tables'][t]['sha256'], 'capture_export_hash_binding')
        binding = decoded(local_bytes(root / 'intake_result', 'authorization_binding.json'))
        require(binding == {'scope': m['config']['forecast_scope'], 'authorization_text_sha256': m['authorization_text_sha256'],
            'capture_receipt_sha256': result['capture_receipt_sha256'], 'contract_sha256': digest(c)}, 'forecast_authorization_binding')
        f = decoded(local_bytes(root / 'intake_result', 'intake_result.json'))
        from modeling.phase5c1_runner import verify_run
        run_dir = root / 'journal/runs/real_intake_v1'
        verified = verify_run(run_dir, f['checksums_sha256'], expected_contract=c)
        if not (run_dir / 'projection.json').exists():
            # Independently reproduce a structural/identity blocker, rather
            # than accepting only the saved BLOCKED flag and its checksums.
            from modeling.phase5c1_sources import validate_package, project
            from modeling.phase5c1_runner import check
            run = decoded(local_bytes(run_dir, 'run.json'))
            try:
                package = validate_package(obs, receipt_pin=result['capture_receipt_sha256'], contract=c)
                check(package, project(package), forecast_at=run['started_at'])
            except (ValueError, KeyError, TypeError, OSError) as exc:
                reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
                require(reason == run['failure'], 'blocked_failure_reconstruction_mismatch')
            else:
                raise Blocked('blocked_failure_not_reproduced')
        from modeling.phase5c1_journal import latest
        _, tail, entries = latest(root / 'journal')
        verified.update(activation_checksums_sha256=activation_pin, registry_tail_checksums_sha256=tail,
                        registry_entries=len(entries), verified_at=now())
        return verified
