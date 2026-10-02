"""Offline execution boundaries for this explicitly authorized lifecycle."""
import builtins
from contextlib import contextmanager, ExitStack
import io
import os
from pathlib import Path
import socket
from unittest.mock import patch

from modeling.phase5b4_contract_v1 import ROOT, OUTPUT, ContractError, now, sha256


@contextmanager
def offline_guards():
    def forbidden(*args, **kwargs):
        raise RuntimeError('Phase5B4 forbids network, warehouse and production writes')

    def checked(original):
        def guarded(file, mode='r', *args, **kwargs):
            if isinstance(file, (str, bytes, Path)):
                path = Path(file).resolve()
                if path == ROOT / '.env':
                    raise RuntimeError('Phase5B4 forbids credential reads')
                if path.is_relative_to(ROOT / 'data/audits') or (path.is_relative_to(ROOT / 'data/holdouts') and path.name not in {'manifest.json', 'predictions.csv', 'identity-exclusions.json'}):
                    raise RuntimeError('Phase5B4 forbids frozen outcome/evaluation reads')
                if any(m in mode for m in 'wax+') and path.is_relative_to(ROOT):
                    if not path.is_relative_to(OUTPUT):
                        raise RuntimeError('Phase5B4 writes must remain in the challenger root')
            return original(file, mode, *args, **kwargs)
        return guarded

    with ExitStack() as stack:
        stack.enter_context(patch('dotenv.load_dotenv', lambda *a, **k: False))
        stack.enter_context(patch('psycopg2.connect', forbidden))
        stack.enter_context(patch.object(socket.socket, 'connect', forbidden))
        stack.enter_context(patch('socket.create_connection', forbidden))
        stack.enter_context(patch('socket.getaddrinfo', forbidden))
        stack.enter_context(patch('urllib.request.urlopen', forbidden))
        stack.enter_context(patch('requests.sessions.Session.request', forbidden))
        stack.enter_context(patch('builtins.open', checked(builtins.open)))
        stack.enter_context(patch('io.open', checked(io.open)))
        for name in ('warehouse.db.get_connection', 'warehouse.db.upsert'):
            stack.enter_context(patch(name, forbidden))
        yield


@contextmanager
def loading_guards():
    def forbidden(*args, **kwargs):
        raise RuntimeError('Loading/inference cannot fit any component')
    with ExitStack() as stack:
        for name in ('xgboost.XGBClassifier.fit', 'xgboost.train',
                     'sklearn.linear_model.LogisticRegression.fit',
                     'features.debut_prior.compute_debut_priors'):
            stack.enter_context(patch(name, forbidden))
        yield


def preservation(baseline):
    """File-descriptor hashes only: no parsing of protected model/outcome bytes."""
    changed, missing = [], []
    for name, expected in baseline['files'].items():
        path = ROOT / name
        if not path.is_file():
            missing.append(name)
            continue
        import hashlib
        h = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY)
        try:
            while chunk := os.read(fd, 1024 * 1024):
                h.update(chunk)
        finally:
            os.close(fd)
        if h.hexdigest() != expected:
            changed.append(name)
    protected = [ROOT / p for p in ('models', 'data/raw', 'data/holdouts', 'data/audits')]
    protected += [p for p in (ROOT / 'data/experiments').iterdir() if p != OUTPUT]
    new = sorted(str(p.relative_to(ROOT)) for d in protected for p in d.rglob('*')
                 if p.is_file() and '__pycache__' not in p.parts and str(p.relative_to(ROOT)) not in baseline['files'])
    if changed or missing or new:
        raise ContractError(f'Preservation failed: {changed}, {missing}, {new}')
    return {'status': 'VERIFIED', 'checked_at': now(), 'files_checked': len(baseline['files']),
            'changed': changed, 'missing': missing, 'new_protected_files': new,
            'protected_outcomes': 'hash-only; never parsed'}
