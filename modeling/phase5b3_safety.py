"""Execution guards for offline preparation and its explicit safe tests."""
from contextlib import contextmanager, ExitStack
import builtins
import io
from pathlib import Path
import socket
from unittest.mock import patch


@contextmanager
def preparation_guards():
    """Install before importing reconstruction (which imports warehouse.db).

    Hash-only preservation uses os.open separately; protected outcome files are
    never opened by preparation or tests. No saved estimator is deserialized.
    """
    def forbidden(*args, **kwargs):
        raise RuntimeError('Phase5B3 forbids fitting, prediction, network, warehouse or estimator loading')

    root = Path(__file__).resolve().parents[1]
    original_open, original_io = builtins.open, io.open

    def checked(original):
        def guarded(file, *args, **kwargs):
            if isinstance(file, (str, bytes, Path)):
                p = Path(file).resolve()
                if p == root / '.env':
                    raise RuntimeError('Phase5B3 forbids credential reads')
                if p.is_relative_to(root / 'data/audits') or (
                    p.is_relative_to(root / 'data/holdouts') and
                    p.name not in {'manifest.json', 'predictions.csv', 'identity-exclusions.json'}
                ):
                    raise RuntimeError('Phase5B3 forbids frozen outcome/evaluation reads')
            return original(file, *args, **kwargs)
        return guarded

    with ExitStack() as stack:
        stack.enter_context(patch('dotenv.load_dotenv', lambda *a, **k: False))
        stack.enter_context(patch('psycopg2.connect', forbidden))
        stack.enter_context(patch.object(socket.socket, 'connect', forbidden))
        stack.enter_context(patch('socket.create_connection', forbidden))
        stack.enter_context(patch('socket.getaddrinfo', forbidden))
        stack.enter_context(patch('urllib.request.urlopen', forbidden))
        stack.enter_context(patch('requests.sessions.Session.request', forbidden))
        stack.enter_context(patch('joblib.load', forbidden))
        stack.enter_context(patch('builtins.open', checked(original_open)))
        stack.enter_context(patch('io.open', checked(original_io)))
        for method in ('fit', 'predict', 'predict_proba'):
            stack.enter_context(patch('xgboost.XGBClassifier.' + method, forbidden))
            stack.enter_context(patch('sklearn.linear_model.LogisticRegression.' + method, forbidden))
        for method in ('train',):
            stack.enter_context(patch('xgboost.' + method, forbidden))
        for method in ('predict', 'inplace_predict', 'load_model'):
            stack.enter_context(patch('xgboost.Booster.' + method, forbidden))
        # These modules can now be imported without dotenv or connection effects.
        for name in ('warehouse.db.get_connection', 'warehouse.db.upsert',
                     'features.debut_prior.compute_debut_priors',
                     'modeling.refit_preflight.compute_debut_priors'):
            stack.enter_context(patch(name, forbidden))
        yield
