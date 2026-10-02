"""Active implementation-session guards; synthetic pipelines never call learners."""
from contextlib import ExitStack, contextmanager
import builtins
import io
from pathlib import Path
import socket
from unittest.mock import patch
from modeling.phase5c1_contract import OUTPUT, ROOT


@contextmanager
def implementation_guards(*, allow_loading=False, write_roots=()):
    def forbidden(*a, **kw):
        raise RuntimeError('Phase5C1 forbids fitting, real prediction, source access and production writes')

    def checked(original):
        def opened(file, mode='r', *a, **kw):
            if isinstance(file, (str, bytes, Path)):
                p = Path(file).resolve()
                if p == ROOT / '.env' or p.is_relative_to(ROOT / 'data/audits') or p.is_relative_to(ROOT / 'data/holdouts'):
                    raise RuntimeError('Phase5C1 forbids credential and outcome reads')
                if any(c in mode for c in 'wax+') and p.is_relative_to(ROOT):
                    if not any(p.is_relative_to(Path(d).resolve()) for d in (OUTPUT, *write_roots)):
                        forbidden()
            return original(file, mode, *a, **kw)
        return opened

    with ExitStack() as s:
        s.enter_context(patch('dotenv.load_dotenv', lambda *a, **kw: False))
        for name in ('psycopg2.connect', 'socket.create_connection', 'socket.getaddrinfo',
                     'urllib.request.urlopen', 'requests.sessions.Session.request'):
            s.enter_context(patch(name, forbidden))
        s.enter_context(patch.object(socket.socket, 'connect', forbidden))
        s.enter_context(patch('builtins.open', checked(builtins.open)))
        s.enter_context(patch('io.open', checked(io.open)))
        for name in ('warehouse.db.get_connection', 'warehouse.db.upsert',
                     'features.data_loader.load_all_data', 'features.debut_prior.compute_debut_priors'):
            s.enter_context(patch(name, forbidden))
        for cls in ('xgboost.XGBClassifier', 'sklearn.linear_model.LogisticRegression'):
            for method in ('fit', 'predict', 'predict_proba'):
                s.enter_context(patch(cls + '.' + method, forbidden))
        for name in ('xgboost.train', 'xgboost.Booster.predict', 'xgboost.Booster.inplace_predict'):
            s.enter_context(patch(name, forbidden))
        if not allow_loading:
            for name in ('joblib.load', 'xgboost.XGBClassifier.load_model', 'xgboost.Booster.load_model'):
                s.enter_context(patch(name, forbidden))
        yield
