"""Offline, exclusive Phase 5B.1 artifacts and fixed Phase 5A trust anchor."""
from pathlib import Path
import json

from modeling.phase5_current_data import verify_checksums, versions, now
from modeling.refit_preflight import ROOT, PreflightError, json_bytes, sha256

PARENT = ROOT / 'data/experiments/phase5a_prospective_shadow/20261002_phase5a_current_preparation_v2_blocked'
PARENT_ROOT = 'eda0256815b59a749d215fe00ec533826150bd2a8f2240b90d2baa6f8370bf56'
PARENT_MANIFEST = 'e23f7635c096e54090ffd229ab547ceb9e5f26fa7b71b2eac876be70f14ebe79'
POINTER = 'ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa'
BASE = '0585077675968c2a3537ffd3fddb87a9eb610c98d36200b64dd5fe148599389a'
METADATA = 'd10a0e6fd17996edac5d6aa90b4436ade1a5d2424b7dbe2f9ed379c469c4e02a'
OUTPUT = ROOT / 'data/experiments/phase5b1_reference_and_history'


def parent_inputs():
    checks = verify_checksums(PARENT, expected_checksums_sha256=PARENT_ROOT)
    if checks['training_manifest.json'] != PARENT_MANIFEST:
        raise PreflightError('Phase5A manifest pin mismatch')
    source = json.loads((PARENT / 'source_manifest.json').read_bytes())
    manifest = json.loads((PARENT / 'training_manifest.json').read_bytes())
    for n, entry in source['files'].items():
        if checks[n] != entry['sha256'] or len((PARENT/n).read_bytes()) != entry['bytes']:
            raise PreflightError('Source manifest mismatch')
    if any(checks[n] != h for n,h in manifest['source_sha256'].items()):
        raise PreflightError('Training source provenance mismatch')
    return checks


def code_hashes(paths):
    return {p: sha256((ROOT/p).read_bytes()) for p in sorted(paths)}


def verify_code_and_packages(directory, paths):
    if json.loads((directory/'package_versions.json').read_bytes()) != versions():
        raise PreflightError('Incompatible package provenance')
    if json.loads((directory/'code_versions.json').read_bytes()) != code_hashes(paths):
        raise PreflightError('Incompatible code provenance')


def publish(directory: Path, payloads: dict[str, bytes]):
    directory = directory.resolve()
    if directory.is_relative_to(ROOT) and not directory.is_relative_to(OUTPUT):
        raise PreflightError('Phase5B1 publication must remain in isolated artifact root')
    if directory.exists():
        raise PreflightError('Never overwrite a Phase5B1 artifact')
    if any(Path(n).is_absolute() or '..' in Path(n).parts for n in payloads):
        raise PreflightError('Artifact path escape')
    sums = json_bytes({'files': {n: sha256(b) for n,b in sorted(payloads.items())}})
    directory.mkdir(parents=True, exist_ok=False)
    marker = directory/'INCOMPLETE'
    marker.write_bytes(b'Phase5B1 incomplete\n')
    for n,b in {**payloads, 'checksums.json': sums}.items():
        p = directory/n
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as stream:
            stream.write(b)
        p.chmod(0o444)
    verify_checksums(directory, expected_checksums_sha256=sha256(sums), allow_incomplete=True)
    marker.unlink()
    return sha256(sums)


def preservation(baseline):
    changed, missing = [], []
    for n,h in baseline['files'].items():
        p = ROOT/n
        if not p.is_file(): missing.append(n)
        elif sha256(p.read_bytes()) != h: changed.append(n)
    # All new implementation/artifacts are outside these immutable roots.
    protected = ['models', 'data/raw', 'data/holdouts', 'data/audits']
    protected += [str(p.relative_to(ROOT)) for p in (ROOT/'data/experiments').iterdir()
                  if p != OUTPUT]
    new = sorted(str(p.relative_to(ROOT)) for n in protected for p in (ROOT/n).rglob('*')
                 if p.is_file() and '__pycache__' not in p.parts
                 and str(p.relative_to(ROOT)) not in baseline['files'])
    if changed or missing or new:
        raise PreflightError(f'Preservation failed: {changed}, {missing}, {new}')
    return {'status':'VERIFIED', 'checked_at':now(), 'files_checked':len(baseline['files']),
            'changed':changed, 'missing':missing, 'new_protected_files':new,
            'meaning':'hash-only; protected outcomes were not parsed'}
