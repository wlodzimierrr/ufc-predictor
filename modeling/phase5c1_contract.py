"""Versioned local shadow contracts; no data or estimator I/O at import time."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'data/experiments/phase5c1_shadow_workflow'
VERSION = 'phase5c1_prospective_shadow_v1'
SOURCE_VERSION = 'phase5c1_independent_source_roles_v1'
INFERENCE_VERSION = 'phase5c1_capture_receipt_inference_v2'
PINS = {
    'challenger': {
        'path': 'data/experiments/phase5b4_current_challenger/20261002T183215Z_phase5b4_current_challenger_v1_fixed',
        'checksums': '59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300',
        'manifest_name': 'training_manifest.json',
        'manifest': '7330bbebd06a6f5c0233cf8892be5c1728edc8805add02b1511499bf198b0c73'},
    'reference': {
        'path': 'data/experiments/phase5b1_reference_and_history/20261002_phase5b1_frozen_reference_v3_identity_guarded',
        'checksums': '12c17761fe76998e49fb5e79ea8a455861e22eaf57b37071f692bab17c1ba381',
        'manifest_name': 'manifest.json',
        'manifest': '20a9695de6cd77d74ff67f5cb0b62da2f127fbcdc181fa9d9fc2b3f8f62ad48a'},
    'march': {
        'path': 'data/experiments/phase3b_xgb_pre_april_2026/20261001_phase3b_retrospective_v1',
        'checksums': '41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f'}
}
TABLES = ('events', 'fighters', 'fights', 'fight_stats_aggregate')
LEGACY = ROOT / 'data/experiments/phase5a_prospective_shadow/20261002_phase5a_current_preparation_v2_blocked'
LEGACY_PIN = 'eda0256815b59a749d215fe00ec533826150bd2a8f2240b90d2baa6f8370bf56'
IDENTITY = ('fight_id', 'event_id', 'fighter_1_id', 'fighter_2_id')
CODE = [f'modeling/phase5c1_{n}.py' for n in ('contract', 'sources', 'evidence', 'adapters', 'journal', 'runner', 'safety', 'synthetic')]
CODE += ['tools/run_phase5c1_shadow.py', 'modeling/tests/test_phase5c1_shadow.py',
         'docs/phase5c1-shadow-evidence-contract-v1.md', 'features/forecast_replay.py']


class Blocked(ValueError):
    pass


def require(test, reason):
    if not test:
        raise Blocked(reason)


def now():
    return datetime.now(timezone.utc).isoformat()


def instant(value):
    try:
        t = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(t.tzinfo is not None, 'timezone_required')
        return t.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError) as exc:
        raise Blocked('invalid_observation_time') from exc


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()


def decoded(raw):
    def pairs(entries):
        result = {}
        for key, value in entries:
            require(key not in result, 'duplicate_json_key')
            result[key] = value
        return result
    def constant(value):
        raise Blocked('nonfinite_json_constant')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def digest(value):
    return sha(encoded(value))


def uuid(value):
    try:
        require(str(UUID(value)) == value, 'invalid_uuid')
    except (ValueError, TypeError, AttributeError) as exc:
        raise Blocked('invalid_uuid') from exc
    return value


def local_bytes(root, name):
    p = Path(name)
    require(not p.is_absolute() and '..' not in p.parts and str(p) == name, 'unsafe_path')
    target = Path(root) / p
    require(target.resolve().is_relative_to(Path(root).resolve()), 'escaping_path')
    require(not any(part.is_symlink() for part in [target, *target.parents]), 'symlink_forbidden')
    require(target.is_file(), 'missing_body:' + name)
    return target.read_bytes()


def inventory(root, pin):
    root = Path(root)
    raw = local_bytes(root, 'checksums.json')
    require(sha(raw) == pin, 'checksum_root_mismatch')
    files = decoded(raw)['files']
    actual = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    require(actual == set(files) | {'checksums.json'}, 'inventory_mismatch_or_incomplete')
    for n, h in files.items():
        require(sha(local_bytes(root, n)) == h, 'component_hash_mismatch:' + n)
    return files


def verify_components():
    result = {}
    for name, p in PINS.items():
        files = inventory(ROOT / p['path'], p['checksums'])
        if 'manifest' in p:
            require(files[p['manifest_name']] == p['manifest'], 'manifest_pin_mismatch')
        # Verify artifact-pinned code too, without rebuilding sources or fitting.
        if 'code_versions.json' in files:
            code = json.loads(local_bytes(ROOT / p['path'], 'code_versions.json'))
            for path, h in code.items():
                require(sha(local_bytes(ROOT, path)) == h, 'pinned_code_changed:' + path)
        result[name] = files
    return result


def load_contract(directory, pin):
    files = inventory(directory, pin)
    c = json.loads(local_bytes(directory, 'contract.json'))
    require(c['version'] == VERSION and c['inference_version'] == INFERENCE_VERSION and c['source_version'] == SOURCE_VERSION, 'incompatible_contract')
    require(c['components'] == PINS and set(c['code_sha256']) == set(CODE), 'contract_component_or_code_pins')
    require(c['code_sha256'] == {n: sha(local_bytes(ROOT, n)) for n in CODE}, 'orchestration_code_changed')
    require(c['frozen_at'] == json.loads(local_bytes(directory, 'freeze_receipt.json'))['frozen_at'], 'freeze_receipt_mismatch')
    instant(c['frozen_at'])
    verify_components()
    return c


def publish(directory, payloads):
    """Exclusive staged directory, immutable after completion; failures stay visible."""
    d = Path(directory)
    require(not d.resolve().is_relative_to(ROOT) or d.resolve().is_relative_to(OUTPUT), 'isolated_phase5c_root_required')
    d.mkdir(parents=True, exist_ok=False)
    (d / 'INCOMPLETE').write_bytes(b'publication in progress\n')
    for n, raw in sorted(payloads.items()):
        require(n != 'checksums.json' and not Path(n).is_absolute() and '..' not in Path(n).parts, 'unsafe_publication_path')
        p = d / n
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as f:
            f.write(raw)
            f.flush()
            import os
            os.fsync(f.fileno())
    checks = encoded({'files': {n: sha(raw) for n, raw in sorted(payloads.items())}})
    with (d / 'checksums.json').open('xb') as f:
        f.write(checks)
    for n, raw in payloads.items():
        require(local_bytes(d, n) == raw, 'publication_readback_failure')
    (d / 'INCOMPLETE').unlink()
    inventory(d, sha(checks))
    for p in d.rglob('*'):
        if p.is_file():
            p.chmod(0o444)
    return sha(checks)
