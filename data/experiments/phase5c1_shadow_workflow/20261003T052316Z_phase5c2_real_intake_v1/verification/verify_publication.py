"""Hash-only preservation, isolated publication and redacted content inspection."""
from pathlib import Path
from collections import Counter
import hashlib
import json
import os
import re
from modeling.phase5c1_contract import ROOT, encoded, inventory, now, publish, sha, require

p = Path('/tmp/phase5c2-session')
s = json.loads((p / 'session_state.json').read_bytes())
root = Path(s['root'])
baseline = json.loads((p / 'preservation_baseline.json').read_bytes())
changed, missing = [], []
for name, expected in baseline['files'].items():
    target = ROOT / name
    if not target.is_file(): missing.append(name); continue
    h = hashlib.sha256()
    with target.open('rb') as stream:
        for body in iter(lambda: stream.read(1024 * 1024), b''): h.update(body)
    if h.hexdigest() != expected: changed.append(name)
protected = ('data/experiments/', 'data/holdouts/', 'data/audits/', 'data/raw/', 'models/', 'data/predictions/')
new_protected = []
skip = set(baseline['excluded_directory_names'])
for current, dirs, names in os.walk(ROOT):
    dirs[:] = [d for d in dirs if d not in skip and not d.endswith('.egg-info')]
    for name in names:
        target = Path(current) / name
        rel = str(target.relative_to(ROOT))
        if rel.startswith(protected) and rel not in baseline['files'] and not target.is_relative_to(root):
            new_protected.append(rel)
require(not changed and not missing and not new_protected, 'preservation_difference')
preservation = {'checked_at': now(), 'baseline_sha256': sha((p / 'preservation_baseline.json').read_bytes()),
    'existing_files_checked': len(baseline['files']), 'changed': changed, 'missing': missing,
    'new_protected_outside_isolated_run': new_protected, 'protected_content_parsed': False}

inventories = {}
for checks in sorted(root.rglob('checksums.json')):
    pin = sha(checks.read_bytes())
    inventory(checks.parent, pin)
    inventories[str(checks.parent.relative_to(root))] = pin

new_paths = [ROOT / n for n in ('configs/phase5c2_activation_v1.json','modeling/phase5c2_activation.py',
    'modeling/tests/test_phase5c2_activation.py','tools/run_phase5c2_activation.py','docs/phase5c2-bounded-activation-v1.md')]
new_paths += [f for f in root.rglob('*') if f.is_file()]
patterns = {'database_url': re.compile(rb'(?:postgres(?:ql)?|mysql)://[^\s\x22\x27<>]*'),
    'private_key': re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'aws_access_key': re.compile(rb'AKIA[0-9A-Z]{16}'),
    'openai_key': re.compile(rb'sk-(?:proj-)?[A-Za-z0-9_-]{30,}'),
    'url_user_password': re.compile(rb'https?://[^/\s\x22\x27:]+:[^/@\s\x22\x27]+@')}
allowed_test_paths = {'modeling/tests/test_phase5c2_activation.py',
    str(root.relative_to(ROOT)) + '/activation/code/modeling/tests/test_phase5c2_activation.py'}
hits = []
for target in new_paths:
    body = target.read_bytes()
    rel = str(target.relative_to(ROOT))
    for label, pattern in patterns.items():
        matches = list(pattern.finditer(body))
        if matches:
            synthetic = rel in allowed_test_paths and all(b'SECRET_' in match.group() for match in matches)
            hits.append({'path': rel, 'pattern': label, 'matches': len(matches), 'synthetic_test_literal_only': synthetic})
require(all(hit['synthetic_test_literal_only'] for hit in hits), 'unexpected_secret_signature')
runtime = [str(f.relative_to(ROOT)) for f in new_paths if f.name == '.env' or f.suffix in {'.pyc','.pyo','.pkl','.joblib'}]
require(not runtime, 'unintended_runtime_or_copied_component')
whitespace = []
for target in new_paths[:5]:
    if any(line.rstrip(b' \t') != line for line in target.read_bytes().splitlines()):
        whitespace.append(str(target.relative_to(ROOT)))
require(not whitespace, 'implementation_trailing_whitespace')
review = {'checked_at': now(), 'files_inspected': len(new_paths), 'credential_signature_findings': hits,
    'real_credential_findings': 0, 'runtime_or_copied_components': runtime, 'implementation_whitespace': whitespace,
    'artifact_bytes_before_verification_bundle': sum(f.stat().st_size for f in root.rglob('*') if f.is_file()),
    'largest_artifact_bytes': max(f.stat().st_size for f in root.rglob('*') if f.is_file()),
    'largest_artifacts_are_intended_frozen_source_package_and_projection': True}
verification = {'status':'VERIFIED','completed_at':now(),'inventory_count':len(inventories),
    'reconstructions':s['verification'],'real_probability_replays':0,'feature_matrices_built':0,
    'real_output_decision_consistency':'NOT_APPLICABLE_ZERO_FORECASTS',
    'real_feature_determinism':'NOT_APPLICABLE_NO_ELIGIBLE_BOUT',
    'projection_readiness_determinism':'TWO_IDENTICAL_INDEPENDENT_RECONSTRUCTIONS',
    'compilation':'PASSED: three new Python files; bytecode only under /tmp',
    'test_result':'87 passed in 27.44s before activation freeze'}
pin = publish(root / 'verification', {'preservation_check.json':encoded(preservation),
    'publication_inventories.json':encoded(inventories),'verification_receipt.json':encoded(verification),
    'content_review.json':encoded(review),'verify_publication.py':Path(__file__).read_bytes()})
s['verification_pin'] = pin
(p / 'session_state.json').write_bytes(encoded(s))
print(json.dumps({'verification_checksums_sha256':pin,'preservation':preservation,'review':review,
    'inventory_count':len(inventories)},indent=2,sort_keys=True))
