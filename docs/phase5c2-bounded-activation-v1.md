# Phase 5C.2 bounded activation

`tools/run_phase5c2_activation.py` is a separate entrypoint for the explicitly
authorized Phase 5C.2 session. The accepted Phase 5C.1 CLI, contract, evidence,
identity, timing and inference implementations remain frozen and unchanged.
The configuration is `configs/phase5c2_activation_v1.json`.

The session permits one warehouse observation and conditional saved inference.
It permits no source refresh, repair, website acquisition, fitting, recalibration,
threshold change, outcome evaluation or production operation. Capture recency
certifies only the time of this read, not complete results, statistics, profiles
or experience. No trusted supplemental evidence was supplied. The resulting
evidence package therefore has empty `sources` and `assertions`.

Use a new exclusive session directory under
`data/experiments/phase5c1_shadow_workflow/`. The commands are sequential:

```text
python3 tools/run_phase5c2_activation.py freeze \
  --activation SESSION/activation --authorization-text AUTHORIZATION.txt
python3 tools/run_phase5c2_activation.py capture \
  --activation SESSION/activation --activation-pin ACTIVATION_CHECKSUMS
python3 tools/run_phase5c2_activation.py intake \
  --activation SESSION/activation --activation-pin ACTIVATION_CHECKSUMS \
  --capture-result-pin CAPTURE_RESULT_CHECKSUMS
python3 tools/run_phase5c2_activation.py verify \
  --activation SESSION/activation --activation-pin ACTIVATION_CHECKSUMS \
  --intake-result-pin INTAKE_RESULT_CHECKSUMS
```

These commands document the already consumed authorization. They do not grant
a future session another capture or forecast attempt. A fresh acquisition or
supplemental evidence observation requires separately scoped authorization.

`freeze` copies and pins the exact entrypoint, module, tests, connection helper,
configuration and complete authorization text, plus the accepted workflow root,
contract and all component freeze clocks. It records actual UTC. Code changes
after that freeze are refused. Capture must start after every freeze.

`capture` fixes its destination to `SESSION/capture_attempt` and reserves it
exclusively before opening the connection. It invokes the unchanged
`authorized_readonly_capture` API with
`separately_authorized_phase5c2_readonly_capture`. The transparent audit wrapper
allows exactly the API's eight statements in their accepted order. It adds no
queries. Read-only and repeatable-read verification precede schema/table reads.
There is no `bout_features` read. The API rolls back and closes the connection.

Each started/completed query receipt and exact JSON export is flushed to an
exclusive file during acquisition. Failed queries retain started receipts and
earlier bodies; connection failures retain the reservation. Driver messages,
tracebacks, DSNs and environment values are never serialized. Failure or crash
consumes the attempt; another destination, retry or replacement is refused.
Successful acquisition preserves the API receipt, transaction snapshot/timezone,
exact schemas/table exports and common observation cutoff. The source scope
includes every captured upcoming row, including past-dated announcements.
Expired dates do not imply cancellation, replacement or resolved identity.

`intake` verifies the external activation/capture-result pins and exact frozen
observation inventory, then calls the existing real forecasting API with
`separately_authorized_phase5c2_forecast`. It binds authorization text, exact
capture receipt and contract hashes. The original runner validates raw rows and
identity before computational indexing, registers every considered row while
copying all 35 original record bytes, and applies the unchanged evidence and
timing gates. With no eligible bouts it publishes BLOCKED with zero calls and
no feature matrices. Saved components are loaded lazily only after readiness,
registration and reservation of the first eligible attempt. All eligible rows
are scored together with no confidence filtering. March is the predeclared
secondary control; its absence stays explicit. The runner retains failed and
partial attempts and rejects later automatic selection.

`intake` and `verify` run under offline guards that forbid network/warehouse
access, fitting, outcome-file reads and production writes. `verify` checks
inventories, exact API/audit/source-byte correspondence, authorization bindings,
registry lineage and frozen-input reconstruction. It reproduces structural
blockers independently. It invokes `verify_run` without predictor capability:
feature/decision verification can run, but real probabilities are never replayed.
The inclusive 0.40–0.60 NO PICK rule is unchanged and applies only to scored
probabilities; data-blocked bouts remain unscored.

Safe tests are limited to:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -q -p no:cacheprovider \
  modeling/tests/test_phase5c2_activation.py modeling/tests/test_phase5c1_shadow.py
```

The Phase 5C.2 tests use in-memory mock connections exclusively. Historical
comparison remains STILL_BLOCKED and no production promotion is authorized.
