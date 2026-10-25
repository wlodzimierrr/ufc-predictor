# Phase 9 runtime validation and setup draft

2026-10-25 continuation. **CI IMAGE VALIDATION PASSED; REAL-BUNDLE TRANSFER
AWAITING EXPLICIT APPROVAL.** Runtime setup remains a draft. No refresh, scoring,
source acquisition, warehouse migration or scheduled workload was executed.

## T9.5.1: published image validation

Source `95a01a44c299350d8c8ac4d8ec570805fde3a053` built and published successfully
in [run 40235748210](https://github.com/wlodzimierrr/ufc-predictor/actions/runs/40235748210).
The immutable image is:

`ghcr.io/wlodzimierrr/ufc-data@sha256:6e164d546fbdc000c13c62eeccfa6eef57708157f8c0d87b66b7b89613c47346`

The new manual `validate-image.yml` workflow ran that exact digest in
[run 40236050959](https://github.com/wlodzimierrr/ufc-predictor/actions/runs/40236050959),
successfully checking its image identity, UID/GID 10001, all 12 command imports,
read-only root, read-only input mounts, writable disposable copies, byte parity,
model-pointer translation and host/source immutability with networking disabled.
The fixture has an intentionally non-serialized model sentinel. This validates
the container contract; it does not validate real model deserialization or certify
current input data. The harness can exercise local bundles without uploading them
to CI, but no such run has occurred.

The downloaded artifact ZIP matched GitHub's artifact digest
`sha256:fb0a952a7a6a761dc76c4e2058a9ccd2d5d77d086963d58670a745f010c64d6d`.
Its sanitized JSON is retained under the audit namespace below. Anonymous read
access to the exact corrected image manifest was separately verified; dev does
not need a registry pull secret while this package remains publicly readable.

## Corrected precheck and read-only warehouse evidence

A bounded read-only inspection found that the first implementation queried
`fight_stats`, which is not a warehouse table. The actual schema and feature
reader use `fight_stats_aggregate`. The precheck now queries that table; its fake
database also rejects table names absent from checked-in CREATE TABLE statements.
All 32 runtime tests pass. The corrected precheck subsequently passed against the
configured warehouse with `default_transaction_read_only=on` and an explicitly
read-only transaction. Only SELECT queries and transaction completion occurred.

The required existing prediction/betting relations are present. Migrations 019
and 020 are recorded as applied; migration 021 and `automation_refresh_runs` are
absent. No migration or row write was attempted. The retained receipt binds the
database/role identity by SHA-256 without publishing connection values.

## T9.5.2: prepared runtime inputs and configuration

The new local-only bundle builder selects five required CSVs, the unchanged
production pointer, and the selected artifact's ordinary files. It rejects
symlinks, overlapping/existing output paths and changing source bytes, preserves
file bytes/modes, and leaves an incomplete inventory after failed copies.
The deterministic manifest explicitly records `candidate` / `not_reviewed`.

The current candidate contains nine files totaling 68,122,942 bytes:

| Input | Rows |
|---|---:|
| events.csv | 818 |
| fighters.csv | 4,517 |
| fights.csv | 9,165 |
| manifests/events_manifest.csv | 797 |
| odds/fight_odds.csv | 159,424 |

Selected artifact: `models/xgb/20260328T221117Z`. Manifest SHA-256:
`edaab9f452709b1d4eb0e664ed8930a7ecbf840f3b309b11c55df7e3ad972f37`.
Copies remain in the local task's temporary directory; no candidate contents were
committed, uploaded to CI, or transferred to the cluster. Row counts describe
files, not source qualification, deduplicated warehouse counts or freshness.

Read-only cluster inspection confirmed the active dashboard reports `APP_ENV=dev`,
its database URI comes from `ufc-dashboard-database`, the database Service exposes
5432, and the default storage class is `local-path`. The dev refresh source pins
the validated image digest and removes the unnecessary registry-secret reference.
The CronJob remains suspended; prod remains an unconfigured template.

A runtime Secret for the dev refresh namespace was prepared using the existing
configured warehouse credentials and the verified in-cluster database service
name. It is encrypted to the homelab SOPS age recipient and its local decryption
round trip was verified. Only the encrypted draft exists in a private local
temporary directory; no secret values entered Git, logs or the cluster. No Secret
was applied. The current workstation lacks KSOPS, so real KSOPS overlay rendering
also remains pending. Do not count placeholder-secret rendering as verification.

Separate AppProject/Application templates now describe the new dev namespace
and manual sync. They are not registered in the homelab bootstrap. The existing
dashboard AppProject only permits the dashboard namespace and must not be reused.
No new PVC, scheduled workload, Secret or Argo registration has been applied.

## Automatic approval block and temporary resource cleanup

The concrete real-bundle test plan uses a temporary random namespace, deny-all
ingress/egress, a nonroot read-only pod, no service-account token, no credentials,
and only ephemeral storage. Its seed helper verifies exact archive membership,
sizes and hashes; the main program independently rechecks the pinned manifest
before existing trusted model deserialization. It does not score fights or run
the refresh command. The source and seed plan are retained for review.

The namespace, policy and empty pod were created for validation. **Automatic
approval review rejected the subsequent transfer** because it would send a
sensitive 68 MB data/model bundle to a cluster pod without explicit user approval
for that payload and destination. The transfer command did not run. No bundle
file reached the pod and model deserialization did not run. The empty namespace
was deleted, and a read-only check confirmed its absence.

The requested next approval is limited to transferring that pinned candidate to
the temporary validation namespace for no-network preflight and deserialization.
It does not authorize production setup, migrations, forecast generation or
unsuspending a schedule. T9.5.1 remains in progress on actual-bundle qualification;
T9.5.2 remains in progress on reviewed inputs and actual runtime delivery.

## Retained evidence and verification

Audit directory:
`data/audits/phase9_runtime_validation/20261025_image_95a01a4/`.
Eight component files plus `checksums.json` retain CI smoke, anonymous registry
access, read-only warehouse inspection, candidate manifest, sanitized encrypted
secret preparation, the cluster test plan/helper and the transfer approval block.
Checksum index SHA-256:
`65c4d85ed715d6538ff0e33d99bf50f5c306a4ae345e8e99b01f9d075741101e`.
No bundle payload, raw warehouse rows or credential values are in this evidence.

The combined safe unit suite passed 176 tests with live DB connections disabled:
37 new image-harness cases, 22 new builder cases, the 32 runtime cases and selected
existing container/manifest/connection/recommendation/backtest/transform/probe tests.
Dev/prod overlays render. All 7,631 preservation-baseline files remain unchanged,
including the 19 pre-existing worktree paths. The homelab workload copy remains
unregistered, uncommitted source alongside its pre-existing unrelated changes.

Commit hashes and push verification belong to the final handoff rather than a
self-referential report update. The image above remains the validated deployment
candidate even when later documentation commits publish another image.
