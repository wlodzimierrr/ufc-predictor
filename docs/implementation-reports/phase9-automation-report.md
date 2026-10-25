# Phase 9 automation implementation

Started 2026-10-25 under the user's instruction to follow the documented plan
and report after every ticket. The Phase 9 ticket graph is the control record.
The Phase 5C evidence-policy gates remain separate and blocked.

## Preservation and execution scope

Starting HEAD: `0ed362a`, branch `main`, upstream `origin/main`, remote
`https://github.com/wlodzimierrr/ufc-predictor.git`. No applicable AGENTS.md was
found. Existing recommendation/loader edits, event/fighter/fight/manifest/alias
CSV edits, refresh audits/card reports, migration 020 and its test predate this
work and are excluded from Phase 9 commits. Frozen models, holdouts, experiment
and evidence files must remain byte-identical.

No live refresh, source acquisition, model scoring, migration or cluster deployment
is authorized by an offline implementation test. Source-policy gates are not
resolved by a container or a schedule.

## Ticket evidence

| Ticket | Implementation and verification | Operational evidence |
|---|---|---|
| T9.1.1 | Defined `docs/automation-refresh-contract.md` against Makefile and source readers; reviewed required runtime bundles, database variables, counts, failure and partial-commit behavior. | Contract complete; no live job run. |
| T9.1.3 | Strict shell entrypoint, serial make command and original failure exit codes; wrapper/runtime synthetic tests. | No actual refresh invoked. |
| T9.1.2 | Python 3.11 nonroot image, explicit build-context allowlist, runtime dependency pins and read-only source bundle/private-copy interface; image tests guard secret/artifact exclusion. Build includes command-import smoke check. | Local Docker build/run unavailable; serialization compatibility still requires reviewed image/bundle validation. |
| T9.2.1 | Main/manual GHCR publishing and pull-request build, full SHA tags; follows the dashboard build/login/metadata pattern. No deployment or refresh in CI. | Publication outcome belongs to the final Git handoff/next image validation ticket. |
| T9.2.2 | Base/dev/prod manifests render; Sunday 12:00 Europe/London, suspended, Forbid, no automatic retries, separate namespaces, readonly inputs and persistent audit storage. Copied source into `homelab/workloads/apps/ufc-data-refresh` with approved filesystem escalation; source parity verified. | No Argo registration, cluster apply, image tag selection or schedule activation. |
| T9.2.3 | `DATABASE_URL` support preserves split variables; two fake-connection tests. ConfigMap, default deny/DNS/database policies, optional excluded HTTPS policy and SOPS/KSOPS encryption helper. | Real runtime/pull credentials remain unconfigured; generators disabled until the actual encrypted secret exists. |
| T9.3.1 | New migration 021 and durable run audit; fresh reports/current-card checks, sanitized failure receipts, explicit unknown affected-row counts. 32 runtime tests cover success, failure and preservation. | Migration not applied; local Postgres server binaries unavailable for disposable SQL parsing. Signals/node loss can leave an interrupted `running` record. |
| T9.3.2 | Source review and explicit deferred decision in `automation-scraper-readiness.md`; disabled reasons logged/audited. | Neither source exercised from Docker; acquisition remains disabled and the input feed remains an operator responsibility. |
| T9.4.1 | Runbook covers builds, no-network preflight, inspection, audited status, manual retry, secret rotation, schedule/fallback and partial commits. | Operational commands are documented, not executed. |

## Review and checks

Subagents handled roadmap review, runtime/audit implementation, and container/CI
implementation. Root reviewed and integrated their work and owns the control plan
and Git handoff. Review caught and corrected final-receipt success misreporting,
crossed source/destination overlap, readonly-copy permissions, stale private-copy
outputs, and a default audit path that interfered with input preparation.

The combined safe suite passed 117 tests using an explicit psycopg2 connection
guard and disabled dotenv loading. It includes the new runtime/container/manifest/
connection tests and selected existing recommendation/backtest/transform/probe
unit tests. Both overlays render, shell syntax and CLI help checks pass.
The same 117 tests also pass from an isolated clean HEAD archive overlaid only
with the intended Phase 9 changes. This verifies independence from pre-existing
uncommitted operational fixes. All 7,631 baseline files across existing user
edits/reports, models, holdouts and experiments remain byte-identical.

An earlier test selection mistakenly included `warehouse/tests/test_upsert.py`:
its live connection fixture produced five DNS failures before any database write.
It was excluded rather than retried against the configured warehouse. Raw errors
were not saved as phase evidence. Later checks use explicit live-connection guards.
No live refresh, acquisition, scoring or migration was executed.

## Next tickets and handoff

[`implementation-control.md`](../implementation-control.md) defines T9.5.1–T9.5.4
for exact-image validation, real runtime setup, reviewed activation, and acquisition
qualification. Later phase candidates cover event-week odds/nonstandard schedules,
warehouse snapshot history and explicitly authorized failure notifications.

Phase 9 is **implemented for offline review, operationally incomplete**. Docker,
runtime secrets, migration/input feed and actual cluster validation remain open.
The homelab copy remains uncommitted source files, alongside pre-existing unrelated
README/worktree changes. This task does not publish or register that separate
GitOps repository. Final commit hashes, push outcome and preservation verification
are reported in the user handoff rather than a self-referential report commit.

## Environment limitations

Initial inspection found kubectl, kustomize and sops, but no Docker or Podman
executable and no Docker socket. Container build/live smoke evidence remains
pending unless a build runtime becomes available. Homelab workload sources are
in a separate repository outside this task's writable roots. Its existing
unrelated README edit and untracked worktrees must also be preserved.
