# Implementation control

This chat coordinates implementation under the repository ticket plans. The user
requested the documented plan, subagents where useful, and an update after each
ticket on 2026-10-25. No separate chart was attached; this record makes the
documented dependency graph and current evidence explicit for future handoffs.

The controlling Phase 9 plan is [phase9-automation-tickets.md](phase9-automation-tickets.md).
Its [implementation report](implementation-reports/phase9-automation-report.md)
records delivered work and verification. Update the ticket status and report,
then notify the user after each ticket. Preserve unrelated worktree changes.
Follow the [Git handoff](implementation-reports/next-phase-git-handoff.md) for
logical commits and normal pushes; do not equate publication with deployment.

## Current lanes

| Lane | Current state | Next requirement |
|---|---|---|
| Phase 9 implementation | Contract, wrapper/audit, image/workflow, suspended manifests and runbook delivered. | Validate the published image and configure the reviewed runtime before activation. |
| Phase 9 acquisition | Explicitly deferred; no scheduler calls a scraper. | Bounded image/source qualification and a maintained input feed. |
| Phase 5C modeling evidence | BLOCKED, separate from dashboard automation. | Explicit prospective evidence-policy/source qualification decision; the one-fight correction is not blanket source approval. |
| Phase 8 odds coverage | T8.13.1/.2 remain in progress in their existing plan. | Bounded, reviewed historical target coverage; do not infer completion from refresh automation. |

## Next Phase 9 tickets

These follow-up tickets close measured delivery gaps rather than treating them as
success. T9.5.1 is IN_PROGRESS: CI build/imports and GHCR publication passed, while
the exact-image runtime/input preflight remains pending. T9.5.2–T9.5.4 remain TODO.
Keep the CronJob suspended until the activation ticket.

| Ticket | Dependency | Acceptance |
|---|---|---|
| T9.5.1 Validate the published image | T9.1.2, T9.2.1 | Successful GHCR workflow, full SHA image reference/digest, successful no-network input preflight and command imports from that exact image; retain evidence. |
| T9.5.2 Configure the runtime | T9.5.1, T9.2.3 | Real SOPS-managed runtime/pull credentials, explicit warehouse target/schema, audit migration, readable reviewed input PVC and writable audit PVC. No plaintext secret values in evidence. |
| T9.5.3 Validate one refresh and activate | T9.5.2, operator activation authorization | Review Phase 5C boundaries; execute one bounded full refresh, verify audit/table/current-card parity and partial-failure handling, then review unsuspending the Sunday schedule. Do not enable dev/prod writers to the same DB. |
| T9.5.4 Qualify acquisition and input freshness | T9.5.1, T9.5.2, T9.3.2 | Bounded results/BFO requests from the selected image, raw evidence, identity/semantic checks, cookie/anti-bot failure handling, actual system dependencies and reviewed freshness/feed policy. Keep unreliable sources disabled. |

T9.5.1 and T9.5.2 can advance independently of blocked model research. T9.5.3
requires a concrete runtime review because it writes live data and scores using
the existing production path. Implementing the plan does not silently approve a
new evidence policy or authorize unbounded source capture.

## Later phase candidates

Only plan these after the applicable runtime/source gates pass; no follow-up
schedule or external notification is created by this document.

| Candidate | Entry gate | First ticket |
|---|---|---|
| Phase 10: event-week odds and unusual schedules | T9.5.3, T9.5.4 | Define bounded target selection, timezone/cadence, stale-odds behavior and Friday/overseas/postponed-card overrides. |
| Phase 11: warehouse-native snapshot history | Proven stable audited refresh | Specify immutable odds/report snapshot schemas, retention and backward-compatible readers before replacing CSV staging. |
| Phase 12: failure notifications | Stable audit/status semantics and explicit notification authorization | Select channel and meaningful-change/failure criteria, then build/test delivery without repeated unchanged-status messages. |

These are planning candidates, not claims that prior phases are complete or
authority to send messages, enable schedules, retrain or promote models.
