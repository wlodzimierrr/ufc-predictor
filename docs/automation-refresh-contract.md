# Automated dashboard refresh contract (v1)

Implements T9.1.1. The 2026-10-25 instruction to follow the documented plan
authorizes Phase 9 implementation. The ticket graph in
[phase9-automation-tickets.md](phase9-automation-tickets.md) controls its order;
each completed ticket gets a user update and an evidence entry in the phase report.

## Command and boundaries

The scheduled command is `make update_dashboard_data`, invoked serially from the
repository root by `scripts/automation/refresh.sh`. It loads local events, fights,
upcoming cards and canonical odds; rebuilds the pre-event prediction log and
upcoming features; runs the existing production scorer; persists predictions;
generates both dashboard backtests and current recommendations; and loads betting
report snapshots into Postgres. It does not train a new base model, promote a
model, apply migrations, acquire fresh results/odds, or place bets.

The existing scorer refits its Platt calibration from warehouse validation data.
Automation preserves that existing behavior; it does not turn it into a frozen
calibrator or certify prospective evidence. Phase 5C forecasting/source admission
and historical-comparison gates remain as recorded in the Phase 5C.12 report.
Deploying or enabling this schedule requires a separately reviewed runtime and
input bundle; implementation and offline tests do not run that scorer.

The job runs Sunday at 12:00 in `Europe/London`, including daylight saving changes.
GitHub Actions builds the image; k3s owns the schedule. A Sunday reload alone
cannot settle an event whose results have not arrived in the input bundle.
`make refresh_scrape`, `make post_event`, and `make refresh_current_odds` are
separate acquisition commands, disabled in the first scheduled job pending
T9.3.2 readiness evidence. Event-week odds refresh is a follow-up phase.

## Required inputs

| Input | Requirement |
|---|---|
| Database | Reachable, already migrated warehouse with fighter profiles, historical stats/features and required prediction/betting views. The v1 command is not a database bootstrap. |
| Connection secret | `DATABASE_URL` using a PostgreSQL URI, or the existing `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` variables; optional `POSTGRES_PORT` defaults to 5432. URI support is delivered in T9.2.3. Never put values in the image or logs. |
| Runtime data | Reviewed `events.csv`, `fights.csv`, `fighters.csv`, `manifests/events_manifest.csv`, and canonical `odds/fight_odds.csv` in a mounted data bundle. Optional historical report files may accompany them. |
| Model bundle | Reviewed `production_model.json` and its selected artifact's `model.joblib` and `metadata.json`; preserve any referenced supporting artifacts and saved prediction history. These ignored local artifacts are not available from a clean Git checkout. |
| Writable workspace | Private ephemeral copies of data/models and report/audit directories. Source mounts remain read-only. Generated CSVs are runtime outputs; Postgres is the dashboard source of truth. |
| Metadata | Image revision, command version, unique run ID and UTC start/finish timestamps. Missing optional image metadata is reported as unknown. |

The container must validate inputs before invoking make. A model pointer containing
the developer's absolute path must be translated only in the disposable runtime
copy to the corresponding bundled artifact; the original pointer and model bytes
remain unchanged. No artifact download or model training fills missing files.

The input-bundle owner must refresh/approve input data before scheduling. Keep the
job suspended until the image, mounted bundle, warehouse schema, credentials,
source-policy decision and successful one-off validation are confirmed. A seeded
bundle is not an automatic acquisition feed. No production deployment is performed
by the image publishing workflow.

## Success and failure

A run succeeds only if input checks, every make step, warehouse postchecks and the
final audit write succeed. Verify non-empty events/fights/odds and the generated
default/conservative report summaries in Postgres. If future upcoming fights are
present, verify current predictions and current recommendation report coverage
for the earliest such event; zero bets is valid, missing card rows is not. If no
future card is present, record that explicitly rather than invent recommendations.
Record measured counts and distinguish input rows, database changes and resulting
table counts. Unknown update counts are null, never guessed from file sizes.

Each run records its ID, UTC times, running/succeeded/failed status, git/image
revision, command version, counts, errors and disabled-scraper reasons in a durable
Postgres audit. If Postgres is unavailable, emit a local JSON failure receipt and
non-zero exit code; a database outage cannot promise a database audit write.
Retain Kubernetes logs and failed Job history; mount persistent audit storage if
local JSON receipts must survive pod deletion. Log errors without credential values.

The wrapper uses strict shell settings and preserves the failed command's exit
code. Kubernetes uses `restartPolicy: Never`, `concurrencyPolicy: Forbid`, bounded
execution time and no automatic retry of partially completed refreshes. Loaders
commit independently, so the complete pipeline is not atomic. An operator reviews
the audit and source inputs before manually retrying. Repeated runs reuse the
existing upsert/snapshot behavior and create distinct audit records; they never
commit generated files to Git.

## Offline validation

Use synthetic input bundles, stubbed make commands and fake database connections
to test orchestration and failures. Do not execute `make update_dashboard_data`,
the mutating live integration test, model scoring, migrations or scraper requests
against the operator's configured warehouse during implementation verification.
Docker builds, manifest rendering and CLI help checks are separate from a live
refresh. Unavailable Docker/cluster/source evidence must be recorded as unverified,
not reported as a successful operational deployment.
