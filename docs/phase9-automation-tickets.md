# Phase 9: Automated Data & Betting Refresh

Phase 8 delivered betting reports and BestFightOdds market-benchmark support. Phase 9
turns the currently manual refresh workflow into a repeatable cluster-run process.

The target operating model is:

- GitHub Actions builds and publishes a Docker image for this repo.
- k3s runs the scheduled refresh as a Kubernetes CronJob.
- Postgres remains the dashboard source of truth.
- Generated CSVs are runtime artifacts, not weekly git commits.
- Sunday 12:00 UK handles post-event settlement, with a later event-week odds refresh
  job for fresher betting cards.

This document is an implementation ticket plan. Do not start code implementation until
this file has been reviewed and approved.

---

## Current Repository Context

Relevant existing pieces:

| Area | Existing Files / Targets | Notes |
|---|---|---|
| Dashboard refresh | `Makefile`, `COMMANDS.md` | `make update_dashboard_data` runs the current post-event/dashboard refresh pipeline. |
| Betting reports | `betting/`, `data/reports/betting_*`, `warehouse/load_betting_reports.py` | Dashboard betting cards are loaded from generated betting reports into warehouse tables. |
| BFO odds | `warehouse/adapt_bestfightodds_line_history_payloads.py`, `data/odds/fight_odds.csv` | BFO Mean support exists, but scraping/capture still needs headless automation review. |
| Warehouse | `warehouse/`, `warehouse/sql/` | Postgres should be the canonical dashboard state after each run. |
| Homelab pattern | `../ufc-dashboard/.github/workflows/publish.yml`, `../homelab/workloads/apps/*` | Existing GHCR build and GitOps deployment pattern can be copied. |
| CronJob example | `../homelab/workloads/apps/homelab-api/base/catalog-sync-cronjob.yaml` | Existing Kubernetes CronJob/kustomize structure to mirror. |

---

## Scope

### In Scope

- Dockerize the repo so the refresh pipeline can run away from the developer PC.
- Build and publish the image to GHCR from GitHub Actions.
- Add a k3s CronJob scheduled for Sunday 12:00 UK.
- Provide secrets, config, network policy, and GitOps manifests.
- Add run logging and a simple success/failure audit.
- Decide and document how generated CSVs are handled in automation.
- Prepare the scraper path for headless execution.

### Out of Scope for First Automation Cut

- Automated bet placement.
- Re-training models after every event.
- Committing generated CSVs back to git every Sunday.
- Replacing the whole CSV-based internal pipeline in one step.
- Fully autonomous recovery from UFCStats/BestFightOdds anti-bot failures.

---

## Tickets

#### T9.1.1 Define automated refresh contract
- **Description:** Define the exact command contract for the scheduled refresh job:
  what it runs, what inputs it requires, what outputs it updates, and what counts as
  success or failure.
- **Status:** TODO
- **Acceptance Criteria:**
  - Document the v1 scheduled command as `make update_dashboard_data`.
  - Identify any optional future scraper commands separately from the v1 refresh.
  - List required environment variables, secrets, and runtime files.
  - Define success criteria: warehouse updated, betting reports loaded, non-zero
    dashboard rows where expected, and clear logs.
  - Define failure behavior: non-zero exit code, failed Kubernetes Job, retained logs.
- **Dependencies:** None
- **Complexity:** S
- **Risk:** Low

#### T9.1.2 Add Docker image for `ufc-data`
- **Description:** Add a production-oriented Dockerfile that can run the dashboard
  refresh pipeline in an ephemeral container.
- **Status:** TODO
- **Acceptance Criteria:**
  - Docker image installs Python dependencies and system tools needed by the current
    refresh pipeline.
  - Image can run `make update_dashboard_data` from a clean checkout.
  - Secrets are provided only by environment variables at runtime.
  - Generated reports/CSV outputs are written inside the container filesystem unless
    explicitly mounted.
  - Local smoke command is documented.
- **Dependencies:** T9.1.1
- **Complexity:** M
- **Risk:** Medium

#### T9.1.3 Add refresh entrypoint script
- **Description:** Add a small wrapper script for automation runs so Kubernetes does
  not need to know internal command sequencing.
- **Status:** TODO
- **Acceptance Criteria:**
  - Script lives under `scripts/automation/`.
  - Script runs with strict shell settings.
  - Script prints start time, git/image metadata where available, command steps, and
    end status.
  - Script initially calls `make update_dashboard_data`.
  - Script is safe to run repeatedly.
- **Dependencies:** T9.1.1
- **Complexity:** S
- **Risk:** Low

#### T9.2.1 Add GitHub Actions GHCR publish workflow
- **Description:** Add a GitHub Actions workflow that builds and publishes the
  `ufc-data` Docker image to GHCR, following the existing `ufc-dashboard` pattern.
- **Status:** TODO
- **Acceptance Criteria:**
  - Workflow builds on push to the main branch and supports manual dispatch.
  - Image is published as `ghcr.io/wlodzimierrr/ufc-data`.
  - Tags include immutable SHA tags.
  - Workflow uses the existing GHCR login/metadata/build-push pattern from
    `ufc-dashboard`.
  - Workflow does not run the scraper or scheduled refresh itself.
- **Dependencies:** T9.1.2
- **Complexity:** M
- **Risk:** Low

#### T9.2.2 Add homelab CronJob manifests
- **Description:** Add GitOps manifests for a k3s CronJob that runs the refresh image
  every Sunday at 12:00 UK.
- **Status:** TODO
- **Acceptance Criteria:**
  - New homelab workload exists, for example `apps/ufc-data-refresh`.
  - CronJob uses `schedule: "0 12 * * 0"` and `timeZone: "Europe/London"`.
  - CronJob uses `concurrencyPolicy: Forbid`.
  - Successful and failed job history limits are configured.
  - Dev/prod overlays follow the existing homelab kustomize pattern.
  - Image tag is patched through the env overlay, not hardcoded in multiple places.
- **Dependencies:** T9.2.1
- **Complexity:** M
- **Risk:** Medium

#### T9.2.3 Configure secrets, config, and network policies
- **Description:** Provide the Kubernetes runtime configuration required for the job
  to reach Postgres and external scrape sources safely.
- **Status:** TODO
- **Acceptance Criteria:**
  - SOPS-managed secret includes `DATABASE_URL` and any required scraper credentials,
    cookies, or headers.
  - ConfigMap includes non-secret refresh options.
  - Network policy allows DNS, database egress, and required HTTPS egress.
  - No secret values are committed in plain text.
  - Runbook documents how to rotate/update secrets.
- **Dependencies:** T9.2.2
- **Complexity:** M
- **Risk:** Medium

#### T9.3.1 Add automation run audit
- **Description:** Add a minimal audit trail so the dashboard/user can tell when the
  last automated refresh ran and whether it succeeded.
- **Status:** TODO
- **Acceptance Criteria:**
  - Each run records run ID, started time, finished time, status, image/git SHA, and
    command version.
  - Run summary includes counts for events reviewed, fights updated, odds rows loaded,
    betting report rows loaded, and errors.
  - Audit can be stored in Postgres or a generated report loaded into Postgres.
  - Dashboard can later read this audit without scraping Kubernetes logs.
- **Dependencies:** T9.1.3
- **Complexity:** M
- **Risk:** Low

#### T9.3.2 Validate scraper readiness for headless cluster runs
- **Description:** Determine which scrapers can safely run inside the cluster and what
  extra browser/cookie/user-agent handling they require.
- **Status:** TODO
- **Acceptance Criteria:**
  - Results scraper is tested from the Docker image.
  - BestFightOdds capture path is tested from the Docker image.
  - Any Playwright/browser/system dependencies are added to the image.
  - Anti-bot or cookie requirements are documented as Kubernetes secrets/config.
  - If a scraper is not reliable enough, the CronJob keeps it disabled and logs a
    clear reason.
- **Dependencies:** T9.1.2, T9.2.3
- **Complexity:** M
- **Risk:** High

#### T9.4.1 Add runbook and operational checks
- **Description:** Document how to run, inspect, retry, and troubleshoot the automated
  refresh process.
- **Status:** TODO
- **Acceptance Criteria:**
  - `docs/runbook.md` includes the Sunday automation flow.
  - Commands are documented for checking last CronJob run, viewing logs, and creating
    a one-off Kubernetes Job from the CronJob.
  - Runbook explains why GitHub Actions builds the image but k3s runs the schedule.
  - Runbook documents the CSV policy: generated CSVs are runtime artifacts; Postgres
    is the dashboard source of truth.
  - Manual fallback steps are documented for odd event schedules or scraper failures.
- **Dependencies:** T9.2.2, T9.3.1, T9.3.2
- **Complexity:** S
- **Risk:** Low

---

## Dependency Graph

```text
T9.1.1
  |
  +-- T9.1.2 -- T9.2.1 -- T9.2.2 -- T9.2.3
  |
  +-- T9.1.3 -- T9.3.1

T9.3.2 depends on the image and cluster secrets/networking.
T9.4.1 follows the CronJob, audit, and scraper-readiness tickets.
```

---

## Suggested Execution Order

| Stage | Tickets | Outcome |
|---|---|---|
| 1 | T9.1.1, T9.1.3 | Refresh command contract and stable automation entrypoint. |
| 2 | T9.1.2 | Local Docker image can run the dashboard update. |
| 3 | T9.2.1 | GHCR image publishing exists. |
| 4 | T9.2.2, T9.2.3 | k3s can run the Sunday 12:00 UK CronJob. |
| 5 | T9.3.1 | Last run status is visible outside Kubernetes logs. |
| 6 | T9.3.2 | Scrapers are either enabled headlessly or explicitly deferred. |
| 7 | T9.4.1 | Operational runbook and manual fallback are documented. |

---

## Phase 9 Success Criteria

Phase 9 is successful when:

1. A pushed `ufc-data` commit can produce a GHCR image.
2. The k3s cluster runs the refresh job every Sunday at 12:00 UK.
3. The job updates the warehouse used by the UFC dashboard without the developer PC
   being online.
4. The dashboard has a reliable way to show the last successful data/betting refresh.
5. Generated CSVs no longer need to be manually committed after every event.
6. Scraper automation is either running headlessly or clearly separated into a known
   follow-up ticket.

---

## Follow-Up Phase Candidates

- Add a daily/event-week odds refresh CronJob for current betting cards.
- Move odds snapshots and generated report history fully into warehouse tables.
- Add Slack/Discord/email alerting for failed refresh jobs.
- Add automatic odd-card detection for Friday, Abu Dhabi, Australia, or non-standard
  UFC schedules.
