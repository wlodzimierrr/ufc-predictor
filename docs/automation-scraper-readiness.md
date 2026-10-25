# Headless acquisition readiness (T9.3.2)

2026-10-25 decision: **DEFERRED; disabled in both CronJob overlays.** Docker is
unavailable on the implementation host, so neither source has been tested from
the image or qualified against cluster egress. Offline fixture tests do not
establish live source reliability. Do not enable acquisition from this report.

| Path | Existing implementation | First-cut decision |
|---|---|---|
| UFCStats results | Scrapy spiders, bounded queues, robots handling, throttling, retries, challenge detection and raw manifests. Optional `UFCSTATS_COOKIE_HEADER`, `UFCSTATS_USER_AGENT`, `UFCSTATS_HEADER_BLOCK` browser session settings. | Disabled. The refresh image does not include the scraper project or Scrapy; build a separately reviewed scraper image/target only when bounded cluster testing is possible. |
| BestFightOdds snapshot | Standard-library urllib probe; explicit single event/fighter URL, request cap, timeout/backoff, offline decoder and promotion. User agent is an explicit CLI setting; no automated browser/challenge handling. | Disabled. CLI dry-run/fixture tests can run without network; live cookie/challenge requirements remain unverified. |

No Playwright/browser is currently required by these implementations. No browser
or scraper credentials were added without an exercised need. A transient HTTP
200 can still be a challenge page; status alone is not source qualification.
The existing Phase 5C source/evidence restrictions remain in force.

Before a separate enablement ticket, capture a bounded result request and one
explicit BFO request from the candidate container, retain raw hashes/manifests
and sanitized errors, validate semantic parsing and identity mapping, test blocked
and expired-session responses, and confirm the image/system dependencies and
egress protocols. Store any required session values in SOPS-managed runtime
Secrets. Never log or commit cookies. Obtain a real refresh input feed rather
than reloading a permanently seeded PVC and claiming new results.

The CronJob logs and audits the disabled reason. Setting a configuration flag does
not add acquisition to `make update_dashboard_data`. Follow-up work must add and
validate the bounded command explicitly. Friday/nonstandard cards and event-week
odds timing remain follow-up phase candidates in the controlling plan.
