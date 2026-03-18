# Scrapy settings for ufc_scraper project
#
# This file is the single source of truth for source-safety and crawl
# behaviour.  Spider custom_settings should only appear where a spider
# has a documented, justified reason to deviate from these baselines.
#
# Reference: https://docs.scrapy.org/en/latest/topics/settings.html

BOT_NAME = "ufc_scraper"

SPIDER_MODULES = ["ufc_scraper.spiders"]
NEWSPIDER_MODULE = "ufc_scraper.spiders"

ADDONS = {}

# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------
# Identify the crawler so ufcstats.com administrators can contact us if needed.
USER_AGENT = "ufc-predictor-scraper (research/personal project)"

# ---------------------------------------------------------------------------
# Source safety — rate limiting and concurrency
# ---------------------------------------------------------------------------
# Never send more than one request at a time to ufcstats.com.
ROBOTSTXT_OBEY = True
CONCURRENT_REQUESTS_PER_DOMAIN = 1

# Base delay between requests (seconds).  AutoThrottle adjusts this upward
# under load; it will never go below this value.
DOWNLOAD_DELAY = 1
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle dynamically scales the delay to keep concurrency at 1.0
# requests per second toward the target server.
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# ---------------------------------------------------------------------------
# Resilience — retries and timeouts
# ---------------------------------------------------------------------------
# Retry transient server and network errors up to 3 times before giving up.
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# Abandon a request that has not received a response within 30 seconds.
# ufcstats.com pages are lightweight; 30 s is generous.
DOWNLOAD_TIMEOUT = 30

# ---------------------------------------------------------------------------
# Downloader middlewares
# ---------------------------------------------------------------------------
# RawCaptureMiddleware must run at a lower priority number than RetryMiddleware
# (550) so that, on process_response (called in decreasing priority order),
# the retry middleware executes first.  Only the final response — after
# retries succeed or are exhausted — reaches RawCaptureMiddleware at 200.
DOWNLOADER_MIDDLEWARES = {
    "ufc_scraper.middlewares.RawCaptureMiddleware": 200,
}

# ---------------------------------------------------------------------------
# Item pipelines
# ---------------------------------------------------------------------------
# EventsManifestPipeline maintains data/manifests/events_manifest.csv as a
# canonical event registry with discovered_at / last_seen_at tracking.
ITEM_PIPELINES = {
    "ufc_scraper.pipelines.EventsManifestPipeline": 300,
}

# ---------------------------------------------------------------------------
# Output encoding
# ---------------------------------------------------------------------------
FEED_EXPORT_ENCODING = "utf-8"
