import csv
from pathlib import Path

from scrapy.http import HtmlResponse, Request

from ufc_scraper.middlewares import BrowserSessionHeaderMiddleware, RawCaptureMiddleware


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class _Spider:
    logger = _Logger()


def _response(url: str, body: bytes) -> HtmlResponse:
    request = Request(url=url)
    return HtmlResponse(url=url, request=request, body=body)


def test_browser_session_headers_apply_only_to_ufcstats() -> None:
    middleware = BrowserSessionHeaderMiddleware(
        cookie_header="challenge=passed; session=abc",
        user_agent="Mozilla/5.0 test",
    )
    ufc_request = Request(url="http://www.ufcstats.com/event-details/1")
    other_request = Request(url="https://example.com/event-details/1")

    middleware.process_request(ufc_request, _Spider())
    middleware.process_request(other_request, _Spider())

    assert ufc_request.cookies == {"challenge": "passed", "session": "abc"}
    assert ufc_request.headers["User-Agent"] == b"Mozilla/5.0 test"
    assert other_request.cookies == {}
    assert b"User-Agent" not in other_request.headers


def test_browser_session_headers_can_parse_curl_header_block() -> None:
    middleware = BrowserSessionHeaderMiddleware(
        header_block="""
        -H 'accept: text/html'
        -H 'cookie: a=1; b=two'
        -H 'sec-fetch-site: none'
        -H 'user-agent: Mozilla/5.0 from curl'
        """
    )
    request = Request(url="http://www.ufcstats.com/event-details/1")

    middleware.process_request(request, _Spider())

    assert request.cookies == {"a": "1", "b": "two"}
    assert request.headers["Accept"] == b"text/html"
    assert request.headers["Sec-Fetch-Site"] == b"none"
    assert request.headers["User-Agent"] == b"Mozilla/5.0 from curl"


def test_raw_capture_skips_browser_challenge(tmp_path: Path) -> None:
    url = "http://www.ufcstats.com/event-details/1e75e6c9de99fa76"
    body = b"""
    <!doctype html>
    <title>Loading...</title>
    <p>Checking your browser...</p>
    <script>xhr.open('POST', "/__c", true);</script>
    """
    middleware = RawCaptureMiddleware(data_dir=tmp_path)
    middleware._spider_opened(_Spider())

    response = middleware.process_response(Request(url=url), _response(url, body), _Spider())

    raw_path = tmp_path / "raw" / "ufcstats" / "events"
    manifest_path = tmp_path / "manifests" / "fetch_manifest.csv"

    assert list(raw_path.glob("*.html")) == []
    assert response.status == 503

    with manifest_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    assert len(rows) == 1
    assert rows[0]["source_url"] == url
    assert rows[0]["fetch_status"] == "failed"
    assert rows[0]["storage_path"] == ""
    assert rows[0]["error_message"] == "browser challenge page"
