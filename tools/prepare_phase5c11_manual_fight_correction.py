#!/usr/bin/env python3
"""One approved, offline, review-only Cannonier/Duncan correction proposal.

No network, database, production loader, scoring, or apply entry points. Received
text hashes qualify only the pasted bytes, never provider authenticity or capture
time. A frozen bundle can be validated without attachments or live CSVs.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import io
import json
import os
from pathlib import Path
import re
import sys
import uuid

VERSION = "phase5c11_manual_fight_correction_v1"
STARTING_HEAD = "31a144d34e405162fb3cc35ad24123836efe5f09"
REPO_ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = REPO_ROOT / "data/audits/phase5c11_manual_fight_correction"
EVENT_URL = "http://ufcstats.com/event-details/f354c50b8d63d9b3"
FIGHT_URL = "http://ufcstats.com/fight-details/4eff5a845db17572"
EVENT_ID = "68a758a6-bd6a-5ec7-933e-72251614d52f"
FIGHT_ID = "53b9cada-68ba-5c11-8da4-28833cd6b5fe"
PROFILES = (
    "http://ufcstats.com/fighter-details/13a0275fa13c4d26",
    "http://ufcstats.com/fighter-details/a93f94c923c3a9cb",
)
FIGHTER_IDS = (
    "c00bb616-ac26-58fe-af2e-8d953a63322d",
    "364ad7d6-2d46-5e3c-a5f9-5696d76b73a9",
)
NAMES = ("Jared Cannonier", "Christian Leroy Duncan")
EVENT_NAME = "UFC Fight Night: Du Plessis vs. Usman"
EVENT_DATE = "July 18, 2026"
LOCATION = "Oklahoma City, Oklahoma, USA"
SOURCE_PINS = {
    "listing": (29854, "89bf7782105d187da970026e10d5f596306753095372403ade54730333f043df"),
    "card": (43781, "bdfcaf45105fa1a028b06e588198c9146a662f2642472f277e2d742c0d719395"),
    "detail": (44995, "05da35470b65c2bbd35d04b0904eea98c19ea482845f0619753a576d7aa95f29"),
}
ATTACHMENTS = {
    "listing": "/home/wlodzimierrr/.codex/attachments/8db73d66-d51d-4938-b54b-759585cea750/Pasted text.txt",
    "card": "/home/wlodzimierrr/.codex/attachments/eb7700d0-13d2-49b9-a95a-64d14e9a997e/Pasted text.txt",
    "detail": "/home/wlodzimierrr/.codex/attachments/ee0df532-4d32-423a-a651-814813ebc175/Pasted text.txt",
}
BASE_ROW_PINS = {
    "events": "b17ef81a32d2456e9d96416a0a58dbad3e381ae1259be4d79b0e28ae937ae063",
    "fights": "3641667e3ca3c9da3dd983b51ebcc45b81bd16e187278f6d85153da80c1d8737",
}
CHANGES = {
    "fighter_1_outcome": "L", "fighter_2_outcome": "W", "event_status": "completed",
    "finish_method": "Decision - Unanimous", "primary_finish_method": "decision",
    "secondary_finish_method": "unanimous", "finish_round": "3",
    "finish_time_minute": "5", "finish_time_second": "0",
}
AUTHORIZATION = {
    "question": "Do you approve using these manually supplied pages—keeping their unknown capture metadata explicit—to prepare an isolated offline correction proposal for this fight? No live warehouse writes or changes to frozen datasets.",
    "response": "yes, lest continue phases",
    "original_issuance_utc": None,
    "scope": "Offline proposal for this existing fight only; no correction application or source-policy acceptance.",
}
BOUNDARIES = {
    "proposal_disposition": "PROPOSAL_ONLY_NOT_APPLIED",
    "source_admission": "SOURCE_ADMISSION_UNAPPROVED",
    "v1": "ENFORCED", "v2": "PROPOSED_NOT_APPROVED",
    "prospective_forecasting": "BLOCKED", "historical_comparison": "STILL_BLOCKED",
    "production": "UNCHANGED",
    "availability_certificate": "NOT_A_SOURCE_AVAILABILITY_CERTIFICATE",
    "training_scoring_admission": "NOT_ADMISSIBLE",
}
REFERENCE_PATHS = (
    "warehouse/strict_fight_outcomes_v2.py", "warehouse/transform.py",
    "scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/parsers/fight_info_parser.py",
    "docs/normalization-rules.md",
    "docs/implementation-reports/20261004-manual-source-corroboration-and-identity-correction-report.md",
)
PAYLOAD_MEMBERS = frozenset({
    "sources/listing.html", "sources/card.html", "sources/detail.html",
    "base_snapshot.json", "proposal.json", "code_pins.json", "review_tool.py",
})
BUNDLE_MEMBERS = PAYLOAD_MEMBERS | {"validation_receipt.json", "publication.json", "checksums.json"}


class Refusal(ValueError):
    """An ambiguity or invariant failure prevents publication/admission."""


def require(condition, reason):
    if not condition:
        raise Refusal(reason)


def sha256(body):
    return hashlib.sha256(body).hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def normalize_url(url):
    """Only remove the optional www host prefix; keep scheme/path exact."""
    require(isinstance(url, str), "invalid_source_url")
    return url.replace("http://www.ufcstats.com/", "http://ufcstats.com/", 1)


def identity(url):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, normalize_url(url)))


class Node:
    def __init__(self, tag, attrs, line, parent=None):
        self.tag, self.attrs, self.line, self.end_line = tag, attrs, line, line
        self.parent, self.children = parent, []

    def nodes(self, tag=None, cls=None):
        result = []
        for child in self.children:
            if isinstance(child, Node):
                if (tag is None or child.tag == tag) and (cls is None or cls in (child.attrs.get("class") or "").split()):
                    result.append(child)
                result.extend(child.nodes(tag, cls))
        return result

    def text(self):
        parts = [child.text() if isinstance(child, Node) else child for child in self.children]
        return " ".join(" ".join(parts).split())


class SourceParser(HTMLParser):
    VOID = frozenset("area base br col embed hr img input link meta param source track wbr".split())

    def __init__(self, raw):
        super().__init__(convert_charrefs=True)
        self.root = Node("root", {}, 1)
        self.stack = [self.root]
        try:
            text = raw.decode("utf-8", errors="strict")
            require(not re.search(r"captcha|checking your browser|verify you are human|access denied", text, re.I), "challenge_or_denial_html")
            self.feed(text)
            self.close()
        except (UnicodeError, RecursionError) as exc:
            raise Refusal("malformed_source_text") from exc
        require(len(self.stack) == 1, "unclosed_html_elements")
        require(len(self.root.nodes("html")) == 1 and len(self.root.nodes("body")) == 1, "missing_html_document")

    def handle_starttag(self, tag, attrs):
        require(len(dict(attrs)) == len(attrs), "duplicate_html_attribute")
        node = Node(tag, dict(attrs), self.getpos()[0], self.stack[-1])
        self.stack[-1].children.append(node)
        if tag not in self.VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in self.VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if tag in self.VOID:
            return
        require(len(self.stack) > 1 and self.stack[-1].tag == tag, "unbalanced_html_elements")
        self.stack.pop().end_line = self.getpos()[0]

    def handle_data(self, data):
        self.stack[-1].children.append(data)


def one(nodes, reason):
    require(len(nodes) == 1, reason)
    return nodes[0]


def ref(source, node, locator):
    return {"member": f"sources/{source}.html", "sha256": SOURCE_PINS[source][1],
            "line_start": node.line, "line_end": node.end_line, "locator": locator}


def fact(literal, normalized, references, derivation="Whitespace collapse only."):
    return {"literal": literal, "normalized": normalized, "references": references, "derivation": derivation}


def label_value(root, label, label_class):
    node = one([n for n in root.nodes(cls=label_class) if n.text() == label + ":"], "missing_or_duplicate_label_" + label)
    text = node.parent.text()
    require(text.startswith(label + ": "), "malformed_label_" + label)
    return text[len(label) + 2:], node.parent


def extract_evidence(bodies):
    """Parse only this source chain; this helper does not itself admit bytes."""
    require(set(bodies) == set(SOURCE_PINS), "unexpected_source_members")
    roots = {key: SourceParser(value).root for key, value in bodies.items()}
    listing, card, detail = (roots[key] for key in ("listing", "card", "detail"))
    event_rows = [row for row in listing.nodes("tr") if any(normalize_url(a.attrs.get("href", "")) == EVENT_URL for a in row.nodes("a"))]
    listing_row = one(event_rows, "missing_or_duplicate_listing_event")
    listing_link = one([a for a in listing_row.nodes("a") if normalize_url(a.attrs.get("href", "")) == EVENT_URL], "ambiguous_listing_event_link")
    listing_date = one(listing_row.nodes(cls="b-statistics__date"), "ambiguous_listing_date")
    require(listing_link.text() == EVENT_NAME and listing_date.text() == EVENT_DATE, "listing_event_name_or_date_conflict")
    listing_cells = [n for n in listing_row.children if isinstance(n, Node) and n.tag == "td"]
    require(len(listing_cells) == 2 and listing_cells[1].text() == LOCATION, "listing_location_conflict")
    title = one(card.nodes(cls="b-content__title-highlight"), "ambiguous_card_title")
    card_date, date_node = label_value(card, "Date", "b-list__box-item-title")
    card_location, location_node = label_value(card, "Location", "b-list__box-item-title")
    require((title.text(), card_date, card_location) == (EVENT_NAME, EVENT_DATE, LOCATION), "card_event_name_date_or_location_conflict")
    rows = [row for row in card.nodes("tr") if "data-link" in row.attrs]
    roster = [normalize_url(row.attrs["data-link"]) for row in rows]
    require(len(roster) == 12 and len(set(roster)) == len(roster), "missing_duplicate_or_wrong_card_roster")
    require(all(re.fullmatch(r"http://ufcstats.com/fight-details/[0-9a-f]{16}", url) for url in roster), "foreign_card_fight_link")
    target = one([row for row in rows if normalize_url(row.attrs["data-link"]) == FIGHT_URL], "missing_or_duplicate_card_fight")
    cells = [n for n in target.children if isinstance(n, Node) and n.tag == "td"]
    require(len(cells) == 10, "unexpected_card_columns")
    flags = cells[0].nodes("a")
    flag = one(flags, "missing_or_duplicate_card_result")
    require(flag.text() == "win" and normalize_url(flag.attrs.get("href", "")) == FIGHT_URL, "card_result_or_fight_link_conflict")
    links = cells[1].nodes("a")
    card_profiles = [normalize_url(a.attrs.get("href", "")) for a in links]
    require(len(card_profiles) == 2 and set(card_profiles) == set(PROFILES), "card_profile_membership_conflict")
    require([a.text() for a in links] == [NAMES[PROFILES.index(url)] for url in card_profiles], "card_profile_name_conflict")
    require(card_profiles[0] == PROFILES[1], "card_winner_conflict")
    require((cells[6].text(), cells[7].text(), cells[8].text(), cells[9].text()) == ("Middleweight", "U-DEC", "3", "5:00"), "card_weight_method_round_or_time_conflict")
    heading = one(detail.nodes("h2", "b-content__title"), "missing_or_duplicate_detail_event_heading")
    backlink = one(heading.nodes("a"), "missing_or_duplicate_detail_event_link")
    require(normalize_url(backlink.attrs.get("href", "")) == EVENT_URL and backlink.text() == EVENT_NAME, "detail_event_backlink_conflict")
    people = detail.nodes(cls="b-fight-details__person")
    require(len(people) == 2, "missing_or_duplicate_detail_people")
    detail_profiles, outcomes, person_refs = [], [], []
    for person in people:
        link = one(person.nodes("a", "b-fight-details__person-link"), "ambiguous_detail_profile")
        url = normalize_url(link.attrs.get("href", ""))
        require(url in PROFILES and link.text() == NAMES[PROFILES.index(url)], "detail_profile_membership_or_name_conflict")
        detail_profiles.append(url)
        outcomes.append(one(person.nodes(cls="b-fight-details__person-status"), "ambiguous_detail_outcome").text())
        person_refs.append(ref("detail", person, "person/profile href and explicit status"))
    require(detail_profiles == list(PROFILES), "detail_orientation_conflict")
    require(outcomes == ["L", "W"], "detail_completed_outcome_pair_conflict")
    bout = one(detail.nodes(cls="b-fight-details__fight-title"), "ambiguous_detail_bout_type")
    method, method_node = label_value(detail, "Method", "b-fight-details__label")
    finish_round, round_node = label_value(detail, "Round", "b-fight-details__label")
    finish_time, time_node = label_value(detail, "Time", "b-fight-details__label")
    time_format, format_node = label_value(detail, "Time format", "b-fight-details__label")
    require((bout.text(), method, finish_round, finish_time, time_format) == ("Middleweight Bout", "Decision - Unanimous", "3", "5:00", "3 Rnd (5-5-5)"), "detail_bout_method_round_time_or_format_conflict")
    evidence = {
        "event_name": fact(EVENT_NAME, EVENT_NAME, [ref("listing", listing_link, "event href"), ref("card", title, "title"), ref("detail", backlink, "event backlink")]),
        "event_date": fact(EVENT_DATE, "2026-07-18", [ref("listing", listing_date, "event date"), ref("card", date_node, "Date label")], "Parse supplied July 18, 2026 calendar date; not a capture clock."),
        "event_location": fact(LOCATION, LOCATION, [ref("listing", listing_cells[1], "location cell"), ref("card", location_node, "Location label")]),
        "fighter_1_outcome": fact("L", "L", [person_refs[0]]),
        "fighter_2_outcome": fact("W", "W", [person_refs[1], ref("card", target, "win flag attached to first profile Duncan")]),
        "event_status": fact(["L", "W"], "completed", person_refs, "Completed paired L/W for this fight row only; no parent event-status inference."),
        "finish_method": fact(method, method, [ref("detail", method_node, "Method label"), ref("card", cells[7], "U-DEC")], "Whitespace collapse; U-DEC corroborates Decision - Unanimous."),
        "primary_finish_method": fact(method, "decision", [ref("detail", method_node, "Method label")], "Existing CSV parser: split literal on ' - ', lowercase first token; not the DB finish_method column."),
        "secondary_finish_method": fact(method, "unanimous", [ref("detail", method_node, "Method label")], "Existing CSV parser: split literal on ' - ', lowercase second token."),
        "finish_round": fact(finish_round, "3", [ref("detail", round_node, "Round label"), ref("card", cells[8], "round cell")]),
        "finish_time_minute": fact(finish_time, "5", [ref("detail", time_node, "Time label"), ref("card", cells[9], "time cell")], "Split explicit 5:00 at colon, normalize integer minute to CSV string."),
        "finish_time_second": fact(finish_time, "0", [ref("detail", time_node, "Time label"), ref("card", cells[9], "time cell")], "Split explicit 5:00 at colon, normalize integer second to CSV string."),
        "bout_type_literal_only": fact(bout.text(), bout.text(), [ref("detail", bout, "bout title")], "Literal corroboration only; affirmative non-title status remains UNKNOWN under v1."),
        "time_format_literal_only": fact(time_format, time_format, [ref("detail", format_node, "Time format label")], "Corroboration only; scheduled rounds are preserved, not proposed."),
    }
    return {"facts": evidence, "card_roster_urls": roster,
            "identity_orientation": {"event_id": identity(EVENT_URL), "fight_id": identity(FIGHT_URL),
                "detail_profile_urls": detail_profiles, "detail_fighter_ids": [identity(url) for url in detail_profiles],
                "card_profile_urls": card_profiles, "card_to_stored_permutation": [PROFILES.index(url) + 1 for url in card_profiles],
                "join_key": "Exact profile href after only optional www removal; never display order.",
                "detail_self_fight_url": None, "detail_event_date": None,
                "detail_assignment": "Manual text chain corroborated by card fight link, profiles, paired result, finish and event backlink; final browser URL unknown."}}


def check_sources(bodies):
    require(set(bodies) == set(SOURCE_PINS), "unexpected_source_members")
    for key, body in bodies.items():
        require((len(body), sha256(body)) == SOURCE_PINS[key], "received_text_pin_mismatch_" + key)
    return extract_evidence(bodies)


def select_base_csv(raw, kind, path):
    key, wanted_id, wanted_url = ("event_id", EVENT_ID, EVENT_URL) if kind == "events" else ("fight_id", FIGHT_ID, FIGHT_URL)
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""))
        require(reader.fieldnames and len(set(reader.fieldnames)) == len(reader.fieldnames), "missing_or_duplicate_csv_header")
        require(key in reader.fieldnames and "url" in reader.fieldnames, "missing_csv_identity_header")
        selected = []
        for ordinal, row in enumerate(reader, 1):
            require(None not in row and all(isinstance(v, str) for v in row.values()), "malformed_csv_record")
            if row.get(key) == wanted_id or normalize_url(row.get("url", "")) == wanted_url:
                selected.append({"row": row, "data_record": ordinal, "physical_end_line": reader.line_num})
    except (UnicodeError, csv.Error) as exc:
        raise Refusal("malformed_csv") from exc
    require(len(selected) == 1, "missing_duplicate_or_ambiguous_" + kind + "_target")
    return {**selected[0], "input_csv_lineage": {"path": str(path), "sha256": sha256(raw), "bytes": len(raw),
            "coverage": "Full input hash reference only; only selected row archived, not a standalone full CSV archive."}}


def check_base(base):
    require(set(base) == {"events", "fights"}, "unexpected_base_snapshot_members")
    for kind in ("events", "fights"):
        require(set(base[kind]) == {"row", "data_record", "physical_end_line", "input_csv_lineage"}, "invalid_base_snapshot_shape")
        row = base[kind]["row"]
        require(sha256(json_bytes(row)) == BASE_ROW_PINS[kind], "base_row_changed_" + kind)
        lineage = base[kind]["input_csv_lineage"]
        require(set(lineage) == {"path", "sha256", "bytes", "coverage"} and isinstance(lineage["path"], str)
                and re.fullmatch(r"[0-9a-f]{64}", lineage["sha256"]) and isinstance(lineage["bytes"], int) and lineage["bytes"] > 0,
                "invalid_csv_lineage")
        require(isinstance(base[kind]["data_record"], int) and base[kind]["data_record"] > 0
                and isinstance(base[kind]["physical_end_line"], int) and base[kind]["physical_end_line"] > base[kind]["data_record"], "invalid_csv_row_reference")
    event, fight = base["events"]["row"], base["fights"]["row"]
    require(event["event_id"] == EVENT_ID and normalize_url(event["url"]) == EVENT_URL
            and event["name"] == EVENT_NAME and event["date_formatted"] == "2026-07-18"
            and event["event_status"] == "upcoming", "parent_event_base_conflict")
    require(fight["fight_id"] == FIGHT_ID and fight["event_id"] == EVENT_ID and normalize_url(fight["url"]) == FIGHT_URL
            and (fight["fighter_1_id"], fight["fighter_2_id"]) == FIGHTER_IDS, "fight_identity_or_orientation_conflict")
    require(fight["event_status"] == "upcoming" and all(fight[field] == "" for field in CHANGES if field != "event_status"), "fight_already_resolved_or_conflicting_base")


def code_pins():
    tool = Path(__file__).read_bytes()
    return {"tool_version": VERSION, "starting_repo_head": STARTING_HEAD,
            "tool_member": "review_tool.py", "tool_sha256": sha256(tool),
            "reference_file_sha256": {path: sha256((REPO_ROOT / path).read_bytes()) for path in REFERENCE_PATHS},
            "reference_role": "Semantics and prior-scope lineage pins only; no imports or runtime production calls."}, tool


def build_payloads(bodies, base, pins, tool):
    """Deterministic payloads: operational clocks are deliberately outside."""
    extracted = check_sources(bodies)
    check_base(base)
    require(pins["tool_version"] == VERSION and pins["starting_repo_head"] == STARTING_HEAD
            and pins["tool_member"] == "review_tool.py" and pins["tool_sha256"] == sha256(tool), "code_pin_mismatch")
    identities = extracted["identity_orientation"]
    require((identities["event_id"], identities["fight_id"], tuple(identities["detail_fighter_ids"])) == (EVENT_ID, FIGHT_ID, FIGHTER_IDS), "reproduced_identity_conflict")
    fight, event = base["fights"]["row"], base["events"]["row"]
    proposed = dict(fight)
    diff = []
    for field, expected in CHANGES.items():
        evidence = extracted["facts"][field]
        require(evidence["normalized"] == expected, "unsupported_field_value_" + field)
        proposed[field] = evidence["normalized"]
        diff.append({"field": field, "before": fight[field], "after": proposed[field], "evidence_fact": field,
                     "derivation": evidence["derivation"], "references": evidence["references"]})
    changed = {key for key in proposed if proposed[key] != fight[key]}
    require(changed == set(CHANGES), "proposal_change_allowlist_violation")
    parent_urls = [normalize_url(url) for url in event["fight_urls"].split(", ")]
    parent_ids = event["fights"].split(", ")
    require(len(parent_urls) == len(parent_ids) and len(set(parent_urls)) == len(parent_urls)
            and len(set(parent_ids)) == len(parent_ids), "ambiguous_parent_roster")
    require(parent_urls.count(FIGHT_URL) == 1 and parent_ids[parent_urls.index(FIGHT_URL)] == FIGHT_ID, "target_missing_or_conflicting_parent_roster")
    card_urls = extracted["card_roster_urls"]
    proposal = {"schema_version": VERSION, **BOUNDARIES, "authorization": AUTHORIZATION,
        "source_provenance": {key: {"member": f"sources/{key}.html", "received_text_bytes": SOURCE_PINS[key][0],
            "received_text_sha256": SOURCE_PINS[key][1], "capture_utc": None, "final_browser_url": None,
            "http_status": None, "request_cache_provenance": None, "original_http_wire_bytes_verified": False,
            "authenticity": "MANUALLY_SUPPLIED_TEXT_NOT_PROVIDER_AUTHENTICATED"} for key in SOURCE_PINS},
        "identity_orientation": identities, "extracted_facts": extracted["facts"],
        "allowed_changed_fields": sorted(CHANGES), "field_diff": diff, "proposed_fight_row": proposed,
        "proposed_parent_event_changes": [],
        "event_roster_diagnostics": {"base_event_status": event["event_status"], "parent_event_status_preserved": True,
            "parent_roster_count": len(parent_urls), "supplied_card_roster_count": len(card_urls),
            "parent_roster": [{"fight_id": fid, "url": url} for fid, url in zip(parent_ids, parent_urls)],
            "supplied_card_roster_urls": card_urls,
            "card_urls_absent_from_parent_roster": sorted(set(card_urls) - set(parent_urls)),
            "parent_urls_absent_from_card_roster": sorted(set(parent_urls) - set(card_urls)),
            "disposition": "LATER_REVIEW_REQUIRED_NO_EVENT_STATUS_ALIAS_REMATCH_OR_CANCELLATION_RESOLUTION"},
        "clock_semantics": {"retained_scraped_at": "Unchanged base-row lineage only; NEVER timestamps these new manual claims or historical availability.",
            "manual_claim_capture_utc": None, "proposal_computation_publication_utc": "Recorded only in operational publication.json; not source capture clocks."},
        "excluded_updates": ["IDs", "URLs", "participant_order", "scraped_at", "bout_type", "weight_class", "num_rounds",
            "referee", "judges", "aggregate_statistics", "round_statistics", "parent_event_status"],
        "remaining_requirements": ["Manual provider authenticity and missing capture/transport provenance qualification.",
            "Separate authority to accept source evidence and apply any correction.",
            "Parent event status and native/manual roster consistency review.",
            "Historical revision/availability, exhaustive history and affirmative non-title evidence remain unestablished."],
        "validation": {"paired_outcomes": "L/W", "fight_row_status": "completed", "disposition_result_type": "win",
            "disposition_winner_fighter_id": FIGHTER_IDS[1], "finish_time_seconds": 300,
            "admission_effect": "NONE_REVIEW_CONSISTENCY_ONLY"},
        "statistics_note": "Card Str 68/15 corroborated previously as significant landed, not total landed 94/36. No statistics are extracted into candidate updates, normalized features, or model inputs."}
    files = {f"sources/{key}.html": value for key, value in bodies.items()}
    files.update({"base_snapshot.json": json_bytes(base), "proposal.json": json_bytes(proposal),
                  "code_pins.json": json_bytes(pins), "review_tool.py": tool})
    require(set(files) == PAYLOAD_MEMBERS, "unexpected_payload_members")
    return files


def payload_receipt(files):
    return {"schema_version": VERSION, **BOUNDARIES, "validation_status": "OFFLINE_PROPOSAL_CONSISTENCY_PASSED",
            "deterministic_payload_builds": 2, "payload_member_sha256": {name: sha256(body) for name, body in sorted(files.items())},
            "validation_scope": "Rebuild from frozen received-text bytes and exact captured base rows; CSV full hashes remain lineage references.",
            "runtime_dependencies": "Python standard library only; no network/DB/model calls or source acquisition."}


def utc_now():
    return datetime.now(timezone.utc)


def publish_bundle(destination, files, publication):
    """Exclusive, readonly members; failure retains an honest partial directory."""
    require(set(files) == PAYLOAD_MEMBERS | {"validation_receipt.json"}, "invalid_publication_payload_members")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.mkdir()  # Refuse even an empty existing directory or symlink.
    try:
        members = {**files, "publication.json": json_bytes(publication)}
        members["checksums.json"] = json_bytes({"schema_version": VERSION,
            "membership": sorted(BUNDLE_MEMBERS),
            "members": {name: {"sha256": sha256(body), "bytes": len(body)} for name, body in sorted(members.items())},
            "checksum_scope": "Every member except this checksum index; its schema and exact membership are validated separately."})
        for name, body in sorted(members.items()):
            path = destination / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            path.chmod(0o444)
        validate_bundle(destination)
        for directory in (destination / "sources", destination):
            directory.chmod(0o555)
    except Exception as exc:
        # Never fabricate a success marker, overwrite, delete, or reuse this run.
        raise Refusal(f"publication_incomplete: {destination}; retained partial run must not be reused; {exc}") from exc


def load_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    def constant(value):
        raise Refusal("nonfinite_json_constant_" + value)
    try:
        return json.loads(body, object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise Refusal("malformed_bundle_json") from exc


def validate_bundle(directory):
    """Requires only frozen bundle and this pinned review tool; no live inputs."""
    directory = Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), "invalid_bundle_directory")
    all_paths = list(directory.rglob("*"))
    require(not any(path.is_symlink() for path in all_paths), "bundle_symlink_forbidden")
    names = {path.relative_to(directory).as_posix() for path in all_paths if path.is_file()}
    dirs = {path.relative_to(directory).as_posix() for path in all_paths if path.is_dir()}
    require(names == BUNDLE_MEMBERS and dirs == {"sources"}, "bundle_membership_mismatch")
    require(all(path.is_file() or path.is_dir() for path in all_paths), "bundle_special_file_forbidden")
    require(all((directory / name).stat().st_mode & 0o222 == 0 for name in names), "bundle_member_not_readonly")
    files = {name: (directory / name).read_bytes() for name in names}
    checksums = load_json(files["checksums.json"])
    expected_checksums = {"schema_version": VERSION, "membership": sorted(BUNDLE_MEMBERS),
        "members": {name: {"sha256": sha256(body), "bytes": len(body)} for name, body in sorted(files.items()) if name != "checksums.json"},
        "checksum_scope": "Every member except this checksum index; its schema and exact membership are validated separately."}
    require(checksums == expected_checksums and files["checksums.json"] == json_bytes(expected_checksums), "bundle_checksum_mismatch")
    pins = load_json(files["code_pins.json"])
    require(sha256(Path(__file__).read_bytes()) == pins["tool_sha256"], "validator_tool_version_mismatch")
    rebuilt = build_payloads({key: files[f"sources/{key}.html"] for key in SOURCE_PINS},
                             load_json(files["base_snapshot.json"]), pins, files["review_tool.py"])
    require(all(files[name] == body for name, body in rebuilt.items()), "frozen_payload_rebuild_mismatch")
    receipt = payload_receipt(rebuilt)
    require(files["validation_receipt.json"] == json_bytes(receipt), "validation_receipt_mismatch")
    publication = load_json(files["publication.json"])
    require(set(publication) == {"schema_version", "proposal_computation_started_utc", "publication_utc", "source_capture_utc", "publication_status", "clock_scope"}
            and publication["schema_version"] == VERSION and publication["source_capture_utc"] is None
            and publication["publication_status"] == "PROPOSAL_ONLY_NOT_APPLIED"
            and publication["clock_scope"] == "Local audit computation/publication only; not source capture or historical availability.", "invalid_publication_receipt")
    try:
        clocks = [datetime.strptime(publication[key], "%Y-%m-%dT%H:%M:%S.%fZ") for key in ("proposal_computation_started_utc", "publication_utc")]
    except (ValueError, TypeError) as exc:
        raise Refusal("invalid_operational_clock") from exc
    require(clocks[0] <= clocks[1], "operational_clock_order_conflict")
    return {"validation_status": receipt["validation_status"], **BOUNDARIES, "files_checked": len(files),
            "payload_members_rebuilt": len(rebuilt), "bundle_checksums_sha256": sha256(files["checksums.json"])}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="Create one exclusive, review-only offline proposal run.")
    for key in SOURCE_PINS:
        prepare.add_argument("--" + key, type=Path, default=Path(ATTACHMENTS[key]))
    for kind in ("events", "fights"):
        prepare.add_argument("--" + kind + "-csv", type=Path, default=REPO_ROOT / "data" / (kind + ".csv"))
    validate = sub.add_parser("validate", help="Validate/rebuild a frozen bundle without original inputs.")
    validate.add_argument("bundle", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            result = validate_bundle(args.bundle)
        else:
            started = utc_now()
            bodies = {key: getattr(args, key).read_bytes() for key in SOURCE_PINS}
            base = {kind: select_base_csv(getattr(args, kind + "_csv").read_bytes(), kind, getattr(args, kind + "_csv")) for kind in ("events", "fights")}
            pins, tool = code_pins()
            first = build_payloads(bodies, base, pins, tool)
            second = build_payloads(bodies, base, pins, tool)
            require(first == second, "nondeterministic_payload_build")
            publication_time = utc_now()
            destination = AUDIT_ROOT / (publication_time.strftime("%Y%m%dT%H%M%S%fZ") + "_duncan_cannonier_v1_proposal")
            publication = {"schema_version": VERSION, "proposal_computation_started_utc": started.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "publication_utc": publication_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "source_capture_utc": None,
                "publication_status": "PROPOSAL_ONLY_NOT_APPLIED", "clock_scope": "Local audit computation/publication only; not source capture or historical availability."}
            publish_bundle(destination, {**first, "validation_receipt.json": json_bytes(payload_receipt(first))}, publication)
            result = {"bundle": str(destination), **validate_bundle(destination)}
        print(json.dumps(result, sort_keys=True))
        return 0
    except (Refusal, OSError, KeyError, TypeError, UnicodeError) as exc:
        print(json.dumps({"status": "REFUSED_OR_INCOMPLETE", "reason": str(exc), "proposal_applied": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
