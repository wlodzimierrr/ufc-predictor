"""Synthetic-only, offline tests; the pinned actual bodies use a separate CLI run."""

import builtins
import csv
import io
import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prepare_phase5c11_manual_fight_correction as proposal  # noqa: E402


@pytest.fixture(autouse=True)
def forbid_network_db_models(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("network/database/model execution forbidden in offline proposal tests")
    for name in ("create_connection", "getaddrinfo", "gethostbyname"):
        monkeypatch.setattr(socket, name, denied)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"psycopg", "psycopg2", "sqlite3", "sqlalchemy", "xgboost", "sklearn", "torch", "warehouse", "ufc_scraper"}:
            denied()
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded)


def html(inner):
    return ("<!DOCTYPE html><html><body>\n" + inner + "\n</body></html>\n").encode()


def fixtures():
    urls = [proposal.FIGHT_URL] + [f"http://ufcstats.com/fight-details/{n:016x}" for n in range(1, 11)] + ["http://ufcstats.com/fight-details/3bd159c1bed14700"]
    listing = html(f'<table><tr><td><a href="{proposal.EVENT_URL}">{proposal.EVENT_NAME}</a><span class="b-statistics__date">{proposal.EVENT_DATE}</span></td><td>{proposal.LOCATION}</td></tr></table>')
    cells = [f'<a href="{proposal.FIGHT_URL}">win</a>',
        f'<a href="{proposal.PROFILES[1]}">{proposal.NAMES[1]}</a><a href="{proposal.PROFILES[0]}">{proposal.NAMES[0]}</a>',
        "0 0", "68 15", "1 5", "0 0", "Middleweight", "U-DEC", "3", "5:00"]
    target = f'<tr data-link="{proposal.FIGHT_URL}">' + "".join(f"<td>{cell}</td>" for cell in cells) + "</tr>"
    card = html(f'<span class="b-content__title-highlight">{proposal.EVENT_NAME}</span><ul><li><i class="b-list__box-item-title">Date:</i>{proposal.EVENT_DATE}</li><li><i class="b-list__box-item-title">Location:</i>{proposal.LOCATION}</li></ul><table>' + target + "".join(f'<tr data-link="{url}"><td>other fight</td></tr>' for url in urls[1:]) + "</table>")
    people = "".join(f'<div class="b-fight-details__person"><i class="b-fight-details__person-status">{outcome}</i><a class="b-fight-details__person-link" href="{profile}">{name}</a></div>' for profile, name, outcome in zip(proposal.PROFILES, proposal.NAMES, ("L", "W")))
    labels = "".join(f'<i><i class="b-fight-details__label">{name}:</i>{value}</i>' for name, value in (("Method", "Decision - Unanimous"), ("Round", "3"), ("Time", "5:00"), ("Time format", "3 Rnd (5-5-5)")))
    detail = html(f'<h2 class="b-content__title"><a href="{proposal.EVENT_URL}">{proposal.EVENT_NAME}</a></h2>' + people + '<i class="b-fight-details__fight-title">Middleweight Bout</i><p>' + labels + "</p>")
    fight = {"scraped_at": "2026-08-08 23:38:48 UTC", "fight_id": proposal.FIGHT_ID, "event_id": proposal.EVENT_ID,
        "url": proposal.FIGHT_URL.replace("http://", "http://www."), "fighter_1_id": proposal.FIGHTER_IDS[0], "fighter_2_id": proposal.FIGHTER_IDS[1],
        "bout_type": "Middleweight Bout", "weight_class": "middleweight", "num_rounds": "3", "referee": "", "judge_1": "", "judge_2": "", "judge_3": "",
        **{field: "" for field in proposal.CHANGES}, "event_status": "upcoming"}
    parent_urls = urls[:-1] + [f"manual://fight/{proposal.EVENT_ID}/austin-bashi-vs-jose-miguel-delgado"]
    event = {"scraped_at": "2026-08-08 18:44:47 UTC", "event_id": proposal.EVENT_ID, "url": proposal.EVENT_URL,
        "name": proposal.EVENT_NAME, "date": proposal.EVENT_DATE, "date_formatted": "2026-07-18", "city": "Oklahoma City", "state": "Oklahoma", "country": "USA",
        "fights": ", ".join(proposal.identity(url) for url in parent_urls), "fight_urls": ", ".join(parent_urls), "event_status": "upcoming"}
    base = {kind: {"row": row, "data_record": 1, "physical_end_line": 2,
        "input_csv_lineage": {"path": kind + ".csv", "sha256": "a" * 64, "bytes": 1,
            "coverage": "Full input hash reference only; only selected row archived, not a standalone full CSV archive."}}
        for kind, row in (("events", event), ("fights", fight))}
    return {"listing": listing, "card": card, "detail": detail}, base


@pytest.fixture
def case(monkeypatch):
    bodies, base = fixtures()
    monkeypatch.setattr(proposal, "SOURCE_PINS", {key: (len(body), proposal.sha256(body)) for key, body in bodies.items()})
    monkeypatch.setattr(proposal, "BASE_ROW_PINS", {key: proposal.sha256(proposal.json_bytes(value["row"])) for key, value in base.items()})
    pins, tool = proposal.code_pins()
    return bodies, base, pins, tool


def payload(case):
    return proposal.build_payloads(*case)


def publication():
    return {"schema_version": proposal.VERSION, "proposal_computation_started_utc": "2026-10-04T16:00:00.000000Z",
        "publication_utc": "2026-10-04T16:00:01.000000Z", "source_capture_utc": None,
        "publication_status": "PROPOSAL_ONLY_NOT_APPLIED", "clock_scope": "Local audit computation/publication only; not source capture or historical availability."}


def bundle(tmp_path, case):
    files = payload(case)
    destination = tmp_path / "proposal"
    proposal.publish_bundle(destination, {**files, "validation_receipt.json": proposal.json_bytes(proposal.payload_receipt(files))}, publication())
    return destination


def csv_bytes(rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode()


def test_real_case_pins_are_closed_and_exact():
    assert proposal.SOURCE_PINS == {
        "listing": (29854, "89bf7782105d187da970026e10d5f596306753095372403ade54730333f043df"),
        "card": (43781, "bdfcaf45105fa1a028b06e588198c9146a662f2642472f277e2d742c0d719395"),
        "detail": (44995, "05da35470b65c2bbd35d04b0904eea98c19ea482845f0619753a576d7aa95f29")}
    assert proposal.BASE_ROW_PINS["fights"] == "3641667e3ca3c9da3dd983b51ebcc45b81bd16e187278f6d85153da80c1d8737"
    assert proposal.BASE_ROW_PINS["events"] == "b17ef81a32d2456e9d96416a0a58dbad3e381ae1259be4d79b0e28ae937ae063"
    assert tuple(map(proposal.identity, proposal.PROFILES)) == proposal.FIGHTER_IDS
    assert proposal.identity(proposal.EVENT_URL) == proposal.EVENT_ID
    assert proposal.identity(proposal.FIGHT_URL) == proposal.FIGHT_ID
    assert proposal.identity(proposal.FIGHT_URL.replace("http:", "https:")) != proposal.FIGHT_ID


def test_reversed_card_order_joins_profiles_and_only_nine_fields_change(case):
    before = proposal.json_bytes(case[1])
    result = json.loads(payload(case)["proposal.json"])
    candidate = result["proposed_fight_row"]
    base_fight = case[1]["fights"]["row"]
    assert {key for key in candidate if candidate[key] != base_fight[key]} == set(proposal.CHANGES)
    assert len(result["field_diff"]) == 9
    assert result["identity_orientation"]["card_to_stored_permutation"] == [2, 1]
    assert [candidate["fighter_1_id"], candidate["fighter_2_id"]] == list(proposal.FIGHTER_IDS)
    assert [candidate["fighter_1_outcome"], candidate["fighter_2_outcome"]] == ["L", "W"]
    assert candidate["primary_finish_method"] == "decision" and candidate["secondary_finish_method"] == "unanimous"
    assert result["validation"]["disposition_winner_fighter_id"] == proposal.FIGHTER_IDS[1]
    assert result["proposed_parent_event_changes"] == []
    assert result["event_roster_diagnostics"]["base_event_status"] == "upcoming"
    assert result["event_roster_diagnostics"]["card_urls_absent_from_parent_roster"] == ["http://ufcstats.com/fight-details/3bd159c1bed14700"]
    assert "aggregate_statistics" in result["excluded_updates"]
    assert not any("strikes" in field or "is_title" in field for field in candidate)
    assert proposal.json_bytes(case[1]) == before


def test_unknown_provenance_and_base_scraped_clock_remain_unknown(case):
    result = json.loads(payload(case)["proposal.json"])
    for source in result["source_provenance"].values():
        assert all(source[key] is None for key in ("capture_utc", "final_browser_url", "http_status", "request_cache_provenance"))
        assert source["original_http_wire_bytes_verified"] is False
    assert result["authorization"]["original_issuance_utc"] is None
    assert result["source_admission"] == "SOURCE_ADMISSION_UNAPPROVED"
    assert result["v1"] == "ENFORCED" and result["v2"] == "PROPOSED_NOT_APPROVED"
    assert result["historical_comparison"] == "STILL_BLOCKED"
    assert result["identity_orientation"]["detail_self_fight_url"] is None
    assert "NEVER timestamps" in result["clock_semantics"]["retained_scraped_at"]
    assert "non-title status remains UNKNOWN" in result["extracted_facts"]["bout_type_literal_only"]["derivation"]
    assert result["proposed_fight_row"]["scraped_at"] == case[1]["fights"]["row"]["scraped_at"]
    assert all(f["references"] for f in result["field_diff"])


def test_two_builds_are_deterministic_with_clocks_separate(case):
    assert payload(case) == payload(case)
    assert "publication.json" not in payload(case)
    assert "2026-10-04T16:" not in payload(case)["proposal.json"].decode()


@pytest.mark.parametrize("source", ["listing", "card", "detail"])
def test_received_bytes_tamper_refused_before_parsing(case, source):
    case[0][source] += b" "
    with pytest.raises(proposal.Refusal, match="received_text_pin_mismatch"):
        payload(case)


@pytest.mark.parametrize("bad", [b"Checking your browser", b"CAPTCHA", b"access denied", b"verify you are human", b"<html><body><div></body></html>", b"plain text", b"\xff"])
def test_challenge_and_malformed_html_refused(case, bad):
    case[0]["detail"] = bad
    with pytest.raises(proposal.Refusal):
        proposal.extract_evidence(case[0])


@pytest.mark.parametrize("source,old,new", [
    ("listing", b"July 18, 2026", b"July 19, 2026"),
    ("card", b"July 18, 2026", b"July 19, 2026"),
    ("card", b"U-DEC", b"S-DEC"), ("card", b"5:00", b"4:59"),
    ("card", b">win<", b">draw<"),
    ("card", proposal.FIGHT_URL.encode(), b"http://foreign.example/fight-details/4eff5a845db17572"),
    ("card", proposal.PROFILES[1].encode(), b"http://ufcstats.com/fighter-details/ffffffffffffffff"),
    ("detail", proposal.EVENT_URL.encode(), b"http://ufcstats.com/event-details/ffffffffffffffff"),
    ("detail", proposal.PROFILES[0].encode(), b"http://ufcstats.com/fighter-details/ffffffffffffffff"),
    ("detail", b">L<", b">W<"), ("detail", b">W<", b"><"),
    ("detail", b"Decision - Unanimous", b"Decision - Split"),
    ("detail", b">3</i>", b">2</i>"), ("detail", b"5:00", b"5:01"),
    ("detail", b"3 Rnd (5-5-5)", b"5 Rnd (5-5-5-5-5)"),
])
def test_source_chain_conflicts_refused(case, source, old, new):
    assert old in case[0][source]
    case[0][source] = case[0][source].replace(old, new)
    with pytest.raises(proposal.Refusal):
        proposal.extract_evidence(case[0])


@pytest.mark.parametrize("source,marker", [("listing", b"<tr>"), ("card", b'<tr data-link="http://ufcstats.com/fight-details/4eff5a845db17572">'), ("detail", b'<div class="b-fight-details__person">')])
def test_duplicate_source_target_or_person_refused(case, source, marker):
    end = b"</div>" if source == "detail" else b"</tr>"
    body = case[0][source]
    start = body.index(marker)
    stop = body.index(end, start) + len(end)
    case[0][source] = body[:stop] + body[start:stop] + body[stop:]
    with pytest.raises(proposal.Refusal):
        proposal.extract_evidence(case[0])


@pytest.mark.parametrize("kind", ["events", "fights"])
def test_csv_target_absent_duplicate_and_url_identity_ambiguity(case, kind):
    row = case[1][kind]["row"]
    assert proposal.select_base_csv(csv_bytes([row]), kind, "copied.csv")["row"] == row
    with pytest.raises(proposal.Refusal, match="ambiguous"):
        proposal.select_base_csv(csv_bytes([row, row]), kind, "duplicate.csv")
    wrong_id = "event_id" if kind == "events" else "fight_id"
    unrelated = {**row, wrong_id: "unrelated", "url": "http://ufcstats.com/other"}
    with pytest.raises(proposal.Refusal, match="missing"):
        proposal.select_base_csv(csv_bytes([unrelated]), kind, "absent.csv")
    with pytest.raises(proposal.Refusal, match="ambiguous"):
        proposal.select_base_csv(csv_bytes([row, {**row, wrong_id: "different"}]), kind, "alias.csv")


@pytest.mark.parametrize("kind,field,value", [
    ("fights", "fighter_1_id", proposal.FIGHTER_IDS[1]), ("fights", "event_id", "different"),
    ("fights", "url", "http://ufcstats.com/fight-details/ffffffffffffffff"),
    ("fights", "event_status", "completed"), ("fights", "fighter_1_outcome", "L"),
    ("fights", "finish_method", "Decision - Split"), ("fights", "bout_type", "Title Bout"),
    ("events", "event_status", "completed"), ("events", "date_formatted", "2026-07-19"),
    ("events", "name", "Different card"),
])
def test_wrong_changed_or_resolved_base_refused(case, kind, field, value):
    case[1][kind]["row"][field] = value
    with pytest.raises(proposal.Refusal, match="base_row_changed"):
        payload(case)


def test_allowlist_comes_from_supported_facts(case, monkeypatch):
    monkeypatch.setitem(proposal.CHANGES, "referee", "Herb Dean")
    with pytest.raises((proposal.Refusal, KeyError)):
        payload(case)


def test_frozen_bundle_validates_without_original_inputs(tmp_path, case, monkeypatch):
    destination = bundle(tmp_path, case)
    case[0].clear()
    case[1].clear()
    def external_reads_forbidden(*args, **kwargs):
        raise AssertionError("no external inputs during frozen validation")
    monkeypatch.setattr(proposal, "select_base_csv", external_reads_forbidden)
    monkeypatch.setattr(proposal, "code_pins", external_reads_forbidden)
    result = proposal.validate_bundle(destination)
    assert result["files_checked"] == 10 and result["payload_members_rebuilt"] == 7
    assert result["proposal_disposition"] == "PROPOSAL_ONLY_NOT_APPLIED"
    assert all(path.stat().st_mode & 0o222 == 0 for path in destination.rglob("*") if path.is_file())


@pytest.mark.parametrize("change", ["tamper", "extra_file", "extra_directory", "symlink", "missing", "writable"])
def test_bundle_integrity_refuses_tamper_extras_and_missing(tmp_path, case, change):
    destination = bundle(tmp_path, case)
    destination.chmod(0o755)
    member = destination / "proposal.json"
    if change == "tamper":
        member.chmod(0o644)
        member.write_bytes(member.read_bytes() + b" ")
        member.chmod(0o444)
    elif change == "extra_file":
        (destination / "unexpected.txt").write_text("unexpected")
    elif change == "extra_directory":
        (destination / "unexpected").mkdir()
    elif change == "symlink":
        (destination / "unexpected").symlink_to(member)
    elif change == "missing":
        member.unlink()
    else:
        member.chmod(0o644)
    with pytest.raises(proposal.Refusal):
        proposal.validate_bundle(destination)


def test_checksum_rewrite_does_not_admit_changed_proposal(tmp_path, case):
    destination = bundle(tmp_path, case)
    member = destination / "proposal.json"
    changed = json.loads(member.read_bytes())
    changed["proposed_fight_row"]["referee"] = "invented"
    member.chmod(0o644)
    member.write_bytes(proposal.json_bytes(changed))
    member.chmod(0o444)
    index = destination / "checksums.json"
    checksums = json.loads(index.read_bytes())
    checksums["members"]["proposal.json"] = {"sha256": proposal.sha256(member.read_bytes()), "bytes": member.stat().st_size}
    index.chmod(0o644)
    index.write_bytes(proposal.json_bytes(checksums))
    index.chmod(0o444)
    with pytest.raises(proposal.Refusal, match="rebuild_mismatch"):
        proposal.validate_bundle(destination)


@pytest.mark.parametrize("existing", ["empty", "populated"])
def test_publication_refuses_every_existing_destination(tmp_path, case, existing):
    destination = tmp_path / "existing"
    destination.mkdir()
    if existing == "populated":
        (destination / "owned.txt").write_text("preserve")
    files = payload(case)
    with pytest.raises(FileExistsError):
        proposal.publish_bundle(destination, {**files, "validation_receipt.json": proposal.json_bytes(proposal.payload_receipt(files))}, publication())
    assert sorted(p.name for p in destination.iterdir()) == ([] if existing == "empty" else ["owned.txt"])


def test_partial_write_failure_retained_and_never_validated(tmp_path, case, monkeypatch):
    destination = tmp_path / "partial"
    files = payload(case)
    original_open = Path.open
    def failing_open(path, mode="r", *args, **kwargs):
        if path.name == "proposal.json" and mode == "xb":
            raise OSError("simulated mid-publication write failure")
        return original_open(path, mode, *args, **kwargs)
    monkeypatch.setattr(Path, "open", failing_open)
    with pytest.raises(proposal.Refusal, match="publication_incomplete"):
        proposal.publish_bundle(destination, {**files, "validation_receipt.json": proposal.json_bytes(proposal.payload_receipt(files))}, publication())
    assert destination.exists() and list(destination.iterdir())
    with pytest.raises(proposal.Refusal, match="membership_mismatch"):
        proposal.validate_bundle(destination)
    with pytest.raises(FileExistsError):
        proposal.publish_bundle(destination, {**files, "validation_receipt.json": b"{}"}, publication())


@pytest.mark.parametrize("body", [b'{"a":NaN}', b'{"a":Infinity}', b'{"a":-Infinity}', b'{"a":1,"a":2}', b"\xff", b"{"])
def test_strict_json_refusal(body):
    with pytest.raises(proposal.Refusal):
        proposal.load_json(body)


def test_cli_has_no_apply_option_and_unicode_errors_are_refused(monkeypatch, capsys):
    with pytest.raises(SystemExit) as caught:
        proposal.main(["prepare", "--apply"])
    assert caught.value.code == 2
    def invalid_text(*args, **kwargs):
        raise UnicodeError("invalid input text")
    monkeypatch.setattr(Path, "read_bytes", invalid_text)
    assert proposal.main(["prepare"]) == 2
    assert '"status": "REFUSED_OR_INCOMPLETE"' in capsys.readouterr().err
