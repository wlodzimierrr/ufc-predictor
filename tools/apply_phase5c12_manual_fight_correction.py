#!/usr/bin/env python3
"""Bounded current-data correction primitives for one approved fight.

No CSV writer, connection discovery, loader, model call, or automatic rollback.
The operator supplies a connection factory and applies the reviewed CSV patch.
CSV and PostgreSQL do not share an atomic commit; ambiguous commits require an
independent bounded reread before the operator decides whether to compensate.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path

VERSION = "phase5c12_manual_fight_correction_v1"
REPO_ROOT = Path(__file__).resolve().parents[1]
PROPOSAL_RUN = REPO_ROOT / "data/audits/phase5c11_manual_fight_correction/20261004T203255672479Z_duncan_cannonier_v1_proposal"
PROPOSAL_CHECKSUM_SHA = "e65b01dddfae1b4af42a63a1a2d4fe541b5b683bbc8e8cea6e6411819b9b40b2"
PROPOSAL_TOOL_SHA = "8c489087a4f2c7db326ea248da82eed26071f3065934a61890fea5a22abf9af4"
FIGHT_ID = "53b9cada-68ba-5c11-8da4-28833cd6b5fe"
EVENT_ID = "68a758a6-bd6a-5ec7-933e-72251614d52f"
FIGHTER_IDS = ("c00bb616-ac26-58fe-af2e-8d953a63322d", "364ad7d6-2d46-5e3c-a5f9-5696d76b73a9")
FIGHT_URL = "http://ufcstats.com/fight-details/4eff5a845db17572"
EVENT_URL = "http://ufcstats.com/event-details/f354c50b8d63d9b3"
PROFILE_URLS = ("http://ufcstats.com/fighter-details/13a0275fa13c4d26", "http://ufcstats.com/fighter-details/a93f94c923c3a9cb")
CSV_CHANGES = {
    "fighter_1_outcome": "L", "fighter_2_outcome": "W", "event_status": "completed",
    "finish_method": "Decision - Unanimous", "primary_finish_method": "decision",
    "secondary_finish_method": "unanimous", "finish_round": "3",
    "finish_time_minute": "5", "finish_time_second": "0",
}
DB_CHANGES = {
    "winner_fighter_id": FIGHTER_IDS[1], "result_type": "win",
    "finish_method": "decision", "finish_detail": "unanimous",
    "finish_round": 3, "finish_time_seconds": 300,
}
FIGHT_COLUMNS = (
    "fight_id", "event_id", "fighter_1_id", "fighter_2_id", "winner_fighter_id",
    "result_type", "weight_class", "is_title_fight", "is_interim_title",
    "scheduled_rounds", "finish_method", "finish_detail", "finish_round",
    "finish_time_seconds", "referee", "source_url", "scraped_at",
)
BOUNDARIES = {
    "source_admission": "CASE_SCOPED_CURRENT_DATA_ONLY",
    "v1": "ENFORCED", "v2": "PROPOSED_NOT_APPROVED",
    "source_capture_utc": None, "provider_authenticity_verified": False,
    "availability_certificate": "NOT_A_SOURCE_AVAILABILITY_CERTIFICATE",
    "historical_comparison": "STILL_BLOCKED", "prospective_forecasting": "BLOCKED",
    "training_scoring_admission": "NOT_ADMISSIBLE", "production_models": "UNCHANGED",
    "history_completeness": "UNESTABLISHED", "debut": "UNESTABLISHED",
    "affirmative_non_title_evidence": "UNKNOWN",
    "scraped_at_semantics": "Retained base lineage only; does not timestamp this correction or source availability.",
    "cross_store_atomicity": "CSV and database are separate stores; operator compensation requires exact current-state checks.",
}


class Refusal(ValueError):
    """Changed, ambiguous, or out-of-scope state must be reviewed before writing."""


def require(condition, reason):
    if not condition:
        raise Refusal(reason)


def sha256(body):
    return hashlib.sha256(body).hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def normalized_url(value):
    return value.replace("http://www.ufcstats.com/", "http://ufcstats.com/", 1)


def load_pinned_proposal(directory=PROPOSAL_RUN):
    """Verify external pins BEFORE executing the stored, previously reviewed tool."""
    directory = Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), "invalid_proposal_directory")
    for name, pin in (("checksums.json", PROPOSAL_CHECKSUM_SHA), ("review_tool.py", PROPOSAL_TOOL_SHA)):
        path = directory / name
        require(path.is_file() and not path.is_symlink() and sha256(path.read_bytes()) == pin,
                "proposal_pin_mismatch_" + name)
    spec = importlib.util.spec_from_file_location("phase5c12_pinned_review", directory / "review_tool.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    validation = module.validate_bundle(directory)
    base = module.load_json((directory / "base_snapshot.json").read_bytes())
    proposal = module.load_json((directory / "proposal.json").read_bytes())
    check_proposal(base, proposal)
    return {"base": base, "proposal": proposal, "validation": validation,
            "checksum_sha256": PROPOSAL_CHECKSUM_SHA, "tool_sha256": PROPOSAL_TOOL_SHA}


def check_proposal(base, proposal):
    before, after = base["fights"]["row"], proposal["proposed_fight_row"]
    require(after == {**before, **CSV_CHANGES}, "proposal_nine_field_projection_mismatch")
    require(set(proposal["allowed_changed_fields"]) == set(CSV_CHANGES)
            and len(proposal["field_diff"]) == 9, "proposal_diff_allowlist_mismatch")
    diff = {item["field"]: (item["before"], item["after"]) for item in proposal["field_diff"]}
    require(diff == {key: (before[key], value) for key, value in CSV_CHANGES.items()}, "proposal_exact_diff_mismatch")
    require((before["fight_id"], before["event_id"], before["fighter_1_id"], before["fighter_2_id"])
            == (FIGHT_ID, EVENT_ID, *FIGHTER_IDS) and normalized_url(before["url"]) == FIGHT_URL,
            "proposal_identity_or_orientation_mismatch")
    require(proposal["identity_orientation"]["card_to_stored_permutation"] == [2, 1], "proposal_orientation_mismatch")
    require(proposal["proposed_parent_event_changes"] == [], "proposal_parent_change_forbidden")
    for source in proposal["source_provenance"].values():
        require(all(source[key] is None for key in ("capture_utc", "final_browser_url", "http_status", "request_cache_provenance"))
                and source["original_http_wire_bytes_verified"] is False, "proposal_provenance_changed")


def selected_csv_record(raw, identity_key, wanted_id, wanted_url):
    """Return an exact physical record span while validating all CSV identities."""
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
        fields = reader.fieldnames
        require(fields and len(fields) == len(set(fields)) and identity_key in fields and "url" in fields,
                "invalid_csv_header")
        lines = raw.splitlines(keepends=True)
        offsets = [0]
        for line in lines:
            offsets.append(offsets[-1] + len(line))
        previous_line = reader.line_num
        selected = []
        for ordinal, row in enumerate(reader, 1):
            require(None not in row and all(isinstance(value, str) for value in row.values()), "malformed_csv_record")
            if row[identity_key] == wanted_id or normalized_url(row["url"]) == wanted_url:
                selected.append({"row": row, "start": offsets[previous_line], "end": offsets[reader.line_num],
                                 "data_record": ordinal, "fields": fields})
            previous_line = reader.line_num
    except (UnicodeError, csv.Error) as exc:
        raise Refusal("malformed_csv") from exc
    require(len(selected) == 1, "missing_duplicate_or_conflicting_csv_target")
    return selected[0]


def field_spans(record):
    """Locate lexical fields; CSV parsing separately verifies their values."""
    end = len(record)
    while end and record[end - 1] in (10, 13):
        end -= 1
    spans, start, quoted, index = [], 0, False, 0
    while index < end:
        byte = record[index]
        if byte == 34:
            if quoted and index + 1 < end and record[index + 1] == 34:
                index += 2
                continue
            quoted = not quoted
        elif byte == 44 and not quoted:
            spans.append((start, index))
            start = index + 1
        index += 1
    require(not quoted, "unclosed_csv_quote")
    return spans + [(start, end)]


def replace_fields(record, fields, changes):
    spans = field_spans(record)
    require(len(spans) == len(fields), "csv_lexical_field_count_mismatch")
    for index in reversed(range(len(fields))):
        if fields[index] in changes:
            start, end = spans[index]
            value = changes[fields[index]].encode()
            require(not any(byte in value for byte in b',"\r\n'), "unexpected_correction_token")
            if record[start:end].startswith(b'"'):
                value = b'"' + value + b'"'
            record = record[:start] + value + record[end:]
    return record


@dataclass(frozen=True)
class CsvPlan:
    status: str
    before_bytes: bytes
    after_bytes: bytes
    start: int
    end: int
    before_row: dict
    after_row: dict

    def receipt(self):
        return {"status": self.status, "before_sha256": sha256(self.before_bytes),
                "after_sha256": sha256(self.after_bytes), "before_bytes": len(self.before_bytes),
                "after_bytes": len(self.after_bytes), "record_byte_start": self.start,
                "record_byte_end_before": self.end, "changed_fields": sorted(CSV_CHANGES),
                "before_row": self.before_row, "after_row": self.after_row,
                "all_other_records_header_and_unchanged_field_bytes_preserved": True}


def plan_csv(fights_raw, events_raw, package):
    base = package["base"]
    event = selected_csv_record(events_raw, "event_id", EVENT_ID, EVENT_URL)
    require(event["row"] == base["events"]["row"]
            and sha256(events_raw) == base["events"]["input_csv_lineage"]["sha256"], "parent_csv_changed")
    target = selected_csv_record(fights_raw, "fight_id", FIGHT_ID, FIGHT_URL)
    before, after = base["fights"]["row"], package["proposal"]["proposed_fight_row"]
    require(target["row"] in (before, after), "csv_target_changed_or_conflicting")
    record = fights_raw[target["start"]:target["end"]]
    if target["row"] == after:
        reverted_record = replace_fields(record, target["fields"], {key: before[key] for key in CSV_CHANGES})
        original = fights_raw[:target["start"]] + reverted_record + fights_raw[target["end"]:]
        require(sha256(original) == base["fights"]["input_csv_lineage"]["sha256"], "csv_non_target_bytes_changed")
        return CsvPlan("VERIFIED_ALREADY_CORRECT", fights_raw, fights_raw, target["start"], target["end"], after, after)
    require(sha256(fights_raw) == base["fights"]["input_csv_lineage"]["sha256"], "csv_expected_base_hash_mismatch")
    corrected_record = replace_fields(record, target["fields"], CSV_CHANGES)
    corrected = fights_raw[:target["start"]] + corrected_record + fights_raw[target["end"]:]
    require(selected_csv_record(corrected, "fight_id", FIGHT_ID, FIGHT_URL)["row"] == after,
            "csv_corrected_projection_mismatch")
    return CsvPlan("READY_FOR_EXACT_PATCH", fights_raw, corrected, target["start"], target["end"], before, after)


def require_file_hash(path, expected_sha):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and sha256(path.read_bytes()) == expected_sha,
            "file_expected_hash_mismatch")


def write_artifact(directory, name, value):
    """Create durable evidence exclusively; never overwrite any prior artifact."""
    directory = Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), "audit_directory_missing_or_symlink")
    require(Path(name).name == name and name not in (".", ".."), "invalid_audit_member_name")
    body = value if isinstance(value, bytes) else json_bytes(value)
    path = directory / name
    with path.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"member": name, "bytes": len(body), "sha256": sha256(body)}


def _json_rows(cursor, sql, params=()):
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    require(all(len(row) == 1 and isinstance(row[0], dict) for row in rows), "unexpected_database_json_shape")
    return [row[0] for row in rows]


def read_db_snapshot(connection, lock_fight=False):
    """Bounded exact-case read; the caller controls transaction/connection lifetime."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), current_schema(), inet_server_addr()::text, inet_server_port()")
        database_identity = list(cursor.fetchone())
        require(len(database_identity) == 4, "database_identity_shape_mismatch")
        database_identity_sha256 = sha256(json.dumps(database_identity, separators=(",", ":")).encode())
        if lock_fight:
            # Normal UPDATE table lock: protects metadata while locking just this fight row.
            cursor.execute("LOCK TABLE public.fights IN ROW EXCLUSIVE MODE")
        schema = _json_rows(cursor, """SELECT jsonb_build_object(
            'name', column_name, 'type', data_type, 'nullable', is_nullable,
            'default', column_default, 'generated', is_generated)
            FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'fights'
            ORDER BY ordinal_position""")
        tables = _json_rows(cursor, """SELECT jsonb_build_object(
            'name', c.relname, 'kind', c.relkind, 'rls', c.relrowsecurity, 'force_rls', c.relforcerowsecurity)
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = ANY(%s) ORDER BY c.relname""",
            (["fights", "events", "fighters", "fight_stats_aggregate", "fight_stats_by_round"],))
        triggers = _json_rows(cursor, """SELECT jsonb_build_object(
            'name', t.tgname, 'internal', t.tgisinternal, 'enabled', t.tgenabled,
            'definition', pg_get_triggerdef(t.oid)) FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = 'public.fights'::regclass ORDER BY t.tgname""")
        rules = _json_rows(cursor, """SELECT jsonb_build_object(
            'name', r.rulename, 'definition', pg_get_ruledef(r.oid)) FROM pg_catalog.pg_rewrite r
            WHERE r.ev_class = 'public.fights'::regclass AND r.rulename <> '_RETURN' ORDER BY r.rulename""")
        suffix = " FOR UPDATE OF f" if lock_fight else ""
        fights = _json_rows(cursor, """SELECT to_jsonb(f) FROM public.fights f
            WHERE f.fight_id = %s OR f.source_url = ANY(%s) ORDER BY f.fight_id LIMIT 3""" + suffix,
            (FIGHT_ID, [FIGHT_URL, FIGHT_URL.replace("http://", "http://www.")]))
        events = _json_rows(cursor, """SELECT to_jsonb(e) FROM public.events e
            WHERE e.event_id = %s OR e.source_url = ANY(%s) ORDER BY e.event_id LIMIT 3""",
            (EVENT_ID, [EVENT_URL, EVENT_URL.replace("http://", "http://www.")]))
        fighters = _json_rows(cursor, """SELECT to_jsonb(p) FROM public.fighters p
            WHERE p.fighter_id = ANY(%s::uuid[]) OR p.source_url = ANY(%s)
            ORDER BY p.fighter_id LIMIT 5""",
            (list(FIGHTER_IDS), [url for original in PROFILE_URLS for url in (original, original.replace("http://", "http://www."))]))
        aggregate = _json_rows(cursor, """SELECT to_jsonb(s) FROM public.fight_stats_aggregate s
            WHERE s.fight_id = %s ORDER BY s.fight_stat_id LIMIT 3""", (FIGHT_ID,))
        by_round = _json_rows(cursor, """SELECT to_jsonb(s) FROM public.fight_stats_by_round s
            WHERE s.fight_id = %s ORDER BY s.fight_stat_by_round_id LIMIT 11""", (FIGHT_ID,))
    snapshot = {"database_identity_sha256": database_identity_sha256, "schema": schema, "tables": tables,
                "triggers": triggers, "rules": rules, "fights": fights, "events": events,
                "fighters": fighters, "statistics_aggregate": aggregate, "statistics_by_round": by_round}
    check_db_snapshot(snapshot)
    return snapshot


def check_db_snapshot(snapshot):
    identity_pin = snapshot["database_identity_sha256"]
    require(isinstance(identity_pin, str) and len(identity_pin) == 64
            and all(character in "0123456789abcdef" for character in identity_pin), "database_identity_shape_mismatch")
    require(tuple(column["name"] for column in snapshot["schema"]) == FIGHT_COLUMNS,
            "database_fight_schema_changed")
    expected_types = {key: "text" for key in FIGHT_COLUMNS}
    expected_types.update({key: "uuid" for key in FIGHT_COLUMNS if key.endswith("_id")})
    expected_types.update({key: "boolean" for key in ("is_title_fight", "is_interim_title")})
    expected_types.update({key: "smallint" for key in ("scheduled_rounds", "finish_round", "finish_time_seconds")})
    expected_types["scraped_at"] = "timestamp with time zone"
    require(all(column["type"] == expected_types[column["name"]] and column["generated"] == "NEVER"
                for column in snapshot["schema"]), "database_fight_column_type_or_generation_changed")
    require({table["name"] for table in snapshot["tables"]}
            == {"fights", "events", "fighters", "fight_stats_aggregate", "fight_stats_by_round"}
            and all(table["kind"] == "r" and not table["rls"] and not table["force_rls"] for table in snapshot["tables"]),
            "database_table_kind_or_rls_changed")
    require(not snapshot["rules"] and all(trigger["internal"] for trigger in snapshot["triggers"]),
            "database_custom_rules_or_triggers_present")
    require(len(snapshot["fights"]) == 1 and len(snapshot["events"]) == 1
            and len(snapshot["fighters"]) == 2, "missing_duplicate_or_conflicting_database_identity")
    row = snapshot["fights"][0]
    require(set(row) == set(FIGHT_COLUMNS), "database_full_fight_row_shape_changed")
    require((row["fight_id"], row["event_id"], row["fighter_1_id"], row["fighter_2_id"])
            == (FIGHT_ID, EVENT_ID, *FIGHTER_IDS) and normalized_url(row["source_url"]) == FIGHT_URL,
            "database_fight_identity_or_orientation_changed")
    event = snapshot["events"][0]
    require(event["event_id"] == EVENT_ID and normalized_url(event["source_url"]) == EVENT_URL
            and event["event_date"] == "2026-07-18"
            and event["event_name"] == "UFC Fight Night: Du Plessis vs. Usman", "database_parent_identity_changed")
    profiles = {row["fighter_id"]: row for row in snapshot["fighters"]}
    require(set(profiles) == set(FIGHTER_IDS)
            and all(normalized_url(profiles[fid]["source_url"]) == url and profiles[fid]["full_name"] == name
                    for fid, url, name in zip(FIGHTER_IDS, PROFILE_URLS, ("Jared Cannonier", "Christian Leroy Duncan"))),
            "database_profile_identity_changed")
    require(len(snapshot["statistics_aggregate"]) < 3 and len(snapshot["statistics_by_round"]) < 11,
            "database_statistics_bound_exceeded")


def db_disposition(row):
    projected = {key: row[key] for key in DB_CHANGES}
    if projected == DB_CHANGES:
        return "VERIFIED_ALREADY_CORRECT"
    unresolved = {key: None for key in DB_CHANGES}
    unresolved["result_type"] = "upcoming"
    require(projected == unresolved, "database_result_conflicting_or_already_resolved")
    return "READY_FOR_SIX_COLUMN_CORRECTION"


def corrected_snapshot(before):
    check_db_snapshot(before)
    db_disposition(before["fights"][0])
    return {**before, "fights": [{**before["fights"][0], **DB_CHANGES}]}


def read_db_independently(connection_factory):
    connection = connection_factory()
    try:
        connection.set_session(readonly=True, autocommit=False, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute("SET LOCAL statement_timeout = '15s'")
        snapshot = read_db_snapshot(connection)
        connection.rollback()
        return snapshot
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _cas_update(cursor, before_row):
    require(set(before_row) == set(FIGHT_COLUMNS), "database_cas_full_before_row_required")
    assignments = ", ".join('"' + key + '" = %s' for key in DB_CHANGES)
    conditions = " AND ".join('f."' + key + '" IS NOT DISTINCT FROM %s' for key in FIGHT_COLUMNS)
    cursor.execute("UPDATE public.fights AS f SET " + assignments + " WHERE " + conditions + " RETURNING to_jsonb(f)",
                   tuple(DB_CHANGES.values()) + tuple(before_row[key] for key in FIGHT_COLUMNS))
    rows = cursor.fetchall()
    require(cursor.rowcount == 1 and len(rows) == 1 and len(rows[0]) == 1,
            "database_cas_rowcount_or_returning_mismatch")
    require(rows[0][0] == {**before_row, **DB_CHANGES}, "database_returning_non_allowlisted_change")
    return rows[0][0]


def apply_db_case(connection_factory, expected_before, csv_path, expected_csv_sha, journal=None):
    """One quick DB transaction; independent reread resolves every commit result.

    The operator must already have durably frozen complete before-state and
    verified the applied CSV patch. This function never edits/compensates files.
    Journal receives explicit state dictionaries; it must durably write them.
    """
    check_db_snapshot(expected_before)
    disposition = db_disposition(expected_before["fights"][0])
    expected_after = corrected_snapshot(expected_before)
    require_file_hash(csv_path, expected_csv_sha)
    events = []

    def record(state, **facts):
        event = {"state": state, "audit_utc": utc_now(), **facts}
        events.append(event)
        if journal is not None:
            journal(event)

    record("GUARDED_TRANSACTION_START", csv_sha256=expected_csv_sha)
    connection = connection_factory()
    attempted_commit, commit_error, close_error, wrote = False, None, None, False
    try:
        connection.set_session(readonly=False, autocommit=False, isolation_level="SERIALIZABLE")
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute("SET LOCAL statement_timeout = '15s'")
        before = read_db_snapshot(connection, lock_fight=True)
        require(before == expected_before, "database_expected_before_state_drift")
        require_file_hash(csv_path, expected_csv_sha)
        if disposition != "VERIFIED_ALREADY_CORRECT":
            with connection.cursor() as cursor:
                _cas_update(cursor, before["fights"][0])
            wrote = True
            require(read_db_snapshot(connection) == expected_after, "database_transaction_after_state_mismatch")
            require_file_hash(csv_path, expected_csv_sha)
            record("DB_SIX_COLUMN_UPDATE_VERIFIED_UNCOMMITTED", changed_columns=sorted(DB_CHANGES))
            # Journal failure before this point aborts safely. No automatic rollback
            # is attempted once commit has been invoked and its outcome is ambiguous.
            record("COMMIT_ATTEMPT")
            attempted_commit = True
            try:
                connection.commit()
            except Exception as exc:
                commit_error = type(exc).__name__
        else:
            connection.rollback()
    except Exception:
        if not attempted_commit:
            connection.rollback()
        raise
    finally:
        try:
            connection.close()
        except Exception as exc:
            if not attempted_commit:
                raise
            close_error = type(exc).__name__

    # A failure here is inconclusive, even if commit() returned normally.
    try:
        reread = read_db_independently(connection_factory)
    except Exception as exc:
        status = "COMMIT_OUTCOME_UNRESOLVED" if wrote else "ALREADY_CORRECT_REREAD_UNRESOLVED"
        record(status, reread_error_type=type(exc).__name__, commit_error_type=commit_error, close_error_type=close_error)
        return {"status": status, "events": events, "independent_after": None, "csv_sha256": expected_csv_sha}
    if reread == expected_after:
        status = ("VERIFIED_ALREADY_CORRECT" if not wrote else
                  "VERIFIED_COMMIT_BY_REREAD" if commit_error or close_error else "VERIFIED_CORRECTION_COMMITTED")
    elif wrote and reread == expected_before:
        status = "VERIFIED_NOT_COMMITTED"
    else:
        status = "COMMIT_OUTCOME_UNRESOLVED"
    require_file_hash(csv_path, expected_csv_sha)
    record(status, commit_error_type=commit_error, close_error_type=close_error,
           independent_snapshot_sha256=sha256(json_bytes(reread)))
    return {"status": status, "events": events, "independent_after": reread,
            "csv_sha256": expected_csv_sha, "commit_attempts": int(attempted_commit),
            "changed_columns": sorted(DB_CHANGES) if wrote else [], **BOUNDARIES}
