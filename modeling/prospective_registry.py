"""Append-only metadata registry. No scoring, fitting or outcome ingestion."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import json
import os
from pathlib import Path

from modeling.refit_preflight import PreflightError, json_bytes, sha256

IDENTITY = ("fight_id", "event_id", "fighter_1_id", "fighter_2_id")
RECIPES = {"current_challenger": "phase5_current_candidate_v1",
           "frozen_reference": "production_derived_reference_v1",
           "march_research_control": "phase3b_retrospective_v1"}
FORBIDDEN = {"probability", "p", "raw_probability", "calibrated_probability",
             "calibrated_prob_f1", "predicted_prob_f1", "label", "winner_fighter_id",
             "actual_label", "correct", "outcome", "result_type"}


def instant(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise PreflightError("Observation times must have a timezone")
    return result.astimezone(timezone.utc)


def no_predictions_or_outcomes(value) -> None:
    if isinstance(value, dict):
        if FORBIDDEN & value.keys():
            raise PreflightError("Preparation registry cannot accept predictions/outcomes")
        for child in value.values():
            no_predictions_or_outcomes(child)
    elif isinstance(value, list):
        for child in value:
            no_predictions_or_outcomes(child)


def lead_time_reasons(event_date: str, captured_at: str) -> list[str]:
    boundary = datetime.combine(date.fromisoformat(event_date), time(), timezone.utc)
    lead = boundary - instant(captured_at)
    if lead > timedelta(days=14):
        return ["outside_14_day_window"]
    if lead < timedelta(hours=24):
        return ["less_than_24_hours_before_utc_event_boundary"]
    return []


def eligibility(bout: dict, *, observation_cutoff: str) -> list[str]:
    no_predictions_or_outcomes(bout)
    reasons = []
    if any(not isinstance(bout.get(k), str) or not bout[k].strip() for k in IDENTITY):
        reasons.append("unresolved_identity")
    if bout.get("fighter_1_id") == bout.get("fighter_2_id"):
        reasons.append("invalid_orientation")
    try:
        reasons.extend(lead_time_reasons(bout["announced_event_date"], observation_cutoff))
    except (ValueError, KeyError, TypeError):
        reasons.append("unknown_announced_event_date")
    if type(bout.get("is_title_fight")) is not bool or not bout.get("title_evidence"):
        reasons.append("unknown_title_status")
    if not bout.get("weight_class"):
        reasons.append("unknown_weight_class")
    for side in (1, 2):
        if bout.get(f"fighter_{side}_profile_present") is not True:
            reasons.append(f"missing_fighter_{side}_profile")
        if bout.get(f"fighter_{side}_experience_status") not in {"verified_history", "verified_debut"}:
            reasons.append(f"unresolved_fighter_{side}_experience")
        elif not bout.get(f"fighter_{side}_experience_evidence"):
            reasons.append(f"missing_fighter_{side}_experience_evidence")
    return sorted(set(reasons))


def registration_records(bouts: list[dict], *, captured_at: str, source_sha256: str,
                         registered_at: str | None = None) -> list[dict]:
    """Never filter consideration by readiness; include provisional identities."""
    records = []
    keys = set()
    for index, bout in enumerate(bouts):
        no_predictions_or_outcomes(bout)
        key = bout.get("fight_id") or f"provisional:{source_sha256}:{index}"
        if key in keys:
            raise PreflightError("Duplicate considered bout identity")
        keys.add(key)
        reasons = eligibility(bout, observation_cutoff=captured_at)
        records.append({"schema_version": 1, "experiment_id": "prospective_shadow_v1",
            "kind": "registration", "registry_key": key, "bout": bout,
            "registered_at": registered_at or captured_at, "observation_cutoff": captured_at,
            "source_sha256": source_sha256, "recipes": RECIPES,
            "eligibility_reasons": reasons,
            "status": "data_blocked_not_scored" if reasons else "eligible_awaiting_frozen_models",
            "real_predictions_recorded": False})
    return records


def validate_pair_capture(registration: dict, capture: dict, earlier: list[dict]) -> dict:
    """Metadata-only selection of first complete pair; models remain future work."""
    no_predictions_or_outcomes(capture)
    bout = registration["bout"]
    if capture.get("registry_key") != registration["registry_key"]:
        raise PreflightError("Capture is not registered")
    if capture.get("identity") != {k: bout.get(k) for k in IDENTITY}:
        raise PreflightError("Capture orientation/identity changed; append revision first")
    if capture.get("announced_event_date") != bout.get("announced_event_date"):
        raise PreflightError("Announced date changed; append revision first")
    cutoff = capture["observation_cutoff"]
    forecast_at = capture["forecast_at"]
    if instant(registration["registered_at"]) > instant(forecast_at):
        raise PreflightError("Register bout before forecasts")
    if instant(forecast_at) < instant(cutoff):
        raise PreflightError("Forecast precedes completed input capture")
    reasons = eligibility(bout, observation_cutoff=cutoff)
    reasons.extend(lead_time_reasons(bout["announced_event_date"], forecast_at))
    components = capture.get("pipelines", {})
    for role in ("current_challenger", "frozen_reference"):
        pipeline = components.get(role)
        if not pipeline:
            reasons.append("missing_counterpart_inputs")
            continue
        if pipeline.get("observation_cutoff") != cutoff:
            reasons.append("unmatched_observation_cutoffs")
        try:
            if instant(pipeline["input_capture_end"]) > instant(cutoff):
                reasons.append("input_captured_after_observation_cutoff")
            if instant(pipeline["components_frozen_at"]) > instant(cutoff):
                reasons.append("components_not_frozen_before_capture")
        except (ValueError, KeyError, TypeError):
            reasons.append("missing_component_freeze_or_capture_time")
        for field in ("input_sha256", "component_sha256", "recipe_version"):
            if not pipeline.get(field):
                reasons.append("missing_pipeline_provenance")
        if pipeline.get("complete") is not True:
            reasons.append("incomplete_pipeline_inputs")
    for previous in earlier:
        if previous["registry_key"] == registration["registry_key"]:
            if instant(previous["observation_cutoff"]) >= instant(cutoff):
                raise PreflightError("Captures must append in observation order")
            if previous.get("primary_capture"):
                reasons.append("primary_already_frozen_supplemental_only")
    reasons = sorted(set(reasons))
    return {**capture, "kind": "capture", "blocking_reasons": reasons,
            "primary_capture": not reasons, "real_predictions_recorded": False}


def append_record(directory: Path, record: dict) -> Path:
    """An exclusive immutable record per sequence, with hash-linked predecessors.

    O_EXCL resolves concurrent append races by failing; callers retry only after
    revalidating against the now-current chain. Never overwrite or auto-rebase.
    """
    no_predictions_or_outcomes(record)
    directory.mkdir(parents=True, exist_ok=True)
    records = read_records(directory)
    kind = record.get("kind")
    key = record.get("registry_key")
    related = [r for r in records if r["record"].get("registry_key") == key]
    if kind == "registration":
        if related:
            raise PreflightError("Bout is already registered; append revision")
        if not isinstance(record.get("bout"), dict) or not record.get("source_sha256"):
            raise PreflightError("Registration requires complete considered metadata/source receipt")
        instant(record["registered_at"])
        instant(record["observation_cutoff"])
        reasons = eligibility(record["bout"], observation_cutoff=record["observation_cutoff"])
        record = {**record, "eligibility_reasons": reasons,
                  "status": "data_blocked_not_scored" if reasons else "eligible_awaiting_frozen_models"}
    elif kind in {"revision", "cancellation", "capture", "identity_resolution"}:
        if not related:
            raise PreflightError("Register every considered bout before later records")
        if kind in {"revision", "identity_resolution"} and not record.get("changes"):
            raise PreflightError("Revision needs explicit changes")
        if kind == "cancellation" and not record.get("reason"):
            raise PreflightError("Cancellation needs explicit reason")
        if kind in {"revision", "cancellation", "identity_resolution"}:
            if not record.get("recorded_at") or not record.get("source_sha256"):
                raise PreflightError("Revision/cancellation needs observation time and source hash")
            instant(record["recorded_at"])
        if kind == "capture":
            current = effective_registration([r["record"] for r in records], key)
            record = validate_pair_capture(current, record,
                [r["record"] for r in related if r["record"]["kind"] == "capture"])
    else:
        raise PreflightError("Unsupported preparation record kind")
    envelope = {"sequence": len(records) + 1,
                "previous_sha256": records[-1]["sha256"] if records else None,
                "record": record}
    raw = json_bytes(envelope)
    path = directory / f"{envelope['sequence']:08d}.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return path


def effective_registration(records: list[dict], key: str) -> dict:
    """Replay immutable metadata revisions; never mutate the original record."""
    selected = [r for r in records if r.get("registry_key") == key]
    if not selected or selected[0]["kind"] != "registration":
        raise PreflightError("Unregistered bout")
    current = {**selected[0], "bout": dict(selected[0]["bout"])}
    for record in selected[1:]:
        if record["kind"] == "cancellation":
            raise PreflightError("Cancelled bout cannot receive a primary capture")
        if record["kind"] in {"revision", "identity_resolution"}:
            current["bout"].update(record["changes"])
    return current


def read_records(directory: Path) -> list[dict]:
    records = []
    paths = sorted(directory.glob("*.json"))
    for number, path in enumerate(paths, 1):
        if path.name != f"{number:08d}.json":
            raise PreflightError("Registry sequence gap")
        raw = path.read_bytes()
        envelope = json.loads(raw)
        if envelope["sequence"] != number or envelope["previous_sha256"] != (
                records[-1]["sha256"] if records else None):
            raise PreflightError("Registry checksum chain mismatch")
        no_predictions_or_outcomes(envelope)
        records.append({**envelope, "sha256": sha256(raw)})
    return records


def verify_coverage(directory: Path, considered_keys: list[str]) -> None:
    registered = [r["record"]["registry_key"] for r in read_records(directory)
                  if r["record"]["kind"] == "registration"]
    if sorted(registered) != sorted(considered_keys) or len(set(registered)) != len(registered):
        raise PreflightError("Registry must cover every considered bout exactly once")
