"""Exercise accepted contracts on writable temporary copies of frozen evidence."""

import csv
import hashlib
import json
from pathlib import Path
import subprocess
import shutil
import sys
from uuid import UUID

import pandas as pd
import pytest

from modeling.holdout import (
    COHORT_NAME, PRE_EVENT_COHORT, HoldoutError, assert_no_holdout_fights, load_holdout_fight_ids,
)
from tools.validate_prospective_holdout import (
    ROOT, EXPECTED_METRICS, PRE_EVENT_METRICS, _probability, strongest_predictions, validate_holdout,
)


def _write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _save(directory, predictions, outcomes, manifest):
    _write_csv(directory / "predictions.csv", predictions)
    _write_csv(directory / "outcomes.csv", outcomes)
    for role in ("predictions", "outcomes"):
        manifest["files"][role]["sha256"] = hashlib.sha256((directory / f"{role}.csv").read_bytes()).hexdigest()
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.fixture
def frozen_fixture(tmp_path):
    for name in (COHORT_NAME, PRE_EVENT_COHORT):
        shutil.copytree(ROOT / "data/holdouts" / name, tmp_path / name)
        for path in (tmp_path / name).iterdir():
            path.chmod(0o644)
    directory = tmp_path / COHORT_NAME
    predictions = list(csv.DictReader((directory / "predictions.csv").open()))
    outcomes = list(csv.DictReader((directory / "outcomes.csv").open()))
    manifest = json.loads((directory / "manifest.json").read_text())
    return directory, predictions, outcomes, manifest


def test_offline_validation_is_deterministic_and_read_only(frozen_fixture):
    directory, *_ = frozen_fixture
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    assert validate_holdout(directory) == validate_holdout(directory)
    result = validate_holdout(directory)
    assert result["metrics"]["high_confidence"]["correct"] == 30
    assert result["metrics"]["strongest_57"]["correct"] == 39
    assert before == {p.name: p.read_bytes() for p in directory.iterdir()}


def test_guard_never_reads_outcomes(frozen_fixture):
    directory, predictions, *_ = frozen_fixture
    (directory / "outcomes.csv").unlink()
    assert len(load_holdout_fight_ids(directory)) == 166
    df = pd.DataFrame({"fight_id": ["unseen-training-fight"]})
    before = df.copy(deep=True)
    assert_no_holdout_fights(df, directory)
    pd.testing.assert_frame_equal(df, before)
    with pytest.raises(HoldoutError, match="holdout fight IDs"):
        assert_no_holdout_fights(pd.DataFrame({"fight_id": [UUID(predictions[0]["fight_id"])]}), directory)


@pytest.mark.parametrize("data", [{}, {"fight_id": [None]}, {"fight_id": [" "]}])
def test_guard_requires_valid_training_ids(frozen_fixture, data):
    with pytest.raises(HoldoutError, match="Training DataFrame"):
        assert_no_holdout_fights(pd.DataFrame(data), frozen_fixture[0])


def test_guard_checks_empty_dataframe_and_padded_overlap(frozen_fixture):
    directory, predictions, *_ = frozen_fixture
    assert_no_holdout_fights(pd.DataFrame({"fight_id": []}), directory)
    with pytest.raises(HoldoutError, match="holdout fight IDs"):
        assert_no_holdout_fights(pd.DataFrame({"fight_id": [" " + predictions[0]["fight_id"] + " "]}), directory)


def test_missing_holdout_fails_closed(tmp_path):
    with pytest.raises(HoldoutError, match="Cannot read frozen manifest"):
        assert_no_holdout_fights(pd.DataFrame({"fight_id": []}), tmp_path)


@pytest.mark.parametrize("status", ["BLOCKED", "EVIDENCE_ONLY", "CANDIDATE"])
def test_unaccepted_manifest_fails_closed(frozen_fixture, status):
    directory, predictions, outcomes, manifest = frozen_fixture
    manifest["status"] = status
    _save(directory, predictions, outcomes, manifest)
    with pytest.raises(HoldoutError, match="status"):
        load_holdout_fight_ids(directory)


@pytest.mark.parametrize("role", ["predictions", "outcomes"])
def test_hash_tampering(frozen_fixture, role):
    directory, *_ = frozen_fixture
    with (directory / f"{role}.csv").open("a") as stream:
        stream.write("\n")
    with pytest.raises(HoldoutError, match="SHA-256 mismatch"):
        validate_holdout(directory)


@pytest.mark.parametrize("mutation, expected", [
    ("prediction_duplicate", "Duplicate prediction fight IDs"),
    ("outcome_duplicate", "Duplicate outcome fight IDs"),
    ("missing_prediction", "exactly 146"),
    ("missing_outcome", "one-to-one"),
    ("different_outcome_id", "one-to-one"),
    ("missing_field", "Missing required fields"),
    ("blank_field", "Missing scored_at"),
    ("probability_high", "Probability outside"),
    ("probability_low", "Probability outside"),
    ("probability_nan", "Probability outside"),
    ("raw_probability_inf", "Probability outside"),
    ("wrong_correctness", "latent correctness"),
    ("unresolved", "Unresolved fight"),
    ("wrong_actual_label", "Invalid actual label"),
    ("outcome_in_predictions", "outcome columns"),
    ("changed_high_band", "high_confidence invariant failed"),
    ("changed_uncertain_band", "uncertain invariant failed"),
    ("changed_correct_total", "total invariant failed"),
    ("changed_uncertain_correct", "uncertain invariant failed"),
    ("changed_high_correct", "high_confidence invariant failed"),
    ("manifest_metrics", "metrics differ"),
    ("unsafe_cutoff", "Training cutoff"),
    ("undocumented_exception", "prospectivity audit differs"),
    ("wrong_model", "Unexpected artifact"),
])
def test_validation_rejects_rehashed_invalid_data(frozen_fixture, mutation, expected):
    directory, predictions, outcomes, manifest = frozen_fixture
    if mutation == "prediction_duplicate": predictions[-1]["fight_id"] = predictions[0]["fight_id"]
    elif mutation == "outcome_duplicate": outcomes[-1]["fight_id"] = outcomes[0]["fight_id"]
    elif mutation == "missing_prediction": predictions.pop()
    elif mutation == "missing_outcome": outcomes.pop()
    elif mutation == "different_outcome_id": outcomes[-1]["fight_id"] = "different-fight"
    elif mutation == "missing_field":
        for row in predictions: del row["event_id"]
    elif mutation == "blank_field": predictions[0]["scored_at"] = ""
    elif mutation.startswith("probability_"):
        predictions[0]["calibrated_prob_f1"] = {"probability_high": "1.001", "probability_low": "-0.001", "probability_nan": "NaN"}[mutation]
    elif mutation == "raw_probability_inf": predictions[0]["predicted_prob_f1"] = "Infinity"
    elif mutation == "wrong_correctness": outcomes[0]["correct"] = str(outcomes[0]["correct"] == "False")
    elif mutation == "unresolved": outcomes[0]["resolved"] = "False"
    elif mutation == "wrong_actual_label": outcomes[0]["actual_label"] = "2"
    elif mutation == "outcome_in_predictions":
        for row in predictions: row["actual_label"] = "1"
    elif mutation == "changed_high_band":
        next(r for r in predictions if _probability(r["calibrated_prob_f1"]) >= _probability(".7"))["calibrated_prob_f1"] = "0.65"
    elif mutation == "changed_uncertain_band":
        next(r for r in predictions if _probability(".5") <= _probability(r["calibrated_prob_f1"]) <= _probability(".6"))["calibrated_prob_f1"] = "0.65"
    elif mutation in {"changed_correct_total", "changed_uncertain_correct", "changed_high_correct"}:
        if mutation == "changed_correct_total":
            indices = [0]
        else:
            def group(index):
                p = _probability(predictions[index]["calibrated_prob_f1"])
                return _probability(".4") <= p <= _probability(".6") if mutation == "changed_uncertain_correct" else p <= _probability(".3") or p >= _probability(".7")
            indices = [next(i for i,r in enumerate(outcomes) if group(i) and r["correct"] == "True"),
                       next(i for i,r in enumerate(outcomes) if not group(i) and r["correct"] == "False")]
        for index in indices:
            outcomes[index]["actual_label"] = str(1 - int(float(outcomes[index]["actual_label"])))
            outcomes[index]["correct"] = str(outcomes[index]["correct"] == "False")
    elif mutation == "manifest_metrics": manifest["metrics"]["total"]["correct"] = 80
    elif mutation == "unsafe_cutoff": manifest["proposed_training_cutoff"]["event_date_exclusive_upper_bound"] = "2026-04-01"
    elif mutation == "undocumented_exception": predictions[0]["scored_at"] = "2026-04-04T00:00:00+00:00"
    elif mutation == "wrong_model": predictions[0]["model_artifact"] = "different-model"
    _save(directory, predictions, outcomes, manifest)
    with pytest.raises(HoldoutError, match=expected):
        validate_holdout(directory)


def test_inclusive_boundaries_and_latent_tie():
    from tools.audit_holdout_recovery import summarize
    rows = [{"fight_id": str(i), "event_date": "2026-04-04", "resolved": True,
             "correct": True, "calibrated_prob_f1": p}
            for i,p in enumerate(("0.30", "0.40", "0.50", "0.60", "0.70"))]
    metrics = summarize(rows)["metrics"]
    assert metrics["uncertain"]["count"] == 3
    assert metrics["high_confidence"]["count"] == 2
    assert _probability("0.50") >= _probability("0.5")


def test_file_path_cannot_escape_holdout(frozen_fixture):
    directory, predictions, outcomes, manifest = frozen_fixture
    _save(directory, predictions, outcomes, manifest)
    manifest["files"]["predictions"]["path"] = "../predictions.csv"
    (directory / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(HoldoutError, match="inside the holdout"):
        validate_holdout(directory)


def test_cli_missing_holdout_is_deterministic(tmp_path):
    command = [sys.executable, "tools/validate_prospective_holdout.py", "--holdout-dir", str(tmp_path)]
    root = Path(__file__).resolve().parents[2]
    first = subprocess.run(command, cwd=root, capture_output=True, text=True)
    second = subprocess.run(command, cwd=root, capture_output=True, text=True)
    assert first.returncode == second.returncode == 1
    assert first.stdout == second.stdout == ""
    assert first.stderr == second.stderr
    assert "HOLDOUT VALIDATION FAILED" in first.stderr


@pytest.mark.parametrize("name, expected", [(COHORT_NAME, EXPECTED_METRICS), (PRE_EVENT_COHORT, PRE_EVENT_METRICS)])
def test_both_explicit_population_contracts(frozen_fixture, name, expected):
    directory = frozen_fixture[0].parent / name
    result = validate_holdout(directory)
    assert {k: (v["count"], v["correct"]) for k,v in result["metrics"].items()} == expected
    assert result["parent_verified"] == (name == PRE_EVENT_COHORT)
    assert result["identity_audit"]["clean_evaluation_certified"] == (name == PRE_EVENT_COHORT)


@pytest.mark.parametrize("name", [COHORT_NAME, PRE_EVENT_COHORT])
def test_training_reads_only_manifest_predictions_and_label_free_exclusions(frozen_fixture, monkeypatch, name):
    directory = frozen_fixture[0].parent / name
    identity = json.loads((directory / "identity-exclusions.json").read_bytes())
    original = next(r for r in identity["identities"] if r["alternate_bout"]
                    and r["alternate_bout"]["status"] == "verified_same_bout")
    allowed = {directory / filename for filename in ("manifest.json", "predictions.csv", "identity-exclusions.json")}
    opened = set()
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        assert path in allowed, f"Training attempted to read outcomes or joined evidence: {path}"
        opened.add(path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    assert len(load_holdout_fight_ids(directory)) == 166
    for fid in (original["fight_id"], original["alternate_bout"]["fight_id"]):
        with pytest.raises(HoldoutError, match="holdout fight IDs"):
            assert_no_holdout_fights(pd.DataFrame({"fight_id": [fid]}), directory)
    assert opened == allowed


def test_strongest_ranking_tie_break_and_no_outcome_dependency():
    rows = [{"fight_id": fid, "calibrated_prob_f1": p, "correct": correct}
            for fid,p,correct in (("z", ".70", True), ("a", ".30", False), ("b", ".90", False))]
    assert [r["fight_id"] for r in strongest_predictions(rows, 2)] == ["b", "a"]
    for row in rows:
        row["correct"] = not row["correct"]
        row["actual_label"] = "invalid-outcome-is-not-a-ranking-input"
    assert [r["fight_id"] for r in strongest_predictions(rows, 2)] == ["b", "a"]


@pytest.mark.parametrize("field", ["calibrated_prob_f1", "scored_at", "actual_label"])
def test_original_precision_timestamps_and_outcome_strings_are_preserved(frozen_fixture, field):
    directory, predictions, outcomes, manifest = frozen_fixture
    if field == "calibrated_prob_f1": predictions[0][field] += "0"
    elif field == "scored_at": predictions[0][field] = predictions[0][field].replace(" ", "T")
    else: outcomes[0][field] = str(int(float(outcomes[0][field])))
    _save(directory, predictions, outcomes, manifest)
    with pytest.raises(HoldoutError, match="Original snapshot field changed"):
        validate_holdout(directory)


@pytest.mark.parametrize("mutation", ["outcome", "missing_original", "status", "orientation", "exclusion_ids", "alias_date"])
def test_rehashed_identity_exclusion_inconsistencies_fail_closed(frozen_fixture, mutation):
    directory, predictions, outcomes, manifest = frozen_fixture
    path = directory / "identity-exclusions.json"
    identity = json.loads(path.read_bytes())
    verified = next(r["alternate_bout"] for r in identity["identities"]
                    if r["alternate_bout"] and r["alternate_bout"]["status"] == "verified_same_bout")
    if mutation == "outcome": identity["identities"][0]["actual_label"] = "1"
    elif mutation == "missing_original": identity["identities"].pop()
    elif mutation == "status": verified["status"] = "unresolved_identity"
    elif mutation == "orientation": verified["orientation_reversed"] = True
    elif mutation == "exclusion_ids": identity["exclusion_ids"].pop()
    elif mutation == "alias_date": verified["event_date"] = "2026-08-31"
    path.write_text(json.dumps(identity))
    manifest["files"]["identity_exclusions"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    _save(directory, predictions, outcomes, manifest)
    with pytest.raises(HoldoutError):
        load_holdout_fight_ids(directory)


@pytest.mark.parametrize("role", ["predictions", "identity_exclusions"])
def test_training_file_roles_cannot_be_redirected_to_outcomes(frozen_fixture, role):
    directory, predictions, outcomes, manifest = frozen_fixture
    manifest["files"][role] = dict(manifest["files"]["outcomes"])
    _save(directory, predictions, outcomes, manifest)
    with pytest.raises(HoldoutError, match="path must be"):
        load_holdout_fight_ids(directory)


def test_pre_event_parent_hash_and_uncertainty_are_checked(frozen_fixture):
    directory = frozen_fixture[0].parent / PRE_EVENT_COHORT
    path = directory / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["identity_audit"]["unresolved_fight_ids"] = ["uncertain-test-bout"]
    path.write_text(json.dumps(manifest))
    with pytest.raises(HoldoutError, match="identity audit differs"):
        validate_holdout(directory)
    manifest["identity_audit"]["unresolved_fight_ids"] = []
    manifest["parent"]["manifest_sha256"] = "0" * 64
    path.write_text(json.dumps(manifest))
    with pytest.raises(HoldoutError, match="SHA-256 mismatch"):
        validate_holdout(directory)


def test_pre_event_subset_cannot_drop_or_admit_catchup_rows(frozen_fixture):
    directory = frozen_fixture[0].parent / PRE_EVENT_COHORT
    predictions = list(csv.DictReader((directory / "predictions.csv").open()))
    outcomes = list(csv.DictReader((directory / "outcomes.csv").open()))
    manifest = json.loads((directory / "manifest.json").read_bytes())
    predictions.pop()
    _save(directory, predictions, outcomes, manifest)
    with pytest.raises(HoldoutError, match="exactly 108"):
        validate_holdout(directory)


def test_symlink_escape_fails_closed(frozen_fixture, tmp_path):
    directory, *_ = frozen_fixture
    outside = directory.parent / "outside-predictions.csv"
    outside.write_bytes((directory / "predictions.csv").read_bytes())
    (directory / "predictions.csv").unlink()
    (directory / "predictions.csv").symlink_to(outside)
    with pytest.raises(HoldoutError, match="symlink escapes"):
        load_holdout_fight_ids(directory)


def test_publication_is_exclusive_and_rerun_refuses_existing_artifacts(tmp_path, monkeypatch):
    from tools import freeze_phase1_holdouts as publisher
    path = tmp_path / "frozen.csv"
    publisher.exclusive_write(path, b"original\n")
    with pytest.raises(FileExistsError):
        publisher.exclusive_write(path, b"replacement\n")
    assert path.read_bytes() == b"original\n"
    assert path.stat().st_mode & 0o222 == 0
    monkeypatch.setattr(publisher, "capture_adopted_warehouse_evidence", lambda: pytest.fail("rerun queried warehouse"))
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        publisher.main()
