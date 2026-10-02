"""Offline, checksum-checked candidate inference and bundle verification CLI.

Joblib components must come from the locally built, checksum-verified bundle;
the checksum manifest provides integrity, not authenticity for untrusted files.
No warehouse data, legacy artifact, or preprocessing fit is used here.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from features.debut_prior import apply_debut_features
from modeling.decisions import DECISION_POLICY_VERSION, attach_decisions
from modeling.holdout import assert_no_holdout_fights
from modeling.refit_preflight import (
    ALGORITHM_VERSION, DEBUT_COLS, FEATURE_ORDER, FEATURE_VERSION, PreflightError,
    load_prepared_snapshot, sha256,
)
from modeling.xgb_candidate_contract import (
    CALIBRATION_CONTRACT, CONFIG_SHA256, EVENT_CUTOFF, FOLDS_SHA256,
    LABEL_MAPPING, MANIFEST_SHA256, ORIENTATION, REQUIRED_COMPONENTS, SNAPSHOT,
    SOURCE_COMMIT, SOURCE_MODE, TRAINING_SHA256, calibrate, check_priors, digest,
    feature_provenance, package_versions, probabilities,
)


def _read(directory: Path, name: str):
    try:
        return json.loads((directory / name).read_bytes())
    except (OSError, ValueError) as exc:
        raise PreflightError(f"Missing/invalid bundle component: {name}") from exc


def verify_checksums(directory: Path) -> dict:
    checksums = _read(directory, "checksums.json")
    if checksums.get("schema_version") != 1 or checksums.get("self_excluded") != "checksums.json":
        raise PreflightError("Invalid checksum manifest contract")
    files = checksums.get("files", {})
    if not isinstance(files, dict) or not REQUIRED_COMPONENTS - {"checksums.json"} <= files.keys():
        raise PreflightError("Required components are absent from checksum manifest")
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*') if p.is_file()}
    if actual != set(files) | {"checksums.json"}:
        raise PreflightError("Bundle has missing or unlisted components")
    for name, expected in files.items():
        path = directory / name
        if (Path(name).is_absolute() or ".." in Path(name).parts or path.is_symlink()
                or not path.resolve().is_relative_to(directory.resolve())):
            raise PreflightError("Bundle component path escapes directory")
        if not path.is_file() or sha256(path.read_bytes()) != expected:
            raise PreflightError(f"Bundle checksum mismatch: {name}")
    return checksums


def check_calibrator(model, contract: dict) -> None:
    if (contract != CALIBRATION_CONTRACT or not isinstance(model, LogisticRegression)
            or list(model.classes_) != [0, 1] or model.n_features_in_ != 1):
        raise PreflightError("Incompatible calibrator clipping/input/class contract")
    for key in ("C", "solver", "max_iter"):
        if model.get_params()[key] != contract[key]:
            raise PreflightError("Incompatible calibrator estimator settings")
    if model.coef_.shape != (1, 1) or model.intercept_.shape != (1,) or not np.isfinite(
            np.r_[model.coef_.ravel(), model.intercept_]).all():
        raise PreflightError("Invalid fitted calibrator coefficients")


class CandidateBundle:
    def __init__(self, directory: str | Path, *, _allow_pending_verification: bool = False):
        self.directory = Path(directory).resolve()
        self.checksums = verify_checksums(self.directory)
        self.metadata = _read(self.directory, "metadata.json")
        m = self.metadata
        expected = {"schema_version": 1, "feature_version": FEATURE_VERSION,
                    "feature_algorithm_version": ALGORITHM_VERSION,
                    "feature_order": FEATURE_ORDER, "label_mapping": LABEL_MAPPING,
                    "orientation": ORIENTATION, "decision_policy_version": DECISION_POLICY_VERSION,
                    "source_mode": SOURCE_MODE, "event_cutoff_exclusive": EVENT_CUTOFF,
                    "knowledge_cutoff": "2026-03-31T12:09:04.077607+00:00",
                    "preparation_manifest_sha256": MANIFEST_SHA256,
                    "training_csv_sha256": TRAINING_SHA256, "folds_sha256": FOLDS_SHA256,
                    "configuration_sha256": CONFIG_SHA256, "source_commit": SOURCE_COMMIT,
                    "training_rows": 8400, "actual_training_endpoint": "2026-03-07",
                    "input_feature_provenance": feature_provenance()}
        if any(m.get(k) != v for k, v in expected.items()):
            raise PreflightError("Incompatible candidate metadata/feature/algorithm contract")
        if m.get("status") != "COMPLETED" and not (
                _allow_pending_verification and m.get("status") == "PENDING_VERIFICATION"):
            raise PreflightError("Candidate bundle is incomplete")
        if not _allow_pending_verification and (self.directory / "INCOMPLETE.json").exists():
            raise PreflightError("Candidate bundle is incomplete")
        saved_packages = _read(self.directory, "package_versions.json")
        current_packages = package_versions()
        if any(saved_packages.get(p) != current_packages[p] for p in
               ("numpy", "pandas", "scikit-learn", "xgboost", "joblib")):
            raise PreflightError("Incompatible inference package versions")
        selection = _read(self.directory, "selection.json")
        self.rounds = selection["selected_rounds"]
        if (not isinstance(self.rounds, int) or not 1 <= self.rounds <= 500
                or m.get("selected_settings_sha256") != digest(selection["frozen_settings"])
                or selection["frozen_settings"] != {
                    "parameters": m.get("base_parameters"), "rounds": self.rounds}):
            raise PreflightError("Selected learner settings contract differs")
        prior_record = _read(self.directory, "final_debut_priors.json")
        training = _read(self.directory, "training_manifest.json")
        if (prior_record.get("training_rows") != 8400
                or prior_record.get("training_ids_sha256") != training.get("training_ids_sha256")
                or prior_record.get("training_end_exclusive") != EVENT_CUTOFF
                or prior_record.get("actual_training_endpoint") != "2026-03-07"
                or m.get("final_prior_sha256") != self.checksums["files"]["final_debut_priors.json"]):
            raise PreflightError("Final prior training lineage differs")
        self.priors = prior_record["priors"]
        check_priors(self.priors)
        self.learner = XGBClassifier()
        self.learner.load_model(self.directory / "base_learner.json")
        booster = self.learner.get_booster()
        learner_config = json.loads(booster.save_config())["learner"]
        if (list(self.learner.classes_) != [0, 1] or booster.feature_names != FEATURE_ORDER
                or booster.num_boosted_rounds() != self.rounds
                or learner_config["objective"]["name"] != "binary:logistic"
                or self.learner.get_params().get("early_stopping_rounds") is not None):
            raise PreflightError("Incompatible base estimator class/features/rounds/objective")
        self.calibrator = joblib.load(self.directory / "calibrator.joblib")
        check_calibrator(self.calibrator, m.get("calibration_contract"))
        details = m["calibration"]
        if (details["coefficient"] != float(self.calibrator.coef_[0, 0])
                or details["intercept"] != float(self.calibrator.intercept_[0])
                or details["iterations"] != int(self.calibrator.n_iter_[0])):
            raise PreflightError("Fitted calibrator differs from recorded coefficients")
        if sha256((self.directory / "folds.json").read_bytes()) != FOLDS_SHA256:
            raise PreflightError("Candidate fold membership file differs from pinned preparation")
        source = _read(self.directory, "source_manifest.json")
        if (source.get("source", {}).get("commit") != SOURCE_COMMIT
                or source.get("feature_order") != FEATURE_ORDER
                or source.get("files", {}).get("training.csv", {}).get("sha256") != TRAINING_SHA256
                or sha256((self.directory / "source_manifest.json").read_bytes()) != MANIFEST_SHA256):
            raise PreflightError("Candidate source manifest differs from accepted snapshot")
        if sha256((self.directory / "effective_configuration.toml").read_bytes()) != CONFIG_SHA256:
            raise PreflightError("Candidate configuration differs from accepted preparation")
        folds = _read(self.directory, "folds.json")
        training_ids = training.get("training_fight_ids", [])
        if (len(training_ids) != 8400 or len(set(training_ids)) != 8400
                or digest(sorted(training_ids)) != folds["final_training_ids_sha256"]
                or training.get("training_ids_sha256") != folds["final_training_ids_sha256"]
                or training.get("training_rows") != 8400 or training.get("rounds") != self.rounds
                or not training.get("no_early_stopping") or not training.get("no_legacy_reservation")):
            raise PreflightError("Final training membership/round contract differs")
        oof = pd.read_csv(self.directory / "oof.csv", float_precision="round_trip", parse_dates=["event_date"])
        oof_manifest = _read(self.directory, "oof_manifest.json")
        expected_oof = {fid: f for f in folds["folds"] if f["phase"] == "calibration_oof"
                        for fid in f["prediction_fight_ids"]}
        if (len(oof) != 459 or oof.fight_id.duplicated().any() or set(oof.fight_id) != set(expected_oof)
                or set(oof.label) != {0, 1} or oof_manifest.get("rows") != 459
                or oof_manifest.get("oof_csv_sha256") != self.checksums["files"]["oof.csv"]
                or oof_manifest.get("settings_rounds_sha256") != m["selected_settings_sha256"]):
            raise PreflightError("Persisted OOF coverage/provenance differs")
        probabilities(oof.raw_prob_f1)
        for row in oof.itertuples(index=False):
            fold = expected_oof[row.fight_id]
            prior_name = "fold_priors/" + fold["name"] + ".json"
            if (row.fold != fold["name"] or row.training_ids_sha256 != fold["train_ids_sha256"]
                    or row.settings_rounds_sha256 != m["selected_settings_sha256"]
                    or row.prior_sha256 != self.checksums["files"].get(prior_name)
                    or not pd.Timestamp(fold["prediction_start_inclusive"]) <= row.event_date <
                    pd.Timestamp(fold["prediction_end_exclusive"])):
                raise PreflightError("Persisted OOF fold/settings/prior provenance differs")
        from modeling.xgb_candidate_contract import log_odds
        calibration_input_sha = digest({"fight_ids": oof.fight_id.tolist(), "labels": oof.label.tolist(),
            "raw_prob_f1": oof.raw_prob_f1.tolist(), "log_odds": log_odds(oof.raw_prob_f1)[:, 0].tolist()})
        if calibration_input_sha != details["calibration_input_sha256"]:
            raise PreflightError("Persisted calibration input differs")

    def predict(self, frame: pd.DataFrame, *, provenance: dict) -> pd.DataFrame:
        if provenance != self.metadata["input_feature_provenance"]:
            raise PreflightError("Explicit compatible feature provenance is required")
        required = {"fight_id", "fighter_1_id", "fighter_2_id", "event_date", "weight_class",
                    "feature_version", *FEATURE_ORDER}
        if frame.empty or not required <= set(frame.columns) or frame.columns.duplicated().any():
            raise PreflightError("Missing required inference features/identities")
        if [c for c in frame.columns if c in FEATURE_ORDER] != FEATURE_ORDER:
            raise PreflightError("Inference feature order differs")
        if frame.fight_id.isna().any() or frame.fight_id.duplicated().any():
            raise PreflightError("Missing/duplicate inference identity")
        for col in ("fighter_1_id", "fighter_2_id", "fight_id"):
            if frame[col].isna().any() or frame[col].map(lambda v: not str(v).strip()).any():
                raise PreflightError("Missing inference orientation identity")
        if frame.fighter_1_id.eq(frame.fighter_2_id).any():
            raise PreflightError("Invalid inference fighter orientation")
        if not frame.feature_version.eq(FEATURE_VERSION).all():
            raise PreflightError("Incompatible input feature version")
        if pd.to_datetime(frame.event_date, errors="raise").isna().any():
            raise PreflightError("Invalid inference event date")
        values = frame[FEATURE_ORDER].to_numpy(dtype=float)
        if np.isinf(values).any() or not frame.scheduled_rounds.isna().all():
            raise PreflightError("Incompatible schedule/numeric input features")
        if not frame[DEBUT_COLS].isna().all().all() or not frame.both_debuting.isin([0, 1]).all():
            raise PreflightError("Expected deferred debut slots and binary debut indicator")
        processed = apply_debut_features(frame.copy(), self.priors)
        raw = probabilities(self.learner.predict_proba(
            processed[FEATURE_ORDER], iteration_range=(0, self.rounds))[:, 1])
        calibrated = calibrate(self.calibrator, raw)
        identifiers = [c for c in ("fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id",
                                   "fighter_1_name", "fighter_2_name") if c in frame]
        result = frame[identifiers].copy()
        result["raw_prob_f1"] = raw
        result["raw_prob_f2"] = 1 - raw
        result["calibrated_prob_f1"] = calibrated
        result["calibrated_prob_f2"] = 1 - calibrated
        return attach_decisions(result, origin="recorded_at_scoring")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT)
    parser.add_argument("--manifest-sha256", required=True, choices=[MANIFEST_SHA256])
    parser.add_argument("--source-mode", required=True, choices=[SOURCE_MODE])
    parser.add_argument("--publish", action="store_true", help="Publish a validated staging run after preservation")
    args = parser.parse_args()
    if args.snapshot.resolve() != SNAPSHOT.resolve():
        parser.error("Verification must use the accepted metadata-safe snapshot")
    frame, _, _ = load_prepared_snapshot(args.snapshot, expected_manifest_sha256=args.manifest_sha256,
                                          source_mode=args.source_mode)
    assert_no_holdout_fights(frame)
    if args.publish:
        publish_candidate(args.bundle)
        return
    bundle = CandidateBundle(args.bundle)
    predictions = bundle.predict(frame, provenance=feature_provenance())
    print(json.dumps({"status": "VERIFIED", "rows": len(predictions),
                      "raw_probability_digest": digest(predictions.raw_prob_f1.tolist()),
                      "calibrated_probability_digest": digest(predictions.calibrated_prob_f1.tolist()),
                      "meaning": "serialization/inference verification; no predictive performance claim"},
                     indent=2))


def publish_candidate(directory: Path) -> Path:
    """Validate staging, independently reload predictions, then atomically rename."""
    from datetime import datetime, timezone
    import os
    from modeling.xgb_candidate_contract import OUTPUT_ROOT
    from modeling.train_xgb_candidate import write_checksums, write_json

    directory = directory.resolve()
    if directory.parent != OUTPUT_ROOT or not directory.name.startswith(".incomplete-"):
        raise PreflightError("Publication requires an isolated incomplete run")
    preservation = _read(directory, "preservation_check.json")
    verification = _read(directory, "verification.json")
    receipt = _read(directory, "run_receipt.json")
    if (preservation.get("status") != "VERIFIED"
            or preservation.get("baseline_sha256") != sha256((directory / "preservation_baseline.json").read_bytes())
            or preservation.get("changed") or preservation.get("missing") or preservation.get("new_protected_files")
            or verification.get("status") != "VERIFIED" or not verification.get("exact_probability_parity")
            or receipt.get("completed_stages") != ["A", "B", "C", "D"]):
        raise PreflightError("Candidate is not validated for publication")
    destination = directory.parent / directory.name.removeprefix(".incomplete-")
    if destination.exists():
        raise PreflightError("Run already exists; refusing overwrite")
    # The separate preservation command changed its placeholder; verify all
    # other original checksums before incorporating that one new receipt.
    old = _read(directory, "checksums.json")
    for name, expected in old["files"].items():
        if name != "preservation_check.json" and sha256((directory / name).read_bytes()) != expected:
            raise PreflightError(f"Staging component changed: {name}")
    write_checksums(directory)
    bundle = CandidateBundle(directory, _allow_pending_verification=True)
    whole, _, _ = load_prepared_snapshot(SNAPSHOT, expected_manifest_sha256=MANIFEST_SHA256, source_mode=SOURCE_MODE)
    assert_no_holdout_fights(whole)
    predictions = bundle.predict(whole, provenance=feature_provenance())
    if (digest(predictions.raw_prob_f1.tolist()) != verification["raw_probability_digest"]
            or digest(predictions.calibrated_prob_f1.tolist()) != verification["calibrated_probability_digest"]):
        raise PreflightError("Independent load no longer matches recorded in-memory parity")
    metadata = _read(directory, "metadata.json")
    metadata["status"] = "COMPLETED"
    metadata["published_at"] = datetime.now(timezone.utc).isoformat()
    receipt["status"] = "COMPLETED"
    receipt["published_at"] = metadata["published_at"]
    write_json(directory / "metadata.json", metadata)
    write_json(directory / "run_receipt.json", receipt)
    (directory / "INCOMPLETE.json").unlink()
    write_checksums(directory)
    CandidateBundle(directory)
    # mkdir is exclusive even against a concurrent publisher; os.rename may
    # replace only our empty reserved destination, never an existing run.
    destination.mkdir(exist_ok=False)
    os.rename(directory, destination)
    for path in destination.rglob('*'):
        if path.is_file():
            path.chmod(0o444)
    print(json.dumps({"status": "COMPLETED", "bundle": str(destination),
                      "rows_verified": len(predictions), "checksums_sha256": sha256(
                          (destination / "checksums.json").read_bytes())}, indent=2))
    return destination


if __name__ == "__main__":
    main()
