"""Execute Phase 3B A–D offline, then leave a validated isolated staging run.

Publication follows a separate preservation check and bundle-loader invocation.
An exception leaves INCOMPLETE.json and a receipt; no existing run is replaced.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import itertools
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
import warnings

import joblib
import numpy as np
import pandas as pd
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from modeling.holdout import assert_no_holdout_fights, load_holdout_fight_ids
from modeling.refit_preflight import (
    ALGORITHM_VERSION, DEBUT_COLS, FEATURE_ORDER, FEATURE_VERSION, ROOT,
    SOURCE_MODE, PreflightError, json_bytes, load_prepared_snapshot, sha256,
    split_window, validate_oof_inputs, validate_refit_config,
)
from modeling.xgb_candidate_contract import (
    CALIBRATION_CONTRACT, CONFIG_SHA256, EVENT_CUTOFF, FOLDS_SHA256,
    LABEL_MAPPING, MANIFEST_SHA256, ORIENTATION, OUTPUT_ROOT, SNAPSHOT,
    SOURCE_COMMIT, TRAINING_SHA256, calibrate, digest, feature_provenance,
    fit_partition_priors, guard_partition, log_odds, membership, metrics,
    package_versions, predict_best_iteration, preprocess_partition, select_development,
)

TRAINER_CODE = ["modeling/train_xgb_candidate.py", "modeling/xgb_candidate_contract.py",
                "modeling/xgb_candidate_bundle.py", "tools/verify_candidate_preservation.py",
                "modeling/decisions.py"]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json_bytes(value))


def write_checksums(directory: Path) -> None:
    files = {str(p.relative_to(directory)): sha256(p.read_bytes())
             for p in sorted(directory.rglob('*')) if p.is_file() and p.name != "checksums.json"}
    write_json(directory / "checksums.json", {"schema_version": 1,
               "self_excluded": "checksums.json", "files": files})


def preflight(config_path: Path, *, snapshot: Path, manifest_sha256: str, source_mode: str):
    if snapshot.resolve() != SNAPSHOT.resolve() or manifest_sha256 != MANIFEST_SHA256 or source_mode != SOURCE_MODE:
        raise PreflightError("Only the pinned accepted metadata-safe source is authorized")
    config_raw = config_path.read_bytes()
    if sha256(config_raw) != CONFIG_SHA256:
        raise PreflightError("Configuration differs from accepted preparation provenance")
    config = tomllib.loads(config_raw.decode())
    validate_refit_config(config)
    frame, manifest, folds = load_prepared_snapshot(snapshot,
        expected_manifest_sha256=manifest_sha256, source_mode=source_mode)
    discrepancies = []
    if manifest["config_sha256"] != sha256(config_raw):
        discrepancies.append("preparation/configuration hash differs")
    if (manifest["source"]["commit"] != SOURCE_COMMIT or config["source_ref"] != SOURCE_COMMIT
            or manifest["files"]["training.csv"]["sha256"] != TRAINING_SHA256
            or manifest["files"]["folds.json"]["sha256"] != FOLDS_SHA256):
        discrepancies.append("pinned source/data/folds differ")
    versions = package_versions()
    for package, version in manifest["package_versions"].items():
        if versions.get(package) != version:
            discrepancies.append(f"package version differs: {package}")
    current_code = {name: sha256((ROOT / name).read_bytes()) for name in manifest["code_sha256"]}
    for name, expected in manifest["code_sha256"].items():
        if current_code[name] != expected:
            discrepancies.append(f"preparation code differs: {name}")
    feature_metadata = json.loads((ROOT / config["feature_order_source"]).read_bytes())
    if feature_metadata["feature_cols"] != FEATURE_ORDER or feature_metadata["feature_version"] != FEATURE_VERSION:
        discrepancies.append("reference feature order/version differs")
    summary = manifest["summary"]
    actual_summary = {"eligible_rows": len(frame), "event_count": int(frame.event_id.nunique()),
        "date_count": int(frame.event_date.nunique()),
        "min_event_date": frame.event_date.min().date().isoformat(),
        "max_event_date": frame.event_date.max().date().isoformat(),
        "label_counts": {str(k): int(v) for k, v in frame.label.value_counts().sort_index().items()},
        "missingness": {c: int(frame[c].isna().sum()) for c in FEATURE_ORDER}}
    if any(summary[k] != v for k, v in actual_summary.items()) or len(frame) != 8400:
        discrepancies.append("snapshot count/date/label/missingness summary differs")
    if not frame[DEBUT_COLS].isna().all().all() or not frame.scheduled_rounds.isna().all():
        discrepancies.append("preparation prior/schedule contract differs")
    exclusions = load_holdout_fight_ids()
    if len(exclusions) != 166:
        discrepancies.append("holdout exclusion count differs")
    expected_counts = [(6414, 502), (6916, 519), (7435, 506),
                       (7941, 129), (8070, 126), (8196, 128), (8324, 76)]
    if [(f["train_rows"], f["prediction_rows"]) for f in folds["folds"]] != expected_counts:
        discrepancies.append("exact fold counts differ")
    source_checks = {}
    # Verify source schema/count, Git blobs and archived label/orientation. No
    # today's source CSV, warehouse query, or frozen outcome file is opened.
    for name, entry in manifest["source"]["files"].items():
        source_file = snapshot / "sources" / (name + ".csv")
        raw = source_file.read_bytes()
        with source_file.open(newline="") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            rows = list(reader)
        blob = subprocess.check_output(["git", "rev-parse", f"{SOURCE_COMMIT}:{entry['git_path']}"],
                                       cwd=ROOT, text=True).strip()
        git_raw = subprocess.check_output(["git", "show", f"{SOURCE_COMMIT}:{entry['git_path']}"], cwd=ROOT)
        if (not fields or len(fields) != len(set(fields)) or len(rows) != entry["rows"]
                or any(None in row or None in row.values() for row in rows)
                or sha256(raw) != entry["sha256"] or raw != git_raw or blob != entry["blob"]):
            discrepancies.append(f"source schema/count/blob/hash differs: {name}")
        source_checks[name] = {"rows": len(rows), "columns": fields, "blob": blob, "sha256": sha256(raw)}
        if name == "fights":
            archived = {r["fight_id"]: r for r in rows}
            for row in frame.itertuples(index=False):
                source = archived.get(row.fight_id, {})
                if (source.get("event_id") != row.event_id
                        or source.get("fighter_1_id") != row.fighter_1_id
                        or source.get("fighter_2_id") != row.fighter_2_id
                        or (source.get("fighter_1_outcome"), source.get("fighter_2_outcome")) !=
                        (("W", "L") if row.label == 1 else ("L", "W"))):
                    discrepancies.append(f"archived label/orientation differs: {row.fight_id}")
                    break
    if discrepancies:
        raise PreflightError("Preflight discrepancies: " + "; ".join(discrepancies))
    report = {"status": "VALID", "discrepancies": discrepancies, "checked_at": now(),
              "preparation_code_sha256": current_code, "configuration_sha256": sha256(config_raw),
              "package_versions": versions, "source_checks": source_checks,
              "summary": actual_summary, "holdout_exclusion_count": len(exclusions),
              "holdout_exclusion_ids_sha256": digest(sorted(exclusions)),
              "reference_feature_metadata_sha256": sha256((ROOT / config["feature_order_source"]).read_bytes()),
              "exact_fold_memberships_validated": True,
              "new_lifecycle_code_sha256": {name: sha256((ROOT / name).read_bytes()) for name in TRAINER_CODE}}
    return config, frame, manifest, folds, report


def guarded_fit(whole, train, *, fold, parameters, rounds, validation=None):
    """The only XGBoost fit site: OOF/final cannot supply an eval set."""
    end = fold["train_end_exclusive"] if fold else EVENT_CUTOFF
    expected = fold["train_fight_ids"] if fold else sorted(whole.fight_id)
    guard_partition(whole, train, expected_ids=expected, end=end)
    if validation is not None:
        if not fold or fold["phase"] != "development":
            raise PreflightError("OOF/final fits cannot use early-stopping labels")
        guard_partition(whole, validation, expected_ids=fold["prediction_fight_ids"],
                        start=fold["prediction_start_inclusive"], end=fold["prediction_end_exclusive"])
        model = XGBClassifier(**parameters, n_estimators=rounds, early_stopping_rounds=50)
        model.fit(train[FEATURE_ORDER], train.label,
                  eval_set=[(validation[FEATURE_ORDER], validation.label)], verbose=False)
    else:
        if any(k in parameters for k in ("early_stopping_rounds", "callbacks", "n_estimators")):
            raise PreflightError("Frozen fits cannot carry early-stopping/tuning settings")
        model = XGBClassifier(**parameters, n_estimators=rounds)
        model.fit(train[FEATURE_ORDER], train.label, verbose=False)
        if model.get_booster().num_boosted_rounds() != rounds:
            raise PreflightError("Frozen round count differs from actual fitted rounds")
    return model


def validate_oof_provenance(oof, whole, folds, selection, priors) -> None:
    validate_oof_inputs(oof, whole, folds)
    required = {"event_id", "event_date", "settings_rounds_sha256", "training_ids_sha256", "prior_sha256"}
    if not required <= set(oof):
        raise PreflightError("Missing OOF provenance")
    indexed = whole.set_index("fight_id")
    fold_index = {f["name"]: f for f in folds["folds"] if f["phase"] == "calibration_oof"}
    for row in oof.itertuples(index=False):
        fold = fold_index[row.fold]
        if (row.settings_rounds_sha256 != selection["settings_rounds_sha256"]
                or row.training_ids_sha256 != fold["train_ids_sha256"]
                or row.prior_sha256 != digest(priors[row.fold])
                or row.event_id != indexed.at[row.fight_id, "event_id"]
                or pd.Timestamp(row.event_date) != indexed.at[row.fight_id, "event_date"]):
            raise PreflightError("OOF settings/training/prior/event provenance differs")
        prior = priors[row.fold]
        if prior["training_ids_sha256"] != fold["train_ids_sha256"] or prior["training_rows"] != fold["train_rows"]:
            raise PreflightError("OOF prior fitting lineage differs")
    if len(oof) != folds["oof_rows"]:
        raise PreflightError("OOF coverage count differs")


def fit_calibrator(oof, whole, folds, selection, priors):
    # Both membership/provenance and exclusion checks happen immediately before
    # transforming calibration inputs, and again immediately before fitting.
    validate_oof_provenance(oof, whole, folds, selection, priors)
    x = log_odds(oof.raw_prob_f1)
    labels = oof.label.to_numpy(dtype=int)
    input_sha = digest({"fight_ids": oof.fight_id.tolist(), "labels": labels.tolist(),
                        "raw_prob_f1": oof.raw_prob_f1.tolist(), "log_odds": x[:, 0].tolist()})
    model = LogisticRegression(C=1e10, solver="lbfgs", max_iter=1000)
    validate_oof_provenance(oof, whole, folds, selection, priors)
    assert_no_holdout_fights(oof)
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        model.fit(x, labels)
    warning_details = [{"category": type(w.message).__name__, "message": str(w.message)} for w in captured]
    converged = not any(issubclass(w.category, ConvergenceWarning) for w in captured)
    details = {"coefficient": float(model.coef_[0, 0]), "intercept": float(model.intercept_[0]),
               "iterations": int(model.n_iter_[0]), "converged": converged, "warnings": warning_details,
               "calibration_input_sha256": input_sha, "rows": len(oof),
               "label_counts": {str(k): int(v) for k, v in oof.label.value_counts().sort_index().items()},
               "raw_oof_metrics": metrics(labels, oof.raw_prob_f1),
               "calibration_fit_diagnostics": metrics(labels, calibrate(model, oof.raw_prob_f1)),
               "diagnostic_classification": "calibration-fit diagnostics; not independent validation"}
    return model, details


def _fold_inputs(whole, fold, directory):
    train, valid = split_window(whole, fold["prediction_start_inclusive"], fold["prediction_end_exclusive"])
    record = fit_partition_priors(whole, train, expected_ids=fold["train_fight_ids"],
                                 end=fold["train_end_exclusive"], name=fold["name"])
    write_json(directory / "fold_priors" / (fold["name"] + ".json"), record)
    train = preprocess_partition(whole, train, expected_ids=fold["train_fight_ids"],
                                 prior_record=record, end=fold["train_end_exclusive"])
    valid = preprocess_partition(whole, valid, expected_ids=fold["prediction_fight_ids"],
                                 prior_record=record, start=fold["prediction_start_inclusive"],
                                 end=fold["prediction_end_exclusive"])
    return train, valid, record


def build(args) -> Path:
    # Exclusive mkdir happens before preflight so concrete failures are retained.
    run_name = args.run_name or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    if Path(run_name).name != run_name or run_name.startswith('.'):
        raise PreflightError("Run name must be one ordinary directory name")
    destination = OUTPUT_ROOT / run_name
    staging = OUTPUT_ROOT / (".incomplete-" + run_name)
    if destination.exists():
        raise PreflightError("Run already exists; refusing overwrite")
    staging.mkdir(parents=True, exist_ok=False)
    receipt = {"schema_version": 1, "status": "INCOMPLETE", "started_at": now(),
               "authorized_source_mode": args.source_mode,
               "authorization_kind": "explicit user request for this Phase 3B run",
               "authorization_recorded_at": now(),
               "authorization_request_sha256": sha256(args.authorization_request.read_bytes()),
               "authorization_scope": "isolated development/OOF/calibration/final fit and bundle verification",
               "prohibited_scope": "108/146 forecast scoring, promotion, production/live changes",
               "accepted_limitations": ["chronological retrospective reconstruction; not certified replay",
                   "global pre-cutoff source versions; per-bout availability unverified",
                   "scheduled rounds unknown; no result/finish/title inference",
                   "exactly 8400 archived binary bouts through 2026-03-07; no warehouse supplementation",
                   "frozen forecast outcomes previously inspected; not blind/unseen tests"],
               "preparation_approval_flags_preserved": True, "completed_stages": [],
               "command": [sys.executable, "-m", "modeling.train_xgb_candidate", *sys.argv[1:]],
               "published_destination": str(destination.relative_to(ROOT))}
    write_json(staging / "run_receipt.json", receipt)
    write_json(staging / "INCOMPLETE.json", {"status": "INCOMPLETE", "completed_stages": []})
    try:
        shutil.copyfile(args.authorization_request, staging / "user_authorization.txt")
        shutil.copyfile(args.preservation_baseline, staging / "preservation_baseline.json")
        config, whole, source_manifest, folds, checks = preflight(args.config, snapshot=args.snapshot,
            manifest_sha256=args.manifest_sha256, source_mode=args.source_mode)
        write_json(staging / "preflight.json", checks)
        shutil.copyfile(args.config, staging / "effective_configuration.toml")
        shutil.copyfile(args.snapshot / "manifest.json", staging / "source_manifest.json")
        shutil.copyfile(args.snapshot / "folds.json", staging / "folds.json")
        write_json(staging / "package_versions.json", package_versions())

        def completed(stage):
            receipt["completed_stages"].append(stage)
            write_json(staging / "run_receipt.json", receipt)
            write_json(staging / "INCOMPLETE.json", {"status": "INCOMPLETE",
                       "completed_stages": receipt["completed_stages"]})

        print(f"Preflight VALID: {len(whole)} rows, exact source/code/config/package/fold provenance", flush=True)
        development = [f for f in folds["folds"] if f["phase"] == "development"]
        prepared = {f["name"]: _fold_inputs(whole, f, staging) for f in development}
        grid = config["hyperparameter_grid"]
        base = config["base_parameters"]
        results = []
        for depth, child, lam in itertools.product(grid["max_depth"], grid["min_child_weight"], grid["reg_lambda"]):
            parameters = {"max_depth": depth, "min_child_weight": child, "reg_lambda": lam}
            for fold in development:
                train, valid, prior = prepared[fold["name"]]
                model = guarded_fit(whole, train, fold=fold, parameters={**base, **parameters},
                    rounds=config["development"]["maximum_boosting_rounds"], validation=valid)
                p = predict_best_iteration(model, valid)
                results.append({"fold": fold["name"], "grid_parameters": parameters,
                    "effective_parameters": {**base, **parameters,
                        "n_estimators": config["development"]["maximum_boosting_rounds"],
                        "early_stopping_rounds": 50, "missing": "NaN (native)"}, "training_rows": len(train),
                    "prediction_rows": len(valid), "train_ids_sha256": membership(train),
                    "prediction_ids_sha256": membership(valid), "prior_sha256": digest(prior),
                    "prior_path": "fold_priors/" + fold["name"] + ".json",
                    "best_iteration": int(model.best_iteration), "best_score": float(model.best_score),
                    "fitted_rounds": model.get_booster().num_boosted_rounds(),
                    "prediction_iteration_range": [0, int(model.best_iteration) + 1],
                    "metrics": metrics(valid.label, p)})
                write_json(staging / "development_results.json", results)
                print(f"A {len(results):02d}/81 {parameters} {fold['name']} "
                      f"rounds={model.best_iteration + 1} logloss={results[-1]['metrics']['log_loss']:.9f}", flush=True)
        if len(results) != 81:
            raise PreflightError("Development must contain all 81 fits")
        selection = select_development(results)
        selection["base_parameters"] = base
        settings = {"parameters": {**base, **selection["selected_parameters"]},
                    "rounds": selection["selected_rounds"]}
        selection.update({"frozen_settings": settings, "settings_rounds_sha256": digest(settings),
                          "frozen_at": now(), "development_results_sha256": digest(results),
                          "development_fit_count": 81, "oof_used_for_selection": False})
        write_json(staging / "selection.json", selection)
        (staging / "selection.json").chmod(0o444)
        selection_file_sha = sha256((staging / "selection.json").read_bytes())
        completed("A")
        print(f"A SELECTED {selection['selected_parameters']} rounds={settings['rounds']}", flush=True)

        pieces, oof_priors, oof_details = [], {}, []
        for fold in (f for f in folds["folds"] if f["phase"] == "calibration_oof"):
            if sha256((staging / "selection.json").read_bytes()) != selection_file_sha:
                raise PreflightError("Frozen selection changed during OOF")
            train, valid, prior = _fold_inputs(whole, fold, staging)
            model = guarded_fit(whole, train, fold=fold, parameters=settings["parameters"], rounds=settings["rounds"])
            raw = model.predict_proba(valid[FEATURE_ORDER], iteration_range=(0, settings["rounds"]))[:, 1].astype(float)
            out = valid[["fight_id", "event_id", "event_date", "fighter_1_id", "fighter_2_id", "label"]].copy()
            out["raw_prob_f1"] = raw
            out["fold"] = fold["name"]
            out["settings_rounds_sha256"] = selection["settings_rounds_sha256"]
            out["training_ids_sha256"] = fold["train_ids_sha256"]
            out["prior_sha256"] = digest(prior)
            pieces.append(out)
            oof_priors[fold["name"]] = prior
            oof_details.append({"name": fold["name"], "train_rows": len(train), "prediction_rows": len(valid),
                "training_ids_sha256": membership(train), "prediction_ids_sha256": membership(valid),
                "prior_sha256": digest(prior), "settings_rounds_sha256": digest(settings),
                "boosting_rounds": model.get_booster().num_boosted_rounds(),
                "early_stopping": False, "eval_set": False, "raw_metrics": metrics(valid.label, raw)})
            print(f"B {fold['name']}: {len(train)}/{len(valid)}, fixed {settings['rounds']} rounds", flush=True)
        oof = pd.concat(pieces).sort_values(["event_date", "fight_id"]).reset_index(drop=True)
        validate_oof_provenance(oof, whole, folds, selection, oof_priors)
        if len(oof) != 459:
            raise PreflightError("Expected exactly 459 calibration rows")
        oof.to_csv(staging / "oof.csv", index=False, date_format="%Y-%m-%d", float_format="%.17g", lineterminator="\n")
        (staging / "oof.csv").chmod(0o444)
        oof_sha = sha256((staging / "oof.csv").read_bytes())
        # Calibrate the actual persisted, round-trip input, not a parallel copy.
        oof = pd.read_csv(staging / "oof.csv", float_precision="round_trip", parse_dates=["event_date"])
        validate_oof_provenance(oof, whole, folds, selection, oof_priors)
        oof_manifest = {"schema_version": 1, "frozen_at": now(), "rows": 459,
            "oof_csv_sha256": oof_sha, "settings_rounds_sha256": digest(settings),
            "selection_file_sha256": selection_file_sha, "folds": oof_details,
            "input_manifest_sha256": MANIFEST_SHA256,
            "orientation": ORIENTATION, "raw_metrics": metrics(oof.label, oof.raw_prob_f1),
            "eligible_coverage": "exactly once; 2025-03-31 <= event_date < 2026-03-31",
            "no_oof_early_stopping_or_tuning": True}
        write_json(staging / "oof_manifest.json", oof_manifest)
        (staging / "oof_manifest.json").chmod(0o444)
        completed("B")

        calibrator, calibration = fit_calibrator(oof, whole, folds, selection, oof_priors)
        if sha256((staging / "oof.csv").read_bytes()) != oof_sha:
            raise PreflightError("Frozen raw OOF artifact changed")
        joblib.dump(calibrator, staging / "calibrator.joblib")
        completed("C")
        print(f"C Platt persisted: converged={calibration['converged']} iterations={calibration['iterations']}", flush=True)

        final_prior = fit_partition_priors(whole, whole, expected_ids=sorted(whole.fight_id),
                                           end=EVENT_CUTOFF, name="final")
        write_json(staging / "final_debut_priors.json", final_prior)
        final_train = preprocess_partition(whole, whole, expected_ids=sorted(whole.fight_id), prior_record=final_prior)
        learner = guarded_fit(whole, final_train, fold=None, parameters=settings["parameters"], rounds=settings["rounds"])
        learner.save_model(staging / "base_learner.json")
        training_manifest = {"schema_version": 1, "training_rows": len(whole),
            "training_fight_ids": sorted(whole.fight_id), "training_ids_sha256": membership(whole),
            "actual_training_endpoint": "2026-03-07", "event_cutoff_exclusive": EVENT_CUTOFF,
            "training_csv_sha256": TRAINING_SHA256, "preparation_manifest_sha256": MANIFEST_SHA256,
            "source_mode": SOURCE_MODE, "feature_algorithm_version": ALGORITHM_VERSION,
            "no_early_stopping": True, "no_legacy_reservation": True,
            "selected_settings_sha256": digest(settings), "rounds": settings["rounds"],
            "final_prior_sha256": digest(final_prior)}
        write_json(staging / "training_manifest.json", training_manifest)
        metadata = {"schema_version": 1, "status": "PENDING_VERIFICATION", "feature_version": FEATURE_VERSION,
            "feature_algorithm_version": ALGORITHM_VERSION, "feature_order": FEATURE_ORDER,
            "input_feature_provenance": feature_provenance(), "source_mode": SOURCE_MODE,
            "source_commit": SOURCE_COMMIT, "event_cutoff_exclusive": EVENT_CUTOFF,
            "knowledge_cutoff": config["knowledge_cutoff"], "actual_training_endpoint": "2026-03-07",
            "training_rows": 8400, "label_mapping": LABEL_MAPPING, "orientation": ORIENTATION,
            "preparation_manifest_sha256": MANIFEST_SHA256, "training_csv_sha256": TRAINING_SHA256,
            "folds_sha256": FOLDS_SHA256, "configuration_sha256": CONFIG_SHA256,
            "base_parameters": settings["parameters"], "selected_settings_sha256": digest(settings),
            "selected_rounds": settings["rounds"], "final_prior_sha256": digest(final_prior),
            "calibration_contract": CALIBRATION_CONTRACT, "calibration": calibration,
            "decision_policy_version": config["decision_policy_version"],
            "new_lifecycle_code_sha256": checks["new_lifecycle_code_sha256"],
            "limitations": receipt["accepted_limitations"], "promotion_authorized": False}
        write_json(staging / "metadata.json", metadata)
        completed("D")
        write_json(staging / "verification.json", {"status": "PENDING"})
        write_json(staging / "preservation_check.json", {"status": "PENDING_EXTERNAL_CHECK"})
        write_checksums(staging)

        from modeling.xgb_candidate_bundle import CandidateBundle
        loaded = CandidateBundle(staging, _allow_pending_verification=True)
        guard_partition(whole, whole, expected_ids=sorted(whole.fight_id))
        before_raw = learner.predict_proba(final_train[FEATURE_ORDER], iteration_range=(0, settings["rounds"]))[:, 1].astype(float)
        before_calibrated = calibrate(calibrator, before_raw)
        after = loaded.predict(whole, provenance=feature_provenance())
        if not np.array_equal(before_raw, after.raw_prob_f1.to_numpy()) or not np.array_equal(
                before_calibrated, after.calibrated_prob_f1.to_numpy()):
            raise PreflightError("Serialized candidate probabilities differ from in-memory pipeline")
        verification = {"status": "VERIFIED", "rows": len(whole), "input": "eligible non-holdout snapshot rows",
            "raw_max_absolute_difference": float(np.max(np.abs(before_raw - after.raw_prob_f1))),
            "calibrated_max_absolute_difference": float(np.max(np.abs(before_calibrated - after.calibrated_prob_f1))),
            "raw_probability_digest": digest(before_raw.tolist()),
            "calibrated_probability_digest": digest(before_calibrated.tolist()),
            "exact_probability_parity": True, "checked_at": now(),
            "meaning": "serialization/inference verification, not final-model predictive performance"}
        write_json(staging / "verification.json", verification)
        receipt.update({"status": "VALIDATED_PENDING_PRESERVATION_AND_PUBLICATION", "fits_finished_at": now()})
        write_json(staging / "run_receipt.json", receipt)
        write_checksums(staging)
        print(f"A–D and exact 8400-row save/load parity VERIFIED. Staging: {staging}", flush=True)
        return staging
    except Exception as exc:
        receipt.update({"status": "FAILED_INCOMPLETE", "failed_at": now(),
                        "failure": {"type": type(exc).__name__, "message": str(exc)}})
        write_json(staging / "run_receipt.json", receipt)
        write_json(staging / "INCOMPLETE.json", {"status": "FAILED_INCOMPLETE",
                   "completed_stages": receipt["completed_stages"], "failure": receipt["failure"]})
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-mode", required=True, choices=[SOURCE_MODE])
    parser.add_argument("--manifest-sha256", required=True, choices=[MANIFEST_SHA256])
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/xgb_refit_pre_april_2026.toml")
    parser.add_argument("--authorization-request", type=Path, required=True)
    parser.add_argument("--preservation-baseline", type=Path, required=True)
    parser.add_argument("--run-name")
    args = parser.parse_args()
    build(args)


if __name__ == "__main__":
    main()
