"""Outcome-independent winner decisions on unrounded float64 probabilities.

Latent p >= .5 labels remain available for statistics. Public picks use the
nullable fields below; legacy decisions are derived without changing sources.
"""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss

DECISION_POLICY_VERSION = "probability_band_v1"
NO_PICK_LOW, NO_PICK_HIGH = 0.40, 0.60
HIGH_CONFIDENCE_LOW, HIGH_CONFIDENCE_HIGH = 0.30, 0.70
DECISION_FIELDS = (
    "decision_status", "is_actionable", "pick_label", "pick_winner_name",
    "uncertainty_reasons", "decision_policy_version",
)
DECISION_METRIC_FIELDS = (
    "total_count", "resolved_count", "latent_correct_count", "latent_accuracy",
    "actionable_count", "actionable_resolved_count", "actionable_correct_count",
    "actionable_accuracy", "actionable_coverage", "no_pick_count", "no_pick_share",
    "threshold_high_count", "threshold_high_resolved_count",
    "threshold_high_correct_count", "threshold_high_accuracy",
)


def validate_probability(value) -> float:
    try:
        probability = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Probability must be finite and within [0, 1]") from exc
    if not math.isfinite(probability) or not 0 <= probability <= 1:
        raise ValueError("Probability must be finite and within [0, 1]")
    return probability


def validated_probabilities(values) -> np.ndarray:
    return np.asarray([validate_probability(value) for value in values], dtype=float)


def decide_prediction(probability, fighter_1_name=None, fighter_2_name=None) -> dict:
    p = validate_probability(probability)
    no_pick = NO_PICK_LOW <= p <= NO_PICK_HIGH
    label = None if no_pick else int(p > NO_PICK_HIGH)
    name = None if no_pick else (fighter_1_name if label == 1 else fighter_2_name)
    return {
        "decision_status": "no_pick" if no_pick else "pick",
        "is_actionable": not no_pick,
        "pick_label": label,
        "pick_winner_name": None if name is None or pd.isna(name) else str(name),
        "uncertainty_reasons": ["probability_band"] if no_pick else [],
        "decision_policy_version": DECISION_POLICY_VERSION,
    }


def attach_decisions(frame: pd.DataFrame, *, origin: str | None = None) -> pd.DataFrame:
    """Copy a scoring/report frame, retaining probabilities and latent columns.

    Existing policy provenance survives CSV reads; absent metadata is explicitly
    derived. No outcome is used to choose a decision.
    """
    result = frame.copy()
    decisions = []
    origins = []
    for _, row in result.iterrows():
        decisions.append(decide_prediction(
            row["calibrated_prob_f1"], row.get("fighter_1_name", row.get("fighter_1")),
            row.get("fighter_2_name", row.get("fighter_2")),
        ))
        version = row.get("decision_policy_version")
        previous = row.get("decision_origin") if pd.notna(version) and version == DECISION_POLICY_VERSION else None
        origins.append(origin or (previous if pd.notna(previous) else "derived_from_legacy_probability"))
    for field in DECISION_FIELDS:
        result[field] = pd.Series([d[field] for d in decisions], index=result.index,
                                  dtype="Int64" if field == "pick_label" else "object")
    result["is_actionable"] = result["is_actionable"].astype(bool)
    result["decision_origin"] = pd.Series(origins, index=result.index, dtype="object")
    if "actual_label" in result:
        resolved = pd.to_numeric(result["actual_label"], errors="coerce").isin([0, 1])
        if "resolved" in result:
            resolved &= result["resolved"].eq(True)
        result["pick_correct"] = pd.Series(pd.NA, index=result.index, dtype="boolean")
        mask = resolved & result["is_actionable"]
        result.loc[mask, "pick_correct"] = result.loc[mask, "pick_label"].eq(
            pd.to_numeric(result.loc[mask, "actual_label"])
        ).to_numpy()
    return result


def decision_metrics(frame: pd.DataFrame) -> dict:
    """Counts/coverage include pending rows; accuracy uses resolved rows only."""
    frame = attach_decisions(frame)
    p = validated_probabilities(frame.get("calibrated_prob_f1", []))
    labels = pd.to_numeric(frame.get("actual_label", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    resolved = labels.isin([0, 1]).to_numpy()
    if "resolved" in frame:
        resolved = resolved & frame["resolved"].eq(True).fillna(False).to_numpy(dtype=bool)
    actionable = frame["is_actionable"].to_numpy(dtype=bool)
    high = (p <= HIGH_CONFIDENCE_LOW) | (p >= HIGH_CONFIDENCE_HIGH)
    correct = (p >= 0.5) == labels.to_numpy(dtype=float, na_value=np.nan)
    total, n_resolved = len(frame), int(resolved.sum())
    result = {
        "total_count": total, "resolved_count": n_resolved,
        "latent_correct_count": int((correct & resolved).sum()),
        "latent_accuracy": float(correct[resolved].mean()) if n_resolved else None,
        "actionable_coverage": float(actionable.mean()) if total else None,
        "no_pick_count": int((~actionable).sum()),
        "no_pick_share": float((~actionable).mean()) if total else None,
    }
    for prefix, mask in (("actionable", actionable), ("threshold_high", high)):
        eligible = mask & resolved
        result.update({
            f"{prefix}_count": int(mask.sum()),
            f"{prefix}_resolved_count": int(eligible.sum()),
            f"{prefix}_correct_count": int((correct & eligible).sum()),
            f"{prefix}_accuracy": float(correct[eligible].mean()) if eligible.any() else None,
        })
    result["log_loss"] = float(log_loss(labels[resolved].astype(int), p[resolved], labels=[0, 1])) if n_resolved else None
    result["brier_score"] = float(brier_score_loss(labels[resolved].astype(int), p[resolved])) if n_resolved else None
    return result


def json_safe(value):
    """Convert pandas/numpy nullable scalars recursively; reject JSON NaN."""
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if value is None or pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def write_prediction_csv(frame: pd.DataFrame, path) -> None:
    """List fields use JSON arrays; nullable labels/names are empty CSV cells."""
    output = frame.copy()
    if "uncertainty_reasons" in output:
        output["uncertainty_reasons"] = output["uncertainty_reasons"].map(json.dumps)
    output.to_csv(path, index=False)
