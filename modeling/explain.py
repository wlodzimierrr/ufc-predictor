"""Feature importance and SHAP analysis for LightGBM models.

Provides gain-based importance plots, SHAP beeswarm summaries, and
per-fight SHAP explanations.

Usage:
    from modeling.explain import feature_importance_plot, shap_summary, shap_single_fight
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


# ── Feature importance (gain) ────────────────────────────────────────────────

def feature_importance_plot(
    model: Any,
    feature_names: list[str],
    save_path: str | None = None,
    top_n: int = 20,
) -> pd.DataFrame:
    """Bar chart of top features by LightGBM gain.

    Args:
        model: Fitted LGBMClassifier.
        feature_names: Feature column names (same order as training data).
        save_path: If provided, save the figure as PNG.
        top_n: Number of features to show.

    Returns:
        DataFrame with columns: feature, importance (sorted descending).
    """
    importances = model.feature_importances_
    df = (
        pd.DataFrame({"feature": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    if save_path:
        import matplotlib.pyplot as plt

        top = df.head(top_n)
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(range(len(top)), top["importance"].values, color="#2ecc71", edgecolor="white")
        ax.set_yticks(range(len(top)))
        ax.set_yticklabels(top["feature"].values)
        ax.invert_yaxis()
        ax.set_xlabel("Importance (gain)")
        ax.set_title(f"Top {top_n} Feature Importances — LightGBM")
        fig.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved importance plot to {save_path}")

    return df


# ── SHAP ─────────────────────────────────────────────────────────────────────

def shap_summary(
    model: Any,
    X_test: np.ndarray | pd.DataFrame,
    feature_names: list[str],
    save_path: str | None = None,
    top_n: int = 10,
) -> np.ndarray:
    """Compute SHAP values and optionally save a beeswarm plot.

    Args:
        model: Fitted LGBMClassifier.
        X_test: Test features (n_samples × n_features).
        feature_names: Feature column names.
        save_path: If provided, save the beeswarm plot as PNG.
        top_n: Number of features to print in the console summary.

    Returns:
        SHAP values array (n_samples × n_features) for the positive class.
    """
    import shap

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)

    # For binary classification, shap_values may be a list [class_0, class_1]
    if isinstance(shap_values, list):
        sv = shap_values[1]  # positive class
    else:
        sv = shap_values

    # Console summary: top features by mean |SHAP|
    mean_abs = np.abs(sv).mean(axis=0)
    pairs = sorted(zip(feature_names, mean_abs), key=lambda x: x[1], reverse=True)

    print(f"\n── Top {top_n} features by mean |SHAP value| ──────────────────")
    max_val = pairs[0][1] if pairs else 1
    for name, val in pairs[:top_n]:
        bar_len = int(40 * val / max_val) if max_val > 0 else 0
        print(f"  {name:<45s} {val:>8.4f}  {'█' * bar_len}")

    # Beeswarm plot
    if save_path:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        X_df = pd.DataFrame(np.asarray(X_test), columns=feature_names) if not isinstance(X_test, pd.DataFrame) else X_test
        explanation = shap.Explanation(values=sv, data=X_df.values, feature_names=feature_names)

        fig, ax = plt.subplots(figsize=(10, 8))
        shap.plots.beeswarm(explanation, max_display=20, show=False)
        fig = plt.gcf()
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved SHAP summary plot to {save_path}")

    return sv


def shap_single_fight(
    model: Any,
    X_row: np.ndarray | pd.DataFrame,
    feature_names: list[str],
) -> dict:
    """Return SHAP values for a single prediction.

    Args:
        model: Fitted LGBMClassifier.
        X_row: Single row of features (1 × n_features).
        feature_names: Feature column names.

    Returns:
        Dict with keys:
            - shap_values: dict mapping feature_name → SHAP value
            - base_value: expected value (model output at population mean)
            - prediction: model predicted probability for this fight
    """
    import shap

    explainer = shap.TreeExplainer(model)

    # Ensure 2D
    if hasattr(X_row, "values"):
        X_2d = X_row.values.reshape(1, -1) if X_row.values.ndim == 1 else X_row.values
    else:
        X_2d = np.asarray(X_row).reshape(1, -1) if np.asarray(X_row).ndim == 1 else np.asarray(X_row)

    shap_values = explainer.shap_values(X_2d)

    # Handle binary classification
    if isinstance(shap_values, list):
        sv = shap_values[1][0]
        base = explainer.expected_value[1] if isinstance(explainer.expected_value, (list, np.ndarray)) else explainer.expected_value
    else:
        sv = shap_values[0]
        base = explainer.expected_value
        if isinstance(base, (list, np.ndarray)):
            base = base[0]

    pred = model.predict_proba(X_2d)[0, 1]

    return {
        "shap_values": dict(zip(feature_names, sv.tolist())),
        "base_value": float(base),
        "prediction": float(pred),
    }
