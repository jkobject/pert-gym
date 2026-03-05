"""Evaluation harness for perturbation-response models (Phase 5.4).

Usage
-----
>>> from pert_gym.evaluate import evaluate_model
>>> results = evaluate_model(model, X_test, perts_test, X_ctrl)
>>> print(results)

The harness computes, for each held-out perturbation:
  - Pearson r (delta)  — across all genes, top-20, top-100 DE genes.
  - RMSE (delta)       — same slices.
  - R²                 — mean across genes.
  - MSE                — mean across genes.

Delta profiles are: mean(perturbed) – mean(control).
DE gene ranking uses ``|delta_true|`` (computed from test cells).
"""

from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable

import numpy as np
import pandas as pd

from pert_gym.metrics import mse, pearson_delta, r2_score, rmse_delta


@runtime_checkable
class PerturbationModel(Protocol):
    """Minimal interface every model must implement."""

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        """Return predicted expression, shape (n_perturbations, n_genes)."""
        ...


def _per_pert_means(
    X: np.ndarray, perturbations: np.ndarray
) -> dict[str, np.ndarray]:
    """Compute mean expression per unique perturbation label."""
    means: dict[str, np.ndarray] = {}
    for label in np.unique(perturbations):
        means[str(label)] = X[perturbations == label].mean(axis=0)
    return means


def evaluate_model(
    model: Any,
    X_test: np.ndarray,
    perturbations_test: Sequence[str],
    X_control: np.ndarray,
    top_ks: tuple[int, ...] = (20, 100),
    control_label: str = "control",
) -> pd.DataFrame:
    """Evaluate a trained model on held-out cells.

    For each unique non-control perturbation in ``perturbations_test`` the
    function:

    1. Computes ``delta_true  = mean(test cells) - mean(control)``.
    2. Calls ``model.predict([perturbation])`` to get ``predicted``.
    3. Computes ``delta_pred  = predicted - mean(control)``.
    4. Computes metrics over all genes and over top-K DE genes (ranked by
       ``|delta_true|``).

    Args:
        model: Fitted model with a ``predict(perturbations) -> np.ndarray``
            method.
        X_test: Expression matrix of test cells (n_cells, n_genes).
        perturbations_test: Per-cell perturbation label for test cells.
        X_control: Control cells used to compute the reference mean.
            Can be from the training split.
        top_ks: Gene-count thresholds for DE-gene subsets.
        control_label: Label used for control cells (excluded from evaluation).

    Returns:
        DataFrame with one row per evaluated perturbation and columns:
        ``perturbation``, ``n_cells``,
        ``pearson_all``, ``rmse_all``, ``r2_all``, ``mse_all``,
        ``pearson_top{k}``, ``rmse_top{k}`` for each k in *top_ks*.
    """
    ctrl_mean = X_control.mean(axis=0)
    perts_array = np.asarray(perturbations_test)

    unique_perts = [
        str(p)
        for p in np.unique(perts_array)
        if str(p) != control_label
    ]

    if not unique_perts:
        raise ValueError("No non-control perturbations found in perturbations_test.")

    test_means = _per_pert_means(X_test, perts_array)
    records = []

    for pert in unique_perts:
        if pert not in test_means:
            continue

        true_mean = test_means[pert]
        delta_true = (true_mean - ctrl_mean).tolist()

        pred_matrix = model.predict([pert])  # (1, n_genes)
        pred_mean = pred_matrix[0]
        delta_pred = (pred_mean - ctrl_mean).tolist()

        n_cells = int((perts_array == pert).sum())

        row: dict[str, Any] = {
            "perturbation": pert,
            "n_cells": n_cells,
            "pearson_all": pearson_delta(delta_true, delta_pred),
            "rmse_all": rmse_delta(delta_true, delta_pred),
            "r2_all": r2_score(true_mean.tolist(), pred_mean.tolist()),
            "mse_all": mse(true_mean.tolist(), pred_mean.tolist()),
        }

        for k in top_ks:
            row[f"pearson_top{k}"] = pearson_delta(delta_true, delta_pred, top_k=k)
            row[f"rmse_top{k}"] = rmse_delta(delta_true, delta_pred, top_k=k)

        records.append(row)

    return pd.DataFrame(records)


def summarise(results: pd.DataFrame) -> pd.Series:
    """Return mean of each numeric metric column across all perturbations."""
    numeric = results.select_dtypes(include="number").drop(columns=["n_cells"], errors="ignore")
    return numeric.mean()
