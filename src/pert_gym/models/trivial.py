"""Trivial baseline models for perturbation-response prediction (Phase 5.1).

These baselines define MIN / MAX reference points:
- MeanControl     — predict the mean of unperturbed cells (floor).
- MeanPerturbation — predict the per-perturbation mean (ceiling for in-sample).
- BinarySplit     — predict group mean after splitting perturbations into
                    strong / weak by mean effect magnitude.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np


class MeanControl:
    """Predict control-cell mean expression for every perturbation.

    This is the floor baseline: it carries no perturbation signal at all.
    """

    _control_mean: np.ndarray | None = None

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "MeanControl":
        """Compute mean of control cells.

        Args:
            X: Expression matrix (n_cells, n_genes).
            perturbations: Per-cell perturbation label.
            control_label: Label used for control cells.
        """
        mask = np.array([p == control_label for p in perturbations])
        if not mask.any():
            raise ValueError(f"No cells with label '{control_label}' found.")
        self._control_mean = X[mask].mean(axis=0)
        return self

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        """Return control mean for every query perturbation.

        Args:
            perturbations: List of perturbation labels to predict.

        Returns:
            Array of shape (len(perturbations), n_genes).
        """
        if self._control_mean is None:
            raise RuntimeError("Call fit() before predict().")
        return np.tile(self._control_mean, (len(perturbations), 1))


class MeanPerturbation:
    """Predict the per-perturbation mean observed during training.

    For perturbations not seen in training, falls back to the grand mean
    across all training cells.
    """

    _pert_means: dict[str, np.ndarray]
    _grand_mean: np.ndarray | None = None

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "MeanPerturbation":
        """Compute per-perturbation mean profiles.

        Args:
            X: Expression matrix (n_cells, n_genes).
            perturbations: Per-cell perturbation label.
            control_label: Unused; kept for API consistency.
        """
        pert_array = np.asarray(perturbations)
        self._pert_means = {}
        for label in np.unique(pert_array):
            mask = pert_array == label
            self._pert_means[str(label)] = X[mask].mean(axis=0)
        self._grand_mean = X.mean(axis=0)
        return self

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        """Return per-perturbation training mean (fallback: grand mean).

        Args:
            perturbations: List of perturbation labels to predict.

        Returns:
            Array of shape (len(perturbations), n_genes).
        """
        if self._grand_mean is None:
            raise RuntimeError("Call fit() before predict().")
        rows = [self._pert_means.get(p, self._grand_mean) for p in perturbations]
        return np.stack(rows, axis=0)


class BinarySplit:
    """Predict group mean after splitting perturbations into strong / weak.

    Perturbations are split into two groups by the L2 norm of their mean
    expression delta from control.  Weak perturbations are predicted with
    the weak-group mean; strong with the strong-group mean.
    """

    _control_mean: np.ndarray | None = None
    _strong_mean: np.ndarray | None = None
    _weak_mean: np.ndarray | None = None
    _strong_labels: set[str]

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "BinarySplit":
        """Compute strong / weak group means.

        Args:
            X: Expression matrix (n_cells, n_genes).
            perturbations: Per-cell perturbation label.
            control_label: Label used for control cells.
        """
        pert_array = np.asarray(perturbations)

        ctrl_mask = pert_array == control_label
        if not ctrl_mask.any():
            raise ValueError(f"No cells with label '{control_label}' found.")
        self._control_mean = X[ctrl_mask].mean(axis=0)

        non_ctrl = sorted(
            {str(p) for p in np.unique(pert_array) if p != control_label}
        )
        if not non_ctrl:
            raise ValueError("No non-control perturbations found.")

        pert_means: dict[str, np.ndarray] = {}
        for label in non_ctrl:
            mask = pert_array == label
            pert_means[label] = X[mask].mean(axis=0)

        deltas = {
            label: float(np.linalg.norm(mean - self._control_mean))
            for label, mean in pert_means.items()
        }

        median_delta = float(np.median(list(deltas.values())))
        strong_labels = {label for label, d in deltas.items() if d >= median_delta}

        strong_profiles = [pert_means[l] for l in strong_labels]
        weak_profiles = [pert_means[l] for l in non_ctrl if l not in strong_labels]

        self._strong_labels = strong_labels
        self._strong_mean = np.stack(strong_profiles).mean(axis=0)
        self._weak_mean = (
            np.stack(weak_profiles).mean(axis=0) if weak_profiles else self._strong_mean
        )
        return self

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        """Return strong or weak group mean for each perturbation.

        Args:
            perturbations: List of perturbation labels to predict.

        Returns:
            Array of shape (len(perturbations), n_genes).
        """
        if self._strong_mean is None:
            raise RuntimeError("Call fit() before predict().")
        rows = [
            self._strong_mean if p in self._strong_labels else self._weak_mean
            for p in perturbations
        ]
        return np.stack(rows, axis=0)
