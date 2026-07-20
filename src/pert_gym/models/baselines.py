"""Trivial pure-Python perturbation prediction baselines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from .base import Matrix, Vector


@dataclass
class MeanControlBaseline:
    """Predict the training control centroid for every requested perturbation."""

    name: str = "mean_control"
    n_features_: int | None = None
    control_mean_: list[float] = field(default_factory=list)

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "MeanControlBaseline":
        _validate_training_inputs(X, perturbations, controls)
        controls = _default_controls(perturbations, controls)
        self.n_features_ = _n_features(X)

        control_rows = [row for row, is_control in zip(X, controls) if is_control]
        if not control_rows:
            raise ValueError("MeanControlBaseline requires at least one control row.")
        self.control_mean_ = _centroid(control_rows)
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        del controls
        self._require_fitted()
        return [list(self.control_mean_) for _ in perturbations]

    def _require_fitted(self) -> None:
        if self.n_features_ is None:
            raise RuntimeError("Model must be fit before calling predict().")


@dataclass
class MeanPerturbationBaseline:
    """Predict the global training centroid across all perturbed rows."""

    name: str = "mean_perturbation"
    n_features_: int | None = None
    perturbation_mean_: list[float] = field(default_factory=list)

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "MeanPerturbationBaseline":
        _validate_training_inputs(X, perturbations, controls)
        controls = _default_controls(perturbations, controls)
        self.n_features_ = _n_features(X)

        perturbed_rows = [row for row, is_control in zip(X, controls) if not is_control]
        if not perturbed_rows:
            raise ValueError(
                "MeanPerturbationBaseline requires at least one perturbed row."
            )
        self.perturbation_mean_ = _centroid(perturbed_rows)
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        del controls
        self._require_fitted()
        return [list(self.perturbation_mean_) for _ in perturbations]

    def _require_fitted(self) -> None:
        if self.n_features_ is None:
            raise RuntimeError("Model must be fit before calling predict().")


def _default_controls(
    perturbations: Sequence[str], controls: Sequence[bool] | None
) -> Sequence[bool]:
    if controls is not None:
        return controls
    return [
        perturbation.lower() in {"control", "ctrl", "vehicle"}
        for perturbation in perturbations
    ]


def _validate_training_inputs(
    X: Matrix, perturbations: Sequence[str], controls: Sequence[bool] | None
) -> None:
    if not X:
        raise ValueError("X must contain at least one row.")
    if len(X) != len(perturbations):
        raise ValueError("X and perturbations must have the same number of rows.")
    if controls is not None and len(controls) != len(X):
        raise ValueError("controls must have the same number of rows as X.")
    n_features = _n_features(X)
    for row in X:
        if len(row) != n_features:
            raise ValueError("All rows in X must have the same number of features.")


def _n_features(X: Matrix) -> int:
    return len(X[0])


def _centroid(rows: Sequence[Vector]) -> list[float]:
    if not rows:
        raise ValueError("rows must not be empty.")
    n_features = len(rows[0])
    sums = [0.0] * n_features
    for row in rows:
        if len(row) != n_features:
            raise ValueError("All rows must have the same number of features.")
        for idx, value in enumerate(row):
            sums[idx] += float(value)
    return [value / len(rows) for value in sums]
