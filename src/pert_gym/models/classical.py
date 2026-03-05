"""Classical regression / classification baselines (Phase 5.2).

All models use one-hot perturbation encoding and therefore only predict
for perturbation labels seen during training.  Prediction for unseen
perturbations raises ``KeyError``.

Models
------
- LinearPerturbationRegressor  — OLS multi-output linear regression per gene.
- RidgePerturbationRegressor   — Ridge-regularised regression.
- ElasticNetPerturbationRegressor — ElasticNet-regularised regression.
- RandomForestPerturbationRegressor — Random-forest multi-output regressor.
- GradientBoostingPerturbationRegressor — Gradient-boosted trees per gene.
- CellStateClassifier — Logistic regression to predict obs["cell_state"].
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import ElasticNet, LinearRegression, LogisticRegression, Ridge
from sklearn.multioutput import MultiOutputRegressor
from sklearn.preprocessing import LabelEncoder, OneHotEncoder


def _build_onehot(
    perturbations: Sequence[str],
    encoder: OneHotEncoder | None = None,
) -> tuple[np.ndarray, OneHotEncoder]:
    """One-hot encode perturbation labels."""
    arr = np.asarray(perturbations).reshape(-1, 1)
    if encoder is None:
        encoder = OneHotEncoder(handle_unknown="error", sparse_output=False)
        X_enc = encoder.fit_transform(arr)
    else:
        X_enc = encoder.transform(arr)
    return X_enc, encoder


class _BaseOneHotRegressor:
    """Shared fit / predict logic for one-hot–based multi-output regressors."""

    _encoder: OneHotEncoder | None = None
    _model = None

    def _fit_impl(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        model,
    ) -> None:
        X_enc, self._encoder = _build_onehot(perturbations)
        self._model = model
        self._model.fit(X_enc, X)

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        """Predict mean expression for each perturbation label.

        Args:
            perturbations: List of perturbation labels to predict.

        Returns:
            Array of shape (len(perturbations), n_genes).
        """
        if self._encoder is None or self._model is None:
            raise RuntimeError("Call fit() before predict().")
        X_enc, _ = _build_onehot(perturbations, self._encoder)
        return self._model.predict(X_enc)


class LinearPerturbationRegressor(_BaseOneHotRegressor):
    """Multi-output OLS linear regression (one model per gene)."""

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "LinearPerturbationRegressor":
        self._fit_impl(X, perturbations, LinearRegression())
        return self


class RidgePerturbationRegressor(_BaseOneHotRegressor):
    """Multi-output Ridge regression."""

    def __init__(self, alpha: float = 1.0) -> None:
        self.alpha = alpha

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "RidgePerturbationRegressor":
        self._fit_impl(X, perturbations, Ridge(alpha=self.alpha))
        return self


class ElasticNetPerturbationRegressor(_BaseOneHotRegressor):
    """Multi-output ElasticNet regression."""

    def __init__(self, alpha: float = 1.0, l1_ratio: float = 0.5) -> None:
        self.alpha = alpha
        self.l1_ratio = l1_ratio

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "ElasticNetPerturbationRegressor":
        self._fit_impl(
            X,
            perturbations,
            MultiOutputRegressor(
                ElasticNet(alpha=self.alpha, l1_ratio=self.l1_ratio), n_jobs=-1
            ),
        )
        return self


class RandomForestPerturbationRegressor(_BaseOneHotRegressor):
    """Multi-output random-forest regressor."""

    def __init__(self, n_estimators: int = 100, n_jobs: int = -1, **kwargs) -> None:
        self.n_estimators = n_estimators
        self.n_jobs = n_jobs
        self.kwargs = kwargs

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "RandomForestPerturbationRegressor":
        self._fit_impl(
            X,
            perturbations,
            RandomForestRegressor(
                n_estimators=self.n_estimators, n_jobs=self.n_jobs, **self.kwargs
            ),
        )
        return self


class GradientBoostingPerturbationRegressor(_BaseOneHotRegressor):
    """Gradient-boosted trees — one regressor per gene via MultiOutputRegressor."""

    def __init__(self, n_estimators: int = 100, **kwargs) -> None:
        self.n_estimators = n_estimators
        self.kwargs = kwargs

    def fit(
        self,
        X: np.ndarray,
        perturbations: Sequence[str],
        control_label: str = "control",
    ) -> "GradientBoostingPerturbationRegressor":
        self._fit_impl(
            X,
            perturbations,
            MultiOutputRegressor(
                GradientBoostingRegressor(
                    n_estimators=self.n_estimators, **self.kwargs
                ),
                n_jobs=-1,
            ),
        )
        return self


class CellStateClassifier:
    """Predict cell state from perturbation identity (logistic regression).

    Targets are provided as a per-cell cell-state label array (strings).
    """

    _encoder: OneHotEncoder | None = None
    _label_enc: LabelEncoder | None = None
    _model: LogisticRegression | None = None
    classes_: list[str] = []

    def __init__(self, **logistic_kwargs) -> None:
        self._logistic_kwargs = {"max_iter": 1000, **logistic_kwargs}

    def fit(
        self,
        perturbations: Sequence[str],
        cell_states: Sequence[str],
        control_label: str = "control",
    ) -> "CellStateClassifier":
        """Fit logistic classifier: perturbation → cell state.

        Args:
            perturbations: Per-cell perturbation label.
            cell_states: Per-cell cell state label (target).
            control_label: Unused; kept for API consistency.
        """
        X_enc, self._encoder = _build_onehot(perturbations)
        self._label_enc = LabelEncoder()
        y = self._label_enc.fit_transform(np.asarray(cell_states))
        self._model = LogisticRegression(**self._logistic_kwargs)
        self._model.fit(X_enc, y)
        self.classes_ = list(self._label_enc.classes_)
        return self

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        """Predict most-likely cell state for each perturbation.

        Returns:
            Array of predicted cell-state label strings, shape (n,).
        """
        if self._model is None or self._label_enc is None:
            raise RuntimeError("Call fit() before predict().")
        X_enc, _ = _build_onehot(perturbations, self._encoder)
        y_pred = self._model.predict(X_enc)
        return self._label_enc.inverse_transform(y_pred)

    def predict_proba(self, perturbations: Sequence[str]) -> np.ndarray:
        """Return class probabilities.

        Returns:
            Array of shape (n, n_classes).
        """
        if self._model is None:
            raise RuntimeError("Call fit() before predict_proba().")
        X_enc, _ = _build_onehot(perturbations, self._encoder)
        return self._model.predict_proba(X_enc)
