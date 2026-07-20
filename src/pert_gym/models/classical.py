"""Classical perturbation-response baselines.

The estimators in this module keep the public in-memory ``PerturbationModel``
interface from :mod:`pert_gym.models.base`. Scikit-learn is imported lazily by
fit methods so the base Lamin/data environment can still import ``pert_gym``
without installing the optional classical modeling extra.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Sequence

from .base import Matrix
from .baselines import (
    _centroid,
    _default_controls,
    _n_features,
    _validate_training_inputs,
)

GroupName = Literal["weak", "strong"]


@dataclass
class BinarySplitBaseline:
    """Split perturbations into weak/strong mean-effect groups.

    The split is based on each perturbation centroid's Euclidean distance from
    the control centroid. Predictions for seen perturbations use their group's
    centroid; unseen perturbations fall back to the global perturbed centroid.
    """

    name: str = "binary_split"
    n_features_: int | None = None
    control_mean_: list[float] = field(default_factory=list)
    global_perturbation_mean_: list[float] = field(default_factory=list)
    perturbation_groups_: dict[str, GroupName] = field(default_factory=dict)
    group_means_: dict[GroupName, list[float]] = field(default_factory=dict)

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "BinarySplitBaseline":
        _validate_training_inputs(X, perturbations, controls)
        controls = _default_controls(perturbations, controls)
        self.n_features_ = _n_features(X)

        control_rows = [row for row, is_control in zip(X, controls) if is_control]
        perturbed_rows = [row for row, is_control in zip(X, controls) if not is_control]
        if not control_rows:
            raise ValueError("BinarySplitBaseline requires at least one control row.")
        if not perturbed_rows:
            raise ValueError("BinarySplitBaseline requires at least one perturbed row.")

        self.control_mean_ = _centroid(control_rows)
        self.global_perturbation_mean_ = _centroid(perturbed_rows)

        per_perturbation_rows: dict[str, list[Sequence[float]]] = {}
        for row, perturbation, is_control in zip(X, perturbations, controls):
            if is_control:
                continue
            per_perturbation_rows.setdefault(perturbation, []).append(row)

        magnitudes = {
            perturbation: _euclidean_distance(_centroid(rows), self.control_mean_)
            for perturbation, rows in per_perturbation_rows.items()
        }
        sorted_magnitudes = sorted(magnitudes.values())
        median = sorted_magnitudes[len(sorted_magnitudes) // 2]
        if len(sorted_magnitudes) > 1 and len(sorted_magnitudes) % 2 == 0:
            median = (
                sorted_magnitudes[(len(sorted_magnitudes) // 2) - 1] + median
            ) / 2.0

        self.perturbation_groups_ = {
            perturbation: "weak" if magnitude <= median else "strong"
            for perturbation, magnitude in magnitudes.items()
        }

        grouped_rows: dict[GroupName, list[Sequence[float]]] = {
            "weak": [],
            "strong": [],
        }
        for row, perturbation, is_control in zip(X, perturbations, controls):
            if is_control:
                continue
            grouped_rows[self.perturbation_groups_[perturbation]].append(row)

        self.group_means_ = {
            group: _centroid(rows) if rows else list(self.global_perturbation_mean_)
            for group, rows in grouped_rows.items()
        }
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        self._require_fitted()
        controls = _default_controls(perturbations, controls)
        predictions = []
        for perturbation, is_control in zip(perturbations, controls):
            if is_control:
                predictions.append(list(self.control_mean_))
                continue
            group = self.perturbation_groups_.get(perturbation)
            if group is None:
                predictions.append(list(self.global_perturbation_mean_))
            else:
                predictions.append(list(self.group_means_[group]))
        return predictions

    def _require_fitted(self) -> None:
        if self.n_features_ is None:
            raise RuntimeError("Model must be fit before calling predict().")


@dataclass
class _SklearnPerturbationRegressor:
    name: str
    random_state: int | None = None
    n_features_: int | None = None
    estimator_: Any = None
    encoder_: Any = None

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "_SklearnPerturbationRegressor":
        _validate_training_inputs(X, perturbations, controls)
        self.n_features_ = _n_features(X)
        self.encoder_, design = _encode_perturbations_for_fit(perturbations, controls)
        self.estimator_ = self._make_estimator()
        self.estimator_.fit(design, _as_float_matrix(X))
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        self._require_fitted()
        design = _encode_perturbations_for_predict(
            self.encoder_, perturbations, controls
        )
        predictions = self.estimator_.predict(design)
        return _as_prediction_list(predictions)

    def _make_estimator(self) -> Any:
        raise NotImplementedError

    def _require_fitted(self) -> None:
        if self.n_features_ is None or self.estimator_ is None or self.encoder_ is None:
            raise RuntimeError("Model must be fit before calling predict().")


@dataclass
class LinearPerturbationRegressor(_SklearnPerturbationRegressor):
    """Per-gene ordinary least-squares baseline over perturbation features."""

    name: str = "linear_perturbation_regressor"
    random_state: int | None = None

    def _make_estimator(self) -> Any:
        _require_sklearn()
        from sklearn.linear_model import LinearRegression

        return LinearRegression()


@dataclass
class RidgePerturbationRegressor(_SklearnPerturbationRegressor):
    """Per-gene ridge regression over perturbation features."""

    name: str = "ridge_perturbation_regressor"
    random_state: int | None = None
    alpha: float = 1.0

    def _make_estimator(self) -> Any:
        _require_sklearn()
        from sklearn.linear_model import Ridge

        return Ridge(alpha=self.alpha, random_state=self.random_state)


@dataclass
class ElasticNetPerturbationRegressor(_SklearnPerturbationRegressor):
    """Per-gene ElasticNet regression over perturbation features."""

    name: str = "elasticnet_perturbation_regressor"
    random_state: int | None = None
    alpha: float = 0.001
    l1_ratio: float = 0.5
    max_iter: int = 10_000

    def _make_estimator(self) -> Any:
        _require_sklearn()
        from sklearn.linear_model import ElasticNet
        from sklearn.multioutput import MultiOutputRegressor

        return MultiOutputRegressor(
            ElasticNet(
                alpha=self.alpha,
                l1_ratio=self.l1_ratio,
                max_iter=self.max_iter,
                random_state=self.random_state,
            )
        )


@dataclass
class RandomForestPerturbationRegressor(_SklearnPerturbationRegressor):
    """Random-forest expression regressor over perturbation features."""

    name: str = "random_forest_perturbation_regressor"
    random_state: int | None = None
    n_estimators: int = 32
    max_depth: int | None = 4

    def _make_estimator(self) -> Any:
        _require_sklearn()
        from sklearn.ensemble import RandomForestRegressor

        return RandomForestRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=self.random_state,
        )


@dataclass
class GradientBoostingPerturbationRegressor(_SklearnPerturbationRegressor):
    """Gradient-boosted tree expression regressor over perturbation features."""

    name: str = "gradient_boosting_perturbation_regressor"
    random_state: int | None = None
    n_estimators: int = 50
    max_depth: int = 2

    def _make_estimator(self) -> Any:
        _require_sklearn()
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.multioutput import MultiOutputRegressor

        return MultiOutputRegressor(
            GradientBoostingRegressor(
                n_estimators=self.n_estimators,
                max_depth=self.max_depth,
                random_state=self.random_state,
            )
        )


@dataclass
class CellStateLogisticClassifier:
    """Logistic cell-state classifier from perturbation identity.

    This baseline is only meaningful when caller-supplied cell-state labels are
    available. It deliberately has a classifier-specific fit signature instead
    of pretending expression ``X`` is required.
    """

    name: str = "cell_state_logistic_classifier"
    random_state: int | None = None
    max_iter: int = 1_000
    classifier_: Any = None
    encoder_: Any = None
    classes_: list[str] = field(default_factory=list)

    def fit(
        self,
        perturbations: Sequence[str],
        labels: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "CellStateLogisticClassifier":
        if not perturbations:
            raise ValueError("perturbations must contain at least one row.")
        if len(perturbations) != len(labels):
            raise ValueError("perturbations and labels must have the same length.")
        if len(set(labels)) < 2:
            raise ValueError("At least two cell-state labels are required.")

        _require_sklearn()
        from sklearn.linear_model import LogisticRegression

        self.encoder_, design = _encode_perturbations_for_fit(perturbations, controls)
        self.classifier_ = LogisticRegression(
            max_iter=self.max_iter,
            random_state=self.random_state,
        )
        self.classifier_.fit(design, list(labels))
        self.classes_ = [str(label) for label in self.classifier_.classes_]
        return self

    def predict(
        self, perturbations: Sequence[str], controls: Sequence[bool] | None = None
    ) -> list[str]:
        self._require_fitted()
        design = _encode_perturbations_for_predict(
            self.encoder_, perturbations, controls
        )
        return [str(label) for label in self.classifier_.predict(design)]

    def predict_proba(
        self, perturbations: Sequence[str], controls: Sequence[bool] | None = None
    ) -> list[dict[str, float]]:
        self._require_fitted()
        design = _encode_perturbations_for_predict(
            self.encoder_, perturbations, controls
        )
        probabilities = self.classifier_.predict_proba(design)
        return [
            {label: float(value) for label, value in zip(self.classes_, row)}
            for row in probabilities
        ]

    def _require_fitted(self) -> None:
        if self.classifier_ is None or self.encoder_ is None:
            raise RuntimeError("Classifier must be fit before calling predict().")


def _require_sklearn() -> None:
    try:
        import sklearn  # noqa: F401
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Classical baselines require scikit-learn. Install with "
            "`uv sync --extra classical --extra dev` or run with "
            "`uv run --extra classical ...`."
        ) from exc


def _make_one_hot_encoder() -> Any:
    _require_sklearn()
    from sklearn.preprocessing import OneHotEncoder

    try:
        kwargs: dict[str, Any] = {"handle_unknown": "ignore", "sparse_output": False}
        return OneHotEncoder(**kwargs)
    except TypeError:  # scikit-learn < 1.2
        kwargs = {"handle_unknown": "ignore", "sparse": False}
        return OneHotEncoder(**kwargs)


def _encode_perturbations_for_fit(
    perturbations: Sequence[str], controls: Sequence[bool] | None
) -> tuple[Any, Any]:
    encoder = _make_one_hot_encoder()
    rows = _feature_rows(perturbations, controls)
    return encoder, encoder.fit_transform(rows)


def _encode_perturbations_for_predict(
    encoder: Any, perturbations: Sequence[str], controls: Sequence[bool] | None
) -> Any:
    return encoder.transform(_feature_rows(perturbations, controls))


def _feature_rows(
    perturbations: Sequence[str], controls: Sequence[bool] | None
) -> list[list[object]]:
    controls = _default_controls(perturbations, controls)
    return [
        [str(perturbation), bool(is_control)]
        for perturbation, is_control in zip(perturbations, controls)
    ]


def _as_float_matrix(X: Matrix) -> list[list[float]]:
    return [[float(value) for value in row] for row in X]


def _as_prediction_list(predictions: Any) -> list[list[float]]:
    if hasattr(predictions, "tolist"):
        predictions = predictions.tolist()
    if predictions and not isinstance(predictions[0], list):
        return [[float(value)] for value in predictions]
    return [[float(value) for value in row] for row in predictions]


def _euclidean_distance(left: Sequence[float], right: Sequence[float]) -> float:
    return sum((float(a) - float(b)) ** 2 for a, b in zip(left, right)) ** 0.5
