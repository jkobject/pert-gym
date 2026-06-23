"""Evaluation scaffold for small perturbation-response prediction batches."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Mapping, Sequence

from .metrics import mean_absolute_error
from .models.base import Matrix, PerturbationModel


@dataclass(frozen=True)
class EvaluationBatch:
    """Small in-memory batch for tests, smoke checks, and mocked loaders."""

    X: Matrix
    perturbations: Sequence[str]
    controls: Sequence[bool] | None = None

    def __post_init__(self) -> None:
        _validate_batch(self.X, self.perturbations, self.controls)


@dataclass(frozen=True)
class EvaluationResult:
    """Result returned by the lightweight evaluation scaffold."""

    model_name: str
    metrics: Mapping[str, float]
    predictions: list[list[float]]
    n_obs: int
    n_features: int


def evaluate_model(
    model: PerturbationModel,
    *,
    train: EvaluationBatch,
    test: EvaluationBatch,
) -> EvaluationResult:
    """Fit a model on a small batch and evaluate predictions on another batch."""
    fitted = model.fit(
        X=train.X,
        perturbations=train.perturbations,
        controls=train.controls,
    )
    predictions = fitted.predict(
        perturbations=test.perturbations,
        controls=test.controls,
    )
    metrics = evaluate_predictions(y_true=test.X, y_pred=predictions)
    return EvaluationResult(
        model_name=getattr(fitted, "name", fitted.__class__.__name__),
        metrics=metrics,
        predictions=predictions,
        n_obs=len(test.X),
        n_features=_n_features(test.X),
    )


def evaluate_predictions(y_true: Matrix, y_pred: Matrix) -> dict[str, float]:
    """Compute basic expression-vector metrics for in-memory predictions."""
    _validate_prediction_shapes(y_true, y_pred)
    if not y_true:
        return {"mae": 0.0, "rmse": 0.0}

    flattened_true = [float(value) for row in y_true for value in row]
    flattened_pred = [float(value) for row in y_pred for value in row]
    mae = mean_absolute_error(flattened_true, flattened_pred)
    mse = sum(
        (true_value - pred_value) ** 2
        for true_value, pred_value in zip(flattened_true, flattened_pred)
    ) / len(flattened_true)
    return {"mae": mae, "rmse": sqrt(mse)}


def _validate_batch(
    X: Matrix, perturbations: Sequence[str], controls: Sequence[bool] | None
) -> None:
    if len(X) != len(perturbations):
        raise ValueError("X and perturbations must have the same number of rows.")
    if controls is not None and len(controls) != len(X):
        raise ValueError("controls must have the same number of rows as X.")
    _n_features(X)


def _validate_prediction_shapes(y_true: Matrix, y_pred: Matrix) -> None:
    if len(y_true) != len(y_pred):
        raise ValueError("y_true and y_pred must have the same number of rows.")
    for true_row, pred_row in zip(y_true, y_pred):
        if len(true_row) != len(pred_row):
            raise ValueError("Prediction rows must match true row feature counts.")


def _n_features(X: Matrix) -> int:
    if not X:
        return 0
    n_features = len(X[0])
    for row in X:
        if len(row) != n_features:
            raise ValueError("All rows in X must have the same number of features.")
    return n_features
