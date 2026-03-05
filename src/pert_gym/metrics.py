"""Metrics for perturbation-response evaluation.

This module includes:
- Arc VCC challenge metrics: MAE, PDS, DES.
- Weighted metrics from arXiv:2506.22641 (WMSE, weighted delta R2).
- Systema metrics from Nature (2025): differential profile, Pearson(delta),
  RMSE, and centroid accuracy.
"""

from __future__ import annotations

from math import sqrt
from typing import Callable, Iterable, Mapping, Sequence

Vector = Sequence[float]
Profile = Mapping[str, float]


def mean_absolute_error(y_true: Vector, y_pred: Vector) -> float:
    """Arc VCC MAE for one perturbation profile."""
    _validate_equal_length(y_true, y_pred)
    if not y_true:
        return 0.0
    return sum(abs(t - p) for t, p in zip(y_true, y_pred)) / len(y_true)


def perturbation_distance_similarity(delta_true: Vector, delta_pred: Vector) -> float:
    """Arc VCC PDS score.

    PDS = 1 / (1 + (1/M) * sum((delta_pred - delta_true)^2))
    """
    _validate_equal_length(delta_true, delta_pred)
    if not delta_true:
        return 0.0
    mse = sum((p - t) ** 2 for t, p in zip(delta_true, delta_pred)) / len(delta_true)
    return 1.0 / (1.0 + mse)


def differential_expression_similarity(
    de_true: Iterable[str],
    de_pred: Iterable[str],
    pred_logfc: Profile | None = None,
) -> float:
    """Arc VCC DES score using the challenge formula.

    DES = 2 * |D_true ∩ D_pred| / (|D_true| + |D_pred|)

    If |D_pred| > |D_true|, predicted genes are truncated to the top |D_true|
    by absolute log-fold-change using ``pred_logfc``.
    """
    true_set = set(de_true)
    pred_set = set(de_pred)

    if not true_set and not pred_set:
        return 1.0
    if not true_set or not pred_set:
        return 0.0

    if len(pred_set) > len(true_set):
        if pred_logfc is None:
            raise ValueError(
                "pred_logfc is required when len(de_pred) > len(de_true) to "
                "truncate predicted DE genes by absolute logFC."
            )
        pred_set = set(
            sorted(pred_set, key=lambda gene: abs(pred_logfc.get(gene, 0.0)), reverse=True)[
                : len(true_set)
            ]
        )

    overlap = len(true_set & pred_set)
    return 2.0 * overlap / (len(true_set) + len(pred_set))


def deg_weights_from_t_scores(t_scores: Vector) -> list[float]:
    """Compute DEG weights from t-scores as in arXiv:2506.22641."""
    if not t_scores:
        return []

    abs_scores = [abs(v) for v in t_scores]
    min_score = min(abs_scores)
    max_score = max(abs_scores)

    if max_score == min_score:
        return [1.0 / len(abs_scores)] * len(abs_scores)

    normalized = [(score - min_score) / (max_score - min_score) for score in abs_scores]
    squared = [value**2 for value in normalized]
    total = sum(squared)

    if total == 0:
        return [1.0 / len(abs_scores)] * len(abs_scores)
    return [value / total for value in squared]


def weighted_mse(y_true: Vector, y_pred: Vector, weights: Vector) -> float:
    """Weighted MSE (WMSE) from arXiv:2506.22641."""
    _validate_equal_length(y_true, y_pred, weights)
    if not y_true:
        return 0.0
    return sum(w * (t - p) ** 2 for t, p, w in zip(y_true, y_pred, weights))


def weighted_delta_r2(
    y_true: Vector,
    y_pred: Vector,
    control_true: Vector,
    control_pred: Vector,
    weights: Vector,
) -> float:
    """Weighted delta R2 from arXiv:2506.22641."""
    _validate_equal_length(y_true, y_pred, control_true, control_pred, weights)
    if not y_true:
        return 0.0

    numerator = 0.0
    denominator = 0.0
    for true_i, pred_i, true_ctrl_i, pred_ctrl_i, w_i in zip(
        y_true, y_pred, control_true, control_pred, weights
    ):
        true_delta = true_i - true_ctrl_i
        pred_delta = pred_i - pred_ctrl_i
        numerator += w_i * (pred_delta - true_delta) ** 2
        denominator += w_i * (true_delta**2)

    if denominator == 0:
        return 1.0 if numerator == 0 else 0.0
    return 1.0 - (numerator / denominator)


def differential_expression_profile(
    perturbation_samples: Sequence[Vector], reference_samples: Sequence[Vector]
) -> list[float]:
    """Differential expression profile d_p from the Systema paper."""
    if not perturbation_samples:
        raise ValueError("perturbation_samples must contain at least one sample.")
    if not reference_samples:
        raise ValueError("reference_samples must contain at least one sample.")

    pert_centroid = centroid(perturbation_samples)
    ref_centroid = centroid(reference_samples)
    _validate_equal_length(pert_centroid, ref_centroid)
    return [p - r for p, r in zip(pert_centroid, ref_centroid)]


def pearson_delta(delta_true: Vector, delta_pred: Vector, top_k: int | None = None) -> float:
    """Pearson correlation on differential profiles (Systema reference-based metric)."""
    selected_true, selected_pred = _select_top_k_by_abs_true(delta_true, delta_pred, top_k)
    return pearson_correlation(selected_true, selected_pred)


def rmse_delta(delta_true: Vector, delta_pred: Vector, top_k: int | None = None) -> float:
    """RMSE on differential profiles (Systema reference-based metric)."""
    selected_true, selected_pred = _select_top_k_by_abs_true(delta_true, delta_pred, top_k)
    if not selected_true:
        return 0.0
    mse = sum((t - p) ** 2 for t, p in zip(selected_true, selected_pred)) / len(selected_true)
    return sqrt(mse)


def mean_train_differential_profile(train_profiles: Mapping[str, Vector]) -> list[float]:
    """Mean perturbation differential profile o_pert from the Systema paper."""
    if not train_profiles:
        raise ValueError("train_profiles must not be empty.")
    return centroid(list(train_profiles.values()))


def centroid_accuracy(
    predicted_profiles: Mapping[str, Vector],
    true_profiles: Mapping[str, Vector],
    distance: Callable[[Vector, Vector], float] | None = None,
) -> float:
    """Systema centroid accuracy.

    A prediction is counted as correct if its distance to the true matched target
    centroid is less than or equal to its distance to every other target centroid.
    """
    if set(predicted_profiles) != set(true_profiles):
        raise ValueError("predicted_profiles and true_profiles must share the same keys.")
    if not predicted_profiles:
        return 0.0

    dist = distance or euclidean_distance
    labels = list(predicted_profiles.keys())
    correct = 0

    for label in labels:
        pred = predicted_profiles[label]
        target = true_profiles[label]
        d_match = dist(pred, target)
        if all(d_match <= dist(pred, true_profiles[other]) for other in labels if other != label):
            correct += 1

    return correct / len(labels)


def centroid(samples: Sequence[Vector]) -> list[float]:
    """Compute centroid of vectors."""
    if not samples:
        raise ValueError("samples must contain at least one vector.")
    n_features = len(samples[0])
    if n_features == 0:
        return []
    for sample in samples[1:]:
        _validate_equal_length(samples[0], sample)

    sums = [0.0] * n_features
    for sample in samples:
        for idx, value in enumerate(sample):
            sums[idx] += value
    return [value / len(samples) for value in sums]


def pearson_correlation(x: Vector, y: Vector) -> float:
    """Pearson correlation with stable edge-case handling."""
    _validate_equal_length(x, y)
    if not x:
        return 0.0

    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    num = sum((x_i - mean_x) * (y_i - mean_y) for x_i, y_i in zip(x, y))
    den_x = sum((x_i - mean_x) ** 2 for x_i in x)
    den_y = sum((y_i - mean_y) ** 2 for y_i in y)
    den = sqrt(den_x * den_y)

    if den == 0:
        return 0.0
    return num / den


def euclidean_distance(x: Vector, y: Vector) -> float:
    """Euclidean distance."""
    _validate_equal_length(x, y)
    return sqrt(sum((x_i - y_i) ** 2 for x_i, y_i in zip(x, y)))


def _select_top_k_by_abs_true(
    delta_true: Vector, delta_pred: Vector, top_k: int | None
) -> tuple[list[float], list[float]]:
    _validate_equal_length(delta_true, delta_pred)
    if top_k is None:
        return list(delta_true), list(delta_pred)
    if top_k <= 0:
        raise ValueError("top_k must be positive when provided.")

    n = min(top_k, len(delta_true))
    indices = sorted(range(len(delta_true)), key=lambda idx: abs(delta_true[idx]), reverse=True)[:n]
    return [delta_true[idx] for idx in indices], [delta_pred[idx] for idx in indices]


def mse(y_true: Vector, y_pred: Vector) -> float:
    """Mean squared error."""
    _validate_equal_length(y_true, y_pred)
    if not y_true:
        return 0.0
    return sum((t - p) ** 2 for t, p in zip(y_true, y_pred)) / len(y_true)


def r2_score(y_true: Vector, y_pred: Vector) -> float:
    """Coefficient of determination (R²)."""
    _validate_equal_length(y_true, y_pred)
    if not y_true:
        return 0.0
    mean_true = sum(y_true) / len(y_true)
    ss_res = sum((t - p) ** 2 for t, p in zip(y_true, y_pred))
    ss_tot = sum((t - mean_true) ** 2 for t in y_true)
    if ss_tot == 0:
        return 1.0 if ss_res == 0 else 0.0
    return 1.0 - ss_res / ss_tot


def _validate_equal_length(*vectors: Vector) -> None:
    lengths = {len(v) for v in vectors}
    if len(lengths) > 1:
        raise ValueError(f"All vectors must have equal length, got lengths={sorted(lengths)}")
