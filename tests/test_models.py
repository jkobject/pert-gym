"""Tests for Phase 5.1 / 5.2 models and the evaluate harness."""

import numpy as np
import pytest

from pert_gym.evaluate import evaluate_model, summarise
from pert_gym.models import (
    BinarySplit,
    CellStateClassifier,
    ElasticNetPerturbationRegressor,
    LinearPerturbationRegressor,
    MeanControl,
    MeanPerturbation,
    RidgePerturbationRegressor,
)
from pert_gym.splits import cell_indices_for_split, split_perturbations


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

RNG = np.random.default_rng(0)
N_GENES = 20


def _make_data(n_ctrl=30, n_pert=20, n_perts=5, seed=0):
    rng = np.random.default_rng(seed)
    labels = ["control"] * n_ctrl
    X_rows = [rng.normal(0, 1, N_GENES) for _ in range(n_ctrl)]
    for i in range(n_perts):
        shift = rng.normal(i * 0.5, 0.1, N_GENES)
        labels += [f"pert_{i}"] * n_pert
        X_rows += [rng.normal(0, 1, N_GENES) + shift for _ in range(n_pert)]
    return np.stack(X_rows), np.asarray(labels)


X, perts = _make_data()


# ---------------------------------------------------------------------------
# Phase 5.1 — trivial baselines
# ---------------------------------------------------------------------------


def test_mean_control_shape():
    model = MeanControl().fit(X, perts)
    pred = model.predict(["control", "pert_0", "pert_1"])
    assert pred.shape == (3, N_GENES)
    # All rows identical (always predicts control mean)
    assert np.allclose(pred[0], pred[1])


def test_mean_perturbation_seen_unseen():
    model = MeanPerturbation().fit(X, perts)
    pred_seen = model.predict(["pert_0"])
    pred_unseen = model.predict(["unknown_pert"])
    assert pred_seen.shape == (1, N_GENES)
    assert pred_unseen.shape == (1, N_GENES)


def test_binary_split_shape():
    model = BinarySplit().fit(X, perts)
    pred = model.predict(["pert_0", "pert_1", "pert_2", "pert_3", "pert_4"])
    assert pred.shape == (5, N_GENES)
    # Only two unique row values (strong / weak group means)
    unique_rows = np.unique(pred, axis=0)
    assert unique_rows.shape[0] <= 2


# ---------------------------------------------------------------------------
# Phase 5.2 — classical models
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cls",
    [LinearPerturbationRegressor, RidgePerturbationRegressor],
)
def test_linear_ridge_fit_predict(cls):
    model = cls().fit(X, perts)
    pred = model.predict(["pert_0", "pert_1"])
    assert pred.shape == (2, N_GENES)


def test_elasticnet_fit_predict():
    model = ElasticNetPerturbationRegressor(alpha=0.1).fit(X, perts)
    pred = model.predict(["pert_0"])
    assert pred.shape == (1, N_GENES)


def test_cell_state_classifier():
    # Use group identity as a proxy "cell state"
    cell_states = np.where(perts == "control", "ctrl_state", "pert_state")
    clf = CellStateClassifier().fit(perts, cell_states)
    predictions = clf.predict(["pert_0", "control"])
    assert set(predictions).issubset({"ctrl_state", "pert_state"})
    proba = clf.predict_proba(["pert_0"])
    assert proba.shape == (1, 2)


# ---------------------------------------------------------------------------
# Splits
# ---------------------------------------------------------------------------


def test_split_perturbations_disjoint():
    all_perts = list(perts)
    train, val, test = split_perturbations(all_perts, val_frac=0.2, test_frac=0.2)
    assert "control" in train
    assert not (set(train) & set(val))
    assert not (set(train) & set(test))
    assert not (set(val) & set(test))


def test_cell_indices_cover_all():
    all_perts = list(perts)
    train_l, val_l, test_l = split_perturbations(all_perts)
    ti, vi, tsi = cell_indices_for_split(all_perts, train_l, val_l, test_l)
    assert len(ti) + len(vi) + len(tsi) == len(all_perts)


# ---------------------------------------------------------------------------
# Evaluate harness
# ---------------------------------------------------------------------------


def test_evaluate_model_returns_dataframe():
    model = MeanControl().fit(X, perts)
    # Use a subset of cells as "test" (all perturbation types present)
    results = evaluate_model(
        model,
        X_test=X,
        perturbations_test=list(perts),
        X_control=X[perts == "control"],
    )
    assert "perturbation" in results.columns
    assert "pearson_all" in results.columns
    assert "pearson_top20" in results.columns
    assert "pearson_top100" in results.columns
    assert len(results) == 5  # 5 non-control perturbations


def test_summarise():
    model = MeanPerturbation().fit(X, perts)
    results = evaluate_model(
        model,
        X_test=X,
        perturbations_test=list(perts),
        X_control=X[perts == "control"],
    )
    summary = summarise(results)
    assert "pearson_all" in summary.index
