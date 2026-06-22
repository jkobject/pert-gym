from __future__ import annotations

from math import isfinite

import pytest

pytest.importorskip("torch")

from pert_gym.benchmarks import load_model_ready_v0_or_synthetic
from pert_gym.evaluate import EvaluationBatch, evaluate_model
from pert_gym.models import CompositionalPerturbationAutoencoder


def test_compositional_perturbation_autoencoder_runs_tiny_synthetic_smoke() -> None:
    train = EvaluationBatch(
        X=[
            [0.0, 0.0, 0.0],
            [0.1, 0.0, 0.0],
            [1.0, 0.9, 1.1],
            [1.2, 1.0, 0.9],
            [-0.8, -1.0, -0.9],
            [-1.0, -0.8, -1.1],
        ],
        perturbations=["control", "control", "drug_a", "drug_a", "drug_b", "drug_b"],
        controls=[True, True, False, False, False, False],
    )
    test = EvaluationBatch(
        X=[[1.1, 1.0, 1.0], [-0.9, -0.9, -1.0], [0.05, 0.0, 0.0]],
        perturbations=["drug_a", "drug_b", "control"],
        controls=[False, False, True],
    )
    model = CompositionalPerturbationAutoencoder(
        latent_dim=2,
        hidden_dim=8,
        perturbation_dim=2,
        epochs=30,
        lr=0.03,
        seed=17,
    )

    result = evaluate_model(model, train=train, test=test)

    assert result.model_name == "cpa_standalone"
    assert result.n_obs == 3
    assert result.n_features == 3
    assert len(result.predictions) == 3
    assert all(len(row) == 3 for row in result.predictions)
    assert all(isfinite(value) for row in result.predictions for value in row)
    assert isfinite(result.metrics["mae"])
    assert model.loss_ is not None and isfinite(model.loss_)
    assert model.perturbation_to_index_ == {"drug_a": 0, "drug_b": 1}


def test_compositional_perturbation_autoencoder_runs_model_ready_v0_metadata_smoke() -> None:
    dataset = load_model_ready_v0_or_synthetic()
    model = CompositionalPerturbationAutoencoder(
        latent_dim=2,
        hidden_dim=8,
        perturbation_dim=2,
        epochs=20,
        lr=0.03,
        seed=19,
    )

    result = evaluate_model(model, train=dataset.train, test=dataset.test)

    assert dataset.metadata["loader"] == "model_ready_v0_or_synthetic"
    assert dataset.metadata["fallback"] == "synthetic"
    assert dataset.metadata["model_ready_collection_key"] == "pert-gym/model-ready/20260621"
    assert dataset.metadata["model_ready_member_count"] == 1
    assert result.model_name == "cpa_standalone"
    assert result.n_obs == len(dataset.test.X)
    assert result.n_features == len(dataset.test.X[0])
    assert all(isfinite(value) for row in result.predictions for value in row)


def test_compositional_perturbation_autoencoder_rejects_control_only_training() -> None:
    model = CompositionalPerturbationAutoencoder(epochs=1)

    with pytest.raises(ValueError, match="perturbed row"):
        model.fit(
            X=[[0.0, 0.0], [0.1, 0.0]],
            perturbations=["control", "control"],
            controls=[True, True],
        )
