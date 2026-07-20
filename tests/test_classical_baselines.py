from __future__ import annotations

import pytest

from pert_gym.models import (
    BinarySplitBaseline,
    CellStateLogisticClassifier,
    ElasticNetPerturbationRegressor,
    GradientBoostingPerturbationRegressor,
    LinearPerturbationRegressor,
    RandomForestPerturbationRegressor,
    RidgePerturbationRegressor,
)


@pytest.fixture
def synthetic_expression_batch() -> tuple[list[list[float]], list[str], list[bool]]:
    return (
        [
            [0.0, 0.0],
            [0.2, 0.0],
            [1.0, 1.0],
            [1.2, 1.1],
            [8.0, 8.0],
            [8.2, 8.1],
        ],
        ["control", "control", "weak", "weak", "strong", "strong"],
        [True, True, False, False, False, False],
    )


def test_binary_split_baseline_predicts_weak_and_strong_group_centroids(
    synthetic_expression_batch: tuple[list[list[float]], list[str], list[bool]],
) -> None:
    X, perturbations, controls = synthetic_expression_batch

    model = BinarySplitBaseline().fit(X, perturbations, controls)

    predictions = model.predict(["weak", "strong", "unseen"], [False, False, False])

    assert predictions[0] == pytest.approx([1.1, 1.05])
    assert predictions[1] == pytest.approx([8.1, 8.05])
    assert predictions[2] == pytest.approx(model.global_perturbation_mean_)
    assert model.perturbation_groups_["weak"] == "weak"
    assert model.perturbation_groups_["strong"] == "strong"


def test_binary_split_baseline_predicts_control_mean_for_requested_controls(
    synthetic_expression_batch: tuple[list[list[float]], list[str], list[bool]],
) -> None:
    X, perturbations, controls = synthetic_expression_batch
    model = BinarySplitBaseline().fit(X, perturbations, controls)

    predictions = model.predict(["weak", "unseen", "strong"], [True, True, False])

    assert predictions[0] == pytest.approx(model.control_mean_)
    assert predictions[1] == pytest.approx(model.control_mean_)
    assert predictions[2] == pytest.approx(model.group_means_["strong"])


@pytest.mark.parametrize(
    "model_cls",
    [
        LinearPerturbationRegressor,
        RidgePerturbationRegressor,
        ElasticNetPerturbationRegressor,
        RandomForestPerturbationRegressor,
        GradientBoostingPerturbationRegressor,
    ],
)
def test_classical_regressors_predict_one_expression_vector_per_perturbation(
    model_cls: type[LinearPerturbationRegressor],
    synthetic_expression_batch: tuple[list[list[float]], list[str], list[bool]],
) -> None:
    X, perturbations, controls = synthetic_expression_batch

    model = model_cls(random_state=0).fit(X, perturbations, controls)
    predictions = model.predict(["weak", "strong"], [False, False])

    assert len(predictions) == 2
    assert len(predictions[0]) == 2
    assert predictions[0][0] < predictions[1][0]
    assert predictions[0][1] < predictions[1][1]


def test_logistic_cell_state_classifier_predicts_state_labels_when_labels_available() -> (
    None
):
    classifier = CellStateLogisticClassifier(random_state=0).fit(
        perturbations=["control", "control", "weak", "weak", "strong", "strong"],
        labels=["baseline", "baseline", "mild", "mild", "stressed", "stressed"],
    )

    assert classifier.predict(["control", "weak", "strong"]) == [
        "baseline",
        "mild",
        "stressed",
    ]
    probabilities = classifier.predict_proba(["weak"])
    assert set(probabilities[0]) == {"baseline", "mild", "stressed"}
    assert sum(probabilities[0].values()) == pytest.approx(1.0)
