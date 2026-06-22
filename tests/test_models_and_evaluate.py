from math import isclose

from pert_gym.evaluate import EvaluationBatch, evaluate_model, evaluate_predictions
from pert_gym.models import MeanControlBaseline, MeanPerturbationBaseline


def test_mean_control_baseline_predicts_training_control_centroid() -> None:
    model = MeanControlBaseline().fit(
        X=[[1.0, 2.0], [3.0, 6.0], [10.0, 10.0]],
        perturbations=["control", "control", "drug_a"],
        controls=[True, True, False],
    )

    predictions = model.predict(perturbations=["drug_a", "unseen"], controls=[False, False])

    assert predictions == [[2.0, 4.0], [2.0, 4.0]]
    assert model.n_features_ == 2


def test_mean_perturbation_baseline_predicts_global_perturbed_centroid() -> None:
    model = MeanPerturbationBaseline().fit(
        X=[[1.0, 1.0], [3.0, 5.0], [10.0, 10.0], [14.0, 18.0]],
        perturbations=["control", "control", "drug_a", "drug_a"],
        controls=[True, True, False, False],
    )

    predictions = model.predict(perturbations=["drug_a", "unseen"], controls=[False, False])

    assert predictions == [[12.0, 14.0], [12.0, 14.0]]


def test_evaluate_predictions_computes_basic_expression_metrics() -> None:
    metrics = evaluate_predictions(
        y_true=[[2.0, 4.0], [4.0, 8.0]],
        y_pred=[[1.0, 4.0], [5.0, 6.0]],
    )

    assert isclose(metrics["mae"], 1.0)
    assert isclose(metrics["rmse"], (6.0 / 4.0) ** 0.5)


def test_evaluate_model_accepts_small_in_memory_batch() -> None:
    model = MeanControlBaseline()
    train = EvaluationBatch(
        X=[[1.0, 2.0], [3.0, 6.0], [10.0, 10.0]],
        perturbations=["control", "control", "drug_a"],
        controls=[True, True, False],
    )
    test = EvaluationBatch(
        X=[[2.0, 4.0], [8.0, 8.0]],
        perturbations=["drug_a", "drug_b"],
        controls=[False, False],
    )

    result = evaluate_model(model, train=train, test=test)

    assert result.model_name == "mean_control"
    assert result.n_obs == 2
    assert result.n_features == 2
    assert result.predictions == [[2.0, 4.0], [2.0, 4.0]]
    assert isclose(result.metrics["mae"], 2.5)
