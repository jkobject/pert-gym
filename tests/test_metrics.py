from math import isclose

from pert_gym.metrics import (
    centroid_accuracy,
    deg_weights_from_t_scores,
    differential_expression_profile,
    differential_expression_similarity,
    mean_absolute_error,
    pearson_delta,
    perturbation_distance_similarity,
    rmse_delta,
    weighted_delta_r2,
    weighted_mse,
)


def test_arc_metrics() -> None:
    mae = mean_absolute_error([1.0, 2.0, 3.0], [1.5, 1.0, 2.5])
    assert isclose(mae, 2.0 / 3.0)

    pds = perturbation_distance_similarity([1.0, 2.0], [1.0, 3.0])
    assert isclose(pds, 2.0 / 3.0)

    des = differential_expression_similarity(
        de_true={"A", "B", "C"},
        de_pred={"A", "C", "D", "E"},
        pred_logfc={"A": 0.3, "C": 1.1, "D": 0.1, "E": 0.05},
    )
    assert isclose(des, 2.0 / 3.0)


def test_weighted_metrics() -> None:
    weights = deg_weights_from_t_scores([0.5, 2.0, 1.0])
    assert isclose(sum(weights), 1.0)
    assert weights[1] > weights[2] > weights[0]

    wmse = weighted_mse([1.0, 2.0], [1.5, 1.0], [0.25, 0.75])
    assert isclose(wmse, 0.8125)

    r2w = weighted_delta_r2(
        y_true=[3.0, 2.0],
        y_pred=[3.0, 2.0],
        control_true=[1.0, 1.0],
        control_pred=[1.0, 1.0],
        weights=[0.5, 0.5],
    )
    assert isclose(r2w, 1.0)


def test_systema_metrics() -> None:
    delta = differential_expression_profile(
        perturbation_samples=[[2.0, 4.0], [4.0, 6.0]],
        reference_samples=[[1.0, 1.0], [3.0, 1.0]],
    )
    assert delta == [1.0, 4.0]

    corr = pearson_delta([0.0, 1.0, 2.0], [0.1, 0.9, 2.1], top_k=2)
    assert corr > 0.99

    rmse = rmse_delta([1.0, 2.0], [1.0, 4.0])
    assert isclose(rmse, 2**0.5)

    acc = centroid_accuracy(
        predicted_profiles={"p1": [0.1, 0.0], "p2": [1.0, 1.1]},
        true_profiles={"p1": [0.0, 0.0], "p2": [1.0, 1.0]},
    )
    assert isclose(acc, 1.0)
