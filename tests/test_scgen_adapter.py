import pytest

from pert_gym.models import ScgenPerturbationAdapter


def test_scgen_adapter_fit_predict_generic_contract() -> None:
    pytest.importorskip("torch")

    model = ScgenPerturbationAdapter(
        epochs=1, latent_dim=2, hidden_dim=4, condition_dim=2
    )
    X = [
        [1.0, 1.0],
        [1.1, 0.9],
        [2.0, 1.0],
        [1.0, 2.0],
    ]
    perturbations = ["control", "control", "pert_a", "pert_b"]
    controls = [True, True, False, False]

    model.fit(X, perturbations=perturbations, controls=controls)
    pred = model.predict(["pert_a", "pert_b"], controls=[False, False])

    assert len(pred) == 2
    assert len(pred[0]) == 2
    assert model.loss_ is not None
    assert model.data_contract_ is not None
    assert model.data_contract_["condition_key"] == "condition"


def test_scgen_adapter_requires_anndata_condition_key() -> None:
    class FakeObs(dict):
        pass

    class FakeAnnData:
        X = [[1.0, 1.0]]
        obs = FakeObs()

    model = ScgenPerturbationAdapter()
    try:
        model.fit_anndata(FakeAnnData())
    except ValueError as exc:
        assert "condition_key" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("fit_anndata should reject missing condition_key")
