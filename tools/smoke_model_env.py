#!/usr/bin/env python3
"""Read-only smoke checks for isolated pert-gym model environments."""

from __future__ import annotations

import argparse
from pathlib import Path

DEFAULT_MODEL_READY_MANIFEST = Path("artifacts/schema_audit/model_ready_subset_20260621.json")


def smoke_baselines() -> None:
    from pert_gym.evaluate import EvaluationBatch, evaluate_model
    from pert_gym.models import MeanControlBaseline, MeanPerturbationBaseline

    train = EvaluationBatch(
        X=[[1.0, 2.0], [3.0, 6.0], [10.0, 10.0], [14.0, 18.0]],
        perturbations=["control", "control", "drug_a", "drug_b"],
        controls=[True, True, False, False],
    )
    test = EvaluationBatch(
        X=[[2.0, 4.0], [12.0, 14.0]],
        perturbations=["drug_a", "drug_b"],
        controls=[False, False],
    )
    for model in (MeanControlBaseline(), MeanPerturbationBaseline()):
        result = evaluate_model(model, train=train, test=test)
        assert result.n_obs == 2
        assert result.n_features == 2
        assert "mae" in result.metrics
        print(f"{result.model_name}: mae={result.metrics['mae']:.3f}")


def smoke_classical_model_ready(manifest: Path = DEFAULT_MODEL_READY_MANIFEST) -> None:
    from pert_gym.benchmarks import load_model_ready_v0_or_synthetic
    from pert_gym.evaluate import evaluate_model
    from pert_gym.models import (
        BinarySplitBaseline,
        ElasticNetPerturbationRegressor,
        GradientBoostingPerturbationRegressor,
        LinearPerturbationRegressor,
        RandomForestPerturbationRegressor,
        RidgePerturbationRegressor,
    )

    dataset = load_model_ready_v0_or_synthetic(manifest_path=manifest)
    models = (
        BinarySplitBaseline(),
        LinearPerturbationRegressor(random_state=0),
        RidgePerturbationRegressor(random_state=0),
        ElasticNetPerturbationRegressor(random_state=0),
        RandomForestPerturbationRegressor(random_state=0, n_estimators=8),
        GradientBoostingPerturbationRegressor(random_state=0, n_estimators=8),
    )
    print(
        "model-ready-v0 smoke: "
        f"source={dataset.source} fallback={dataset.metadata.get('fallback')} "
        f"collection={dataset.metadata.get('model_ready_collection_key')}"
    )
    for model in models:
        result = evaluate_model(model, train=dataset.train, test=dataset.test)
        assert result.n_obs == len(dataset.test.X)
        assert result.n_features == len(dataset.test.X[0])
        assert "mae" in result.metrics
        print(f"{result.model_name}: mae={result.metrics['mae']:.3f}")


def smoke_scpram_import() -> None:
    """Import scPRAM and instantiate its CPU module; no training or data writes."""

    import scpram
    from scpram import models

    model = models.SCPRAM(input_dim=4, device="cpu")
    assert getattr(model, "device", None) == "cpu"
    print(f"scpram import smoke passed: version={getattr(scpram, '__version__', 'unknown')}")
    print(f"scpram model instantiated: {model.__class__.__name__}(input_dim=4, device=cpu)")


def smoke_chemcpa() -> None:
    """Import chemCPA deps and compute a tiny RDKit fingerprint; no training/writes."""

    import importlib.util

    import numpy as np
    import torch
    from rdkit import Chem
    from rdkit.Chem import rdFingerprintGenerator

    smoke_baselines()
    if importlib.util.find_spec("chemCPA") is None and importlib.util.find_spec("chemcpa") is None:
        raise RuntimeError("Neither chemCPA nor chemcpa import spec is available")
    mol = Chem.MolFromSmiles("CCO")
    if mol is None:
        raise RuntimeError("RDKit failed to parse ethanol SMILES")
    generator = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=128)
    fp = np.asarray(generator.GetFingerprint(mol), dtype=np.int8)
    if fp.shape != (128,) or int(fp.sum()) <= 0:
        raise RuntimeError(f"Unexpected fingerprint shape/sum: {fp.shape}, {fp.sum()}")
    print(f"chemcpa: torch={torch.__version__} rdkit_morgan_bits={int(fp.sum())}/128")
    print("chemcpa: chemCPA import/fingerprint smoke passed; full model adapter pending")


def smoke_import_only(model: str) -> None:
    # Deep envs may not have model implementations yet. This still proves the
    # isolated env imports the local package without touching Lamin.
    smoke_baselines()
    print(f"{model}: local pert_gym import smoke passed; model implementation pending")


def smoke_cpa_model_ready(manifest: Path = DEFAULT_MODEL_READY_MANIFEST) -> None:
    from pert_gym.benchmarks import load_model_ready_v0_or_synthetic
    from pert_gym.evaluate import evaluate_model
    from pert_gym.models import CompositionalPerturbationAutoencoder

    dataset = load_model_ready_v0_or_synthetic(manifest_path=manifest)
    print(
        "model-ready-v0 smoke: "
        f"source={dataset.source} fallback={dataset.metadata.get('fallback')} "
        f"collection={dataset.metadata.get('model_ready_collection_key')}"
    )
    model = CompositionalPerturbationAutoencoder(
        latent_dim=2,
        hidden_dim=8,
        perturbation_dim=2,
        epochs=20,
        lr=0.03,
        seed=13,
    )
    result = evaluate_model(model, train=dataset.train, test=dataset.test)
    assert result.n_obs == len(dataset.test.X)
    assert result.n_features == len(dataset.test.X[0])
    assert "mae" in result.metrics
    print(
        f"{result.model_name}: mae={result.metrics['mae']:.3f} "
        f"loss={model.loss_:.3f}"
    )


def smoke_trvae_replacement_model_ready(
    manifest: Path = DEFAULT_MODEL_READY_MANIFEST,
) -> None:
    """Run the maintained trVAE replacement on the safe model-ready smoke path."""

    from pert_gym.benchmarks import load_model_ready_v0_or_synthetic
    from pert_gym.evaluate import evaluate_model
    from pert_gym.models import ConditionalPerturbationVAE

    dataset = load_model_ready_v0_or_synthetic(manifest_path=manifest)
    print(
        "model-ready-v0 smoke: "
        f"source={dataset.source} fallback={dataset.metadata.get('fallback')} "
        f"collection={dataset.metadata.get('model_ready_collection_key')}"
    )
    model = ConditionalPerturbationVAE(
        latent_dim=2,
        hidden_dim=8,
        condition_dim=2,
        epochs=25,
        lr=0.03,
        beta_kl=1e-3,
        seed=31,
    )
    result = evaluate_model(model, train=dataset.train, test=dataset.test)
    assert result.n_obs == len(dataset.test.X)
    assert result.n_features == len(dataset.test.X[0])
    assert "mae" in result.metrics
    print(
        f"{result.model_name}: mae={result.metrics['mae']:.3f} "
        f"loss={model.loss_:.3f} recon={model.reconstruction_loss_:.3f} "
        f"kl={model.kl_loss_:.3f}"
    )


def smoke_scgen_model_ready(manifest: Path = DEFAULT_MODEL_READY_MANIFEST) -> None:
    """Run the pert-gym scGEN-style tiny adapter in the isolated env."""

    from pert_gym.benchmarks import load_model_ready_v0_or_synthetic
    from pert_gym.evaluate import evaluate_model
    from pert_gym.models import ScgenPerturbationAdapter

    dataset = load_model_ready_v0_or_synthetic(manifest_path=manifest)
    print(
        "model-ready-v0 scgen smoke: "
        f"source={dataset.source} fallback={dataset.metadata.get('fallback')} "
        f"collection={dataset.metadata.get('model_ready_collection_key')}"
    )
    model = ScgenPerturbationAdapter(
        latent_dim=2,
        hidden_dim=8,
        condition_dim=2,
        epochs=20,
        lr=0.03,
        beta_kl=1e-3,
        seed=41,
    )
    result = evaluate_model(model, train=dataset.train, test=dataset.test)
    assert result.n_obs == len(dataset.test.X)
    assert result.n_features == len(dataset.test.X[0])
    assert "mae" in result.metrics
    print("upstream scgen import smoke skipped: package assessed separately; current route uses in-repo adapter")
    print(
        f"{result.model_name}: mae={result.metrics['mae']:.3f} "
        f"loss={model.loss_:.3f} recon={model.reconstruction_loss_:.3f} "
        f"kl={model.kl_loss_:.3f}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MODEL_READY_MANIFEST,
        help="Path to the model-ready v0 manifest JSON for safe metadata-only smoke.",
    )
    args = parser.parse_args()
    if args.model == "baselines":
        smoke_baselines()
    elif args.model == "classical":
        smoke_classical_model_ready(args.manifest)
    elif args.model == "cpa":
        smoke_cpa_model_ready(args.manifest)
    elif args.model == "trvae-replacement":
        smoke_trvae_replacement_model_ready(args.manifest)
    elif args.model == "scgen":
        smoke_scgen_model_ready(args.manifest)
    elif args.model == "gears":
        from tools.run_gears_benchmark import build_payload

        payload, _markdown = build_payload(args)
        import_smoke = payload["dependency_import_smoke"]
        if import_smoke["status"] != "passed":
            raise RuntimeError(
                f"GEARS import smoke failed: {import_smoke.get('error_type')}: "
                f"{import_smoke.get('error')}"
            )
        synthetic = payload["synthetic_contract_smoke"]
        print(
            "gears: import/API smoke passed; "
            f"synthetic contract mae={synthetic['mae']:.3f} "
            f"rmse={synthetic['rmse']:.3f}"
        )
    elif args.model == "chemcpa":
        smoke_chemcpa()
    elif args.model == "scpram":
        smoke_scpram_import()
    else:
        smoke_import_only(args.model)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
