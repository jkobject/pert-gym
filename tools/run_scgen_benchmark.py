#!/usr/bin/env python3
"""Run the scGEN/scArches-style adapter benchmark smoke.

This is intentionally a tiny, read-only smoke. It validates the isolated scGEN
model environment, then exercises pert-gym's explicit AnnData/condition-control
contract through a lightweight scGEN-style adapter. The legacy model-ready-v0 path
still records manifest provenance with deterministic synthetic fallback, but
``--real-artifact`` runs on a bounded local real-expression export with no Lamin
access from the model env. Metrics are adapter/API smoke evidence only, not
biological performance claims.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pert_gym.benchmarks import (
    BenchmarkBatch,
    BenchmarkDataset,
    load_model_ready_v0_or_synthetic,
    load_scgen_viperturb_tiny,
)
from pert_gym.evaluate import evaluate_model
from pert_gym.models import MeanControlBaseline, ScgenPerturbationAdapter


@dataclass(frozen=True)
class RouteAssessment:
    route: str
    source_or_version: str
    dependency_posture: str
    mac_python_fit: str
    decision: str


ROUTES = [
    RouteAssessment(
        route="upstream scgen",
        source_or_version="PyPI scgen==2.1.0; upstream https://github.com/theislab/scgen",
        dependency_posture="Maintained PyPI package with AnnData/Scanpy runtime, but current resolver combinations failed import under Python 3.11 because scgen expects older scvi/anndata private APIs.",
        mac_python_fit="PyPI metadata requires Python >=3.7,<4.0 and installs, but import failed with latest scvi-tools (`scvi._compat` missing) and with scvi-tools<1 plus older anndata due mudata/anndata private-API mismatch.",
        decision="Do not enable upstream scgen training yet; use only as the semantic source for the AnnData condition/control contract until a frozen scverse stack/container is selected.",
    ),
    RouteAssessment(
        route="scArches/scvi-tools equivalent",
        source_or_version="PyPI scarches==0.6.1; current scvi-tools latest 1.4.3 requires Python >=3.12",
        dependency_posture="Heavier scvi/scanpy/lightning stack; useful future route but not needed for a tiny smoke adapter.",
        mac_python_fit="Not selected for this Python 3.11 env because current scvi-tools latest metadata moved to Python >=3.12.",
        decision="Documented alternative; defer until a production scArches route and Python policy are selected.",
    ),
    RouteAssessment(
        route="pert-gym ScgenPerturbationAdapter",
        source_or_version="src/pert_gym/models/scgen_adapter.py",
        dependency_posture="Tiny torch/anndata smoke path using maintained deps and no global scanpy import at package import time.",
        mac_python_fit="Runs in the same .venv-models/scgen env and keeps deep deps out of the base Lamin env.",
        decision="Chosen adapter for the current benchmark smoke; real-expression runs use a bounded local export and remain adapter/API evidence, not upstream scGen parity or biological performance evidence.",
    ),
]


def run_benchmark(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    if args.real_artifact is not None:
        dataset = load_scgen_viperturb_tiny(artifact_path=args.real_artifact)
    else:
        dataset = load_model_ready_v0_or_synthetic(manifest_path=args.manifest)
    model = ScgenPerturbationAdapter(
        condition_key="condition",
        control_value="control",
        batch_key="batch",
        cell_type_key="cell_type",
        latent_dim=args.latent_dim,
        hidden_dim=args.hidden_dim,
        condition_dim=args.condition_dim,
        epochs=args.epochs,
        lr=args.lr,
        beta_kl=args.beta_kl,
        seed=args.seed,
    )
    baseline = MeanControlBaseline()
    model_result = evaluate_model(model, train=dataset.train, test=dataset.test)
    baseline_result = evaluate_model(baseline, train=dataset.train, test=dataset.test)

    anndata_probe = run_tiny_anndata_probe(dataset.train, args)
    versions = package_versions(["scgen", "anndata", "scanpy", "scvi-tools", "mudata", "torch", "pert-gym"])
    upstream_import = probe_upstream_scgen_import()

    status = "synthetic_dependency_api_smoke_passed"
    if dataset.metadata.get("fallback") is None:
        status = "real_subset_smoke_passed"

    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "task_id": args.task_id,
        "status": status,
        "model": "scgen",
        "chosen_adapter": "pert_gym.models.ScgenPerturbationAdapter",
        "upstream_package": "scgen==2.1.0",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_executable": platform.python_implementation(),
        "dependency_versions": versions,
        "upstream_import_probe": upstream_import,
        "route_assessment": [asdict(route) for route in ROUTES],
        "data_contract": {
            "input_object": "AnnData",
            "X": "cell/sample x gene expression matrix; smoke uses tiny in-memory matrix only",
            "obs_condition_key": "condition",
            "control_value": "control",
            "obs_perturbation_identity": "non-control condition values map to perturbation identities",
            "optional_covariates": ["batch", "cell_type"],
            "held_out_semantics": "train/val/test hold out non-control perturbation identities; controls are available in each split for transfer baseline",
            "pert_gym_mapping": "BenchmarkBatch.X -> AnnData.X; perturbations+controls -> obs.condition/is_control; obs_covariates -> optional batch/cell_type",
        },
        "env_policy": {
            "required_env": ".venv-models/scgen",
            "base_env_dependency_pollution": False,
            "lamin_writes": False,
            "huge_matrix_loads": False,
            "heavy_training": False,
            "legacy_tensorflow_install_attempted": False,
            "scarches_full_stack_installed": False,
        },
        "dataset": dataset_summary(dataset),
        "metrics": {
            "scgen_adapter": {
                "model_name": model_result.model_name,
                "metrics": dict(model_result.metrics),
                "loss": model.loss_,
                "reconstruction_loss": model.reconstruction_loss_,
                "kl_loss": model.kl_loss_,
                "n_obs": model_result.n_obs,
                "n_features": model_result.n_features,
            },
            "mean_control_baseline": {
                "model_name": baseline_result.model_name,
                "metrics": dict(baseline_result.metrics),
                "n_obs": baseline_result.n_obs,
                "n_features": baseline_result.n_features,
            },
        },
        "comparison": {
            "delta_mae_vs_mean_control": model_result.metrics["mae"] - baseline_result.metrics["mae"],
            "delta_rmse_vs_mean_control": model_result.metrics["rmse"] - baseline_result.metrics["rmse"],
        },
        "anndata_probe": anndata_probe,
        "real_benchmark_feasibility": {
            "current_model_ready_v0_suitable_for_real_scgen": args.real_artifact is not None,
            "reason": "real VIPerturb bounded export loaded"
            if args.real_artifact is not None
            else "current model-ready-v0 loader records one tiny VIPerturb member and this runner uses synthetic fallback rather than a reviewed real matrix export",
            "required_follow_up": "Move beyond bounded smoke to a reviewed biological scGEN training/evaluation protocol before making performance claims."
            if args.real_artifact is not None
            else "Promote/export a bounded scGEN-ready expression subset with control rows plus at least three perturbed identities, canonical perturbation/is_control fields, optional batch/cell_type covariates, and a tiny local AnnData/triplet export for read-only model envs.",
        },
        "limitations": _limitations_for_dataset(dataset),
    }
    markdown = render_markdown(payload)
    return payload, markdown


def run_tiny_anndata_probe(train: BenchmarkBatch, args: argparse.Namespace) -> dict[str, Any]:
    import anndata as ad  # type: ignore[import-not-found]
    import numpy as np  # type: ignore[import-not-found]
    import pandas as pd  # type: ignore[import-not-found]

    X = np.asarray(train.X, dtype="float32")
    obs = pd.DataFrame(
        {
            "condition": ["control" if is_control else perturbation for perturbation, is_control in zip(train.perturbations, train.controls or [])],
            "is_control": list(train.controls or []),
            "batch": [covariates.get("assay", "synthetic_batch") for covariates in train.obs_covariates],
            "cell_type": [covariates.get("cell_type") or covariates.get("cell_line", "synthetic_context") for covariates in train.obs_covariates],
        },
        index=[f"cell_{idx:03d}" for idx in range(len(train.X))],
    )
    var = pd.DataFrame(index=list(train.feature_names or [f"gene_{idx}" for idx in range(X.shape[1])]))
    adata = ad.AnnData(X=X, obs=obs, var=var)
    adapter = ScgenPerturbationAdapter(
        condition_key="condition",
        control_value="control",
        batch_key="batch",
        cell_type_key="cell_type",
        latent_dim=args.latent_dim,
        hidden_dim=args.hidden_dim,
        condition_dim=args.condition_dim,
        epochs=max(1, min(args.epochs, 5)),
        lr=args.lr,
        beta_kl=args.beta_kl,
        seed=args.seed,
    )
    adapter.fit_anndata(adata)
    return {
        "status": "passed",
        "adata_shape": list(adata.shape),
        "obs_columns": list(adata.obs.columns),
        "condition_key": adapter.condition_key,
        "control_value": adapter.control_value,
        "observed_conditions": sorted(set(adata.obs["condition"].astype(str))),
        "adapter_contract": adapter.data_contract_,
    }


def probe_upstream_scgen_import() -> dict[str, Any]:
    try:
        import scgen  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001 - diagnostic payload for env smoke
        return {
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "interpretation": "upstream scgen==2.1.0 was assessed but is not enabled for benchmark training under the current Python 3.11 scverse resolver",
        }
    return {
        "status": "passed",
        "module_version": getattr(scgen, "__version__", "unknown"),
    }


def dataset_summary(dataset: BenchmarkDataset) -> dict[str, Any]:
    return {
        "source": dataset.source,
        "split_by": dataset.split_by,
        "metadata": dict(dataset.metadata),
        "splits": {
            "train": batch_summary(dataset.train),
            "val": batch_summary(dataset.val),
            "test": batch_summary(dataset.test),
        },
    }


def _limitations_for_dataset(dataset: BenchmarkDataset) -> list[str]:
    if dataset.metadata.get("fallback") is None:
        return [
            "Real bounded expression smoke only: rows and genes are deliberately tiny and selected for adapter verification, not biological effect-size claims.",
            "The adapter uses a tiny in-repo conditional VAE after upstream scgen dependency/import assessment failed; it does not claim parity with a full upstream scGen training run.",
            "Model env reads local JSON/AnnData export only; no Lamin writes or broad dataset curation are performed from the model env.",
            "Held-out non-control perturbation identities are split disjointly across train/val/test while controls are copied into each split for transfer-baseline semantics.",
        ]
    return [
        "Synthetic dependency/API smoke only unless dataset.metadata.fallback is None; current metrics are not biological performance claims.",
        "The adapter uses a tiny in-repo conditional VAE after upstream scgen dependency/import assessment failed; it does not claim parity with a full upstream scGen training run.",
        "No full X.h5ad payloads, broad Lamin writes, or base-env dependency installs were performed.",
        "Held-out perturbation behavior for unseen identities currently falls back through the tiny conditional adapter; production scGEN needs a reviewed real subset and explicit transfer task definition.",
    ]


def batch_summary(batch: BenchmarkBatch) -> dict[str, Any]:
    controls = list(batch.controls or [])
    non_control = sorted(
        {pert for pert, is_control in zip(batch.perturbations, controls) if not is_control}
    )
    return {
        "n_obs": len(batch.X),
        "n_features": len(batch.X[0]) if batch.X else 0,
        "n_controls": sum(1 for value in controls if value),
        "non_control_perturbations": non_control,
        "feature_names": list(batch.feature_names),
        "covariate_fields": sorted({field for cov in batch.obs_covariates for field in cov}),
    }


def package_versions(packages: list[str]) -> dict[str, str]:
    versions = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "not-installed"
    return versions


def render_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]["scgen_adapter"]
    baseline = payload["metrics"]["mean_control_baseline"]
    comparison = payload["comparison"]
    lines = [
        "# scGEN/scArches-style adapter benchmark smoke",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Decision",
        "",
        _decision_text(payload),
        "",
        "## Route assessment",
        "",
    ]
    for route in payload["route_assessment"]:
        lines.extend(
            [
                f"- **{route['route']}** — {route['decision']}",
                f"  - source/version: {route['source_or_version']}",
                f"  - deps: {route['dependency_posture']}",
                f"  - Mac/Python: {route['mac_python_fit']}",
            ]
        )
    lines.extend(
        [
            "",
            "## Smoke result",
            "",
            f"- status: `{payload['status']}`",
            f"- upstream scgen package: `{payload['dependency_versions'].get('scgen')}`; import probe: `{payload['upstream_import_probe']['status']}`",
            f"- dataset source: `{payload['dataset']['source']}`; fallback: `{payload['dataset']['metadata'].get('fallback')}`",
            _provenance_text(payload),
            f"- scGEN adapter MAE/RMSE: `{metrics['metrics']['mae']:.6f}` / `{metrics['metrics']['rmse']:.6f}`",
            f"- MeanControlBaseline MAE/RMSE: `{baseline['metrics']['mae']:.6f}` / `{baseline['metrics']['rmse']:.6f}`",
            f"- delta MAE/RMSE vs mean-control: `{comparison['delta_mae_vs_mean_control']:.6f}` / `{comparison['delta_rmse_vs_mean_control']:.6f}`",
            f"- adapter final loss/recon/KL: `{metrics['loss']:.6f}` / `{metrics['reconstruction_loss']:.6f}` / `{metrics['kl_loss']:.6f}`",
            "",
            "## AnnData contract",
            "",
        ]
    )
    for key, value in payload["data_contract"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Real benchmark feasibility",
            "",
            f"- current model-ready-v0 suitable for real scGEN: `{payload['real_benchmark_feasibility']['current_model_ready_v0_suitable_for_real_scgen']}`",
            f"- reason: {payload['real_benchmark_feasibility']['reason']}",
            f"- required follow-up: {payload['real_benchmark_feasibility']['required_follow_up']}",
            "",
            "## Safety",
            "",
            "- no Lamin writes from model env; real export was generated separately in the base env",
            "- no huge matrix loads",
            "- no base env dependency pollution",
            "- no TensorFlow 1.x / legacy trVAE install attempts",
            "",
            "## Limitations",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["limitations"])
    lines.append("")
    return "\n".join(lines)


def _provenance_text(payload: dict[str, Any]) -> str:
    metadata = payload["dataset"]["metadata"]
    if metadata.get("fallback") is None:
        source = metadata.get("source", {})
        export = metadata.get("export", {})
        return (
            f"- real export provenance: `{source.get('dataset_prefix')}`; "
            f"AnnData: `{metadata.get('adata_path')}`; "
            f"shape: `{export.get('n_obs')} × {export.get('n_vars')}`"
        )
    return f"- collection provenance: `{metadata.get('model_ready_collection_key')}` ({metadata.get('model_ready_member_count')} member)"


def _decision_text(payload: dict[str, Any]) -> str:
    if payload["dataset"]["metadata"].get("fallback") is None:
        return "Use `pert_gym.models.ScgenPerturbationAdapter` on the bounded real VIPerturb AnnData/JSON export for the current scGEN-ready smoke. Upstream `scgen==2.1.0` remains assessed but not enabled under the current Python 3.11 scverse resolver; the adapter maps scGEN's AnnData `X` + condition/control contract onto a tiny conditional VAE. This is a real-expression adapter benchmark, not a biological performance claim."
    return "Use `pert_gym.models.ScgenPerturbationAdapter` for the current tiny benchmark smoke. Upstream `scgen==2.1.0` was assessed in isolation but is not enabled under the current Python 3.11 scverse resolver. The adapter maps scGEN's AnnData `X` + condition/control contract onto a tiny conditional VAE. This is deliberately smoke-only until a real scGEN-ready subset exists."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("artifacts/schema_audit/model_ready_subset_20260621.json"))
    parser.add_argument("--real-artifact", type=Path, default=None)
    parser.add_argument("--artifact-dir", type=Path, default=Path("artifacts/model_benchmarks"))
    parser.add_argument("--date", default=datetime.now(UTC).strftime("%Y%m%d"))
    parser.add_argument("--task-id", default="t_94e90d90")
    parser.add_argument("--latent-dim", type=int, default=2)
    parser.add_argument("--hidden-dim", type=int, default=8)
    parser.add_argument("--condition-dim", type=int, default=2)
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--lr", type=float, default=0.03)
    parser.add_argument("--beta-kl", type=float, default=1e-3)
    parser.add_argument("--seed", type=int, default=41)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload, markdown = run_benchmark(args)
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    prefix = "scgen_real" if args.real_artifact is not None else "scgen"
    json_path = args.artifact_dir / f"{prefix}_{args.date}.json"
    md_path = args.artifact_dir / f"{prefix}_{args.date}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    md_path.write_text(markdown)
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(
        "scgen smoke: "
        f"status={payload['status']} "
        f"mae={payload['metrics']['scgen_adapter']['metrics']['mae']:.6f} "
        f"rmse={payload['metrics']['scgen_adapter']['metrics']['rmse']:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
