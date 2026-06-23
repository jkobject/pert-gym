#!/usr/bin/env python3
"""Run a read-only CPA smoke benchmark on the canonical benchmark loader.

This runner is intentionally meant for the isolated `.venv-models/cpa` env. It
loads the reviewed model-ready-v0 benchmark contract, adapts the small in-memory
batches to the standalone CPA adapter, and compares against a trivial
mean-control baseline without touching Lamin or materializing large matrices.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from pert_gym.benchmarks import (
    BenchmarkBatch,
    BenchmarkDataset,
    load_model_ready_v0_or_synthetic,
)
from pert_gym.evaluate import evaluate_model
from pert_gym.metrics import pearson_correlation
from pert_gym.models import CompositionalPerturbationAutoencoder, MeanControlBaseline
from pert_gym.models.base import PerturbationModel

DEFAULT_MANIFEST = Path("artifacts/schema_audit/model_ready_subset_20260621.json")
DEFAULT_ARTIFACT_DIR = Path("artifacts/model_benchmarks")
MAX_SAFE_MATRIX_CELLS = 100_000


@dataclass(frozen=True)
class CPAInputMapping:
    expression: str
    target_response: str
    perturbation_encoding: str
    control_encoding: str
    covariates: dict[str, list[str]]
    required_fields: list[str]
    limitations: list[str]


@dataclass(frozen=True)
class SplitIntegrity:
    non_control_train_val_overlap: list[str]
    non_control_train_test_overlap: list[str]
    non_control_val_test_overlap: list[str]
    controls_copied_to_each_split: bool
    leakage_free: bool


@dataclass(frozen=True)
class MatrixSafety:
    n_obs_total_across_splits: int
    n_features: int
    matrix_cells_across_splits: int
    max_safe_matrix_cells: int
    metadata_only_loader: bool
    no_huge_matrix_load: bool


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    parser.add_argument("--date", default=datetime.now(UTC).strftime("%Y%m%d"))
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--seed", type=int, default=13)
    args = parser.parse_args()

    dataset = load_model_ready_v0_or_synthetic(manifest_path=args.manifest)
    split_integrity = verify_split_integrity(dataset)
    matrix_safety = verify_matrix_safety(dataset)
    if not split_integrity.leakage_free:
        raise RuntimeError(f"Non-control perturbation leakage detected: {split_integrity}")
    if not matrix_safety.no_huge_matrix_load:
        raise RuntimeError(f"Benchmark matrix exceeds safety cap: {matrix_safety}")

    cpa = CompositionalPerturbationAutoencoder(
        latent_dim=2,
        hidden_dim=8,
        perturbation_dim=2,
        epochs=args.epochs,
        lr=0.03,
        seed=args.seed,
    )
    baseline = MeanControlBaseline()
    model_results = [
        run_model(cpa, dataset.train, dataset.test),
        run_model(baseline, dataset.train, dataset.test),
    ]

    cpa_metrics = model_results[0]["metrics"]
    baseline_metrics = model_results[1]["metrics"]
    payload: dict[str, Any] = {
        "created_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "runner": "tools/run_cpa_benchmark.py",
        "execution_policy": {
            "required_env": ".venv-models/cpa",
            "no_lamin_writes": True,
            "no_heavy_training": True,
            "epochs": args.epochs,
            "seed": args.seed,
        },
        "source": dataset.source,
        "loader_metadata": dict(dataset.metadata),
        "manifest_path": str(args.manifest),
        "split_by": dataset.split_by,
        "split_integrity": asdict(split_integrity),
        "matrix_safety": asdict(matrix_safety),
        "input_mapping": asdict(describe_cpa_input_mapping(dataset)),
        "splits": split_details(dataset),
        "models": model_results,
        "baseline_comparison": {
            "baseline_model": model_results[1]["model_name"],
            "cpa_model": model_results[0]["model_name"],
            "delta_mse_vs_baseline": cpa_metrics["mse"] - baseline_metrics["mse"],
            "delta_r2_vs_baseline": cpa_metrics["r2"] - baseline_metrics["r2"],
            "delta_pearson_vs_baseline": cpa_metrics["pearson"] - baseline_metrics["pearson"],
        },
    }

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.artifact_dir / f"cpa_{args.date}.json"
    md_path = args.artifact_dir / f"cpa_{args.date}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    md_path.write_text(render_markdown(payload, json_path=json_path) + "\n")

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    for result in model_results:
        metrics = result["metrics"]
        print(
            f"{result['model_name']}: "
            f"r2={metrics['r2']:.4f} pearson={metrics['pearson']:.4f} mse={metrics['mse']:.4f}"
        )
    return 0


def run_model(model: PerturbationModel, train: BenchmarkBatch, test: BenchmarkBatch) -> dict[str, Any]:
    result = evaluate_model(model, train=train, test=test)
    output = {
        "model_name": result.model_name,
        "metrics": expression_metrics(test.X, result.predictions),
        "n_test_obs": result.n_obs,
        "n_features": result.n_features,
    }
    if isinstance(model, CompositionalPerturbationAutoencoder):
        output["training"] = {
            "loss": model.loss_,
            "perturbation_to_index": dict(model.perturbation_to_index_ or {}),
        }
    return output


def describe_cpa_input_mapping(dataset: BenchmarkDataset) -> CPAInputMapping:
    covariates = {
        split: sorted({field for row in batch.obs_covariates for field in row})
        for split, batch in (
            ("train", dataset.train),
            ("val", dataset.val),
            ("test", dataset.test),
        )
    }
    return CPAInputMapping(
        expression="BenchmarkBatch.X -> dense float expression/response matrix passed to CPA fit().",
        target_response="BenchmarkBatch.target_response equals X for direct expression prediction in the current loader contract.",
        perturbation_encoding=(
            "BenchmarkBatch.perturbations string labels are sorted over non-control "
            "training perturbations and mapped to contiguous integer embedding indices; "
            "held-out unseen perturbations currently fall back to index 0 in the smoke adapter."
        ),
        control_encoding="BenchmarkBatch.controls boolean mask zeroes perturbation embeddings for control rows and defines the control latent centroid.",
        covariates=covariates,
        required_fields=["X", "perturbations", "controls/is_control", "obs_covariates optional", "feature_names optional"],
        limitations=[
            "Current benchmark is metadata-only model-ready-v0 with deterministic synthetic fallback.",
            "Covariates are documented and carried through the loader but not consumed by the standalone CPA smoke adapter.",
            "No dose, combination, adversarial covariate, batching, checkpoint, or production scvi-tools CPA route is exercised.",
        ],
    )


def expression_metrics(y_true: Sequence[Sequence[float]], y_pred: Sequence[Sequence[float]]) -> dict[str, float]:
    true = flatten(y_true)
    pred = flatten(y_pred)
    if len(true) != len(pred):
        raise ValueError("y_true and y_pred must flatten to the same length.")
    if not true:
        return {"mse": 0.0, "r2": 0.0, "pearson": 0.0}

    mse = sum((t - p) ** 2 for t, p in zip(true, pred)) / len(true)
    mean_true = sum(true) / len(true)
    total_sum_squares = sum((t - mean_true) ** 2 for t in true)
    if total_sum_squares == 0:
        r2 = 1.0 if mse == 0 else 0.0
    else:
        residual_sum_squares = sum((t - p) ** 2 for t, p in zip(true, pred))
        r2 = 1.0 - (residual_sum_squares / total_sum_squares)
    return {"mse": mse, "r2": r2, "pearson": pearson_correlation(true, pred)}


def verify_split_integrity(dataset: BenchmarkDataset) -> SplitIntegrity:
    train = non_control_perturbations(dataset.train)
    val = non_control_perturbations(dataset.val)
    test = non_control_perturbations(dataset.test)
    train_val = sorted(train & val)
    train_test = sorted(train & test)
    val_test = sorted(val & test)
    controls_copied = all(any(batch.controls or []) for batch in (dataset.train, dataset.val, dataset.test))
    return SplitIntegrity(
        non_control_train_val_overlap=train_val,
        non_control_train_test_overlap=train_test,
        non_control_val_test_overlap=val_test,
        controls_copied_to_each_split=controls_copied,
        leakage_free=not (train_val or train_test or val_test),
    )


def verify_matrix_safety(dataset: BenchmarkDataset) -> MatrixSafety:
    n_obs_total = sum(len(batch.X) for batch in (dataset.train, dataset.val, dataset.test))
    n_features = len(dataset.train.X[0]) if dataset.train.X else 0
    matrix_cells = n_obs_total * n_features
    metadata_only = dataset.metadata.get("loader") == "model_ready_v0_or_synthetic" and dataset.metadata.get("fallback") == "synthetic"
    return MatrixSafety(
        n_obs_total_across_splits=n_obs_total,
        n_features=n_features,
        matrix_cells_across_splits=matrix_cells,
        max_safe_matrix_cells=MAX_SAFE_MATRIX_CELLS,
        metadata_only_loader=metadata_only,
        no_huge_matrix_load=metadata_only and matrix_cells <= MAX_SAFE_MATRIX_CELLS,
    )


def split_details(dataset: BenchmarkDataset) -> dict[str, dict[str, Any]]:
    return {
        "train": describe_batch(dataset.train),
        "val": describe_batch(dataset.val),
        "test": describe_batch(dataset.test),
    }


def describe_batch(batch: BenchmarkBatch) -> dict[str, Any]:
    controls = list(batch.controls or [])
    return {
        "n_obs": len(batch.X),
        "n_features": len(batch.X[0]) if batch.X else 0,
        "n_controls": sum(1 for is_control in controls if is_control),
        "non_control_perturbations": sorted(non_control_perturbations(batch)),
        "all_perturbations": sorted(set(batch.perturbations)),
        "covariate_fields": sorted({field for row in batch.obs_covariates for field in row}),
    }


def non_control_perturbations(batch: BenchmarkBatch) -> set[str]:
    return {
        perturbation
        for perturbation, is_control in zip(batch.perturbations, batch.controls or [])
        if not is_control
    }


def flatten(matrix: Sequence[Sequence[float]]) -> list[float]:
    return [float(value) for row in matrix for value in row]


def render_markdown(payload: dict[str, Any], *, json_path: Path) -> str:
    comparison = payload["baseline_comparison"]
    lines = [
        "# CPA benchmark smoke",
        "",
        f"- JSON artifact: `{json_path}`",
        f"- Source: `{payload['source']}`",
        f"- Split policy: `{payload['split_by']}`",
        f"- Loader: `{payload['loader_metadata'].get('loader')}`",
        f"- Fallback: `{payload['loader_metadata'].get('fallback')}`",
        f"- Model-ready collection: `{payload['loader_metadata'].get('model_ready_collection_key')}`",
        f"- Required env: `{payload['execution_policy']['required_env']}`",
        f"- Epochs: `{payload['execution_policy']['epochs']}`",
        f"- Leakage-free non-control perturbation splits: `{payload['split_integrity']['leakage_free']}`",
        f"- No huge matrix load: `{payload['matrix_safety']['no_huge_matrix_load']}` ({payload['matrix_safety']['matrix_cells_across_splits']} cells across split batches)",
        "",
        "## CPA input mapping",
        "",
    ]
    mapping = payload["input_mapping"]
    lines.extend(
        [
            f"- Expression: {mapping['expression']}",
            f"- Target response: {mapping['target_response']}",
            f"- Perturbation encoding: {mapping['perturbation_encoding']}",
            f"- Control encoding: {mapping['control_encoding']}",
            f"- Required fields: {', '.join(mapping['required_fields'])}",
            f"- Covariates by split: `{mapping['covariates']}`",
            "",
            "## Split details",
            "",
            "| split | n_obs | n_controls | covariates | non-control perturbations |",
            "| --- | ---: | ---: | --- | --- |",
        ]
    )
    for split, details in payload["splits"].items():
        lines.append(
            f"| {split} | {details['n_obs']} | {details['n_controls']} | "
            f"{', '.join(details['covariate_fields'])} | "
            f"{', '.join(details['non_control_perturbations'])} |"
        )
    lines.extend(
        [
            "",
            "## Test metrics",
            "",
            "| model | R2 | Pearson | MSE | notes |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for result in payload["models"]:
        metrics = result["metrics"]
        notes = ""
        if "training" in result:
            notes = f"loss={result['training']['loss']:.6f}; perturbations={result['training']['perturbation_to_index']}"
        lines.append(
            f"| {result['model_name']} | {metrics['r2']:.6f} | "
            f"{metrics['pearson']:.6f} | {metrics['mse']:.6f} | {notes} |"
        )
    lines.extend(
        [
            "",
            "## Trivial baseline comparison",
            "",
            f"Compared to `{comparison['baseline_model']}`, `{comparison['cpa_model']}` has delta MSE `{comparison['delta_mse_vs_baseline']:.6f}`, delta R2 `{comparison['delta_r2_vs_baseline']:.6f}`, and delta Pearson `{comparison['delta_pearson_vs_baseline']:.6f}` on the tiny test split.",
            "",
            "## Safety notes / limitations",
            "",
        ]
    )
    for limitation in mapping["limitations"]:
        lines.append(f"- {limitation}")
    lines.append("- This command performs no Lamin writes and does not materialize heavy matrices.")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
