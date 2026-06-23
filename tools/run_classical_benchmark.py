#!/usr/bin/env python3
"""Run read-only classical baseline benchmarks on the canonical small loader."""

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
from pert_gym.metrics import pearson_correlation
from pert_gym.models import (
    BinarySplitBaseline,
    ElasticNetPerturbationRegressor,
    GradientBoostingPerturbationRegressor,
    LinearPerturbationRegressor,
    MeanControlBaseline,
    MeanPerturbationBaseline,
    RandomForestPerturbationRegressor,
    RidgePerturbationRegressor,
)
from pert_gym.models.base import PerturbationModel

DEFAULT_MANIFEST = Path("artifacts/schema_audit/model_ready_subset_20260621.json")
DEFAULT_ARTIFACT_DIR = Path("artifacts/model_benchmarks")
MAX_SAFE_MATRIX_CELLS = 100_000


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
    args = parser.parse_args()

    dataset = load_model_ready_v0_or_synthetic(manifest_path=args.manifest)
    split_integrity = verify_split_integrity(dataset)
    matrix_safety = verify_matrix_safety(dataset)
    if not split_integrity.leakage_free:
        raise RuntimeError(f"Non-control perturbation leakage detected: {split_integrity}")
    if not matrix_safety.no_huge_matrix_load:
        raise RuntimeError(f"Benchmark matrix exceeds safety cap: {matrix_safety}")

    model_results = []
    for model in benchmark_models():
        model_results.append(run_model(model, dataset.train, dataset.test))

    payload: dict[str, Any] = {
        "created_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": dataset.source,
        "loader_metadata": dict(dataset.metadata),
        "manifest_path": str(args.manifest),
        "split_by": dataset.split_by,
        "split_integrity": asdict(split_integrity),
        "matrix_safety": asdict(matrix_safety),
        "splits": split_details(dataset),
        "models": model_results,
    }

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.artifact_dir / f"classical_{args.date}.json"
    md_path = args.artifact_dir / f"classical_{args.date}.md"
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


def benchmark_models() -> tuple[PerturbationModel, ...]:
    return (
        MeanControlBaseline(),
        MeanPerturbationBaseline(),
        BinarySplitBaseline(),
        LinearPerturbationRegressor(random_state=0),
        RidgePerturbationRegressor(random_state=0),
        ElasticNetPerturbationRegressor(random_state=0),
        RandomForestPerturbationRegressor(random_state=0, n_estimators=8),
        GradientBoostingPerturbationRegressor(random_state=0, n_estimators=8),
    )


def run_model(model: PerturbationModel, train: BenchmarkBatch, test: BenchmarkBatch) -> dict[str, Any]:
    fitted = model.fit(X=train.X, perturbations=train.perturbations, controls=train.controls)
    predictions = fitted.predict(perturbations=test.perturbations, controls=test.controls)
    return {
        "model_name": getattr(fitted, "name", fitted.__class__.__name__),
        "metrics": expression_metrics(test.X, predictions),
        "n_test_obs": len(test.X),
        "n_features": len(test.X[0]) if test.X else 0,
    }


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
    lines = [
        "# Classical baseline benchmark",
        "",
        f"- JSON artifact: `{json_path}`",
        f"- Source: `{payload['source']}`",
        f"- Split policy: `{payload['split_by']}`",
        f"- Loader: `{payload['loader_metadata'].get('loader')}`",
        f"- Fallback: `{payload['loader_metadata'].get('fallback')}`",
        f"- Model-ready collection: `{payload['loader_metadata'].get('model_ready_collection_key')}`",
        f"- Leakage-free non-control perturbation splits: `{payload['split_integrity']['leakage_free']}`",
        f"- No huge matrix load: `{payload['matrix_safety']['no_huge_matrix_load']}` ({payload['matrix_safety']['matrix_cells_across_splits']} cells across split batches)",
        "",
        "## Split details",
        "",
        "| split | n_obs | n_controls | non-control perturbations |",
        "| --- | ---: | ---: | --- |",
    ]
    for split, details in payload["splits"].items():
        lines.append(
            f"| {split} | {details['n_obs']} | {details['n_controls']} | "
            f"{', '.join(details['non_control_perturbations'])} |"
        )
    lines.extend(
        [
            "",
            "## Test metrics",
            "",
            "| model | R2 | Pearson | MSE |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for result in payload["models"]:
        metrics = result["metrics"]
        lines.append(
            f"| {result['model_name']} | {metrics['r2']:.6f} | "
            f"{metrics['pearson']:.6f} | {metrics['mse']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Safety notes",
            "",
            "This benchmark uses `load_model_ready_v0_or_synthetic()` against the reviewed model-ready-v0 manifest. In the current v0 state the loader reads manifest metadata only and uses the deterministic 12-row synthetic fallback, so it performs no Lamin writes and does not materialize PRISM, T-cell GWPS, Tahoe, or other huge matrices.",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
