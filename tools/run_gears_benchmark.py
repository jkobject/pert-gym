#!/usr/bin/env python3
"""Run an honest GEARS dependency/API and data-contract smoke.

The official GEARS package is `cell-gears` from snap-stanford/GEARS. Its API is
AnnData-first and expects perturbational single-cell screens with obs.condition,
obs.cell_type, and var.gene_name. This runner avoids Lamin writes and huge matrix
loads: it builds a tiny synthetic screen only to validate dependency import,
contract mapping, and a bounded train/eval adapter smoke.

Synthetic metrics emitted here are dependency/API smoke evidence only. They are
not biological GEARS benchmark results.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import platform
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from pert_gym.benchmarks import BenchmarkDataset, load_model_ready_v0_or_synthetic

UPSTREAM = {
    "package": "cell-gears",
    "import_name": "gears",
    "upstream_url": "https://github.com/snap-stanford/GEARS",
    "pypi_version": "0.1.2",
    "source_version_file": "gears/version.py reports __version__ = '0.1.2' on master",
    "source_metadata_checked_utc": "2026-06-22T00:00:00+00:00",
    "github_default_branch": "master",
    "github_pushed_at_observed": "2025-02-01T09:35:37Z",
    "core_api": "from gears import PertData, GEARS; PertData.new_data_process(dataset_name, adata) then prepare_split/get_dataloader/GEARS.train/predict",
}


GEARS_DATA_CONTRACT = {
    "X": "cell x gene expression matrix; bounded dense/sparse AnnData for adapter smoke, no huge matrix loads",
    "obs.condition": "GEARS perturbation label. Use 'ctrl' for control rows and gene symbols or '+'-joined gene symbols for perturbations/combinations.",
    "obs.cell_type": "GEARS requires the column, but upstream notes it is not designed for cross-cell-type transfer; use one cell type/context per GEARS run unless explicitly reviewed.",
    "var.gene_name": "gene symbols matching the expression columns and perturbation labels where possible",
    "split": "GEARS supports simulation/combo/custom splits. Pert-gym should use held-out perturbation labels with controls retained in train/test support.",
    "pert_gym_mapping": {
        "perturbation": "obs.condition after normalizing controls to ctrl and gene perturbations to symbols/combos",
        "is_control": "obs.condition == ctrl plus original obs.is_control retained in provenance",
        "gene_identifiers": "prefer var.gene_name; otherwise map var index to gene symbols before GEARS processing",
        "contexts": "cell_type/cell_line/tissue are metadata for filtering one coherent context, not the transfer target",
    },
}


@dataclass(frozen=True)
class SyntheticGEARSSmokeResult:
    n_train: int
    n_test: int
    n_features: int
    n_control_train: int
    test_perturbations: list[str]
    mae: float
    rmse: float


class TinyGEARSContractAdapter:
    """Small GEARS-contract adapter for CI-safe synthetic smoke.

    This is not the upstream graph neural network. It validates the same high-level
    input contract and computes a simple control-plus-delta predictor so the task
    can exercise train/eval plumbing without pretending a tiny synthetic panel is a
    biological GEARS benchmark.
    """

    name = "tiny_gears_contract_adapter"

    def __init__(self) -> None:
        self.control_mean_: np.ndarray | None = None
        self.delta_by_perturbation_: dict[str, np.ndarray] = {}

    def fit(
        self,
        X: Sequence[Sequence[float]],
        perturbations: Sequence[str],
        controls: Sequence[bool],
    ) -> "TinyGEARSContractAdapter":
        matrix = np.asarray(X, dtype="float32")
        ctrl_mask = np.asarray(list(controls), dtype=bool)
        if matrix.ndim != 2 or matrix.shape[0] == 0:
            raise ValueError("X must be a non-empty 2D matrix")
        if not ctrl_mask.any():
            raise ValueError("GEARS train contract requires control rows")
        self.control_mean_ = matrix[ctrl_mask].mean(axis=0)
        for pert in sorted({str(p) for p in perturbations if str(p) != "ctrl"}):
            mask = np.asarray([str(p) == pert for p in perturbations], dtype=bool)
            if mask.any():
                self.delta_by_perturbation_[pert] = matrix[mask].mean(axis=0) - self.control_mean_
        return self

    def predict(self, perturbations: Sequence[str]) -> np.ndarray:
        if self.control_mean_ is None:
            raise RuntimeError("adapter must be fit before predict")
        rows = []
        for pert in perturbations:
            delta = self.delta_by_perturbation_.get(str(pert), np.zeros_like(self.control_mean_))
            rows.append(self.control_mean_ + delta)
        return np.vstack(rows).astype("float32")


def check_upstream_import() -> dict[str, Any]:
    try:
        import gears  # type: ignore
        from gears import GEARS, PertData  # type: ignore

        return {
            "status": "passed",
            "module_file": getattr(gears, "__file__", None),
            "package_version": importlib.metadata.version("cell-gears"),
            "has_GEARS": GEARS is not None,
            "has_PertData": PertData is not None,
        }
    except Exception as exc:  # pragma: no cover - exercised in missing optional envs
        return {
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def dataset_contract_summary(dataset: BenchmarkDataset) -> dict[str, Any]:
    all_batches = (dataset.train, dataset.val, dataset.test)
    perturbations = sorted({p for batch in all_batches for p in batch.perturbations})
    feature_names = list(dataset.train.feature_names)
    return {
        "source": dataset.source,
        "metadata": dict(dataset.metadata),
        "split_by": dataset.split_by,
        "n_train": len(dataset.train.X),
        "n_val": len(dataset.val.X),
        "n_test": len(dataset.test.X),
        "n_features": len(dataset.train.X[0]) if dataset.train.X else 0,
        "feature_name_source": "benchmark feature_names; GEARS would require AnnData.var['gene_name']",
        "example_feature_names": feature_names[:8],
        "perturbations": perturbations,
        "control_label_for_gears": "ctrl",
    }


def run_synthetic_contract_smoke(dataset: BenchmarkDataset) -> SyntheticGEARSSmokeResult:
    model = TinyGEARSContractAdapter().fit(
        dataset.train.X,
        _to_gears_conditions(dataset.train.perturbations, dataset.train.controls),
        list(dataset.train.controls or []),
    )
    test_conditions = _to_gears_conditions(dataset.test.perturbations, dataset.test.controls)
    pred = model.predict(test_conditions)
    target = np.asarray(dataset.test.X, dtype="float32")
    diff = pred - target
    return SyntheticGEARSSmokeResult(
        n_train=len(dataset.train.X),
        n_test=len(dataset.test.X),
        n_features=target.shape[1],
        n_control_train=sum(1 for value in (dataset.train.controls or []) if value),
        test_perturbations=sorted(set(test_conditions)),
        mae=float(np.mean(np.abs(diff))),
        rmse=float(math.sqrt(float(np.mean(diff**2)))),
    )


def _to_gears_conditions(
    perturbations: Sequence[str], controls: Sequence[bool] | None
) -> list[str]:
    if controls is None:
        controls = [False] * len(perturbations)
    return ["ctrl" if is_control else str(pert) for pert, is_control in zip(perturbations, controls)]


def build_payload(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    dataset = load_model_ready_v0_or_synthetic(manifest_path=args.manifest)
    import_smoke = check_upstream_import()
    contract_smoke = run_synthetic_contract_smoke(dataset)
    status = "smoke_passed" if import_smoke["status"] == "passed" else "blocked_import"
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "task_id": "t_ed0290d1",
        "status": status,
        "model": "gears",
        "upstream": UPSTREAM,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "env_policy": {
            "required_env": ".venv-models/gears",
            "base_env_dependency_pollution": False,
            "lamin_writes": False,
            "huge_matrix_loads": False,
            "full_gears_training_attempted": False,
            "synthetic_only": True,
        },
        "dependency_import_smoke": import_smoke,
        "data_contract": GEARS_DATA_CONTRACT,
        "dataset": dataset_contract_summary(dataset),
        "synthetic_contract_smoke": asdict(contract_smoke),
        "real_benchmark_feasibility": {
            "current_model_ready_v0_member_count": dataset.metadata.get("model_ready_member_count"),
            "current_loader_fallback": dataset.metadata.get("fallback"),
            "verdict": "not_feasible_from_current_model_ready_v0",
            "reason": "The current model-ready-v0 path exposes one tiny loader-smoked member and the benchmark loader intentionally falls back to synthetic metadata-only data. GEARS needs a real Perturb-seq-like expression matrix with enough control and perturbed cells per gene/combo, gene symbols in var.gene_name, and a reviewed held-out perturbation split.",
            "follow_up_needed": "Create/promote a GEARS-ready bounded subset with real X materialized safely, obs.condition/is_control, one coherent cell type/context, gene symbols, and enough cells per perturbation.",
        },
        "limitations": [
            "Synthetic contract-smoke metrics are not biological performance claims.",
            "This run does not train the upstream GEARS GNN; full training is deferred until a real GEARS-ready subset exists.",
            "GEARS upstream notes it is not designed for training across multiple cell types or bulk sequencing data.",
            "No Lamin writes, no broad Lamin queries, and no huge X.h5ad loads were performed.",
        ],
    }
    return payload, render_markdown(payload)


def render_markdown(payload: Mapping[str, Any]) -> str:
    smoke = payload["synthetic_contract_smoke"]
    import_smoke = payload["dependency_import_smoke"]
    lines = [
        "# GEARS dependency/API and benchmark smoke",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Decision",
        "",
        "Use the official SNAP `cell-gears==0.1.2` package in an isolated `.venv-models/gears` environment. The unrelated PyPI package `gears==0.7.2` is not GEARS; it is a JavaScript/CSS asset tool and must not be used.",
        "",
        "## Upstream/source",
        "",
        f"- source: {payload['upstream']['upstream_url']}",
        f"- package: `{payload['upstream']['package']}=={payload['upstream']['pypi_version']}`",
        f"- observed source version: {payload['upstream']['source_version_file']}",
        f"- observed GitHub pushed_at: `{payload['upstream']['github_pushed_at_observed']}`",
        "",
        "## Dependency/API smoke",
        "",
        f"- status: `{import_smoke['status']}`",
    ]
    if import_smoke["status"] == "passed":
        lines.extend(
            [
                f"- installed package version: `{import_smoke['package_version']}`",
                f"- module file: `{import_smoke['module_file']}`",
                "- imported: `gears.PertData`, `gears.GEARS`",
            ]
        )
    else:
        lines.extend(
            [
                f"- error: `{import_smoke['error_type']}: {import_smoke['error']}`",
                "- blocker: fix isolated env import before any upstream GEARS training smoke",
            ]
        )
    lines.extend(
        [
            "",
            "## Pert-gym → GEARS data contract",
            "",
            "- `X`: cell × gene expression matrix, bounded and materialized safely",
            "- `obs.condition`: `ctrl` for controls; gene symbol or `+`-joined combo label for perturbations",
            "- `obs.cell_type`: required by GEARS; use one reviewed coherent context per run because GEARS is not a cross-cell-type transfer model",
            "- `var.gene_name`: gene symbols aligned to expression columns and perturbation labels",
            "- split: held-out perturbation identities with controls retained for model support",
            "",
            "## Synthetic contract smoke",
            "",
            f"- status: `{payload['status']}`",
            f"- train/test rows: `{smoke['n_train']}` / `{smoke['n_test']}`",
            f"- features: `{smoke['n_features']}`",
            f"- train controls: `{smoke['n_control_train']}`",
            f"- test perturbations: `{smoke['test_perturbations']}`",
            f"- MAE/RMSE: `{smoke['mae']:.6f}` / `{smoke['rmse']:.6f}`",
            "",
            "These metrics come from a tiny GEARS-contract adapter, not the upstream GEARS GNN, and exist only to prove the dependency/API/data-shape path is wired without touching Lamin or huge matrices.",
            "",
            "## Real benchmark blocker",
            "",
            f"- verdict: `{payload['real_benchmark_feasibility']['verdict']}`",
            f"- reason: {payload['real_benchmark_feasibility']['reason']}",
            f"- follow-up: {payload['real_benchmark_feasibility']['follow_up_needed']}",
            "",
            "## Safety",
            "",
            "- no Lamin writes",
            "- no broad Lamin queries",
            "- no huge matrix loads",
            "- no base environment dependency pollution",
            "",
            "## Limitations",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["limitations"])
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("artifacts/schema_audit/model_ready_subset_20260621.json"))
    parser.add_argument("--artifact-dir", type=Path, default=Path("artifacts/model_benchmarks"))
    parser.add_argument("--date", default=datetime.now(UTC).strftime("%Y%m%d"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload, markdown = build_payload(args)
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.artifact_dir / f"gears_{args.date}.json"
    md_path = args.artifact_dir / f"gears_{args.date}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    md_path.write_text(markdown)
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(
        "gears smoke: "
        f"status={payload['status']} "
        f"import={payload['dependency_import_smoke']['status']} "
        f"synthetic_mae={payload['synthetic_contract_smoke']['mae']:.6f}"
    )
    return 0 if payload["dependency_import_smoke"]["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
