#!/usr/bin/env python3
"""Run the maintained trVAE replacement benchmark smoke.

This runner is intentionally small and read-only. It exercises the
ConditionalPerturbationVAE through the canonical model-ready-v0 loader contract,
which currently records model-ready manifest provenance and falls back to a tiny
synthetic matrix rather than loading huge Lamin payloads.
"""

from __future__ import annotations

import argparse
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
)
from pert_gym.evaluate import evaluate_model
from pert_gym.models import ConditionalPerturbationVAE, MeanControlBaseline


@dataclass(frozen=True)
class CandidateAssessment:
    candidate: str
    maintained_dependency_posture: str
    mac_python_fit: str
    perturbation_model_boundary: str
    decision: str


CANDIDATES = [
    CandidateAssessment(
        candidate="scGen / scArches-style conditional VAE",
        maintained_dependency_posture="Relevant conceptual analogue; PyPI scgen is available and scArches depends on scvi-tools/scanpy stacks.",
        mac_python_fit="Installable routes exist but pull broad scanpy/scvi dependency surfaces; current scvi-tools latest requires Python >=3.12, while this repo targets >=3.10/3.11 model envs.",
        perturbation_model_boundary="Would require an AnnData-first adapter and additional obs/covariate mapping before matching PerturbationModel.fit/predict.",
        decision="Deferred as production integration candidate, not the first replacement smoke.",
    ),
    CandidateAssessment(
        candidate="scVI-tools TOTALVI/SCANVI-style covariate model",
        maintained_dependency_posture="Actively maintained ecosystem, but not a direct perturbation transfer VAE replacement.",
        mac_python_fit="Latest PyPI metadata reports Python >=3.12 and a heavy scanpy/lightning/pyro stack; acceptable only in a separate future env if policy moves.",
        perturbation_model_boundary="Needs a custom training/prediction adapter and careful biological semantics for covariates versus perturbation identity.",
        decision="Not chosen for this smoke; too heavy and not trVAE-like enough at the current boundary.",
    ),
    CandidateAssessment(
        candidate="CPA-like VAE / compositional autoencoder",
        maintained_dependency_posture="Already represented by the repo's standalone CPA smoke adapter using torch/anndata.",
        mac_python_fit="Works on Mac/Python 3.11 in isolated envs.",
        perturbation_model_boundary="Matches PerturbationModel, but it is composition/embedding-oriented rather than conditional VAE transfer.",
        decision="Kept as separate CPA baseline, not used to replace trVAE to avoid collapsing two benchmark families.",
    ),
    CandidateAssessment(
        candidate="Small in-repo conditional perturbation VAE",
        maintained_dependency_posture="Uses maintained torch>=2.3 and anndata>=0.10 only; no TensorFlow 1.x or legacy Keras.",
        mac_python_fit="Fits current Python 3.11 isolated-env policy and does not pollute the base Lamin env.",
        perturbation_model_boundary="Implements PerturbationModel.fit(X, perturbations, controls) and predict(perturbations, controls) directly.",
        decision="Chosen pragmatic replacement for the blocked trVAE smoke benchmark.",
    ),
]


def run_benchmark(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    dataset = load_model_ready_v0_or_synthetic(manifest_path=args.manifest)
    model = ConditionalPerturbationVAE(
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

    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "task_id": "t_a2fcfe9a",
        "status": "smoke_passed",
        "model": "trvae-replacement",
        "replacement_for": "trvae",
        "chosen_adapter": "pert_gym.models.ConditionalPerturbationVAE",
        "platform": platform.platform(),
        "python_executable": platform.python_implementation(),
        "candidate_survey": [asdict(candidate) for candidate in CANDIDATES],
        "choice_rationale": [
            "legacy trVAE remains blocked by TensorFlow 1.15/Keras 2.2 pins and must not be installed silently",
            "ConditionalPerturbationVAE is the closest lightweight maintained analogue for the current fit/predict smoke boundary",
            "torch>=2.3 and anndata>=0.10 install in an isolated Python 3.11 env on the Mac policy",
            "the adapter predicts by decoding a learned control latent under requested perturbation-condition embeddings",
        ],
        "env_policy": {
            "required_env": ".venv-models/trvae-replacement",
            "base_env_dependency_pollution": False,
            "legacy_tensorflow_install_attempted": False,
            "old_lowercase_trvae_install_attempted": False,
            "lamin_writes": False,
            "huge_matrix_loads": False,
            "heavy_training": False,
        },
        "dataset": dataset_summary(dataset),
        "metrics": {
            "conditional_perturbation_vae": {
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
        "limitations": [
            "Synthetic fallback smoke only; metrics are not biological performance claims.",
            "No batching, validation early stopping, covariate adversary, dose/time modeling, or uncertainty calibration yet.",
            "Unseen test perturbation identities fall back to the decoded control condition until a production held-out-perturbation strategy is selected.",
            "No Lamin artifact writes or full X.h5ad materialization were performed.",
        ],
    }
    markdown = render_markdown(payload)
    return payload, markdown


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
    }


def render_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]["conditional_perturbation_vae"]
    baseline = payload["metrics"]["mean_control_baseline"]
    comparison = payload["comparison"]
    lines = [
        "# trVAE replacement smoke benchmark",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Decision",
        "",
        "Replace the blocked legacy `trvae` benchmark path with the in-repo `ConditionalPerturbationVAE` adapter for now. It is a trVAE-like conditional latent transfer smoke model using maintained `torch`/`anndata` dependencies in `.venv-models/trvae-replacement`, not TensorFlow 1.x or the old lowercase `trvae` package.",
        "",
        "## Candidate survey",
        "",
    ]
    for candidate in payload["candidate_survey"]:
        lines.extend(
            [
                f"- **{candidate['candidate']}** — {candidate['decision']}",
                f"  - deps: {candidate['maintained_dependency_posture']}",
                f"  - Mac/Python: {candidate['mac_python_fit']}",
                f"  - boundary: {candidate['perturbation_model_boundary']}",
            ]
        )
    lines.extend(
        [
            "",
            "## Smoke result",
            "",
            f"- status: `{payload['status']}`",
            f"- dataset source: `{payload['dataset']['source']}`; fallback: `{payload['dataset']['metadata'].get('fallback')}`",
            f"- collection provenance: `{payload['dataset']['metadata'].get('model_ready_collection_key')}` ({payload['dataset']['metadata'].get('model_ready_member_count')} member)",
            f"- ConditionalPerturbationVAE MAE/RMSE: `{metrics['metrics']['mae']:.6f}` / `{metrics['metrics']['rmse']:.6f}`",
            f"- MeanControlBaseline MAE/RMSE: `{baseline['metrics']['mae']:.6f}` / `{baseline['metrics']['rmse']:.6f}`",
            f"- delta MAE/RMSE vs mean-control: `{comparison['delta_mae_vs_mean_control']:.6f}` / `{comparison['delta_rmse_vs_mean_control']:.6f}`",
            f"- VAE final loss/recon/KL: `{metrics['loss']:.6f}` / `{metrics['reconstruction_loss']:.6f}` / `{metrics['kl_loss']:.6f}`",
            "",
            "## Safety",
            "",
            "- no Lamin writes",
            "- no huge matrix loads",
            "- no heavy training",
            "- no TensorFlow 1.x, legacy Keras, or old lowercase `trvae` install attempts",
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
    parser.add_argument("--latent-dim", type=int, default=2)
    parser.add_argument("--hidden-dim", type=int, default=8)
    parser.add_argument("--condition-dim", type=int, default=2)
    parser.add_argument("--epochs", type=int, default=25)
    parser.add_argument("--lr", type=float, default=0.03)
    parser.add_argument("--beta-kl", type=float, default=1e-3)
    parser.add_argument("--seed", type=int, default=31)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload, markdown = run_benchmark(args)
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.artifact_dir / f"trvae_replacement_{args.date}.json"
    md_path = args.artifact_dir / f"trvae_replacement_{args.date}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    md_path.write_text(markdown)
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(
        "trvae-replacement smoke: "
        f"mae={payload['metrics']['conditional_perturbation_vae']['metrics']['mae']:.6f} "
        f"rmse={payload['metrics']['conditional_perturbation_vae']['metrics']['rmse']:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
