#!/usr/bin/env python3
"""Run a semantically honest scPRAM adapter smoke.

This script is intentionally self-contained so it can run inside
.venv-models/scpram, whose Python 3.8 dependency stack cannot install the local
pert-gym package because the repo requires Python >=3.10.

The adapter follows the upstream scPRAM contract: predict the stimulated response
for a held-out cell/covariate type under one perturbation condition. It does not
map perturbation identities to scPRAM cell types.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

import anndata as ad
import numpy as np
import pandas as pd
import torch
from scipy import sparse
from scpram import models

KEY_DIC = {
    "condition_key": "condition",
    "cell_type_key": "cell_type",
    "ctrl_key": "control",
    "stim_key": "stimulated",
    "pred_key": "predict",
}

UPSTREAM_CONTRACT = {
    "source": "https://github.com/jiang-q19/scPRAM README and scpram.models.SCPRAM.predict",
    "task": "out-of-sample prediction across cell types/covariate contexts",
    "input": "AnnData with expression X, obs[condition] in {control, stimulated}, obs[cell_type] carrying the biological/covariate context, and var gene metadata",
    "training_semantics": "hold out stimulated rows for the target cell_type; train on controls for all cell types plus stimulated rows for reference cell types",
    "prediction_semantics": "model.predict(train_adata, cell_to_pred, key_dic) returns predicted stimulated expression for the held-out target cell_type controls",
    "pert_gym_mapping": {
        "condition": "binary response state for one selected perturbation identity: control vs stimulated",
        "cell_type": "real obs cell_type when present; otherwise a real context covariate such as cell_line/tissue/disease, never perturbation identity",
        "perturbation": "kept separately as perturbation_identity/target_perturbation; run one scPRAM task per perturbation identity or perturbation class",
        "split": "hold out stimulated rows for one context/cell_type; do not split by perturbation identity for a single scPRAM run",
        "target": "stimulated expression for held-out context under the selected perturbation",
    },
}


def set_seeds(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def build_synthetic_response_adata(
    *, n_per_group: int = 6, n_genes: int = 8, perturbation: str = "synthetic_ifng"
) -> ad.AnnData:
    """Build a tiny paired control/stimulated response panel for scPRAM.

    Three genuine contexts (cell types) share the same perturbation condition.
    The target smoke holds out stimulated rows for ``cell_c`` while preserving
    ``cell_c`` controls in the training data.
    """

    rng = np.random.default_rng(7)
    cell_types = ["cell_a", "cell_b", "cell_c"]
    base = {
        "cell_a": np.array([1.0, 0.8, 0.6, 1.2, 0.4, 0.7, 0.5, 0.3]),
        "cell_b": np.array([0.7, 1.1, 0.9, 0.5, 1.3, 0.6, 0.2, 0.4]),
        "cell_c": np.array([1.2, 0.5, 1.0, 0.8, 0.6, 1.1, 0.4, 0.7]),
    }
    # Shared perturbation response with mild context variation. Values stay
    # non-negative because scPRAM's decoder ends with ReLU and the smoke should
    # not test preprocessing edge cases.
    shared_delta = np.array([0.55, 0.10, 0.35, 0.05, 0.40, 0.20, 0.15, 0.30])
    context_delta_scale = {"cell_a": 1.00, "cell_b": 0.85, "cell_c": 1.15}

    rows: List[Dict[str, Any]] = []
    matrix: List[np.ndarray] = []
    for cell_type in cell_types:
        for condition in (KEY_DIC["ctrl_key"], KEY_DIC["stim_key"]):
            for replicate in range(n_per_group):
                noise = rng.normal(0.0, 0.03, size=n_genes)
                values = base[cell_type].copy()
                is_control = condition == KEY_DIC["ctrl_key"]
                if not is_control:
                    values = values + shared_delta * context_delta_scale[cell_type]
                values = np.clip(values + noise, 0.0, None).astype("float32")
                rows.append(
                    {
                        "cell_id": f"{cell_type}_{condition}_{replicate:02d}",
                        "cell_type": cell_type,
                        "cell_line": f"{cell_type}_line",
                        "condition": condition,
                        "perturbation": "control" if is_control else perturbation,
                        "perturbation_identity": perturbation,
                        "is_control": is_control,
                        "split_role": "target_stimulated_holdout"
                        if (cell_type == "cell_c" and not is_control)
                        else "adapter_train_candidate",
                    }
                )
                matrix.append(values)

    obs = pd.DataFrame(rows).set_index("cell_id")
    var = pd.DataFrame(index=[f"gene_{i:02d}" for i in range(n_genes)])
    return ad.AnnData(X=np.vstack(matrix), obs=obs, var=var)


def adapt_pert_gym_response_to_scpram(
    adata: ad.AnnData,
    *,
    perturbation: str,
    target_cell_type: str,
    context_key: str = "cell_type",
    perturbation_key: str = "perturbation",
    is_control_key: str = "is_control",
) -> Tuple[ad.AnnData, ad.AnnData, Dict[str, Any]]:
    """Map a bounded pert-gym response panel to scPRAM's AnnData contract.

    Requirements for a real pert-gym member/subset:
    - expression rows for controls and for exactly one selected perturbation;
    - a real context key (prefer obs.cell_type; cell_line/tissue/disease can be
      used as lower-quality fallbacks if documented);
    - at least two contexts with both control and stimulated rows, and the target
      context must retain controls while its stimulated rows are held out.
    """

    missing = [
        key
        for key in (context_key, perturbation_key, is_control_key)
        if key not in adata.obs.columns
    ]
    if missing:
        raise ValueError(f"adata.obs missing required pert-gym fields: {missing}")

    obs = adata.obs.copy()
    is_control = obs[is_control_key].map(_as_bool)
    is_stim = obs[perturbation_key].astype(str) == perturbation
    selected = is_control | is_stim
    if int(selected.sum()) == 0:
        raise ValueError(f"no control or stimulated rows selected for {perturbation!r}")

    adapted = adata[selected].copy()
    adapted.obs["condition"] = np.where(
        adapted.obs[is_control_key].map(_as_bool),
        KEY_DIC["ctrl_key"],
        KEY_DIC["stim_key"],
    )
    adapted.obs["cell_type"] = adapted.obs[context_key].astype(str)
    adapted.obs["perturbation_identity"] = perturbation

    _validate_scpram_panel(adapted, target_cell_type=target_cell_type)
    train = adapted[
        ~(
            (adapted.obs[KEY_DIC["cell_type_key"]] == target_cell_type)
            & (adapted.obs[KEY_DIC["condition_key"]] == KEY_DIC["stim_key"])
        )
    ].copy()
    heldout = adapted[
        (adapted.obs[KEY_DIC["cell_type_key"]] == target_cell_type)
        & (adapted.obs[KEY_DIC["condition_key"]] == KEY_DIC["stim_key"])
    ].copy()
    contract = {
        "adapter": "pert_gym_response_to_scpram_cell_context_v1",
        "key_dic": KEY_DIC,
        "target_perturbation": perturbation,
        "target_cell_type": target_cell_type,
        "context_key_source": context_key,
        "condition_source": f"{is_control_key} plus selected {perturbation_key} == {perturbation}",
        "train_rows": int(train.n_obs),
        "heldout_stimulated_rows": int(heldout.n_obs),
        "n_vars": int(adapted.n_vars),
        "contexts": _context_condition_counts(adapted),
        "no_perturbation_identity_to_cell_type_hack": True,
    }
    return train, heldout, contract


def _validate_scpram_panel(adata: ad.AnnData, *, target_cell_type: str) -> None:
    counts = _context_condition_counts(adata)
    complete_contexts = [
        context
        for context, by_condition in counts.items()
        if by_condition.get(KEY_DIC["ctrl_key"], 0) > 0
        and by_condition.get(KEY_DIC["stim_key"], 0) > 0
    ]
    if len(complete_contexts) < 2:
        raise ValueError(
            "scPRAM requires at least two real contexts/cell types with both control "
            "and stimulated rows so reference perturbation responses can transfer."
        )
    if target_cell_type not in counts:
        raise ValueError(f"target_cell_type {target_cell_type!r} is absent")
    target_counts = counts[target_cell_type]
    if target_counts.get(KEY_DIC["ctrl_key"], 0) == 0:
        raise ValueError("target context must include control rows for prediction input")
    if target_counts.get(KEY_DIC["stim_key"], 0) == 0:
        raise ValueError("target context must include stimulated rows for smoke evaluation")


def _context_condition_counts(adata: ad.AnnData) -> Dict[str, Dict[str, int]]:
    table = (
        adata.obs.groupby([KEY_DIC["cell_type_key"], KEY_DIC["condition_key"]])
        .size()
        .to_dict()
    )
    out: Dict[str, Dict[str, int]] = {}
    for (context, condition), count in table.items():
        out.setdefault(str(context), {})[str(condition)] = int(count)
    return out


def _as_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def run_scpram_smoke(adata: ad.AnnData, *, epochs: int, ratio: float) -> Dict[str, Any]:
    set_seeds(13)
    train, heldout, contract = adapt_pert_gym_response_to_scpram(
        adata,
        perturbation="synthetic_ifng",
        target_cell_type="cell_c",
        context_key="cell_type",
    )
    model = models.SCPRAM(
        input_dim=train.n_vars,
        latent_dim=4,
        hidden_dim=16,
        noise_rate=0.01,
        device="cpu",
    )
    model = model.to(model.device)
    model.train_SCPRAM(train, epochs=epochs, batch_size=4, lr=0.01)
    pred = model.predict(
        train_adata=train,
        cell_to_pred="cell_c",
        key_dic=KEY_DIC,
        ratio=ratio,
    )
    pred_matrix = np.asarray(pred.X, dtype="float32")
    target_matrix = np.asarray(heldout.X, dtype="float32")
    # The synthetic smoke keeps equal target-control replicate counts; compare rowwise
    # after clipping to the common count to avoid giving artificial precision.
    n_eval = min(pred_matrix.shape[0], target_matrix.shape[0])
    diff = pred_matrix[:n_eval] - target_matrix[:n_eval]
    mae = float(np.mean(np.abs(diff)))
    rmse = float(math.sqrt(float(np.mean(diff**2))))
    return {
        "status": "passed",
        "mode": "synthetic_semantic_adapter_smoke",
        "contract": contract,
        "prediction": {
            "pred_shape": list(pred_matrix.shape),
            "target_shape": list(target_matrix.shape),
            "n_eval_rows": int(n_eval),
            "metrics": {"mae": mae, "rmse": rmse},
        },
        "training": {"epochs": epochs, "batch_size": 4, "ratio": ratio},
    }


def _densify_for_scpram(adata: ad.AnnData) -> ad.AnnData:
    """Return a small dense AnnData for scPRAM's torch tensor conversion path."""

    out = adata.copy()
    if sparse.issparse(out.X):
        out.X = out.X.toarray().astype("float32", copy=False)
    else:
        out.X = np.asarray(out.X, dtype="float32")
    return out


def run_real_scpram_smoke(
    adata_path: Path,
    *,
    perturbation: str,
    target_cell_type: str,
    context_key: str,
    epochs: int,
    ratio: float,
) -> Dict[str, Any]:
    """Run the same adapter against a bounded real exported AnnData subset."""

    adata = ad.read_h5ad(adata_path)
    if adata.n_obs > 5000:
        raise ValueError(f"real scPRAM smoke input too large: {adata.n_obs} rows")
    adata = _densify_for_scpram(adata)
    train, heldout, contract = adapt_pert_gym_response_to_scpram(
        adata,
        perturbation=perturbation,
        target_cell_type=target_cell_type,
        context_key=context_key,
    )
    model = models.SCPRAM(
        input_dim=train.n_vars,
        latent_dim=8,
        hidden_dim=32,
        noise_rate=0.01,
        device="cpu",
    )
    model = model.to(model.device)
    model.train_SCPRAM(train, epochs=epochs, batch_size=8, lr=0.01)
    pred = model.predict(
        train_adata=train,
        cell_to_pred=target_cell_type,
        key_dic=KEY_DIC,
        ratio=ratio,
    )
    pred_matrix = np.asarray(pred.X, dtype="float32")
    target_matrix = np.asarray(heldout.X, dtype="float32")
    n_eval = min(pred_matrix.shape[0], target_matrix.shape[0])
    diff = pred_matrix[:n_eval] - target_matrix[:n_eval]
    mae = float(np.mean(np.abs(diff)))
    rmse = float(math.sqrt(float(np.mean(diff**2))))
    return {
        "status": "passed",
        "mode": "real_bounded_scpram_adapter_smoke",
        "adata_path": str(adata_path),
        "contract": contract,
        "prediction": {
            "pred_shape": list(pred_matrix.shape),
            "target_shape": list(target_matrix.shape),
            "n_eval_rows": int(n_eval),
            "metrics": {"mae": mae, "rmse": rmse},
        },
        "training": {"epochs": epochs, "batch_size": 8, "ratio": ratio},
    }


def assess_model_ready_v0(manifest_path: Path) -> Dict[str, Any]:
    if not manifest_path.exists():
        return {
            "status": "not_checked",
            "reason": f"manifest missing: {manifest_path}",
        }
    manifest = json.loads(manifest_path.read_text())
    promoted = manifest.get("promoted", [])
    promoted_member = promoted[0] if promoted else {}
    sample_values = promoted_member.get("sample_values", {})
    context_present = promoted_member.get("context_present", [])
    reasons = []
    if manifest.get("model_ready_collection", {}).get("member_count") != 1:
        reasons.append("expected current v0 to contain exactly one reviewed smoke member")
    if "cell_type" not in context_present:
        reasons.append("promoted v0 member has no reviewed obs.cell_type context")
    if "cell_line" in context_present and sample_values.get("cell_line") in (["unknown"], ["Unknown"]):
        reasons.append("available cell_line context is unknown, not a real scPRAM transfer context")
    if int(promoted_member.get("control_count") or 0) <= 0:
        reasons.append("no controls encoded")
    if int(promoted_member.get("noncontrol_count") or 0) <= 0:
        reasons.append("no stimulated/non-control rows encoded")
    # scPRAM needs same perturbation observed across several contexts. The v0
    # manifest only records many CRISPRi target genes in one unknown context.
    reasons.append(
        "v0 is a perturbation-identity screen chunk, while scPRAM needs one perturbation condition paired with controls across at least two real contexts/cell types"
    )
    return {
        "status": "infeasible_for_real_scpram_benchmark",
        "manifest_path": str(manifest_path),
        "model_ready_collection": manifest.get("model_ready_collection", {}),
        "promoted_member": {
            "artifact_key": promoted_member.get("artifact_key"),
            "dataset_id": promoted_member.get("dataset_id"),
            "control_count": promoted_member.get("control_count"),
            "noncontrol_count": promoted_member.get("noncontrol_count"),
            "context_present": context_present,
            "sample_values": sample_values,
        },
        "reasons": reasons,
        "minimal_additional_subset_needed": {
            "shape": "bounded expression triplet or exported local AnnData/parquet slice with controls plus one stimulated perturbation across >=2 real cell_type/context values",
            "required_obs_fields": [
                "perturbation",
                "is_control",
                "cell_type or documented context covariate",
                "organism",
                "modality",
                "assay",
            ],
            "balance": "target context must have control + stimulated rows; at least one reference context must also have control + stimulated rows",
            "size_cap_for_smoke": "<= 5k cells and a small HVG/projection feature set, or an already tiny backed slice; no huge matrix load",
            "split": "hold out stimulated rows for target context, not perturbation identity",
        },
    }


def _real_markdown_lines(payload: Mapping[str, Any]) -> List[str]:
    real = payload["real_adapter_smoke"]
    metrics = real["prediction"]["metrics"]
    contract = real["contract"]
    return [
        "## Real bounded subset smoke",
        "",
        f"- Status: `{real['status']}` on `{real['adata_path']}`.",
        f"- Target perturbation: `{contract['target_perturbation']}`; target context: `{contract['target_cell_type']}`; context source: `{contract['context_key_source']}`.",
        f"- Train rows: `{contract['train_rows']}`; held-out stimulated rows: `{contract['heldout_stimulated_rows']}`; features: `{contract['n_vars']}`.",
        f"- Prediction shape: `{real['prediction']['pred_shape']}`; target shape: `{real['prediction']['target_shape']}`; eval rows: `{real['prediction']['n_eval_rows']}`.",
        f"- Metrics on heldout real response: MAE `{metrics['mae']:.6f}`, RMSE `{metrics['rmse']:.6f}`.",
        "- Interpretation: small real API/semantics smoke only; not a biological performance claim.",
        "",
    ]


def write_markdown(path: Path, payload: Mapping[str, Any]) -> None:
    adapter = payload["adapter_smoke"]
    model_ready = payload["model_ready_v0_feasibility"]
    metrics = adapter["prediction"]["metrics"]
    contract = adapter["contract"]
    path.write_text(
        "\n".join(
            [
                "# scPRAM real adapter semantic smoke",
                "",
                f"Status: `{payload['status']}`.",
                "",
                "## Upstream scPRAM contract",
                "",
                "scPRAM predicts the stimulated expression response for a held-out cell type/context. Its `condition` key is binary (`control` vs `stimulated`), and `cell_type` is a biological/context covariate. Perturbation identity is not a cell type.",
                "",
                "## pert-gym adapter contract",
                "",
                f"- Adapter: `{contract['adapter']}`",
                f"- Target perturbation: `{contract['target_perturbation']}`",
                f"- Target context/cell type: `{contract['target_cell_type']}`",
                f"- Context source: `{contract['context_key_source']}`",
                f"- Train rows: `{contract['train_rows']}`; held-out stimulated rows: `{contract['heldout_stimulated_rows']}`; genes/features: `{contract['n_vars']}`",
                "- Split semantics: train on controls for all contexts plus stimulated rows for reference contexts; hold out only target-context stimulated rows.",
                "- Explicitly not used: perturbation_identity -> cell_type mapping hack from the old MB6 smoke.",
                "",
                "## Synthetic isolated-env smoke",
                "",
                f"- Env: `.venv-models/scpram`; Python `{payload['python']}`; torch `{payload['torch']}`; scPRAM reported `{payload['scpram_reported_version']}`.",
                f"- Epochs: `{adapter['training']['epochs']}` CPU; prediction shape: `{adapter['prediction']['pred_shape']}`; target shape: `{adapter['prediction']['target_shape']}`.",
                f"- Metrics on synthetic heldout response: MAE `{metrics['mae']:.6f}`, RMSE `{metrics['rmse']:.6f}`.",
                "- Interpretation: validates adapter/API wiring only; it is not a biological performance claim.",
                "",
                "## Real model-ready-v0 feasibility",
                "",
                f"Status: `{model_ready['status']}`.",
                "",
                "Reasons:",
                *[f"- {reason}" for reason in model_ready.get("reasons", [])],
                "",
                "Minimal additional model-ready subset needed: a bounded expression triplet/export with controls and one stimulated perturbation across at least two real cell types/contexts, with target-context stimulated rows held out for evaluation. The current one-member VIPerturb v0 chunk is useful for generic loader smoke, not a real scPRAM benchmark.",
                "",
                *(_real_markdown_lines(payload) if "real_adapter_smoke" in payload else []),
                "## Safety",
                "",
                "No Lamin writes, no broad Lamin query, no huge matrix load. The script reads only the model-ready manifest, optional bounded real AnnData exports, and trains on in-memory tiny panels.",
                "",
            ]
        )
        + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("artifacts/schema_audit/model_ready_subset_20260621.json"),
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("artifacts/model_benchmarks"),
    )
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--ratio", type=float, default=0.5)
    parser.add_argument("--real-adata", type=Path, default=None)
    parser.add_argument("--real-perturbation", default="Trametinib")
    parser.add_argument("--real-target-cell-type", default="BICR 31")
    parser.add_argument("--real-context-key", default="cell_type")
    args = parser.parse_args()

    set_seeds(13)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    adata = build_synthetic_response_adata()
    smoke = run_scpram_smoke(adata, epochs=args.epochs, ratio=args.ratio)
    real_smoke = None
    if args.real_adata is not None:
        real_smoke = run_real_scpram_smoke(
            args.real_adata,
            perturbation=args.real_perturbation,
            target_cell_type=args.real_target_cell_type,
            context_key=args.real_context_key,
            epochs=args.epochs,
            ratio=args.ratio,
        )
    payload: Dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed_real_and_synthetic" if real_smoke else "passed",
        "model": "scPRAM",
        "adapter_name": "pert_gym_response_to_scpram_cell_context_v1",
        "upstream_contract": UPSTREAM_CONTRACT,
        "adapter_smoke": smoke,
        "model_ready_v0_feasibility": assess_model_ready_v0(args.manifest),
        "safety": {
            "env": ".venv-models/scpram",
            "lamin_writes": False,
            "broad_lamin_queries": False,
            "huge_matrix_loads": False,
            "data_modes_run": ["synthetic_semantic_adapter_smoke", "model_ready_manifest_feasibility_only"]
            + (["real_bounded_scpram_adapter_smoke"] if real_smoke else []),
        },
        "python": platform.python_version(),
        "torch": torch.__version__,
        "scpram_reported_version": __import__("scpram").__version__,
    }
    if real_smoke is not None:
        payload["real_adapter_smoke"] = real_smoke
    json_path = args.out_dir / f"scpram_real_adapter_{args.date}.json"
    md_path = args.out_dir / f"scpram_real_adapter_{args.date}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    write_markdown(md_path, payload)
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(
        "scPRAM real-adapter smoke passed: "
        f"mae={payload['adapter_smoke']['prediction']['metrics']['mae']:.6f} "
        f"rmse={payload['adapter_smoke']['prediction']['metrics']['rmse']:.6f}"
    )
    print(
        "model-ready-v0 feasibility: "
        f"{payload['model_ready_v0_feasibility']['status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
