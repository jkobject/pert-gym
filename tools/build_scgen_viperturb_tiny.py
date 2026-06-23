#!/usr/bin/env python3
"""Build a tiny real scGEN-ready VIPerturb expression export.

This is a read-only Lamin export from the reviewed model-ready v0 VIPerturb
member. It materializes a bounded local AnnData plus JSON sidecar consumed by
``pert_gym.benchmarks.load_scgen_viperturb_tiny``. Model environments should read
these local artifacts only; they should not connect to or write Lamin.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
_lamin_spec = importlib.util.spec_from_file_location(
    "pert_gym_lamin_context", ROOT / "tools" / "lamin_context.py"
)
if _lamin_spec is None or _lamin_spec.loader is None:
    raise RuntimeError("could not load tools/lamin_context.py")
_lamin_module = importlib.util.module_from_spec(_lamin_spec)
_lamin_spec.loader.exec_module(_lamin_module)
connect_pertdata = _lamin_module.connect_pertdata

DATE = "20260622"
DEFAULT_OUT_DIR = ROOT / "artifacts" / "model_benchmarks"
DEFAULT_JSON = DEFAULT_OUT_DIR / f"scgen_real_viperturb_tiny_{DATE}.json"
DEFAULT_H5AD = DEFAULT_OUT_DIR / f"scgen_real_viperturb_tiny_{DATE}.h5ad"
SOURCE_PREFIX = "viperturb/vimentin_screen_chunk_smoke/chunk_0000"
N_CONTROLS = 12
N_PERTURBATIONS = 5
N_ROWS_PER_PERTURBATION = 2
N_TOP_VARIABLE_GENES = 96
CONTROL_VALUE = "control"


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _dense_matrix(matrix: Any) -> np.ndarray:
    if sparse.issparse(matrix):
        matrix = matrix.toarray()
    return np.asarray(matrix, dtype=np.float32)


def _resolve_feature_value(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def _select_rows(obs: pd.DataFrame) -> list[int]:
    is_control = obs["is_control"].map(_as_bool).to_numpy(dtype=bool)
    control_positions = list(np.flatnonzero(is_control)[:N_CONTROLS])
    if len(control_positions) < N_CONTROLS:
        raise RuntimeError(f"expected at least {N_CONTROLS} controls, found {len(control_positions)}")

    non_control = obs.loc[~is_control].copy()
    perturbation_counts = non_control["perturbation"].astype(str).value_counts()
    perturbations = [
        str(name)
        for name, count in perturbation_counts.items()
        if int(count) >= N_ROWS_PER_PERTURBATION and str(name).upper() != "NO-TARGET"
    ][:N_PERTURBATIONS]
    if len(perturbations) < N_PERTURBATIONS:
        raise RuntimeError(
            f"expected {N_PERTURBATIONS} perturbations with at least "
            f"{N_ROWS_PER_PERTURBATION} rows, found {len(perturbations)}"
        )

    selected = list(control_positions)
    perturbation_values = obs["perturbation"].astype(str).to_numpy()
    for perturbation in perturbations:
        selected.extend(
            list(np.flatnonzero(perturbation_values == perturbation)[:N_ROWS_PER_PERTURBATION])
        )
    return selected


def build_export(*, json_path: Path = DEFAULT_JSON, h5ad_path: Path = DEFAULT_H5AD) -> Path:
    ln = connect_pertdata()
    try:
        ln.track()
    except Exception as exc:  # pragma: no cover - provenance setup can be offline
        print(f"warning: ln.track() failed for local read-only export: {exc}")
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise RuntimeError(f"wrong Lamin instance: {ln.setup.settings.instance.slug}")
    if ln.setup.settings.branch.name != "jkobject":
        raise RuntimeError(f"wrong Lamin branch: {ln.setup.settings.branch.name}")

    obs_artifact = ln.Artifact.get(key=f"{SOURCE_PREFIX}/obs.parquet")
    obs_features = obs_artifact.features.get_values()
    x_artifact = _resolve_feature_value(ln, obs_features["X"])
    var_artifact = _resolve_feature_value(ln, x_artifact.features.get_values()["var"])
    if x_artifact.key != f"{SOURCE_PREFIX}/X.h5ad":
        raise RuntimeError(f"unexpected X link: {x_artifact.key}")
    if var_artifact.key != f"{SOURCE_PREFIX}/var.parquet":
        raise RuntimeError(f"unexpected var link: {var_artifact.key}")

    obs = obs_artifact.load()
    var = var_artifact.load()
    source_adata = x_artifact.load()
    X = _dense_matrix(source_adata.X)
    if X.shape[0] != len(obs):
        raise RuntimeError(f"obs/X row mismatch: {len(obs)} vs {X.shape[0]}")
    if X.shape[1] != len(var):
        raise RuntimeError(f"var/X column mismatch: {len(var)} vs {X.shape[1]}")

    selected_idx = _select_rows(obs)
    selected_X = X[selected_idx, :]
    variances = np.var(selected_X, axis=0)
    top_idx = np.argsort(variances)[-N_TOP_VARIABLE_GENES:]
    top_idx = top_idx[np.argsort(-variances[top_idx])]
    tiny_X = selected_X[:, top_idx].astype(np.float32)
    library = tiny_X.sum(axis=1, keepdims=True)
    library[library <= 0] = 1.0
    tiny_X = np.log1p(tiny_X / library * 10000.0).astype(np.float32)

    obs_tiny = obs.iloc[selected_idx].copy()
    obs_tiny.index = [str(idx) for idx in obs_tiny.index]
    obs_tiny["is_control"] = obs_tiny["is_control"].map(_as_bool)
    obs_tiny["perturbation"] = obs_tiny["perturbation"].astype(str)
    obs_tiny["condition"] = np.where(
        obs_tiny["is_control"], CONTROL_VALUE, obs_tiny["perturbation"].astype(str)
    )
    obs_tiny["control_value"] = CONTROL_VALUE
    obs_tiny["batch"] = obs_tiny.get("assay", "VIPerturb-seq").astype(str)
    obs_tiny["cell_type"] = obs_tiny.get("cell_line", "unknown").astype(str)

    var_tiny = var.iloc[top_idx].copy()
    var_tiny.index = [str(idx) for idx in var_tiny.index]
    adata = ad.AnnData(X=tiny_X, obs=obs_tiny, var=var_tiny)
    adata.uns["pert_gym_scgen_export"] = {
        "source_prefix": SOURCE_PREFIX,
        "matrix_transform": "log1p(CP10k) computed on bounded selected rows after top-variable-gene selection",
        "control_value": CONTROL_VALUE,
        "split_semantics": "controls are present in train/val/test; non-control perturbation identities are disjoint across splits",
    }

    h5ad_path.parent.mkdir(parents=True, exist_ok=True)
    adata.write_h5ad(h5ad_path, compression="gzip")

    rows = []
    for row_idx, (_, row) in enumerate(obs_tiny.iterrows()):
        rows.append(
            {
                "cell_id": str(obs_tiny.index[row_idx]),
                "perturbation": str(row["perturbation"]),
                "condition": str(row["condition"]),
                "control_value": CONTROL_VALUE,
                "is_control": bool(row["is_control"]),
                "perturbation_type": str(row.get("perturbation_type", "CRISPRi")),
                "organism": str(row.get("organism", "human")),
                "modality": str(row.get("modality", "scRNA-seq")),
                "assay": str(row.get("assay", "VIPerturb-seq")),
                "cell_line": str(row.get("cell_line", "unknown")),
                "cell_type": str(row.get("cell_type", row.get("cell_line", "unknown"))),
                "guide_id": str(row.get("guide_id", "")),
                "expression": [round(float(value), 6) for value in tiny_X[row_idx].tolist()],
            }
        )

    payload = {
        "task": "t_8a3acae6 — real bounded scGEN-ready expression subset",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "created",
        "source": {
            "lamin_instance": ln.setup.settings.instance.slug,
            "lamin_branch": ln.setup.settings.branch.name,
            "dataset_prefix": SOURCE_PREFIX,
            "obs_key": obs_artifact.key,
            "x_key": x_artifact.key,
            "var_key": var_artifact.key,
            "n_obs_source": int(X.shape[0]),
            "n_vars_source": int(X.shape[1]),
            "review_status": "member of pert-gym/model-ready/20260621; read-only export, no Lamin writes",
        },
        "export": {
            "adata_path": str(h5ad_path.relative_to(ROOT)),
            "json_path": str(json_path.relative_to(ROOT)),
            "n_obs": int(adata.n_obs),
            "n_vars": int(adata.n_vars),
            "matrix_transform": adata.uns["pert_gym_scgen_export"]["matrix_transform"],
            "feature_selection": f"top {N_TOP_VARIABLE_GENES} variable genes within bounded selected rows",
        },
        "selection": {
            "n_controls": int(obs_tiny["is_control"].sum()),
            "control_value": CONTROL_VALUE,
            "non_control_perturbations": sorted(
                {row["perturbation"] for row in rows if not row["is_control"]}
            ),
            "rows_per_non_control_perturbation": N_ROWS_PER_PERTURBATION,
            "split_by": "perturbation_identity; controls copied into every split by loader",
            "held_out_semantics": "controls present in train/val/test, non-control perturbation identities disjoint across splits",
        },
        "feature_names": list(adata.var_names.astype(str)),
        "rows": rows,
        "safety": {
            "lamin_writes": False,
            "model_env_lamin_access": False,
            "huge_matrix_loads": False,
            "bounded_selected_rows": int(adata.n_obs),
            "bounded_selected_vars": int(adata.n_vars),
        },
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return json_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--h5ad-out", type=Path, default=DEFAULT_H5AD)
    args = parser.parse_args()
    out = build_export(json_path=args.json_out, h5ad_path=args.h5ad_out)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
