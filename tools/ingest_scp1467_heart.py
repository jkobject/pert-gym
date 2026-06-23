#!/usr/bin/env python3
"""Ingest SCP1467 Drosophila embryonic heart SCP TSV exports into Lamin triplets.

Inputs are the browser-auth staged SCP files:
- Heart_counts.tsv: raw counts, genes x cells
- Expression_Heart_only.tsv: normalized/log expression, genes x cells
- Heartmetadata.tsv: SCP metadata, includes a leading TYPE/group row

Representation:
- canonical same-prefix X.h5ad = raw counts, sparse cell x gene
- auxiliary same-prefix X_normalized_expression.h5ad = normalized expression
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

PREFIX = "temporal_pretraining/scp1467_drosophila_embryonic_heart"
RAW_GCS_PREFIX = "gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1467"
STATUS_JSON = ROOT / "artifacts/schema_audit/temporal_scp1467_heart_ingestion_20260623.json"
STATUS_MD = ROOT / "artifacts/schema_audit/temporal_scp1467_heart_ingestion_20260623.md"
CACHE_DIR = ROOT / "data/gcs_cache/browser_auth_scp/SCP1467"
FILES = {
    "counts": ("Heart_counts.tsv", 51_951_316),
    "expression": ("Expression_Heart_only.tsv", 100_298_692),
    "metadata": ("Heartmetadata.tsv", 508_836),
}


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, text=True, capture_output=True, check=True).stdout


def gcs_size(uri: str) -> int:
    out = run(["gcloud", "storage", "ls", "-l", uri])
    return int(out.split()[0])


def ensure_cached() -> dict[str, dict[str, Any]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    result: dict[str, dict[str, Any]] = {}
    for label, (filename, expected_size) in FILES.items():
        uri = f"{RAW_GCS_PREFIX}/{filename}"
        path = CACHE_DIR / filename
        remote_size = gcs_size(uri)
        if remote_size != expected_size:
            raise RuntimeError(f"unexpected remote size for {uri}: {remote_size} != {expected_size}")
        copied = False
        if not path.exists() or path.stat().st_size != remote_size:
            tmp = path.with_suffix(path.suffix + ".partial")
            if tmp.exists():
                tmp.unlink()
            run(["gcloud", "storage", "cp", uri, str(tmp)])
            tmp.rename(path)
            copied = True
        local_size = path.stat().st_size
        if local_size != remote_size:
            raise RuntimeError(f"cache size mismatch for {path}: {local_size} != {remote_size}")
        result[label] = {
            "uri": uri,
            "path": str(path),
            "remote_size": remote_size,
            "local_size": local_size,
            "copied": copied,
        }
    return result


def duplicate_gate(ln) -> dict[str, Any]:
    queries = [
        "SCP1467",
        "scp1467",
        "drosophila",
        "Drosophila",
        "embryonic_heart",
        "scp1467_drosophila_embryonic_heart",
        PREFIX,
    ]
    hits: dict[str, list[str]] = {}
    for query in queries:
        try:
            records = list(ln.Artifact.filter(key__contains=query).all())
        except Exception:
            records = []
        hits[query] = sorted({getattr(r, "key", "") for r in records if getattr(r, "key", None)})[:50]
    planned = [
        f"{PREFIX}/obs.parquet",
        f"{PREFIX}/X.h5ad",
        f"{PREFIX}/var.parquet",
        f"{PREFIX}/X_normalized_expression.h5ad",
        f"{PREFIX}/var_normalized_expression.parquet",
    ]
    existing = [key for key in planned if ln.Artifact.filter(key=key).exists()]
    duplicate_hits = sorted({h for key, vals in hits.items() for h in vals if not h.startswith(PREFIX)})
    return {
        "queries": hits,
        "planned_prefix_existing_suffixes": [key.removeprefix(PREFIX + "/") for key in existing],
        "duplicate_detected": bool(duplicate_hits),
        "non_planned_duplicate_hits": duplicate_hits,
    }


def ensure_artifact_features(ln) -> None:
    for name in ("X", "var", "X_normalized_expression"):
        features = list(ln.Feature.filter(name=name).all())
        if features and features[0].dtype_as_str != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {features[0].dtype_as_str}, expected cat[Artifact]")
        if not features:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def parse_hour(label: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)\s*hour", str(label))
    return float(match.group(1)) if match else None


def load_inputs(cache: dict[str, dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    counts = pd.read_csv(cache["counts"]["path"], sep="\t", index_col=0)
    expr = pd.read_csv(cache["expression"]["path"], sep="\t", index_col=0)
    meta = pd.read_csv(cache["metadata"]["path"], sep="\t")
    meta = meta[meta["NAME"].astype(str) != "TYPE"].copy()
    if not counts.columns.equals(expr.columns):
        raise ValueError("counts and expression cell headers differ")
    if not counts.index.equals(expr.index):
        raise ValueError("counts and expression gene order differs")
    if not counts.columns.is_unique or not counts.index.is_unique:
        raise ValueError("matrix cell or gene identifiers are not unique")
    missing = set(counts.columns) - set(meta["NAME"])
    if missing:
        raise ValueError(f"metadata missing {len(missing)} matrix cells")
    meta = meta.set_index("NAME").loc[counts.columns].copy()
    return counts, expr, meta


def build_obs(meta: pd.DataFrame) -> pd.DataFrame:
    obs = meta.copy()
    obs["raw_scp_name"] = obs.index
    obs["barcode"] = obs.index.to_series().str.split("_", n=1).str[-1]
    obs["cell_id"] = obs.index
    obs["dataset"] = "SCP1467"
    obs["dataset_id"] = "scp1467_drosophila_embryonic_heart"
    obs["source_accession"] = "SCP1467"
    obs["study_title"] = "Six cell types in the developing Drosophila embryonic heart"
    obs["sample"] = obs.index.to_series().str.split("_", n=1).str[0]
    obs["cell_type"] = obs["biosample_id"]
    obs["organism"] = "Drosophila melanogaster"
    obs["organism_ontology_id"] = obs["species"]
    obs["tissue_type"] = obs["organ__ontology_label"]
    obs["tissue_ontology_id"] = obs["organ"]
    obs["disease_ontology_id"] = obs["disease"]
    obs["technology"] = obs["library_preparation_protocol__ontology_label"]
    obs["technology_ontology_id"] = obs["library_preparation_protocol"]
    obs["assay"] = "scRNA-seq"
    obs["modality"] = "scRNA-seq"
    obs["x_semantics"] = "raw_counts"
    obs["auxiliary_x_semantics"] = "normalized_expression"
    obs["perturbation_type"] = "developmental_stage"
    obs["perturbation"] = "developmental_stage:" + obs["embryonic_stage"].astype(str)
    obs["perturbation_technology"] = "none"
    obs["is_control"] = False
    obs["is_bulk"] = False
    obs["is_pseudobulk"] = False
    obs["developmental_stage"] = obs["embryonic_stage"]
    obs["raw_time_label"] = obs["donor_id"]
    obs["timepoint"] = obs["donor_id"].map(parse_hour)
    obs["timepoint_unit"] = "hours_AEL_18C"
    order = {stage: i for i, stage in enumerate(sorted(obs["embryonic_stage"].unique()), start=1)}
    obs["stage_order"] = obs["embryonic_stage"].map(order)
    obs["cell_line"] = "not_applicable"
    obs["source_url"] = "https://singlecell.broadinstitute.org/single_cell/study/SCP1467"
    obs["raw_gcs_prefix"] = RAW_GCS_PREFIX + "/"
    obs["scp_metadata_type_row_present"] = True
    return obs


def build_var(genes: pd.Index, *, auxiliary: bool = False) -> pd.DataFrame:
    var = pd.DataFrame(index=genes.copy())
    var["gene_symbol"] = var.index
    var["feature_name"] = var.index
    var["organism"] = "Drosophila melanogaster"
    var["source_accession"] = "SCP1467"
    if auxiliary:
        var["auxiliary_matrix"] = "normalized_expression"
    return var


def matrix_to_adata(frame: pd.DataFrame, obs_index: pd.Index, var_index: pd.Index) -> ad.AnnData:
    # SCP exports are genes x cells; Lamin X should be cells x genes.
    mat = sparse.csr_matrix(frame.T.to_numpy(dtype=np.float32, copy=True))
    return ad.AnnData(
        X=mat,
        obs=pd.DataFrame(index=obs_index.copy()),
        var=pd.DataFrame(index=var_index.copy()),
    )


def resolve_artifact(ln, value):
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def triplet_complete(ln) -> bool:
    keys = [
        f"{PREFIX}/obs.parquet",
        f"{PREFIX}/X.h5ad",
        f"{PREFIX}/var.parquet",
        f"{PREFIX}/X_normalized_expression.h5ad",
        f"{PREFIX}/var_normalized_expression.parquet",
    ]
    return all(ln.Artifact.filter(key=key).exists() for key in keys)


def write_triplet(ln, counts: pd.DataFrame, expr: pd.DataFrame, obs: pd.DataFrame, var: pd.DataFrame, var_aux: pd.DataFrame, *, overwrite: bool) -> dict[str, str]:
    keys = {
        "obs": f"{PREFIX}/obs.parquet",
        "X": f"{PREFIX}/X.h5ad",
        "var": f"{PREFIX}/var.parquet",
        "X_normalized_expression": f"{PREFIX}/X_normalized_expression.h5ad",
        "var_normalized_expression": f"{PREFIX}/var_normalized_expression.parquet",
    }
    if triplet_complete(ln) and not overwrite:
        return {"status": "skipped_existing", "prefix": PREFIX}

    prev = {name: list(ln.Artifact.filter(key=key).all()) for name, key in keys.items()}
    x_adata = matrix_to_adata(counts, obs.index, var.index)
    x_aux_adata = matrix_to_adata(expr, obs.index, var_aux.index)

    with tempfile.TemporaryDirectory(prefix="scp1467_ingest_") as tmpdir:
        tmp = Path(tmpdir)
        obs_path = tmp / "obs.parquet"
        var_path = tmp / "var.parquet"
        var_aux_path = tmp / "var_normalized_expression.parquet"
        x_path = tmp / "X.h5ad"
        x_aux_path = tmp / "X_normalized_expression.h5ad"
        obs.to_parquet(obs_path)
        var.to_parquet(var_path)
        var_aux.to_parquet(var_aux_path)
        x_adata.write_h5ad(x_path, compression="gzip")
        x_aux_adata.write_h5ad(x_aux_path, compression="gzip")

        obs_art = ln.Artifact.from_dataframe(obs_path, key=keys["obs"], revises=prev["obs"][-1] if overwrite and prev["obs"] else None).save()
        x_art = ln.Artifact.from_anndata(x_path, key=keys["X"], revises=prev["X"][-1] if overwrite and prev["X"] else None).save()
        var_art = ln.Artifact.from_dataframe(var_path, key=keys["var"], revises=prev["var"][-1] if overwrite and prev["var"] else None).save()
        x_aux_art = ln.Artifact.from_anndata(x_aux_path, key=keys["X_normalized_expression"], revises=prev["X_normalized_expression"][-1] if overwrite and prev["X_normalized_expression"] else None).save()
        var_aux_art = ln.Artifact.from_dataframe(var_aux_path, key=keys["var_normalized_expression"], revises=prev["var_normalized_expression"][-1] if overwrite and prev["var_normalized_expression"] else None).save()

    x_art.features.set_values({"var": var_art})
    x_aux_art.features.set_values({"var": var_aux_art})
    obs_art.features.set_values({"X": x_art, "X_normalized_expression": x_aux_art})
    return {
        "status": "ingested",
        "prefix": PREFIX,
        "obs_uid": obs_art.uid,
        "x_uid": x_art.uid,
        "var_uid": var_art.uid,
        "x_aux_uid": x_aux_art.uid,
        "var_aux_uid": var_aux_art.uid,
    }


def verify(ln) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{PREFIX}/obs.parquet")
    x_art = ln.Artifact.get(key=f"{PREFIX}/X.h5ad")
    var_art = ln.Artifact.get(key=f"{PREFIX}/var.parquet")
    x_aux_art = ln.Artifact.get(key=f"{PREFIX}/X_normalized_expression.h5ad")
    var_aux_art = ln.Artifact.get(key=f"{PREFIX}/var_normalized_expression.parquet")
    obs_links = obs_art.features.get_values()
    x_links = x_art.features.get_values()
    x_aux_links = x_aux_art.features.get_values()
    linked_x = resolve_artifact(ln, obs_links.get("X"))
    linked_aux = resolve_artifact(ln, obs_links.get("X_normalized_expression"))
    linked_var = resolve_artifact(ln, x_links.get("var"))
    linked_aux_var = resolve_artifact(ln, x_aux_links.get("var"))
    obs = obs_art.load()
    var = var_art.load()
    var_aux = var_aux_art.load()
    result = {
        "obs_rows": len(obs),
        "obs_cols": len(obs.columns),
        "var_rows": len(var),
        "var_cols": len(var.columns),
        "var_aux_rows": len(var_aux),
        "obs_to_x_ok": linked_x.key == x_art.key,
        "obs_to_aux_ok": linked_aux.key == x_aux_art.key,
        "x_to_var_ok": linked_var.key == var_art.key,
        "x_aux_to_var_aux_ok": linked_aux_var.key == var_aux_art.key,
        "x_n_observations": x_art.n_observations,
        "x_aux_n_observations": x_aux_art.n_observations,
        "x_path_exists": bool(x_art.path.exists()),
        "x_aux_path_exists": bool(x_aux_art.path.exists()),
        "stage_counts": obs["embryonic_stage"].value_counts().sort_index().to_dict(),
        "cell_type_counts": obs["cell_type"].value_counts().sort_index().to_dict(),
        "timepoint_counts": {str(k): int(v) for k, v in obs["timepoint"].value_counts().sort_index().to_dict().items()},
    }
    checks = [
        result["obs_rows"] == 2857,
        result["var_rows"] == 9034,
        result["var_aux_rows"] == 9034,
        result["obs_to_x_ok"],
        result["obs_to_aux_ok"],
        result["x_to_var_ok"],
        result["x_aux_to_var_aux_ok"],
        result["x_n_observations"] == 2857,
        result["x_aux_n_observations"] == 2857,
        result["x_path_exists"],
        result["x_aux_path_exists"],
    ]
    result["ok"] = all(checks)
    return result


def write_status(status: dict[str, Any]) -> None:
    STATUS_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATUS_JSON.write_text(json.dumps(status, indent=2, sort_keys=False) + "\n")
    verification = status.get("verification") or {}
    md = f"""# Temporal SCP1467 Drosophila embryonic heart ingestion — 2026-06-23

Task: `t_d8f20272`
Status: `{status['status']}`

## Source

- SCP: `SCP1467` — Six cell types in the developing Drosophila embryonic heart.
- Raw GCS prefix: `{RAW_GCS_PREFIX}/`.
- Dataset prefix: `{PREFIX}`.

## Representation decision

- Canonical `X.h5ad`: `Heart_counts.tsv`, stored as sparse cell × gene raw counts (`x_semantics = raw_counts`).
- Auxiliary `X_normalized_expression.h5ad`: `Expression_Heart_only.tsv`, stored as a typed same-prefix normalized-expression matrix linked from `obs.features['X_normalized_expression']`.
- Both matrices share the same 2,857-cell header and 9,034-gene order; metadata has one SCP `TYPE` row removed before obs construction.

## Duplicate gate

- Duplicate detected before write: `{status['duplicate_gate']['duplicate_detected']}`.
- Existing planned-prefix suffixes before write: `{status['duplicate_gate']['planned_prefix_existing_suffixes']}`.

## Verification

- obs rows: `{verification.get('obs_rows')}`; var rows: `{verification.get('var_rows')}`; auxiliary var rows: `{verification.get('var_aux_rows')}`.
- obs→X: `{verification.get('obs_to_x_ok')}`; X→var: `{verification.get('x_to_var_ok')}`; X payload: `{verification.get('x_path_exists')}`.
- obs→X_normalized_expression: `{verification.get('obs_to_aux_ok')}`; aux X→aux var: `{verification.get('x_aux_to_var_aux_ok')}`; aux payload: `{verification.get('x_aux_path_exists')}`.
- stage counts: `{verification.get('stage_counts')}`.
- cell type counts: `{verification.get('cell_type_counts')}`.
"""
    STATUS_MD.write_text(md)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Probe/convert locally and query duplicates, but do not write Lamin artifacts.")
    parser.add_argument("--overwrite", action="store_true", help="Revise existing same-key artifacts instead of skipping complete existing triplet.")
    parser.add_argument("--skip-cache-clean", action="store_true")
    args = parser.parse_args()

    ensure_project_cache()
    cache = ensure_cached()
    ln = connect_pertdata()
    ln.track()
    print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)
    ensure_artifact_features(ln)
    gate = duplicate_gate(ln)

    counts, expr, meta = load_inputs(cache)
    obs = build_obs(meta)
    var = build_var(counts.index)
    var_aux = build_var(expr.index, auxiliary=True)
    obs["n_counts"] = np.asarray(counts.sum(axis=0)).ravel().astype(float)
    obs["n_genes"] = np.asarray((counts > 0).sum(axis=0)).ravel().astype(int)

    status: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task": "t_d8f20272",
        "source_accession": "SCP1467",
        "dataset_id": "scp1467_drosophila_embryonic_heart",
        "prefix": PREFIX,
        "raw_gcs_prefix": RAW_GCS_PREFIX + "/",
        "cache": cache,
        "n_obs": int(obs.shape[0]),
        "n_vars": int(var.shape[0]),
        "counts_file": cache["counts"]["uri"],
        "expression_file": cache["expression"]["uri"],
        "metadata_file": cache["metadata"]["uri"],
        "canonical_x_semantics": "raw_counts",
        "auxiliary_matrices": ["X_normalized_expression.h5ad"],
        "stage_counts": obs["embryonic_stage"].value_counts().sort_index().to_dict(),
        "cell_type_counts": obs["cell_type"].value_counts().sort_index().to_dict(),
        "duplicate_gate": gate,
        "counts_nnz": int((counts.to_numpy() != 0).sum()),
        "expression_nnz": int((expr.to_numpy() != 0).sum()),
        "errors": [],
    }
    if args.dry_run:
        status["status"] = "dry_run_ok"
        status["write_result"] = None
        status["verification"] = None
    else:
        write_result = write_triplet(ln, counts, expr, obs, var, var_aux, overwrite=args.overwrite)
        verification = verify(ln)
        status["write_result"] = write_result
        status["verification"] = verification
        status["status"] = "ingested_verified" if verification.get("ok") else "verification_failed"
        if not verification.get("ok"):
            status["errors"].append("verification_failed")
    write_status(status)
    if not args.skip_cache_clean:
        clean_cache(ROOT / ".lamin-cache")
    print(json.dumps(status, indent=2, sort_keys=False))
    return 0 if status["status"] in {"dry_run_ok", "ingested_verified"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
