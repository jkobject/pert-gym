#!/usr/bin/env python3
"""VM-only pilot ingestion for one PerturbAI whole-brain CRISPR atlas parquet shard.

Converts one HF sparse-row parquet shard into a same-prefix pert-gym triplet and
registers it on laminlabs/pertdata branch jkobject. This is intentionally bounded:
one 25k-row parquet shard plus gene metadata, no zarr/full h5ad/full 7.7M-cell load.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_a6f7ee81"
HF_BASE = "https://huggingface.co/datasets/perturbai/wholebrain_crispr_atlas/resolve/main"
SOURCE_PARQUET = "data/WB8588_1_1_part-0.parquet"
GENE_METADATA = "metadata/gene_metadata.parquet"
PREFIX = "perturbai/wholebrain_crispr_atlas/WB8588_1_1_part-0"
ARTIFACT_JSON = ROOT / "artifacts/schema_audit/perturbai_wholebrain_crispr_atlas_pilot_t_a6f7ee81_20260703.json"
ARTIFACT_MD = ROOT / "artifacts/schema_audit/perturbai_wholebrain_crispr_atlas_pilot_t_a6f7ee81_20260703.md"

OBS_REQUIRED_PRESERVE = [
    "cell_id",
    "batch",
    "scp_name",
    "source",
    "sex",
    "sample_label",
    "num_rna_umi",
    "num_genes",
    "pct_mt",
    "scDblFinder.class",
    "scDblFinder.score",
    "log_ambient_mse",
    "log_ambient_mse_norm",
    "gene_target",
    "num_guides",
    "guide_call",
    "guide_umis",
    "guide_umi_top",
    "guide_umi_second",
    "predicted_group",
    "predicted_class",
    "predicted_class_probability",
    "predicted_subclass",
    "predicted_subclass_probability",
    "predicted_supertype",
    "predicted_supertype_probability",
    "predicted_cluster",
    "predicted_cluster_probability",
    "neuron_type",
    "neighborhood",
    "region_level1",
    "region_level2",
    "cluster",
    "passes_qc",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def url(path: str) -> str:
    return f"{HF_BASE}/{path}"


def artifact_keys(prefix: str = PREFIX) -> dict[str, str]:
    return {
        "obs": f"{prefix}/obs.parquet",
        "X": f"{prefix}/X.h5ad",
        "var": f"{prefix}/var.parquet",
    }


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {found[0].dtype!r}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def duplicate_probe(ln: Any, prefix: str = PREFIX) -> list[str]:
    keys = list(artifact_keys(prefix).values())
    return sorted([a.key for a in ln.Artifact.filter(key__in=keys).all() if a.key])


def resolve_artifact(ln: Any, value: Any) -> Any:
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def load_inputs(source_parquet: str, gene_metadata: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    source_url = url(source_parquet)
    gene_url = url(gene_metadata)
    with fsspec.open(source_url, "rb") as handle:
        parquet_file = pq.ParquetFile(handle)
        table = parquet_file.read()
        source_row_groups = int(parquet_file.num_row_groups)
        source_row_group_rows = [
            int(parquet_file.metadata.row_group(i).num_rows)
            for i in range(parquet_file.num_row_groups)
        ]
    raw = table.to_pandas()
    with fsspec.open(gene_url, "rb") as handle:
        genes = pd.read_parquet(handle)
    input_info = {
        "source_url": source_url,
        "gene_metadata_url": gene_url,
        "source_rows": int(raw.shape[0]),
        "source_columns": list(raw.columns),
        "source_row_groups": source_row_groups,
        "source_row_group_rows": source_row_group_rows,
        "gene_metadata_rows": int(genes.shape[0]),
        "gene_metadata_columns": list(genes.columns),
    }
    return raw, genes, input_info


def build_var(genes: pd.DataFrame) -> pd.DataFrame:
    required = {"gene_token_id", "gene_name", "gene_ids"}
    missing = sorted(required - set(genes.columns))
    if missing:
        raise ValueError(f"gene metadata missing columns: {missing}")
    genes = genes.copy()
    genes["gene_token_id"] = genes["gene_token_id"].astype(int)
    genes = genes.sort_values("gene_token_id")
    expected = np.arange(len(genes), dtype=np.int64)
    observed = genes["gene_token_id"].to_numpy(dtype=np.int64)
    if not np.array_equal(observed, expected):
        raise ValueError("gene_token_id is not contiguous 0..n_vars-1; refusing ambiguous sparse-column mapping")
    var = genes.set_index("gene_ids", drop=False)
    var.index.name = "gene_id"
    var["gene_symbol"] = var["gene_name"].astype(str)
    var["source_dataset"] = "perturbai/wholebrain_crispr_atlas"
    var["source_gene_metadata"] = GENE_METADATA
    var["organism"] = "Mus musculus"
    return var


def build_obs(raw: pd.DataFrame, source_parquet: str) -> pd.DataFrame:
    missing = sorted(set(OBS_REQUIRED_PRESERVE) - set(raw.columns))
    if missing:
        raise ValueError(f"source parquet missing required obs columns: {missing}")
    obs = raw.drop(columns=["genes", "expressions"]).copy()
    obs = obs.set_index("cell_id", drop=False)
    if not obs.index.is_unique:
        raise ValueError("cell_id index is not unique within selected shard")
    obs.index.name = "cell_id"
    obs["dataset"] = "perturbai/wholebrain_crispr_atlas"
    obs["source_accession"] = "10.64898/2026.03.16.711480"
    obs["source_repository"] = "https://huggingface.co/datasets/perturbai/wholebrain_crispr_atlas"
    obs["source_file"] = source_parquet
    obs["chunk_id"] = Path(source_parquet).stem
    obs["organism"] = "Mus musculus"
    obs["tissue_type"] = "whole brain"
    obs["assay"] = "in vivo Perturb-seq / single-nucleus RNA-seq"
    obs["technology"] = "single-nucleus RNA-seq"
    obs["modality"] = "snRNA-seq"
    obs["perturbation_type"] = "CRISPRko"
    obs["perturbation_technology"] = "pooled AAV CRISPR knockout"
    obs["x_semantics"] = "raw_counts"
    return obs


def build_csr(raw: pd.DataFrame, n_vars: int) -> tuple[sp.csr_matrix, dict[str, Any]]:
    indptr = np.empty(len(raw) + 1, dtype=np.int64)
    indptr[0] = 0
    lengths = np.fromiter((len(v) for v in raw["genes"]), dtype=np.int64, count=len(raw))
    indptr[1:] = np.cumsum(lengths)
    nnz = int(indptr[-1])
    indices = np.empty(nnz, dtype=np.int32)
    data = np.empty(nnz, dtype=np.int32)
    offset = 0
    duplicate_rows = 0
    for row_genes, row_expr in zip(raw["genes"], raw["expressions"], strict=True):
        g = np.asarray(row_genes, dtype=np.int64)
        x = np.asarray(row_expr, dtype=np.int64)
        if len(g) != len(x):
            raise ValueError("genes/expressions length mismatch")
        if len(g) and (int(g.min()) < 0 or int(g.max()) >= n_vars):
            raise ValueError(f"gene token index outside 0..{n_vars - 1}")
        if len(g) != len(np.unique(g)):
            duplicate_rows += 1
        end = offset + len(g)
        indices[offset:end] = g.astype(np.int32, copy=False)
        data[offset:end] = x.astype(np.int32, copy=False)
        offset = end
    matrix = sp.csr_matrix((data, indices, indptr), shape=(len(raw), n_vars))
    matrix.sum_duplicates()
    info = {
        "x_shape": [int(matrix.shape[0]), int(matrix.shape[1])],
        "x_nnz": int(matrix.nnz),
        "x_dtype": str(matrix.dtype),
        "rows_with_duplicate_gene_tokens_before_sum": int(duplicate_rows),
        "min_nonzero": int(matrix.data.min()) if matrix.nnz else None,
        "max_nonzero": int(matrix.data.max()) if matrix.nnz else None,
    }
    if matrix.nnz and info["min_nonzero"] < 0:
        raise ValueError("negative expression value observed; raw-count assumption violated")
    return matrix, info


def save_triplet(ln: Any, obs: pd.DataFrame, var: pd.DataFrame, x: sp.csr_matrix, prefix: str = PREFIX) -> dict[str, Any]:
    keys = artifact_keys(prefix)
    if duplicate_probe(ln, prefix):
        raise RuntimeError(f"refusing overwrite; exact target keys already exist for {prefix}")
    if x.shape != (len(obs), len(var)):
        raise ValueError(f"X shape {x.shape} does not match obs/var {(len(obs), len(var))}")
    x_adata = ad.AnnData(
        X=x.copy(),
        obs=pd.DataFrame(index=obs.index.astype(str).copy()),
        var=pd.DataFrame(index=var.index.astype(str).copy()),
    )
    with tempfile.TemporaryDirectory(prefix="perturbai_wholebrain_") as tmp_dir:
        x_path = Path(tmp_dir) / "X.h5ad"
        x_adata.write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(obs.copy(), key=keys["obs"]).save()
        x_art = ln.Artifact.from_anndata(str(x_path), key=keys["X"]).save()
        var_art = ln.Artifact.from_dataframe(var.copy(), key=keys["var"], skip_hash_lookup=True).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_key": obs_art.key, "x_key": x_art.key, "var_key": var_art.key}


def verify_prefix(ln: Any, prefix: str = PREFIX) -> dict[str, Any]:
    keys = artifact_keys(prefix)
    obs_art = ln.Artifact.get(key=keys["obs"])
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    return {
        "prefix": prefix,
        "obs_key": obs_art.key,
        "x_key": x_art.key,
        "var_key": var_art.key,
        "obs_rows": int(obs.shape[0]),
        "obs_cols": list(obs.columns),
        "var_rows": int(var.shape[0]),
        "var_cols": list(var.columns),
        "x_n_observations": int(x_art.n_observations or 0),
        "has_obs_x_link": x_art.key == keys["X"],
        "has_x_var_link": var_art.key == keys["var"],
        "same_prefix_var": var_art.key == keys["var"],
        "preserved_required_obs_columns": {c: c in obs.columns for c in OBS_REQUIRED_PRESERVE},
    }


def write_reports(report: dict[str, Any]) -> None:
    ARTIFACT_JSON.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_JSON.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# PerturbAI whole-brain CRISPR atlas pilot triplet ingestion",
        "",
        f"Generated: {report['generated_at']}",
        f"Task: {TASK_ID}",
        f"Target prefix: `{PREFIX}`",
        f"Write: {report['write']}",
        f"Dry run: {report['dry_run']}",
        "",
        "## Source",
        f"- source parquet: `{report['inputs']['source_url']}`",
        f"- gene metadata: `{report['inputs']['gene_metadata_url']}`",
        f"- source rows: {report['inputs']['source_rows']}",
        f"- gene rows: {report['inputs']['gene_metadata_rows']}",
        "",
        "## Duplicate probe / write",
        f"- duplicate keys before: `{report['duplicate_keys_before']}`",
        f"- saved: `{report.get('saved')}`",
        "",
        "## Validation",
        f"- matrix: `{report['matrix']}`",
        f"- obs shape: `{report['obs_shape']}`",
        f"- var shape: `{report['var_shape']}`",
        f"- readback: `{report.get('verification')}`",
        "",
        "## Safety",
        "Ran on pert-gym-worker-eu with tools.lamin_context.connect_pertdata() on laminlabs/pertdata branch jkobject. The converter read one 25k-row HF parquet shard and gene metadata only; it did not load the full 7.7M-cell dataset, zarr tar, or h5ad shards.",
    ]
    ARTIFACT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.dry_run and args.write:
        raise ValueError("choose at most one of --dry-run/--write")

    ensure_project_cache()
    report: dict[str, Any] = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "prefix": PREFIX,
        "dry_run": bool(args.dry_run),
        "write": bool(args.write),
    }
    ln = connect_pertdata()
    if args.write:
        ln.track(path="tools/ingest_perturbai_wholebrain_pilot.py")
        ensure_link_features(ln)
    report["lamin"] = {
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "branch_uid": ln.setup.settings.branch.uid,
    }
    report["duplicate_keys_before"] = duplicate_probe(ln)
    if report["duplicate_keys_before"] and args.write:
        raise RuntimeError(f"duplicate target exists before write: {report['duplicate_keys_before']}")

    raw, genes, input_info = load_inputs(SOURCE_PARQUET, GENE_METADATA)
    var = build_var(genes)
    obs = build_obs(raw, SOURCE_PARQUET)
    x, matrix_info = build_csr(raw, len(var))
    report.update({
        "inputs": input_info,
        "obs_shape": [int(obs.shape[0]), int(obs.shape[1])],
        "obs_columns": list(obs.columns),
        "var_shape": [int(var.shape[0]), int(var.shape[1])],
        "var_columns": list(var.columns),
        "matrix": matrix_info,
        "obs_cell_id_unique": bool(obs.index.is_unique),
        "preserved_required_obs_columns": {c: c in obs.columns for c in OBS_REQUIRED_PRESERVE},
    })
    if x.shape[0] != len(obs) or x.shape[1] != len(var):
        raise ValueError("obs/X/var shape mismatch")
    if args.write:
        report["saved"] = save_triplet(ln, obs, var, x)
        report["verification"] = verify_prefix(ln)
    elif report["duplicate_keys_before"]:
        report["verification"] = verify_prefix(ln)
    report["status"] = "written" if args.write else "dry_run" if args.dry_run else "planned"
    write_reports(report)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
