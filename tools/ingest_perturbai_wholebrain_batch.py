#!/usr/bin/env python3
"""Bounded EU-worker PerturbAI whole-brain CRISPR atlas batch ingestion.

Reconciles the public Hugging Face shard list, checks live Lamin duplicate/partial
state, and ingests at most N absent sparse-row parquet shards into same-prefix
obs -> X -> var triplets on laminlabs/pertdata branch jkobject.
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import shutil
import socket
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import httpx
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_064c160c"
DATASET = "perturbai/wholebrain_crispr_atlas"
HF_BASE = f"https://huggingface.co/datasets/{DATASET}/resolve/main"
HF_API = f"https://huggingface.co/api/datasets/{DATASET}"
DATASET_SERVER_PARQUET = "https://datasets-server.huggingface.co/parquet"
GENE_METADATA = "metadata/gene_metadata.parquet"
TARGET_ROOT = f"{DATASET}"
ARTIFACT_DIR = ROOT / "artifacts/schema_audit"
LEDGER_PATH = ARTIFACT_DIR / f"perturbai_batch_runner_{TASK_ID}_ledger.jsonl"
SUMMARY_JSON = ARTIFACT_DIR / f"perturbai_batch_runner_{TASK_ID}_summary.json"
SUMMARY_MD = ARTIFACT_DIR / f"perturbai_batch_runner_{TASK_ID}_summary.md"
EXPECTED_HOST = "pert-gym-worker-eu"

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


def hf_url(path: str) -> str:
    return f"{HF_BASE}/{path}"


def artifact_keys(prefix: str) -> dict[str, str]:
    return {
        "obs": f"{prefix}/obs.parquet",
        "X": f"{prefix}/X.h5ad",
        "var": f"{prefix}/var.parquet",
    }


def ensure_eu_worker(allow_non_eu: bool = False) -> dict[str, Any]:
    host = socket.gethostname()
    info = {
        "hostname": host,
        "platform": platform.platform(),
        "expected_hostname_contains": EXPECTED_HOST,
        "allow_non_eu": allow_non_eu,
    }
    if not allow_non_eu and EXPECTED_HOST not in host:
        raise RuntimeError(
            f"refusing to run bulk PerturbAI batch on host {host!r}; expected {EXPECTED_HOST!r}"
        )
    return info


def get_json(url: str, params: dict[str, str] | None = None) -> Any:
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        return response.json()


def collect_siblings() -> list[dict[str, Any]]:
    data = get_json(HF_API)
    return list(data.get("siblings") or [])


def collect_tree_data_files() -> list[dict[str, Any]]:
    # The tree endpoint is the best reconciliation source for directory entries.
    try:
        data = get_json(f"{HF_API}/tree/main/data", {"recursive": "true"})
    except Exception:
        return []
    return list(data) if isinstance(data, list) else []


def collect_dataset_server_parquets() -> dict[str, Any]:
    try:
        data = get_json(DATASET_SERVER_PARQUET, {"dataset": DATASET})
    except Exception as exc:
        return {"error": repr(exc), "parquet_files": []}
    parquet_files = data.get("parquet_files") if isinstance(data, dict) else None
    return {"parquet_files": parquet_files if isinstance(parquet_files, list) else []}


def normalize_data_parquet(path: str) -> str | None:
    path = path.lstrip("/")
    if path.startswith("./"):
        path = path[2:]
    if path.startswith("data/") and path.endswith(".parquet"):
        return path
    return None


def reconcile_sources() -> dict[str, Any]:
    siblings = collect_siblings()
    tree = collect_tree_data_files()
    ds = collect_dataset_server_parquets()

    sibling_paths = sorted(
        p for p in (normalize_data_parquet(str(s.get("rfilename") or s.get("path") or "")) for s in siblings) if p
    )
    tree_paths = sorted(
        p for p in (normalize_data_parquet(str(s.get("path") or "")) for s in tree) if p
    )
    ds_paths: list[str] = []
    ds_files = ds.get("parquet_files", [])
    ds_errors = [ds["error"]] if ds.get("error") else []
    for item in ds_files:
        if not isinstance(item, dict):
            continue
        for key in ("filename", "url", "path"):
            value = item.get(key)
            if not value:
                continue
            text = str(value)
            if "/data/" in text:
                text = "data/" + text.split("/data/", 1)[1]
            path = normalize_data_parquet(text)
            if path:
                ds_paths.append(path)
                break
    ds_paths = sorted(set(ds_paths))

    sources = {
        "hf_siblings": sibling_paths,
        "hf_tree_data": tree_paths,
        "dataset_server": ds_paths,
    }
    union = sorted(set().union(*[set(v) for v in sources.values()]))
    intersections = sorted(set(sibling_paths) & set(tree_paths) & (set(ds_paths) if ds_paths else set(sibling_paths) & set(tree_paths)))
    missing_by_source = {name: sorted(set(union) - set(paths)) for name, paths in sources.items()}
    counts = {name: len(paths) for name, paths in sources.items()}
    counts["union"] = len(union)
    counts["all_sources_intersection"] = len(intersections)
    return {
        "counts": counts,
        "dataset_server_converted_parquet_count": len(ds_files),
        "chosen_source_basis": "union(hf_siblings,hf_tree_data,dataset_server) sorted; dataset-server errors recorded if unavailable",
        "discrepancy_resolution": {
            "missing_by_source": missing_by_source,
            "dataset_server_errors": ds_errors,
            "dataset_server_note": "The dataset-server parquet endpoint exposes converted train partition names/URLs, not original data/WB8588_* source shard paths. Its count is recorded separately and not used for source-stem selection unless paths map back to data/*.parquet.",
            "note": "Use union for planning, but refuse existing Lamin prefixes before writing. A source absent from any list is reported for reviewer attention.",
        },
        "sources": sources,
        "reconciled_sources": union,
    }


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {found[0].dtype!r}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def query_lamin_status(ln: Any) -> dict[str, Any]:
    artifacts = list(ln.Artifact.filter(key__startswith=f"{TARGET_ROOT}/").all())
    suffixes = {"obs.parquet", "X.h5ad", "var.parquet"}
    by_prefix: dict[str, set[str]] = defaultdict(set)
    rows_by_prefix: dict[str, int] = {}
    keys: list[str] = []
    for art in artifacts:
        key = art.key
        if not key:
            continue
        keys.append(key)
        parts = key.rsplit("/", 1)
        if len(parts) != 2 or parts[1] not in suffixes:
            continue
        by_prefix[parts[0]].add(parts[1])
        if parts[1] == "obs.parquet" and art.n_observations is not None:
            rows_by_prefix[parts[0]] = int(art.n_observations)
    complete = sorted(p for p, seen in by_prefix.items() if seen == suffixes)
    partial = {p: sorted(seen) for p, seen in by_prefix.items() if seen and seen != suffixes}
    return {
        "artifact_count": len(keys),
        "suffix_counts": dict(Counter(k.rsplit("/", 1)[-1] for k in keys)),
        "complete_triplet_prefixes": complete,
        "complete_triplet_prefix_count": len(complete),
        "partial_triplets": partial,
        "partial_triplet_count": len(partial),
        "obs_rows_sum": int(sum(rows_by_prefix.get(p, 0) for p in complete)),
        "all_triplet_keys": sorted(k for k in keys if k.rsplit("/", 1)[-1] in suffixes),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


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
        raise ValueError("gene_token_id is not contiguous 0..n_vars-1")
    var = genes.set_index("gene_ids", drop=False)
    var.index.name = "gene_id"
    var["gene_symbol"] = var["gene_name"].astype(str)
    var["source_dataset"] = DATASET
    var["source_gene_metadata"] = GENE_METADATA
    var["organism"] = "Mus musculus"
    return var


def load_gene_metadata() -> tuple[pd.DataFrame, dict[str, Any]]:
    with fsspec.open(hf_url(GENE_METADATA), "rb") as handle:
        genes = pd.read_parquet(handle)
    return genes, {"gene_metadata_url": hf_url(GENE_METADATA), "gene_metadata_rows": int(genes.shape[0]), "gene_metadata_columns": list(genes.columns)}


def load_source_parquet(source_parquet: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    with fsspec.open(hf_url(source_parquet), "rb") as handle:
        parquet_file = pq.ParquetFile(handle)
        table = parquet_file.read()
        source_row_groups = int(parquet_file.num_row_groups)
        source_row_group_rows = [int(parquet_file.metadata.row_group(i).num_rows) for i in range(parquet_file.num_row_groups)]
    raw = table.to_pandas()
    return raw, {
        "source_url": hf_url(source_parquet),
        "source_rows": int(raw.shape[0]),
        "source_columns": list(raw.columns),
        "source_row_groups": source_row_groups,
        "source_row_group_rows": source_row_group_rows,
    }


def build_obs(raw: pd.DataFrame, source_parquet: str) -> pd.DataFrame:
    missing = sorted(set(OBS_REQUIRED_PRESERVE) - set(raw.columns))
    if missing:
        raise ValueError(f"source parquet missing required obs columns: {missing}")
    obs = raw.drop(columns=["genes", "expressions"]).copy()
    obs = obs.set_index("cell_id", drop=False)
    if not obs.index.is_unique:
        raise ValueError("cell_id index is not unique within selected shard")
    obs.index.name = "cell_id"
    obs["dataset"] = DATASET
    obs["source_accession"] = "10.64898/2026.03.16.711480"
    obs["source_repository"] = f"https://huggingface.co/datasets/{DATASET}"
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
        "x_format": "csr",
        "dense_materialized": False,
        "rows_with_duplicate_gene_tokens_before_sum": int(duplicate_rows),
        "min_nonzero": int(matrix.data.min()) if matrix.nnz else None,
        "max_nonzero": int(matrix.data.max()) if matrix.nnz else None,
    }
    if matrix.nnz and info["min_nonzero"] < 0:
        raise ValueError("negative expression value observed; raw-count assumption violated")
    return matrix, info


def save_triplet(ln: Any, obs: pd.DataFrame, var: pd.DataFrame, x: sp.csr_matrix, prefix: str, tmp_dir: Path) -> dict[str, Any]:
    keys = artifact_keys(prefix)
    existing = sorted(a.key for a in ln.Artifact.filter(key__in=list(keys.values())).all() if a.key)
    if existing:
        raise RuntimeError(f"refusing overwrite; exact target keys already exist for {prefix}: {existing}")
    if x.shape != (len(obs), len(var)):
        raise ValueError(f"X shape {x.shape} does not match obs/var {(len(obs), len(var))}")
    x_adata = ad.AnnData(
        X=x.copy(),
        obs=pd.DataFrame(index=obs.index.astype(str).copy()),
        var=pd.DataFrame(index=var.index.astype(str).copy()),
    )
    x_path = tmp_dir / f"{Path(prefix).name}_X.h5ad"
    x_adata.write_h5ad(x_path, compression="gzip")
    obs_art = ln.Artifact.from_dataframe(obs.copy(), key=keys["obs"]).save()
    x_art = ln.Artifact.from_anndata(str(x_path), key=keys["X"]).save()
    var_art = ln.Artifact.from_dataframe(var.copy(), key=keys["var"], skip_hash_lookup=True).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_key": obs_art.key, "x_key": x_art.key, "var_key": var_art.key, "temporary_x_path": str(x_path)}


def verify_prefix(ln: Any, prefix: str) -> dict[str, Any]:
    keys = artifact_keys(prefix)
    obs_art = ln.Artifact.get(key=keys["obs"])
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    source_file_values = sorted(set(map(str, obs["source_file"].dropna().unique()))) if "source_file" in obs.columns else []
    x_semantics_values = sorted(set(map(str, obs["x_semantics"].dropna().unique()))) if "x_semantics" in obs.columns else []
    return {
        "prefix": prefix,
        "obs_key": obs_art.key,
        "x_key": x_art.key,
        "var_key": var_art.key,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "has_obs_x_link": x_art.key == keys["X"],
        "has_x_var_link": var_art.key == keys["var"],
        "same_prefix_var": var_art.key == keys["var"],
        "obs_rows_equal_x_n_observations": int(obs.shape[0]) == int(x_art.n_observations or 0),
        "source_file_values": source_file_values,
        "x_semantics_values": x_semantics_values,
        "x_semantics_raw_counts": x_semantics_values == ["raw_counts"],
        "preserved_required_obs_columns": {c: c in obs.columns for c in OBS_REQUIRED_PRESERVE},
    }


def append_ledger(row: dict[str, Any]) -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    with LEDGER_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_summary(summary: dict[str, Any]) -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# PerturbAI whole-brain atlas bounded EU batch runner",
        "",
        f"Task: `{TASK_ID}`",
        f"Generated: {summary['generated_at']}",
        f"Execution host: `{summary['host']['hostname']}`",
        f"Dry run: `{summary['dry_run']}`; write: `{summary['write']}`; max shards: `{summary['max_shards']}`",
        "",
        "## Source list reconciliation",
        f"- Counts: `{summary['source_reconciliation']['counts']}`",
        f"- Basis: {summary['source_reconciliation']['chosen_source_basis']}",
        f"- Discrepancy resolution: `{summary['source_reconciliation']['discrepancy_resolution']}`",
        "",
        "## Lamin status",
        f"- Before: `{summary['lamin_before_brief']}`",
        f"- After: `{summary.get('lamin_after_brief')}`",
        "",
        "## Batch result",
        f"- Enumerated reconciled source shards: {summary['enumerated_source_shards']}",
        f"- Selected for this tranche: `{summary['selected_sources']}`",
        f"- Staged/downloaded/read source shards: {summary['staged_or_read_source_shards']}",
        f"- Written source shards: {summary['written_source_shards']}",
        f"- Failed/skipped source shards: {summary['failed_or_skipped_source_shards']}",
        f"- Cleanup status: `{summary['cleanup_status']}`",
        "",
        "## Commands",
        *[f"- `{cmd}`" for cmd in summary["commands"]],
        "",
        "## Per-shard statuses",
    ]
    for row in summary["ledger_rows"]:
        lines.append(f"- `{row['source_parquet']}` -> `{row['prefix']}`: {row['status']}")
    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-shards", type=int, default=8)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-non-eu-host", action="store_true")
    args = parser.parse_args()
    if args.max_shards < 1 or args.max_shards > 8:
        raise ValueError("this card is bounded to 1..8 shards")
    if args.write and args.dry_run:
        raise ValueError("choose at most one of --write/--dry-run")

    host_info = ensure_eu_worker(args.allow_non_eu_host)
    ensure_project_cache()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    if LEDGER_PATH.exists():
        LEDGER_PATH.unlink()

    commands = [
        "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_perturbai_wholebrain_batch.py --max-shards 8 --write'",
    ]
    source_reconciliation = reconcile_sources()
    ln = connect_pertdata()
    if args.write:
        ln.track(path="tools/ingest_perturbai_wholebrain_batch.py")
        ensure_link_features(ln)
    lamin_before = query_lamin_status(ln)
    if lamin_before["partial_triplets"]:
        summary = {
            "generated_at": now(),
            "task_id": TASK_ID,
            "host": host_info,
            "dry_run": bool(args.dry_run),
            "write": bool(args.write),
            "max_shards": args.max_shards,
            "source_reconciliation": source_reconciliation,
            "lamin_before": lamin_before,
            "lamin_before_brief": {k: lamin_before[k] for k in ["artifact_count", "complete_triplet_prefix_count", "partial_triplet_count", "obs_rows_sum"]},
            "selected_sources": [],
            "ledger_rows": [],
            "enumerated_source_shards": len(source_reconciliation["reconciled_sources"]),
            "staged_or_read_source_shards": 0,
            "written_source_shards": 0,
            "failed_or_skipped_source_shards": len(lamin_before["partial_triplets"]),
            "cleanup_status": "no source cache created; stopped before writes because partial triplet exists",
            "commands": commands,
            "status": "blocked_partial_triplet",
        }
        write_summary(summary)
        print(json.dumps(summary, indent=2, sort_keys=True))
        raise RuntimeError("partial triplets exist; stop and create repair/review card")

    complete_prefixes = set(lamin_before["complete_triplet_prefixes"])
    selected = []
    for source in source_reconciliation["reconciled_sources"]:
        prefix = f"{TARGET_ROOT}/{Path(source).stem}"
        if prefix in complete_prefixes:
            continue
        selected.append((source, prefix))
        if len(selected) >= args.max_shards:
            break

    genes, gene_info = load_gene_metadata()
    var = build_var(genes)
    ledger_rows: list[dict[str, Any]] = []
    staged_or_read = 0
    written = 0
    failed = 0
    tmp_root = Path(tempfile.mkdtemp(prefix=f"perturbai_batch_{TASK_ID}_"))
    cleanup_status = "not attempted"
    try:
        for source, prefix in selected:
            row: dict[str, Any] = {
                "task_id": TASK_ID,
                "timestamp": now(),
                "source_parquet": source,
                "prefix": prefix,
                "planned_keys": artifact_keys(prefix),
                "status": "started",
                "retryable": False,
                "gene_metadata": gene_info,
            }
            try:
                existing = sorted(a.key for a in ln.Artifact.filter(key__in=list(artifact_keys(prefix).values())).all() if a.key)
                row["duplicate_keys_before"] = existing
                if existing:
                    raise RuntimeError(f"refusing overwrite; exact target keys already exist: {existing}")
                raw, source_info = load_source_parquet(source)
                staged_or_read += 1
                obs = build_obs(raw, source)
                x, matrix_info = build_csr(raw, len(var))
                row.update({
                    "source_info": source_info,
                    "obs_shape": [int(obs.shape[0]), int(obs.shape[1])],
                    "var_shape": [int(var.shape[0]), int(var.shape[1])],
                    "matrix": matrix_info,
                    "preserved_required_obs_columns": {c: c in obs.columns for c in OBS_REQUIRED_PRESERVE},
                })
                if args.write:
                    row["saved"] = save_triplet(ln, obs, var, x, prefix, tmp_root)
                    row["validation"] = verify_prefix(ln, prefix)
                    row["status"] = "written_validated"
                    written += 1
                else:
                    row["status"] = "dry_run_validated_locally"
                del raw, obs, x
                gc.collect()
            except Exception as exc:
                row["status"] = "failed"
                row["error_class"] = exc.__class__.__name__
                row["error"] = repr(exc)
                failed += 1
                # Duplicate/schema failures are not retryable and should be reviewed.
            finally:
                ledger_rows.append(row)
                append_ledger(row)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
        cleanup_status = f"removed temporary directory {tmp_root}"

    lamin_after = query_lamin_status(ln)
    summary = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "host": host_info,
        "dry_run": bool(args.dry_run),
        "write": bool(args.write),
        "max_shards": args.max_shards,
        "source_reconciliation": source_reconciliation,
        "lamin_before": lamin_before,
        "lamin_after": lamin_after,
        "lamin_before_brief": {k: lamin_before[k] for k in ["artifact_count", "complete_triplet_prefix_count", "partial_triplet_count", "obs_rows_sum"]},
        "lamin_after_brief": {k: lamin_after[k] for k in ["artifact_count", "complete_triplet_prefix_count", "partial_triplet_count", "obs_rows_sum"]},
        "selected_sources": [s for s, _ in selected],
        "ledger_rows": ledger_rows,
        "enumerated_source_shards": len(source_reconciliation["reconciled_sources"]),
        "staged_or_read_source_shards": staged_or_read,
        "written_source_shards": written,
        "failed_or_skipped_source_shards": failed,
        "cleanup_status": cleanup_status,
        "commands": commands,
        "status": "complete" if failed == 0 else "partial_failure",
    }
    write_summary(summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
