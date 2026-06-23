#!/usr/bin/env python3
"""Stream T-cell GWPS h5ad files from GCS into chunked Lamin triplets.

The T-cell GWPS assigned-guide h5ads are 118-173 GB each, so the PRISM pattern
of caching a whole ``gs://`` object locally is not safe on the Mac mini. This
script opens the staged h5ad with gcsfs/fsspec range reads, slices CSR rows with
h5py, and writes one canonical obs/X/var triplet per bounded cell chunk.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import h5py
import numpy as np
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TCELL_LAMIN_PREFIX = "tcell_gwps"
PROGRESS = ROOT / "artifacts/phase3_ingestion_progress.json"
STATUS = ROOT / "artifacts/tcell_gwps_remote_chunk_ingestion_status.json"
DEFAULT_SOURCE = (
    "gs://scperturb/pert-gym/staging/data/main/tcell_gwps/raw/"
    "genome-scale-tcell-perturb-seq.s3.amazonaws.com/marson2025_data/"
    "D4_Rest.assigned_guide.h5ad"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return default


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")


def upsert(items: list[dict[str, Any]], key: str, entry: dict[str, Any]) -> None:
    items[:] = [item for item in items if item.get(key) != entry.get(key)]
    items.append(entry)


def decode_array(values: Any) -> np.ndarray:
    arr = np.asarray(values)
    if arr.dtype.kind in {"S", "O"}:
        return np.asarray([
            x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else x
            for x in arr
        ], dtype=object)
    return arr


def read_h5ad_dataframe(group: h5py.Group, start: int | None = None, end: int | None = None) -> pd.DataFrame:
    index_key = group.attrs.get("_index", "_index")
    if isinstance(index_key, bytes):
        index_key = index_key.decode("utf-8")
    columns = list(group.attrs.get("column-order", []))
    columns = [c.decode("utf-8") if isinstance(c, bytes) else str(c) for c in columns]
    sl = slice(start, end) if start is not None or end is not None else slice(None)

    data: dict[str, Any] = {}
    index = None
    keys = [index_key] + columns
    for key in keys:
        obj = group[key]
        if isinstance(obj, h5py.Group) and obj.attrs.get("encoding-type") == "categorical":
            codes = np.asarray(obj["codes"][sl])
            cats = decode_array(obj["categories"][:])
            ordered = bool(obj.attrs.get("ordered", False))
            vals = pd.Categorical.from_codes(codes, categories=cats, ordered=ordered)
        else:
            vals = decode_array(obj[sl])
        if key == index_key:
            index = pd.Index(vals, name=None)
        else:
            data[key] = vals
    df = pd.DataFrame(data, index=index)
    return df


def standardize_tcell_obs_df(obs: pd.DataFrame, dataset: str) -> pd.DataFrame:
    obs = obs.copy()
    obs = obs.rename(
        columns={
            "perturbed_gene_name": "perturbation",
            "perturbed_gene_id": "perturbation_id",
            "pct_counts_mt": "percent_mito",
            "total_counts": "n_counts",
            "n_genes_by_counts": "n_genes",
        }
    )
    obs = obs.loc[:, ~obs.columns.duplicated(keep="first")]
    if "guide_id" in obs.columns and "guide_id_primary" not in obs.columns:
        # Preserve original guide_id explicitly; canonical guide_id remains the same column.
        obs["guide_id_primary"] = obs["guide_id"].astype(str)
    if "perturbation" not in obs.columns and "guide_id" in obs.columns:
        obs["perturbation"] = obs["guide_id"].astype(str)
    if "is_control" not in obs.columns:
        source = obs["perturbation"].astype(str) if "perturbation" in obs.columns else obs.get("guide_type", "")
        source = pd.Series(source, index=obs.index).astype(str).str.lower()
        obs["is_control"] = source.str.contains("non.target|non-target|control|safe|ntc|scramble", na=False)
        if "guide_type" in obs.columns:
            obs["is_control"] = obs["is_control"] | obs["guide_type"].astype(str).str.lower().str.contains(
                "control|non.target|non-target", na=False
            )
    for col, default in [
        ("dataset", dataset),
        ("perturbation_type", "CRISPRi"),
        ("perturbation_technology", "CRISPRi"),
        ("perturbation_library", "genome-scale T-cell Perturb-seq"),
        ("organism", "human"),
        ("cell_line", "primary_CD4_T"),
        ("cell_type", "CD4 T cell"),
        ("tissue_type", "blood"),
        ("disease", "healthy"),
        ("cancer", False),
        ("modality", "scRNA-seq"),
        ("assay", "Perturb-seq"),
        ("source_accession", "GSE314342"),
    ]:
        if col not in obs.columns:
            obs[col] = default
    if "n_counts" in obs.columns and "ncounts" not in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns and "ngenes" not in obs.columns:
        obs["ngenes"] = obs["n_genes"]
    return obs


def read_csr_rows(x_group: h5py.Group, start: int, end: int) -> sp.csr_matrix:
    shape = tuple(int(x) for x in x_group.attrs["shape"])
    indptr_abs = np.asarray(x_group["indptr"][start : end + 1], dtype=np.int64)
    data_start = int(indptr_abs[0])
    data_end = int(indptr_abs[-1])
    data = np.asarray(x_group["data"][data_start:data_end])
    indices = np.asarray(x_group["indices"][data_start:data_end], dtype=np.int64)
    indptr = indptr_abs - data_start
    return sp.csr_matrix((data, indices, indptr), shape=(end - start, shape[1]))


def ensure_artifact_features(ln) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {feature[0].dtype}, expected cat[Artifact]")
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def resolve_artifact(ln, value):
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def triplet_exists(ln, prefix: str) -> bool:
    return all(ln.Artifact.filter(key=f"{prefix}/{suffix}").exists() for suffix in ("obs.parquet", "X.h5ad", "var.parquet"))


def save_chunk_triplet(ln, prefix: str, chunk: ad.AnnData, *, overwrite: bool) -> dict[str, Any]:
    obs_key = f"{prefix}/obs.parquet"
    x_key = f"{prefix}/X.h5ad"
    var_key = f"{prefix}/var.parquet"
    prev_obs = list(ln.Artifact.filter(key=obs_key).all())
    prev_x = list(ln.Artifact.filter(key=x_key).all())
    prev_var = list(ln.Artifact.filter(key=var_key).all())

    x_adata = ad.AnnData(
        X=chunk.X.copy(),
        obs=pd.DataFrame(index=chunk.obs_names.copy()),
        var=pd.DataFrame(index=chunk.var_names.copy()),
    )
    obs_art = ln.Artifact.from_dataframe(
        chunk.obs.copy(),
        key=obs_key,
        revises=prev_obs[-1] if (overwrite and prev_obs) else None,
    ).save()
    x_art = ln.Artifact.from_anndata(
        x_adata,
        key=x_key,
        revises=prev_x[-1] if (overwrite and prev_x) else None,
    ).save()
    var_art = ln.Artifact.from_dataframe(
        chunk.var.copy(),
        key=var_key,
        revises=prev_var[-1] if (overwrite and prev_var) else None,
        skip_hash_lookup=True,
    ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    linked_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
    linked_var = resolve_artifact(ln, linked_x.features.get_values()["var"])
    if linked_x.key != x_key or linked_var.key != var_key:
        raise RuntimeError(f"Bad triplet links for {prefix}: {linked_x.key=} {linked_var.key=}")
    return {"obs": obs_art, "X": x_art, "var": var_art}


def verify_prefix(ln, prefix: str) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
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
        "var_rows": int(var.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "required_obs_present": sorted(
            set(["perturbation", "guide_id", "is_control", "organism", "cell_type", "modality", "assay"]).intersection(obs.columns)
        ),
        "controls": int(obs["is_control"].sum()) if "is_control" in obs.columns else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", default=DEFAULT_SOURCE, help="GCS h5ad URI; defaults to smallest staged D4_Rest file")
    parser.add_argument("--dataset", default="D4_Rest.assigned_guide", help="Dataset name under tcell_gwps")
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--start-chunk", type=int, default=0)
    parser.add_argument("--max-chunks", type=int, default=1)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--cache-type", default="readahead")
    parser.add_argument("--block-size-mib", type=int, default=1)
    args = parser.parse_args()
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")

    dataset_prefix = f"{TCELL_LAMIN_PREFIX}/{args.dataset}"
    status = load_json(STATUS, {"runs": [], "datasets": {}})
    run_entry: dict[str, Any] = {
        "started_at": utc_now(),
        "source_uri": args.path,
        "dataset": args.dataset,
        "chunk_size": args.chunk_size,
        "start_chunk": args.start_chunk,
        "max_chunks": args.max_chunks,
        "dry_run": args.dry_run,
        "pid": os.getpid(),
    }

    ln = None
    if not args.dry_run:
        ensure_project_cache()
        ln = connect_pertdata()
        print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)
        ensure_artifact_features(ln)
        if args.verify_only:
            verifications = []
            for chunk_i in range(args.start_chunk, args.start_chunk + args.max_chunks):
                prefix = f"{dataset_prefix}/chunk_{chunk_i:04d}"
                if triplet_exists(ln, prefix):
                    verifications.append(verify_prefix(ln, prefix))
            print(json.dumps({"verifications": verifications}, indent=2), flush=True)
            return 0

    fs, path = fsspec.core.url_to_fs(args.path)
    info = fs.info(path)
    source_size = int(info.get("size") or 0)
    print("SOURCE_INFO", args.path, "bytes", source_size, flush=True)

    open_kwargs = {"block_size": args.block_size_mib * 1024 * 1024}
    if args.cache_type:
        open_kwargs["cache_type"] = args.cache_type

    chunk_entries: list[dict[str, Any]] = []
    t0 = time.time()
    with fs.open(path, "rb", **open_kwargs) as fileobj, h5py.File(fileobj, "r") as h5:
        x_group = h5["X"]
        n_obs, n_vars = (int(x) for x in x_group.attrs["shape"])
        n_chunks_total = math.ceil(n_obs / args.chunk_size)
        end_chunk = min(n_chunks_total, args.start_chunk + args.max_chunks)
        var_all = read_h5ad_dataframe(h5["var"])
        var_all = var_all.loc[:, ~var_all.columns.duplicated(keep="first")]
        if not var_all.index.is_unique:
            shell = ad.AnnData(X=None, obs=pd.DataFrame(index=[]), var=var_all)
            shell.var_names_make_unique()
            var_all = shell.var.copy()

        print(
            "SOURCE",
            "n_obs", n_obs,
            "n_vars", n_vars,
            "chunk_size", args.chunk_size,
            "chunks", f"{args.start_chunk}:{end_chunk}/{n_chunks_total}",
            "elapsed", round(time.time() - t0, 2),
            flush=True,
        )
        for chunk_i in range(args.start_chunk, end_chunk):
            start = chunk_i * args.chunk_size
            end = min(n_obs, start + args.chunk_size)
            prefix = f"{dataset_prefix}/chunk_{chunk_i:04d}"
            if ln is not None and triplet_exists(ln, prefix) and not args.overwrite:
                print("SKIP_CHUNK", prefix, "exists", flush=True)
                chunk_entries.append({"prefix": prefix, "start": start, "end": end, "status": "exists"})
                continue
            print("READ_CHUNK", prefix, start, end, flush=True)
            obs = standardize_tcell_obs_df(read_h5ad_dataframe(h5["obs"], start, end), args.dataset)
            x = read_csr_rows(x_group, start, end)
            chunk = ad.AnnData(X=x, obs=obs, var=var_all.copy())
            chunk.obs_names_make_unique()
            chunk.var_names_make_unique()
            controls = int(chunk.obs["is_control"].sum()) if "is_control" in chunk.obs.columns else None
            entry = {
                "prefix": prefix,
                "start": start,
                "end": end,
                "n_obs": int(chunk.n_obs),
                "n_vars": int(chunk.n_vars),
                "nnz": int(chunk.X.nnz),
                "controls": controls,
            }
            print("CHUNK_READY", json.dumps(entry), flush=True)
            if args.dry_run:
                entry["status"] = "dry_run"
                chunk_entries.append(entry)
            else:
                assert ln is not None
                print("SAVE_CHUNK", prefix, flush=True)
                save_chunk_triplet(ln, prefix, chunk, overwrite=args.overwrite)
                clean_cache(ROOT / ".lamin-cache" / "lamindb")
                verification = verify_prefix(ln, prefix)
                print("VERIFY_CHUNK", json.dumps(verification), flush=True)
                entry["status"] = "ingested"
                entry["verification"] = verification
                chunk_entries.append(entry)

            # Durable per-chunk checkpoint for resume/monitoring. The final block
            # below writes the same shape again with terminal status.
            checkpoint = {
                "dataset": args.dataset,
                "prefix": dataset_prefix,
                "source_uri": args.path,
                "source_size_bytes": source_size,
                "n_obs": n_obs,
                "n_vars": n_vars,
                "chunk_size": args.chunk_size,
                "chunks_total": n_chunks_total,
                "last_run_at": utc_now(),
                "status": "dry_run" if args.dry_run else "chunked_in_progress",
                "chunks": chunk_entries,
                "note": "Remote GCS/fsspec+h5py CSR row streaming; no full h5ad local copy.",
            }
            status.setdefault("datasets", {})[args.dataset] = checkpoint
            save_json(STATUS, status)

    dataset_entry = {
        "dataset": args.dataset,
        "prefix": dataset_prefix,
        "source_uri": args.path,
        "source_size_bytes": source_size,
        "n_obs": n_obs,
        "n_vars": n_vars,
        "chunk_size": args.chunk_size,
        "chunks_total": n_chunks_total,
        "last_run_at": utc_now(),
        "status": "dry_run" if args.dry_run else "chunked_partial" if end_chunk < n_chunks_total else "chunked_complete",
        "chunks": chunk_entries,
        "note": "Remote GCS/fsspec+h5py CSR row streaming; no full h5ad local copy.",
    }
    status.setdefault("datasets", {})[args.dataset] = dataset_entry
    run_entry["finished_at"] = utc_now()
    run_entry["elapsed_sec"] = round(time.time() - t0, 2)
    run_entry["chunks"] = chunk_entries
    status.setdefault("runs", []).append(run_entry)
    save_json(STATUS, status)

    progress = load_json(
        PROGRESS,
        {
            "lamin_instance": "laminlabs/pertdata",
            "lamin_branch": {"name": "jkobject"},
            "ingested": [],
            "downloaded_not_ingested": [],
            "metadata_probed": [],
        },
    )
    target_section = "metadata_probed" if args.dry_run else "downloaded_not_ingested"
    upsert(progress.setdefault(target_section, []), "dataset", dataset_entry)
    save_json(PROGRESS, progress)
    print("DONE_DATASET", dataset_prefix, len(chunk_entries), dataset_entry["status"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
