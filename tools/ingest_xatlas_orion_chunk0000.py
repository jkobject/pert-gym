#!/usr/bin/env python3
"""Write a single bounded XAtlas/Orion canonical chunk triplet.

This is intentionally separate from tools/ingest_xatlas_orion.py because the
legacy script can download huge files locally and materialize backed AnnData
views. This chunker opens the already staged h5ad with fsspec/h5py range reads,
reads exactly one CSR row slice, builds a fresh X-only AnnData, and writes one
same-prefix obs -> X -> var triplet through tools.lamin_context.connect_pertdata().
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import anndata as ad
import fsspec
import h5py
import numpy as np
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_repo_tool_module(module_name: str) -> ModuleType:
    """Load a repo-local tools/*.py helper without importing the `tools` package.

    Hermes agent environments can already have a real third-party/agent `tools`
    package on sys.path. This script must remain runnable as
    `uv run python tools/ingest_xatlas_orion_chunk0000.py ...`, so import the
    required helper files by absolute path under this checkout instead of relying
    on namespace-package resolution for `tools.*`.
    """
    path = ROOT / "tools" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(f"_pert_gym_repo_tools_{module_name}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load repo-local tools/{module_name}.py from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_clean_lamin_cache = load_repo_tool_module("clean_lamin_cache")
_lamin_context = load_repo_tool_module("lamin_context")
clean_cache = _clean_lamin_cache.clean_cache
connect_pertdata = _lamin_context.connect_pertdata
ensure_project_cache = _lamin_context.ensure_project_cache

DEFAULT_DATASET = "hct116_filtered_dual_guide_cells"
DEFAULT_CELL_LINE = "HCT116"
DEFAULT_CHUNK_INDEX = 0
DEFAULT_CHUNK_SIZE = 1000
DATASETS: dict[str, dict[str, str]] = {
    DEFAULT_DATASET: {
        "cell_line": DEFAULT_CELL_LINE,
        "source_uri": "gs://scperturb/pert-gym/staging/data/main/xatlas_orion/raw/ndownloader.figshare.com/files/55021257",
    },
    "hek293t_filtered_dual_guide_cells": {
        "cell_line": "HEK293T",
        "source_uri": "gs://scperturb/pert-gym/staging/data/main/xatlas_orion/raw/ndownloader.figshare.com/files/55074802",
    },
}
DEFAULT_PREFIX_ROOT = "xatlas/orion"
DEFAULT_TARGET_PREFIX = f"{DEFAULT_PREFIX_ROOT}/{DEFAULT_DATASET}/chunk_0000"
DEFAULT_BILLING_PROJECT = "jkobject-1549353370965"
STATUS_PATH = ROOT / "artifacts/schema_audit/xatlas_orion_hct116_chunk0000_status.json"
STATUS_DIR = STATUS_PATH.parent
STATUS_STEM = "xatlas_orion_chunk_status"


def chunk_id_for(chunk_index: int) -> str:
    return f"chunk_{chunk_index:04d}"


def target_prefix_for(dataset: str, chunk_index: int, prefix_root: str = DEFAULT_PREFIX_ROOT) -> str:
    return f"{prefix_root}/{dataset}/{chunk_id_for(chunk_index)}"


def now_local() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


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
    columns = [c.decode("utf-8") if isinstance(c, bytes) else str(c) for c in group.attrs.get("column-order", [])]
    sl = slice(start, end) if start is not None or end is not None else slice(None)

    data: dict[str, Any] = {}
    index = None
    for key in [str(index_key), *columns]:
        obj = group[key]
        if isinstance(obj, h5py.Group) and obj.attrs.get("encoding-type") == "categorical":
            codes = np.asarray(obj["codes"][sl])
            categories = decode_array(obj["categories"][:])
            values = pd.Categorical.from_codes(codes, categories=categories, ordered=bool(obj.attrs.get("ordered", False)))
        else:
            values = decode_array(obj[sl])
        if key == index_key:
            index = pd.Index(values.astype(str), name=None)
        else:
            data[key] = values
    return pd.DataFrame(data, index=index)


def read_csr_rows(x_group: h5py.Group, start: int, end: int) -> sp.csr_matrix:
    shape = tuple(int(x) for x in x_group.attrs["shape"])
    if not (0 <= start < end <= shape[0]):
        raise ValueError(f"Invalid CSR row slice {start}:{end} for shape {shape}")
    indptr_abs = np.asarray(x_group["indptr"][start : end + 1], dtype=np.int64)
    data_start = int(indptr_abs[0])
    data_end = int(indptr_abs[-1])
    data = np.asarray(x_group["data"][data_start:data_end])
    indices = np.asarray(x_group["indices"][data_start:data_end], dtype=np.int64)
    indptr = indptr_abs - data_start
    return sp.csr_matrix((data, indices, indptr), shape=(end - start, shape[1]))


def ensure_artifact_features(ln: Any) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {feature[0].dtype!r}; expected cat[Artifact]")
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def artifact_keys_for(prefix: str) -> list[str]:
    return [f"{prefix}/obs.parquet", f"{prefix}/X.h5ad", f"{prefix}/var.parquet"]


def expected_constraints(
    *,
    dataset: str = DEFAULT_DATASET,
    chunk_index: int = DEFAULT_CHUNK_INDEX,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    prefix_root: str = DEFAULT_PREFIX_ROOT,
) -> dict[str, Any]:
    dataset_info = DATASETS.get(dataset, DATASETS[DEFAULT_DATASET])
    return {
        "dataset": dataset,
        "cell_line": dataset_info["cell_line"],
        "chunk_index": chunk_index,
        "chunk_id": chunk_id_for(chunk_index),
        "chunk_size_rows": chunk_size,
        "source_uri": dataset_info["source_uri"],
        "target_prefix": target_prefix_for(dataset, chunk_index, prefix_root),
        "billing_project": DEFAULT_BILLING_PROJECT,
        "overwrite": False,
    }

def no_go(reason: str, *, patch_plan: list[str] | None = None, **metadata: Any) -> dict[str, Any]:
    entry = {
        "created_at": now_local(),
        "status": "no_go",
        "no_go_reason": reason,
        "patch_plan": patch_plan or [],
        "constraints": expected_constraints(),
    }
    entry.update(metadata)
    write_status(entry)
    return entry


def validate_smoke_constraints(args: Any) -> tuple[str, str, int, int, str]:
    """Return one authorized bounded XAtlas/Orion chunk target."""
    if args.dataset not in DATASETS:
        raise ValueError(f"NO_GO: unsupported XAtlas/Orion dataset {args.dataset!r}")
    dataset_info = DATASETS[args.dataset]
    expected_source = dataset_info["source_uri"]
    if args.source_uri not in (None, expected_source):
        raise ValueError(
            "NO_GO: source URI override is not authorized for this guarded chunker; "
            f"expected {expected_source!r}, got {args.source_uri!r}"
        )
    if args.prefix_root != DEFAULT_PREFIX_ROOT:
        raise ValueError(
            "NO_GO: prefix override is not authorized for this guarded chunker; "
            f"expected {DEFAULT_PREFIX_ROOT!r}, got {args.prefix_root!r}"
        )
    if args.chunk_index < 0:
        raise ValueError("NO_GO: chunk index must be non-negative")
    if args.chunk_size != DEFAULT_CHUNK_SIZE:
        raise ValueError("NO_GO: this guarded chunker is restricted to exactly 1000 rows per chunk")
    if args.billing_project != DEFAULT_BILLING_PROJECT:
        raise ValueError(
            "NO_GO: billing project override is not authorized for this guarded chunker; "
            f"expected {DEFAULT_BILLING_PROJECT!r}, got {args.billing_project!r}"
        )
    return (
        expected_source,
        target_prefix_for(args.dataset, args.chunk_index),
        args.chunk_index,
        args.chunk_size,
        dataset_info["cell_line"],
    )

def duplicate_probe(ln: Any, prefix: str) -> list[str]:
    keys = artifact_keys_for(prefix)
    return sorted([artifact.key for artifact in ln.Artifact.filter(key__in=keys).all() if artifact.key])


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def save_chunk_triplet(ln: Any, *, prefix: str, obs: pd.DataFrame, var: pd.DataFrame, x: sp.csr_matrix) -> dict[str, Any]:
    obs_key, x_key, var_key = artifact_keys_for(prefix)
    if duplicate_probe(ln, prefix):
        raise RuntimeError(f"Refusing overwrite; exact target already exists for {prefix}")
    if x.shape != (len(obs), len(var)):
        raise ValueError(f"X shape {x.shape} does not match obs/var {(len(obs), len(var))}")

    x_adata = ad.AnnData(
        X=x.copy(),
        obs=pd.DataFrame(index=obs.index.astype(str).copy()),
        var=pd.DataFrame(index=var.index.astype(str).copy()),
    )
    with tempfile.TemporaryDirectory(prefix="xatlas_orion_chunk_") as tmp_dir:
        x_path = Path(tmp_dir) / "X.h5ad"
        x_adata.write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(obs.copy(), key=obs_key).save()
        x_art = ln.Artifact.from_anndata(str(x_path), key=x_key).save()
        var_art = ln.Artifact.from_dataframe(var.copy(), key=var_key, skip_hash_lookup=True).save()

    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_key": obs_art.key, "x_key": x_art.key, "var_key": var_art.key}


def verify_prefix(ln: Any, prefix: str) -> dict[str, Any]:
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
        "obs_cols": list(obs.columns),
        "var_rows": int(var.shape[0]),
        "var_cols": list(var.columns),
        "x_n_observations": int(x_art.n_observations or 0),
        "link_ok": x_art.key == f"{prefix}/X.h5ad" and var_art.key == f"{prefix}/var.parquet",
    }


def write_status(entry: dict[str, Any]) -> Path:
    """Write one immutable audit checkpoint for this attempt.

    The guarded chunker may be run repeatedly while debugging no-go reasons or
    duplicate probes.  Each run must preserve prior attempt metadata, so status
    writes use exclusive creation at a timestamped path instead of updating a
    single mutable JSON file.
    """
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    attempt_id = f"{datetime.now().astimezone().strftime('%Y%m%dT%H%M%S%z')}_{time.time_ns()}_{os.getpid()}"
    for suffix in range(100):
        name = f"{STATUS_STEM}_{attempt_id}.json" if suffix == 0 else f"{STATUS_STEM}_{attempt_id}_{suffix}.json"
        path = STATUS_DIR / name
        try:
            entry["status_path"] = str(path)
            with path.open("x", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, indent=2, sort_keys=False) + "\n")
            return path
        except FileExistsError:
            continue
    raise FileExistsError(f"Could not allocate a unique non-overwriting status path in {STATUS_DIR}")


def url_to_filesystem(uri: str, billing_project: str | None) -> tuple[Any, str]:
    """Resolve a URI with requester-pays support for gs://scperturb."""
    if uri.startswith("gs://"):
        fs = fsspec.filesystem(
            "gcs",
            requester_pays=True,
            project=billing_project or DEFAULT_BILLING_PROJECT,
        )
        return fs, uri[len("gs://") :]
    return fsspec.core.url_to_fs(uri)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--source-uri", default=None)
    parser.add_argument("--prefix-root", default=DEFAULT_PREFIX_ROOT)
    parser.add_argument("--chunk-index", type=int, default=0)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--cache-type", default="readahead")
    parser.add_argument("--block-size-mib", type=int, default=1)
    parser.add_argument("--billing-project", default=DEFAULT_BILLING_PROJECT)
    args = parser.parse_args()

    try:
        source_uri, prefix, chunk_index, chunk_size, cell_line = validate_smoke_constraints(args)
    except Exception as exc:
        no_go(
            str(exc),
            patch_plan=["Open a separate reviewed card if a different source, prefix, chunk, size, or billing project is required."],
        )
        print("NO_GO", str(exc), flush=True)
        return 2

    start = chunk_index * chunk_size
    end = start + chunk_size
    started = time.time()

    ensure_project_cache()
    ln = connect_pertdata()
    print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)
    if not args.dry_run and not args.verify_only:
        ln.track(path="tools/ingest_xatlas_orion_chunk0000.py")
        ensure_artifact_features(ln)

    existing_before = duplicate_probe(ln, prefix)
    if existing_before and not args.verify_only:
        no_go(
            f"DUPLICATE_TARGET {prefix}: exact target keys already exist",
            dataset=DEFAULT_DATASET,
            cell_line=cell_line,
            prefix=prefix,
            existing_exact_keys_before=existing_before,
            dry_run=args.dry_run,
            verify_only=args.verify_only,
        )
        print("NO_GO", f"DUPLICATE_TARGET {prefix}: {existing_before}", flush=True)
        return 2
    if args.verify_only:
        verification = verify_prefix(ln, prefix) if existing_before else {"prefix": prefix, "exists": False}
        print(json.dumps({"existing_before": existing_before, "verification": verification}, indent=2), flush=True)
        return 0

    fs, path = url_to_filesystem(source_uri, args.billing_project)
    info = fs.info(path)
    source_size = int(info.get("size") or 0)
    open_kwargs: dict[str, Any] = {"block_size": args.block_size_mib * 1024 * 1024}
    if args.cache_type:
        open_kwargs["cache_type"] = args.cache_type

    print("SOURCE_INFO", source_uri, "bytes", source_size, flush=True)
    chunk_entry: dict[str, Any] = {
        "created_at": now_local(),
        "dataset": args.dataset,
        "cell_line": cell_line,
        "constraints": expected_constraints(dataset=args.dataset, chunk_index=chunk_index, chunk_size=chunk_size),
        "source_uri": source_uri,
        "source_size_bytes": source_size,
        "prefix": prefix,
        "chunk_index": chunk_index,
        "chunk_id": chunk_id_for(chunk_index),
        "chunk_size": chunk_size,
        "start": start,
        "end": end,
        "dry_run": args.dry_run,
        "existing_exact_keys_before": existing_before,
    }

    with fs.open(path, "rb", **open_kwargs) as fileobj, h5py.File(fileobj, "r") as h5:
        x_group = h5["X"]
        n_obs, n_vars = (int(x) for x in x_group.attrs["shape"])
        if end > n_obs:
            raise ValueError(f"NO_GO: requested rows {start}:{end} exceed source n_obs={n_obs}")
        chunk_entry.update({"n_obs_total": n_obs, "n_vars_total": n_vars, "chunks_total": math.ceil(n_obs / chunk_size)})
        print("SOURCE", "n_obs", n_obs, "n_vars", n_vars, "prefix", prefix, flush=True)
        obs = read_h5ad_dataframe(h5["obs"], start, end)
        var = read_h5ad_dataframe(h5["var"])
        var = var.loc[:, ~var.columns.duplicated(keep="first")]
        if not var.index.is_unique:
            shell = ad.AnnData(X=None, obs=pd.DataFrame(index=[]), var=var)
            shell.var_names_make_unique()
            var = shell.var.copy()
        x = read_csr_rows(x_group, start, end)
        if len(obs) != chunk_size or x.shape[0] != chunk_size:
            raise ValueError(
                "NO_GO: guarded chunk must produce exactly requested rows; "
                f"got obs_rows={len(obs)} x_rows={x.shape[0]}"
            )
        if x.shape[1] != len(var):
            raise ValueError(
                f"NO_GO: X n_vars={x.shape[1]} does not match var rows={len(var)}"
            )
        chunk_entry.update({
            "obs_shape": list(obs.shape),
            "obs_columns": list(obs.columns),
            "var_shape": list(var.shape),
            "var_columns": list(var.columns),
            "x_shape": list(x.shape),
            "x_nnz": int(x.nnz),
        })
        print("CHUNK_READY", json.dumps({k: chunk_entry[k] for k in ["obs_shape", "var_shape", "x_shape", "x_nnz"]}), flush=True)
        if args.dry_run:
            chunk_entry["status"] = "dry_run"
        else:
            saved = save_chunk_triplet(ln, prefix=prefix, obs=obs, var=var, x=x)
            clean_cache(ROOT / ".lamin-cache" / "lamindb")
            verification = verify_prefix(ln, prefix)
            chunk_entry.update({"status": "ingested", "saved": saved, "verification": verification})
            print("VERIFY_CHUNK", json.dumps(verification, sort_keys=True), flush=True)

    chunk_entry["elapsed_sec"] = round(time.time() - started, 2)
    write_status(chunk_entry)
    print("DONE", json.dumps({"prefix": prefix, "status": chunk_entry["status"], "elapsed_sec": chunk_entry["elapsed_sec"]}), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        entry = no_go(
            str(exc),
            patch_plan=[
                "Inspect the precise failed contract field above.",
                "Do not write or overwrite Lamin artifacts until the guarded HCT116 chunk_0000 contract is satisfiable.",
            ],
        )
        print("NO_GO", json.dumps(entry, sort_keys=True), flush=True)
        raise SystemExit(2)
