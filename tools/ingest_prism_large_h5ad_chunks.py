#!/usr/bin/env python3
"""Memory-bounded PRISM h5ad -> chunked Lamin triplets from GCS staging.

This script is for PRISM h5ad files that are already staged under the GCS mount
(`/mnt/gcs/scperturb/...`) and are too large for the full AnnData converter.
It opens the source h5ad in backed mode, materializes only one cell slice at a
time, writes one triplet per chunk, verifies links, and clears the project-local
Lamin cache after each chunk.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

PRISM_LAMIN_PREFIX = "prism_collection"
PROGRESS = ROOT / "artifacts/phase3_ingestion_progress.json"

OBS_RENAMES_PRISM = {
    "gene": "perturbation",
    "gene_name": "perturbation",
    "guide_target": "perturbation",
    "pert_gene": "perturbation",
    "pert_name": "perturbation",
    "perturbation_name": "perturbation",
    "pert_type": "perturbation_type",
    "crispr_type": "perturbation_type",
    "cell_line_name": "cell_line",
    "cell_type": "tissue_type",
    "cancer_type": "disease",
}


def load_progress() -> dict[str, Any]:
    if PROGRESS.exists():
        return json.loads(PROGRESS.read_text())
    return {
        "lamin_instance": "laminlabs/pertdata",
        "lamin_branch": {"name": "jkobject", "uid": "GCjqQtGwPzkY"},
        "ingested": [],
        "downloaded_not_ingested": [],
        "metadata_probed": [],
        "gcs_staging": {"bucket": "gs://scperturb", "prefix": "pert-gym/staging"},
    }


def save_progress(data: dict[str, Any]) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")


def upsert(items: list[dict[str, Any]], key: str, entry: dict[str, Any]) -> None:
    items[:] = [item for item in items if item.get(key) != entry.get(key)]
    items.append(entry)


def standardize_prism_obs_df(obs: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    obs = obs.copy()
    obs = obs.rename(columns={k: v for k, v in OBS_RENAMES_PRISM.items() if k in obs.columns})
    obs = obs.loc[:, ~obs.columns.duplicated(keep="first")]

    if "is_control" not in obs.columns and "condition" in obs.columns:
        obs["is_control"] = obs["condition"].astype(str).str.lower().eq("control")
    if "is_control" not in obs.columns and "perturbation" in obs.columns:
        ctrl_patterns = ["non-targeting", "nt", "ctrl", "control", "scramble", "safe_harbor"]
        obs["is_control"] = obs["perturbation"].astype(str).str.lower().str.contains(
            "|".join(ctrl_patterns), na=False
        )

    for col, default in [
        ("perturbation_type", "CRISPRi"),
        ("organism", "human"),
        ("cancer", True),
        ("disease", "unknown"),
        ("tissue_type", "unknown"),
    ]:
        if col not in obs.columns:
            obs[col] = default

    obs["organism"] = obs["organism"].astype(str).replace(
        {
            "human adipocytes": "human",
            "Human": "human",
            "Homo sapiens": "human",
            "Mice (Mus musculus)": "mouse",
            "Mus musculus": "mouse",
        }
    )
    if "cell_line" not in obs.columns:
        obs["cell_line"] = "unknown"
    if "dataset" not in obs.columns:
        obs["dataset"] = dataset_name
    if "modality" not in obs.columns:
        obs["modality"] = "scRNA-seq"
    if "assay" not in obs.columns:
        obs["assay"] = "Perturb-seq"
    if "n_counts" in obs.columns and "ncounts" not in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns and "ngenes" not in obs.columns:
        obs["ngenes"] = obs["n_genes"]
    if "is_control" not in obs.columns:
        obs["is_control"] = False
    return obs


def ensure_artifact_features(ln) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(
                f"Feature '{name}' has dtype '{feature[0].dtype}', expected 'cat[Artifact]'."
            )
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def resolve_artifact(ln, value):
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def triplet_exists(ln, prefix: str) -> bool:
    return all(
        ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
        for suffix in ("obs.parquet", "X.h5ad", "var.parquet")
    )


def save_chunk_triplet(ln, prefix: str, chunk: ad.AnnData, *, overwrite: bool) -> dict[str, Any]:
    obs_key = f"{prefix}/obs.parquet"
    x_key = f"{prefix}/X.h5ad"
    var_key = f"{prefix}/var.parquet"

    prev_obs = list(ln.Artifact.filter(key=obs_key).all())
    prev_x = list(ln.Artifact.filter(key=x_key).all())
    prev_var = list(ln.Artifact.filter(key=var_key).all())

    obs_df = chunk.obs.copy()
    var_df = chunk.var.copy()
    x_adata = ad.AnnData(
        X=chunk.X.copy(),
        obs=pd.DataFrame(index=chunk.obs_names.copy()),
        var=pd.DataFrame(index=chunk.var_names.copy()),
    )

    obs_art = ln.Artifact.from_dataframe(
        obs_df,
        key=obs_key,
        revises=prev_obs[-1] if (overwrite and prev_obs) else None,
    ).save()
    x_art = ln.Artifact.from_anndata(
        x_adata,
        key=x_key,
        revises=prev_x[-1] if (overwrite and prev_x) else None,
    ).save()
    var_art = ln.Artifact.from_dataframe(
        var_df,
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path, help="GCS-mounted or local source .h5ad")
    parser.add_argument("--dataset", required=True, help="Dataset name under prism_collection")
    parser.add_argument("--chunk-size", type=int, default=25000)
    parser.add_argument("--max-chunks", type=int, default=None)
    parser.add_argument("--start-chunk", type=int, default=0)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.path.exists():
        raise FileNotFoundError(args.path)
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")

    ensure_project_cache()
    ln = connect_pertdata()
    print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)
    ensure_artifact_features(ln)

    source = ad.read_h5ad(args.path, backed="r")
    try:
        n_obs, n_vars = int(source.n_obs), int(source.n_vars)
        n_chunks_total = math.ceil(n_obs / args.chunk_size)
        end_chunk = n_chunks_total if args.max_chunks is None else min(n_chunks_total, args.start_chunk + args.max_chunks)
        dataset_prefix = f"{PRISM_LAMIN_PREFIX}/{args.dataset}"
        print(
            "SOURCE",
            args.path,
            "n_obs", n_obs,
            "n_vars", n_vars,
            "chunk_size", args.chunk_size,
            "chunks", f"{args.start_chunk}:{end_chunk}/{n_chunks_total}",
            flush=True,
        )

        obs_all = standardize_prism_obs_df(source.obs.copy(), args.dataset)
        var_all = source.var.copy()
        var_all = var_all.loc[:, ~var_all.columns.duplicated(keep="first")]
        if not var_all.index.is_unique:
            # AnnData's make_unique helper needs an AnnData object; use a tiny shell.
            shell = ad.AnnData(X=None, obs=pd.DataFrame(index=[]), var=var_all)
            shell.var_names_make_unique()
            var_all = shell.var.copy()

        progress = load_progress()
        chunk_entries: list[dict[str, Any]] = []
        for chunk_i in range(args.start_chunk, end_chunk):
            start = chunk_i * args.chunk_size
            end = min(n_obs, start + args.chunk_size)
            chunk_prefix = f"{dataset_prefix}/chunk_{chunk_i:04d}"
            if triplet_exists(ln, chunk_prefix) and not args.overwrite:
                print("SKIP_CHUNK", chunk_prefix, "exists", flush=True)
                chunk_entries.append({"prefix": chunk_prefix, "start": start, "end": end, "status": "exists"})
                continue
            if args.dry_run:
                print("DRY_CHUNK", chunk_prefix, start, end, flush=True)
                chunk_entries.append({"prefix": chunk_prefix, "start": start, "end": end, "status": "dry_run"})
                continue

            print("READ_CHUNK", chunk_prefix, start, end, flush=True)
            chunk = source[start:end, :].to_memory()
            chunk.obs = obs_all.iloc[start:end].copy()
            chunk.var = var_all.copy()
            chunk.obs_names_make_unique()
            chunk.var_names_make_unique()
            print(
                "SAVE_CHUNK",
                chunk_prefix,
                "obs", chunk.n_obs,
                "vars", chunk.n_vars,
                "controls", int(chunk.obs["is_control"].sum()),
                flush=True,
            )
            save_chunk_triplet(ln, chunk_prefix, chunk, overwrite=args.overwrite)
            clean_cache(ROOT / ".lamin-cache" / "lamindb")
            print("DONE_CHUNK", chunk_prefix, flush=True)
            chunk_entries.append({"prefix": chunk_prefix, "start": start, "end": end, "status": "ingested"})

        entry = {
            "dataset": args.dataset,
            "prefix": dataset_prefix,
            "path": str(args.path),
            "gcs_uri": str(args.path).replace("/mnt/gcs/scperturb/", "gs://scperturb/", 1)
            if str(args.path).startswith("/mnt/gcs/scperturb/") else None,
            "n_obs": n_obs,
            "n_vars": n_vars,
            "chunk_size": args.chunk_size,
            "status": "chunked" if not args.dry_run else "dry_run",
            "chunks": chunk_entries,
            "note": "Memory-bounded backed h5ad ingestion; one Lamin triplet per cell chunk.",
        }
        upsert(progress.setdefault("downloaded_not_ingested", []), "dataset", entry)
        save_progress(progress)
        print("DONE_DATASET", dataset_prefix, len(chunk_entries), flush=True)
        return 0
    finally:
        source.file.close()


if __name__ == "__main__":
    raise SystemExit(main())
