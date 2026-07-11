#!/usr/bin/env python3
"""Ingest VIPerturbSeq chunked sparse components as Lamin triplets.

Input directory is produced by tools/export_viperturb_rds_chunks.R and contains:
- manifest.json
- obs.csv
- var.csv
- chunks/chunk_XXXX/matrix_features_by_cells.mtx

Each MatrixMarket file is feature x cell and is transposed to cell x feature for
AnnData. Only one chunk is materialized at a time.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
from scipy import io as scipy_io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

PROGRESS = ROOT / "artifacts/viperturbseq_ingestion_progress.json"
VIPERTURB_LAMIN_PREFIX = "viperturb"


def build_chunk_plan(n_obs: int, chunk_size: int, dataset_name: str) -> list[dict[str, Any]]:
    """Build deterministic cell ranges and Lamin keys for a chunked dataset."""
    if n_obs < 0:
        raise ValueError("n_obs must be non-negative")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if not dataset_name:
        raise ValueError("dataset_name must not be empty")
    return [
        {
            "chunk_id": f"chunk_{chunk_index:04d}",
            "start": start,
            "end": min(n_obs, start + chunk_size),
            "prefix": f"{VIPERTURB_LAMIN_PREFIX}/{dataset_name}/chunk_{chunk_index:04d}",
        }
        for chunk_index, start in enumerate(range(0, n_obs, chunk_size))
    ]


def standardize_viperturb_obs(adata: ad.AnnData) -> ad.AnnData:
    """Map VIPerturbSeq obs columns to the pert-gym schema without Lamin side effects."""
    obs = adata.obs.copy()
    renames = {
        "gene_target": "perturbation",
        "guide_target": "perturbation",
        "target_gene": "perturbation",
        "Gene_assignment": "perturbation",
        "gene": "perturbation",
        "Guide_assignment": "guide_id",
        "guide_ID": "guide_id",
        "hash.ID": "cell_line",
    }
    obs = obs.rename(columns={k: v for k, v in renames.items() if k in obs.columns})

    if "perturbation" not in obs.columns and "guide_id" in obs.columns:
        obs["perturbation"] = obs["guide_id"].astype(str).str.split("_").str[0]

    ctrl_patterns = ["non-targeting", "no.target", "no_target", "nt", "safe_harbor", "control"]
    if "perturbation" in obs.columns:
        obs["is_control"] = obs["perturbation"].astype(str).str.lower().str.contains(
            "|".join(ctrl_patterns), na=False
        )
    elif "guide_id" in obs.columns:
        obs["is_control"] = obs["guide_id"].astype(str).str.lower().str.contains(
            "|".join(ctrl_patterns), na=False
        )

    if "nCount_RNA" in obs.columns and "n_counts" not in obs.columns:
        obs["n_counts"] = obs["nCount_RNA"]
    if "nFeature_RNA" in obs.columns and "n_genes" not in obs.columns:
        obs["n_genes"] = obs["nFeature_RNA"]

    for col, default in [
        ("perturbation_type", "CRISPRi"),
        ("organism", "human"),
        ("cell_line", "unknown"),
        ("modality", "scRNA-seq"),
        ("assay", "VIPerturb-seq"),
        ("cancer", True),
        ("disease", "unknown"),
    ]:
        if col not in obs.columns:
            obs[col] = default

    if "n_counts" in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns:
        obs["ngenes"] = obs["n_genes"]

    adata.obs = obs
    return adata


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def triplet_status(ln: Any, prefix: str) -> set[str]:
    return {
        suffix
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
        if ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
    }


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(
                f"Feature {name!r} has dtype {feature[0].dtype!r}; expected cat[Artifact]."
            )
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def build_chunk_plan(
    *,
    n_obs: int,
    chunk_size: int,
    dataset_name: str,
    prefix_root: str = VIPERTURB_LAMIN_PREFIX,
) -> list[dict[str, Any]]:
    """Return deterministic VIPerturb chunk ranges and Lamin prefixes.

    This helper is intentionally pure: it plans chunk names/ranges but does not
    read matrices or touch Lamin. ``end`` is exclusive.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if n_obs < 0:
        raise ValueError("n_obs must be non-negative")

    dataset_prefix = f"{prefix_root.rstrip('/')}/{dataset_name}"
    chunks: list[dict[str, Any]] = []
    for chunk_index, start in enumerate(range(0, n_obs, chunk_size)):
        end = min(start + chunk_size, n_obs)
        chunk_id = f"chunk_{chunk_index:04d}"
        chunks.append(
            {
                "chunk_id": chunk_id,
                "name": chunk_id,
                "start": start,
                "end": end,
                "prefix": f"{dataset_prefix}/{chunk_id}",
            }
        )
    return chunks


def load_components(components_dir: Path) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    manifest_path = components_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"MISSING_MANIFEST {manifest_path}. The R export did not finish; do not run Lamin ingestion."
        )
    manifest = load_json(manifest_path)
    obs = pd.read_csv(components_dir / manifest.get("obs", "obs.csv"), low_memory=False)
    var = pd.read_csv(components_dir / manifest.get("var", "var.csv"), low_memory=False)
    if "cell_id" not in obs.columns:
        raise ValueError("obs.csv must include a cell_id column")
    if "gene_id" not in var.columns:
        raise ValueError("var.csv must include a gene_id column")
    obs = obs.set_index("cell_id", drop=True)
    var = var.set_index("gene_id", drop=True)
    obs.index = obs.index.astype(str)
    var.index = var.index.astype(str)
    return manifest, obs, var


def write_chunk_triplet(
    ln: Any,
    *,
    prefix: str,
    obs_chunk: pd.DataFrame,
    var_df: pd.DataFrame,
    matrix_path: Path,
    cell_ids: list[str],
    overwrite: bool,
) -> dict[str, Any]:
    status = triplet_status(ln, prefix)
    if status == {"obs.parquet", "X.h5ad", "var.parquet"} and not overwrite:
        print(f"SKIP_COMPLETE {prefix}", flush=True)
        return {"status": "exists", "prefix": prefix}
    if status and not overwrite:
        raise RuntimeError(f"Partial triplet exists for {prefix}: {sorted(status)}; use --overwrite")

    matrix_raw = scipy_io.mmread(matrix_path)
    matrix = matrix_raw.tocsr().T  # type: ignore[attr-defined]
    if matrix.shape[0] != len(obs_chunk):
        raise ValueError(
            f"Chunk matrix rows {matrix.shape[0]} != obs rows {len(obs_chunk)} for {prefix}"
        )
    if matrix.shape[1] != len(var_df):
        raise ValueError(
            f"Chunk matrix cols {matrix.shape[1]} != var rows {len(var_df)} for {prefix}"
        )
    if list(obs_chunk.index.astype(str)) != cell_ids:
        raise ValueError(f"cell_ids do not match obs slice for {prefix}")

    x_adata = ad.AnnData(
        X=matrix,
        obs=pd.DataFrame(index=obs_chunk.index.astype(str)),
        var=pd.DataFrame(index=var_df.index.astype(str)),
    )
    prev_obs = list(ln.Artifact.filter(key=f"{prefix}/obs.parquet").all())
    prev_x = list(ln.Artifact.filter(key=f"{prefix}/X.h5ad").all())
    prev_var = list(ln.Artifact.filter(key=f"{prefix}/var.parquet").all())

    obs_artifact = ln.Artifact.from_dataframe(
        obs_chunk,
        key=f"{prefix}/obs.parquet",
        revises=prev_obs[-1] if (overwrite and prev_obs) else None,
    ).save()
    with tempfile.TemporaryDirectory(prefix="vip_chunk_") as tmp_dir:
        x_path = Path(tmp_dir) / "X.h5ad"
        x_adata.write_h5ad(x_path, compression="gzip")
        x_artifact = ln.Artifact.from_anndata(
            str(x_path),
            key=f"{prefix}/X.h5ad",
            revises=prev_x[-1] if (overwrite and prev_x) else None,
        ).save()
    var_artifact = ln.Artifact.from_dataframe(
        var_df,
        key=f"{prefix}/var.parquet",
        revises=prev_var[-1] if (overwrite and prev_var) else None,
        skip_hash_lookup=True,
    ).save()
    x_artifact.features.set_values({"var": var_artifact})
    obs_artifact.features.set_values({"X": x_artifact})
    return {
        "status": "ingested",
        "prefix": prefix,
        "n_obs": len(obs_chunk),
        "n_vars": len(var_df),
    }


def update_progress(dataset: str, dataset_prefix: str, chunks: list[dict[str, Any]]) -> None:
    if PROGRESS.exists():
        progress = json.loads(PROGRESS.read_text())
    else:
        progress = {"record_id": 18460279, "datasets": {}}
    progress.setdefault("datasets", {})
    entry = progress["datasets"].setdefault(dataset, {})
    entry.update(
        {
            "prefix": dataset_prefix,
            "status": "chunk_ingestion_started",
            "chunks": chunks,
        }
    )
    if chunks and all(chunk.get("status") in {"ingested", "exists", "verified"} for chunk in chunks):
        entry["status"] = "chunk_ingested"
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(progress, indent=2, sort_keys=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("components_dir", type=Path)
    parser.add_argument("--dataset", required=True, help="Dataset stem, e.g. genome_wide_filtered")
    parser.add_argument("--prefix-root", default=VIPERTURB_LAMIN_PREFIX)
    parser.add_argument("--max-chunks", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    components_dir = args.components_dir
    manifest, obs, var = load_components(components_dir)
    dataset_prefix = f"{args.prefix_root.rstrip('/')}/{args.dataset}"

    ensure_project_cache()
    ln = connect_pertdata()
    ln.track(path="tools/ingest_viperturb_chunks.py")
    ensure_link_features(ln)

    chunks = manifest["chunks"]
    if args.max_chunks is not None:
        chunks = chunks[: args.max_chunks]

    results: list[dict[str, Any]] = []
    for chunk in chunks:
        name = chunk["name"]
        start = int(chunk["start"])
        end = int(chunk["end"])
        prefix = f"{dataset_prefix}/{name}"
        matrix_path = components_dir / chunk["matrix"]
        cell_ids_path = components_dir / chunk["cell_ids"]
        cell_ids = [line.strip() for line in cell_ids_path.read_text().splitlines() if line.strip()]
        obs_chunk = obs.iloc[start:end].copy()
        dummy = ad.AnnData(obs=obs_chunk)
        dummy = standardize_viperturb_obs(dummy)
        obs_chunk = dummy.obs
        print(f"INGEST_CHUNK {prefix} obs={obs_chunk.shape} var={var.shape}", flush=True)
        result = write_chunk_triplet(
            ln,
            prefix=prefix,
            obs_chunk=obs_chunk,
            var_df=var,
            matrix_path=matrix_path,
            cell_ids=cell_ids,
            overwrite=args.overwrite,
        )
        result.update({"start": start, "end": end})
        results.append(result)
        clean_cache(ROOT / ".lamin-cache" / "lamindb")
        update_progress(args.dataset, dataset_prefix, results)
        print(f"DONE_CHUNK {prefix} status={result['status']}", flush=True)

    print("DONE_DATASET", dataset_prefix, len(results), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
