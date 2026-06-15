#!/usr/bin/env python3
"""Ingest Arc Virtual Cell Challenge h5ad splits as chunked triplets."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import anndata as ad

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402


ARC_BUCKET = "arc-institute-virtual-cell-atlas"
ARC_BASE = "https://storage.googleapis.com/arc-institute-virtual-cell-atlas"
ARC_FILES = {
    "validation": "virtual-cell-challenge/2025/validation/adata_Validation.h5ad",
    "test": "virtual-cell-challenge/2025/test/adata_Test.h5ad",
    "train": "virtual-cell-challenge/2025/train/adata_Training.h5ad",
}


def download(url: str, output: Path) -> None:
    import requests

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and output.stat().st_size > 0:
        return
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        written = 0
        with output.open("wb") as handle:
            for chunk in response.iter_content(8 * 1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
                written += len(chunk)
                print(f"{output.name}: {written}", flush=True)


def stage_to_gcs(path: Path, bucket: str, prefix: str) -> str:
    subprocess.run(
        [
            "python3",
            "tools/stage_to_gcs.py",
            str(path),
            "--bucket",
            bucket,
            "--prefix",
            prefix,
            "--delete-local",
        ],
        cwd=ROOT,
        check=True,
    )
    return f"gs://{bucket}/{prefix}/{path.as_posix()}"


def standardize_arc_obs(adata: ad.AnnData, split: str) -> ad.AnnData:
    obs = adata.obs.copy()
    if "perturbation" not in obs.columns:
        for candidate in ["target_gene", "gene", "condition", "perturbation_name"]:
            if candidate in obs.columns:
                obs["perturbation"] = obs[candidate].astype(str)
                break
    if "perturbation" not in obs.columns:
        obs["perturbation"] = "unknown"
    if "is_control" not in obs.columns:
        obs["is_control"] = obs["perturbation"].astype(str).str.lower().isin(
            {"control", "non-targeting", "non_targeting", "nt", "scramble"}
        )
    obs["perturbation_type"] = "CRISPRi"
    obs["organism"] = "human"
    obs["cell_line"] = "H1_hESC"
    obs["cell_type"] = "H1 human embryonic stem cell"
    obs["cancer"] = False
    obs["disease"] = "healthy"
    obs["tissue_type"] = "embryonic stem cell"
    obs["modality"] = "scRNA-seq"
    obs["assay"] = "Perturb-seq"
    obs["dataset"] = f"arc_vcc_2025_{split}"
    adata.obs = obs.loc[:, ~obs.columns.duplicated(keep="first")]
    return adata


def triplet_exists(ln, prefix: str) -> bool:
    return all(
        ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
    )


def triplet_status(ln, prefix: str) -> set[str]:
    return {
        suffix
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
        if ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split", choices=sorted(ARC_FILES), required=True)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--bucket", default="scperturb")
    parser.add_argument("--gcs-prefix", default="pert-gym/staging")
    parser.add_argument("--progress", type=Path, default=Path("artifacts/arc_vcc_ingestion_progress.json"))
    args = parser.parse_args()

    ensure_project_cache()
    ln = connect_pertdata()
    from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet

    remote = ARC_FILES[args.split]
    url = f"{ARC_BASE}/{remote}"
    local = Path("data/main/arc_vcc") / args.split / Path(remote).name

    download(url, local)

    backed = ad.read_h5ad(local, backed="r")
    try:
        n_obs, n_vars = int(backed.n_obs), int(backed.n_vars)
        chunks = []
        for start in range(0, n_obs, args.chunk_size):
            end = min(start + args.chunk_size, n_obs)
            idx = len(chunks)
            prefix = f"arc_vcc/2025/{args.split}/chunk_{idx:04d}"
            status = triplet_status(ln, prefix)
            if status == {"obs.parquet", "X.h5ad", "var.parquet"}:
                print(f"SKIP {prefix}")
                chunks.append({"prefix": prefix, "start": start, "end": end, "status": "exists"})
                continue
            if status:
                print(f"REPAIR {prefix} existing={sorted(status)}")
            chunk = backed[start:end, :].to_memory()
            chunk = standardize_arc_obs(chunk, args.split)
            migrate_h5ad_to_triplet(
                chunk,
                ln,
                dataset_prefix=prefix,
                replace_on_instance=bool(status),
            )
            clean_cache(ROOT / ".lamin-cache" / "lamindb")
            chunks.append({"prefix": prefix, "start": start, "end": end, "status": "ingested"})
            print(f"SAVED {prefix}")
    finally:
        if backed.file:
            backed.file.close()

    gcs_uri = stage_to_gcs(local, args.bucket, args.gcs_prefix)
    clean_cache(ROOT / ".lamin-cache" / "lamindb")

    progress = {}
    if args.progress.exists():
        progress = json.loads(args.progress.read_text())
    progress[args.split] = {
        "n_obs": n_obs,
        "n_vars": n_vars,
        "raw_gcs_uri": gcs_uri,
        "chunks": chunks,
    }
    args.progress.parent.mkdir(parents=True, exist_ok=True)
    args.progress.write_text(json.dumps(progress, indent=2, sort_keys=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
