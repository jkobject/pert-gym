#!/usr/bin/env python3
"""Ingest Sanger dual-guide CRC mapping outputs as guide-pair count triplets."""

from __future__ import annotations

import shutil
from collections import Counter
from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse

from tools.clean_lamin_cache import clean_cache
from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet
from tools.ingest_phase3_scrna import download_sanger_dualguide
from tools.lamin_context import connect_pertdata, ensure_project_cache
from tools.stage_to_gcs import access_token, upload_file


PREFIX = "sanger_dual_guide_crc/mapping_counts"
BUCKET = "scperturb"
GCS_PREFIX = "pert-gym/staging"


def triplet_exists(ln, prefix: str = PREFIX) -> bool:
    return all(
        ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
    )


def aggregate_file(path: Path) -> pd.DataFrame:
    sample = path.name.removesuffix(".mapping.out")
    walk = path.parent.name
    counts: Counter[tuple[str, str, str]] = Counter()
    total = 0
    valid = 0
    with path.open("rt", errors="replace") as handle:
        for line in handle:
            if line.startswith(("The library", "Start loading", "Read ")):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 30:
                continue
            total += 1
            guide_a, guide_b, pair_id = parts[27], parts[28], parts[29]
            if (
                pair_id in {"NOMATCH", "None", "NA", ""}
                or guide_a in {"NOMATCH", "None", "NA", ""}
                or guide_b in {"NOMATCH", "None", "NA", ""}
            ):
                continue
            counts[(guide_a, guide_b, pair_id)] += 1
            valid += 1

    frame = pd.DataFrame(
        [
            {
                "sample": sample,
                "walk": walk,
                "guide_a": guide_a,
                "guide_b": guide_b,
                "guide_pair_id": pair_id,
                "perturbation": pair_id,
                "read_count": count,
                "total_mapped_reads": total,
                "valid_pair_reads": valid,
            }
            for (guide_a, guide_b, pair_id), count in counts.items()
        ]
    )
    print(
        f"{path.name}: total={total} valid={valid} pairs={len(frame)}",
        flush=True,
    )
    return frame


def build_anndata(obs: pd.DataFrame) -> ad.AnnData:
    obs = obs.reset_index(drop=True)
    obs.index = (
        obs["sample"].astype(str)
        + ":"
        + obs["guide_pair_id"].astype(str)
        + ":"
        + obs.index.astype(str)
    )
    obs["perturbation_type"] = "CRISPRko_dual_guide"
    obs["organism"] = "human"
    obs["cancer"] = True
    obs["disease"] = "colorectal cancer"
    obs["tissue_type"] = "colon"
    obs["modality"] = "guide-pair readout"
    obs["assay"] = "dual-guide mapping"
    obs["is_control"] = obs["guide_pair_id"].astype(str).str.contains(
        "CTRL|NT|SAFE|NON", case=False, regex=True
    )
    var = pd.DataFrame({"feature_type": ["guide_pair_count"]}, index=["read_count"])
    x = sparse.csr_matrix(obs[["read_count"]].to_numpy(dtype=np.float32))
    return ad.AnnData(X=x, obs=obs, var=var)


def stage_zip(zip_path: Path) -> str:
    object_name = f"{GCS_PREFIX}/{zip_path.as_posix()}"
    metadata = upload_file(zip_path, BUCKET, object_name, access_token())
    return f"gs://{BUCKET}/{object_name}"


def main() -> int:
    ensure_project_cache()
    ln = connect_pertdata()
    if triplet_exists(ln):
        print(f"SKIP existing triplet: {PREFIX}")
        return 0

    extract_dir = download_sanger_dualguide()
    mapping_root = extract_dir / "MAPPING"
    files = sorted(mapping_root.rglob("*.mapping.out"))
    if not files:
        raise FileNotFoundError(f"No .mapping.out files under {mapping_root}")

    obs = pd.concat([aggregate_file(path) for path in files], ignore_index=True)
    print(f"aggregated_rows={len(obs)}", flush=True)
    adata = build_anndata(obs)
    print(f"adata_shape={adata.shape}", flush=True)

    migrate_h5ad_to_triplet(
        adata,
        ln,
        dataset_prefix=PREFIX,
        replace_on_instance=False,
    )
    if not triplet_exists(ln):
        raise RuntimeError(f"Triplet not found after ingest: {PREFIX}")
    print(f"SAVED {PREFIX}", flush=True)

    zip_path = extract_dir.parent / "MAPPING.zip"
    gcs_uri = stage_zip(zip_path)
    print(f"STAGED {gcs_uri}", flush=True)

    shutil.rmtree(extract_dir)
    zip_path.unlink()
    clean_cache(Path(".lamin-cache/lamindb"))
    print("CLEANED local raw and Lamin cache", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
