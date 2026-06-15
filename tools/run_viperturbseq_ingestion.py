#!/usr/bin/env python3
"""Download, convert, and ingest VIPerturbSeq RDS files as Lamin triplets."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import anndata as ad
import httpx
import pandas as pd
from scipy import io as scipy_io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.ingest_phase3_scrna import (  # noqa: E402
    VIPERTURB_LAMIN_PREFIX,
    VIPERTURB_ZENODO_ID,
    get_zenodo_files,
    standardize_viperturb_obs,
)
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402
from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet  # noqa: E402
from tools.stage_to_gcs import upload_file, access_token  # noqa: E402


RAW_GCS_DIR = Path("/mnt/gcs/scperturb/pert-gym/staging/data/main/viperturb/raw")
LOCAL_DIR = ROOT / "data/main/viperturb"
PROGRESS = ROOT / "artifacts/viperturbseq_ingestion_progress.json"


def local_stem(name: str) -> str:
    return Path(name).stem


def triplet_complete(ln: Any, prefix: str) -> bool:
    return all(
        ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
    )


def download_to_mount(file_meta: dict[str, Any], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    expected = int(file_meta.get("size") or 0)
    if target.exists() and (expected == 0 or target.stat().st_size == expected):
        print(f"RAW_EXISTS {target} size={target.stat().st_size}", flush=True)
        return
    if target.exists():
        target.unlink()
    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        with client.stream("GET", file_meta["links"]["self"]) as stream:
            stream.raise_for_status()
            written = 0
            with target.open("wb") as handle:
                for chunk in stream.iter_bytes(8 * 1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    written += len(chunk)
                    print(f"DOWNLOAD {target.name}: {written}/{expected}", flush=True)
    if expected and target.stat().st_size != expected:
        raise RuntimeError(f"Size mismatch for {target}: {target.stat().st_size} != {expected}")


def convert_rds_to_h5ad(rds_path: Path, h5ad_path: Path) -> None:
    h5ad_path.parent.mkdir(parents=True, exist_ok=True)
    h5seurat = h5ad_path.with_suffix(".h5seurat")
    for path in (h5ad_path, h5seurat):
        if path.exists():
            path.unlink()
    env = os.environ.copy()
    lib = str(ROOT / ".r-lib")
    env["R_LIBS_USER"] = lib
    env["R_LIBS"] = lib
    try:
        subprocess.run(
            ["Rscript", "tools/convert_viperturb_rds.R", str(rds_path), str(h5ad_path)],
            cwd=ROOT,
            env=env,
            check=True,
        )
        if not _looks_like_guide_assay_h5ad(h5ad_path):
            return
        print(
            f"SEURATDISK_GUIDE_ASSAY {h5ad_path}; falling back to MatrixMarket RNA export",
            flush=True,
        )
    except subprocess.CalledProcessError as error:
        print(f"SEURATDISK_FAILED exit={error.returncode}; falling back to MatrixMarket", flush=True)

    if h5ad_path.exists():
        h5ad_path.unlink()
    if h5seurat.exists():
        h5seurat.unlink()

    components = h5ad_path.with_suffix(".vip_components")
    if components.exists():
        import shutil

        shutil.rmtree(components)
    subprocess.run(
        ["Rscript", "tools/export_viperturb_rds_components.R", str(rds_path), str(components)],
        cwd=ROOT,
        env=env,
        check=True,
    )
    matrix = scipy_io.mmread(components / "matrix_features_by_cells.mtx").tocsr().T
    obs = pd.read_csv(components / "obs.csv", low_memory=False)
    var = pd.read_csv(components / "var.csv", low_memory=False)
    if "cell_id" in obs.columns:
        obs = obs.set_index("cell_id", drop=True)
    if "gene_id" in var.columns:
        var = var.set_index("gene_id", drop=True)
    adata = ad.AnnData(X=matrix, obs=obs, var=var)
    adata.write_h5ad(h5ad_path, compression="gzip")
    import shutil

    shutil.rmtree(components)


def _looks_like_guide_assay_h5ad(h5ad_path: Path) -> bool:
    """Detect SeuratDisk outputs where guide/barcode assay, not RNA, became X."""
    try:
        backed = ad.read_h5ad(h5ad_path, backed="r")
    except Exception:
        return False
    try:
        if backed.n_vars < 40_000:
            return False
        names = [str(name).upper() for name in backed.var_names[:100]]
        guide_like = sum(name.startswith("R") and "-L" in name for name in names)
        return guide_like >= 20
    finally:
        if backed.file is not None:
            backed.file.close()


def stage_h5ad(path: Path) -> str:
    token = access_token()
    object_name = f"pert-gym/staging/{path.relative_to(ROOT).as_posix()}"
    metadata = upload_file(path, "scperturb", object_name, token)
    uri = f"gs://scperturb/{object_name}"
    print(f"STAGED_H5AD {uri} size={metadata.get('size')}", flush=True)
    return uri


def load_progress() -> dict[str, Any]:
    if PROGRESS.exists():
        return json.loads(PROGRESS.read_text())
    return {"record_id": VIPERTURB_ZENODO_ID, "datasets": {}}


def save_progress(progress: dict[str, Any]) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(progress, indent=2, sort_keys=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only", action="append", default=[])
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--keep-h5ad", action="store_true")
    args = parser.parse_args()

    ensure_project_cache()
    ln = connect_pertdata()
    ln.track()

    files = [
        file_meta
        for file_meta in get_zenodo_files(VIPERTURB_ZENODO_ID)
        if file_meta["key"].lower().endswith(".rds")
    ]
    if args.only:
        wanted = set(args.only)
        files = [file_meta for file_meta in files if file_meta["key"] in wanted or local_stem(file_meta["key"]) in wanted]
    files = sorted(files, key=lambda item: item["key"].lower())
    if args.limit is not None:
        files = files[: args.limit]

    progress = load_progress()
    progress["record_id"] = VIPERTURB_ZENODO_ID
    progress.setdefault("datasets", {})

    for file_meta in files:
        name = local_stem(file_meta["key"])
        prefix = f"{VIPERTURB_LAMIN_PREFIX}/{name}"
        raw_path = RAW_GCS_DIR / file_meta["key"]
        h5ad_path = LOCAL_DIR / f"{name}.h5ad"

        if triplet_complete(ln, prefix) and not args.overwrite:
            print(f"SKIP_LAMIN {prefix}", flush=True)
            progress["datasets"][name] = {
                "prefix": prefix,
                "raw_gcs_uri": f"gs://scperturb/{raw_path.as_posix()[len('/mnt/gcs/scperturb/') :]}",
                "status": "exists",
            }
            save_progress(progress)
            continue

        download_to_mount(file_meta, raw_path)
        convert_rds_to_h5ad(raw_path, h5ad_path)

        adata = ad.read_h5ad(h5ad_path)
        adata.raw = None  # type: ignore[assignment]
        adata = standardize_viperturb_obs(adata)
        migrate_h5ad_to_triplet(
            adata,
            ln,
            dataset_prefix=prefix,
            replace_on_instance=args.overwrite,
        )
        del adata
        clean_cache(ROOT / ".lamin-cache" / "lamindb")

        h5ad_uri = stage_h5ad(h5ad_path)
        if not args.keep_h5ad and h5ad_path.exists():
            h5ad_path.unlink()
        h5seurat = h5ad_path.with_suffix(".h5seurat")
        if h5seurat.exists():
            h5seurat.unlink()

        progress["datasets"][name] = {
            "prefix": prefix,
            "raw_gcs_uri": f"gs://scperturb/{raw_path.as_posix()[len('/mnt/gcs/scperturb/') :]}",
            "h5ad_gcs_uri": h5ad_uri,
            "status": "ingested",
        }
        save_progress(progress)
        print(f"DONE {prefix}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
