#!/usr/bin/env python3
"""Straightforward X-Atlas/Orion ingestion for very large .h5ad files.

Why this version exists:
- The Orion files are extremely large (roughly 150 GB and 350 GB).
- Loading them fully with `ad.read_h5ad(path)` is not practical.
- This pipeline uses backed mode to extract `obs` and `var` without loading full `X`.

Pipeline steps:
1) `download_xatlas_orion()`
   Downloads the two known Orion .h5ad files and (optionally) validates MD5
   checksums by fetching `.md5` sidecar files from the Figshare article API.
2) `preprocess_xatlas_orion()`
   Opens each h5ad with `backed="r"`, copies only `obs` and `var`, then closes.
3) `save_xatlas_orion()`
   Saves triplet artifacts in Lamin schema style:
   - `<dataset>/obs.parquet`
   - `<dataset>/X.h5ad`  (registered from original local file path)
   - `<dataset>/var.parquet`
   and links `obs -> X -> var` through artifact features.
"""

from __future__ import annotations

import hashlib
import logging
import math
import tempfile
from pathlib import Path

import anndata as ad
import httpx
import lamindb as ln
import pandas as pd

LOGGER = logging.getLogger("ingest_xatlas_orion")

ORION_ARTICLE_ID = 29190726
FIGSHARE_FILES_API = "https://api.figshare.com/v2/articles/{article_id}/files"

ORION_H5AD_URLS = {
    "hct116_filtered_dual_guide_cells": "https://plus.figshare.com/ndownloader/files/55021257",
    "hek293t_filtered_dual_guide_cells": "https://plus.figshare.com/ndownloader/files/55074802",
}


def download_xatlas_orion(
    working_dir: Path = Path("data/main/xatlas_orion"),
    verify_md5: bool = True,
    article_id: int = ORION_ARTICLE_ID,
) -> dict[str, Path]:
    """Download Orion h5ad files and optionally verify checksums.

    Parameters
    ----------
    working_dir:
        Local data cache directory.
    verify_md5:
        If True, fetches `.md5` sidecar files from the Figshare article and
        validates downloaded h5ad files.
    article_id:
        Figshare article id used only for md5 sidecar discovery.

    Returns
    -------
    dict[str, Path]
        Mapping from dataset name to local h5ad path.
    """
    raw_dir = working_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    downloaded: dict[str, Path] = {}
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        for dataset_name, url in ORION_H5AD_URLS.items():
            target = raw_dir / f"{dataset_name}.h5ad"
            if target.exists() and target.stat().st_size > 0:
                LOGGER.info("Already downloaded: %s", target)
                downloaded[dataset_name] = target
                continue

            LOGGER.info("Downloading %s -> %s", dataset_name, target)
            with client.stream("GET", url) as stream:
                stream.raise_for_status()
                with target.open("wb") as handle:
                    for chunk in stream.iter_bytes(chunk_size=8 * 1024 * 1024):
                        handle.write(chunk)
            downloaded[dataset_name] = target

    if verify_md5:
        expected_md5 = _fetch_expected_md5(article_id)
        for dataset_name, path in downloaded.items():
            expected = expected_md5.get(path.name)
            if not expected:
                LOGGER.warning("No md5 sidecar found for %s; skipping checksum", path.name)
                continue
            observed = _md5sum(path)
            if observed.lower() != expected.lower():
                raise ValueError(
                    f"MD5 mismatch for {path.name}: expected={expected} observed={observed}"
                )
            LOGGER.info("MD5 verified: %s", path.name)

    return downloaded


def preprocess_xatlas_orion(
    files: dict[str, Path],
) -> dict[str, dict[str, object]]:
    """Extract obs/var with backed-mode reads, no full-matrix load.

    Parameters
    ----------
    files:
        Output from `download_xatlas_orion()`.

    Returns
    -------
    dict[str, dict[str, object]]
        For each dataset:
        - `h5ad_path`: source local path
        - `obs`: pandas DataFrame
        - `var`: pandas DataFrame
    """
    preprocessed: dict[str, dict[str, object]] = {}

    for dataset_name, path in files.items():
        if not path.exists():
            raise FileNotFoundError(path)

        backed = ad.read_h5ad(path, backed="r")
        try:
            obs_df = backed.obs.copy()
            obs_df.index = obs_df.index.astype(str)

            var_df = backed.var.copy()
            var_df.index = var_df.index.astype(str)
        finally:
            if backed.file is not None:
                backed.file.close()

        preprocessed[dataset_name] = {
            "h5ad_path": path,
            "obs": obs_df,
            "var": var_df,
        }

    return preprocessed


def save_xatlas_orion(
    preprocessed: dict[str, dict[str, object]],
    *,
    instance: str = "laminlabs/pertdata",
    dataset_prefix: str = "xatlas/orion",
    schema_uid: str | None = None,
    schema_name: str | None = None,
    chunk_size: int = 600_000,
    overwrite: bool = False,
) -> list[dict[str, str]]:
    """Save triplet artifacts and feature links in Lamin.

    Notes
    -----
    `X.h5ad` is saved from the original source path to avoid loading/re-writing
    massive matrices in memory.
    """
    ln.connect(instance)

    schema = None
    if schema_uid:
        schema = ln.Schema.get(uid=schema_uid)
    elif schema_name:
        schema = ln.Schema.get(name=schema_name)

    for name in ["X", "var"]:
        existing = list(ln.Feature.filter(name=name).all())
        if existing:
            if existing[0].dtype != "cat[Artifact]":
                raise ValueError(
                    f"Feature '{name}' exists with dtype '{existing[0].dtype}', "
                    "expected 'cat[Artifact]'."
                )
        else:
            ln.Feature(name=name, dtype="cat[Artifact]").save()

    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    saved: list[dict[str, str]] = []
    for dataset_name, payload in preprocessed.items():
        h5ad_path = Path(payload["h5ad_path"])
        obs_df = payload["obs"]
        var_df = payload["var"]

        if not isinstance(obs_df, pd.DataFrame) or not isinstance(var_df, pd.DataFrame):
            raise TypeError(f"Expected DataFrame obs/var for {dataset_name}")

        clean_name = "_".join(dataset_name.lower().replace("-", "_").split())
        dataset_base = f"{dataset_prefix.rstrip('/')}/{clean_name}"
        n_obs = len(obs_df)
        n_chunks = max(1, math.ceil(n_obs / chunk_size))

        planned_keys: list[str] = []
        for chunk_idx in range(n_chunks):
            if n_chunks == 1:
                chunk_prefix = dataset_base
            else:
                chunk_prefix = f"{dataset_base}/chunk_{chunk_idx:04d}"
            planned_keys.extend(
                [
                    f"{chunk_prefix}/obs.parquet",
                    f"{chunk_prefix}/X.h5ad",
                    f"{chunk_prefix}/var.parquet",
                ]
            )
        if not overwrite:
            existing = list(ln.Artifact.filter(key__in=planned_keys).all())
            if existing:
                keys = ", ".join(sorted([a.key for a in existing if a.key]))
                raise ValueError(
                    f"Output exists for '{dataset_name}': {keys}. Use overwrite=True."
                )

        if n_chunks == 1:
            chunk_prefix = dataset_base
            obs_key = f"{chunk_prefix}/obs.parquet"
            x_key = f"{chunk_prefix}/X.h5ad"
            var_key = f"{chunk_prefix}/var.parquet"

            obs_artifact = ln.Artifact.from_dataframe(obs_df, key=obs_key, schema=schema).save()
            x_artifact = ln.Artifact.from_anndata(
                str(h5ad_path), key=x_key, schema=schema
            ).save()
            var_artifact = ln.Artifact.from_dataframe(var_df, key=var_key, schema=schema).save()

            x_artifact.features.set_values({"var": var_artifact})
            obs_artifact.features.set_values({"X": x_artifact})

            saved.append(
                {
                    "dataset_name": dataset_name,
                    "chunk_index": "0",
                    "obs_key": obs_key,
                    "x_key": x_key,
                    "var_key": var_key,
                }
            )
            continue

        backed = ad.read_h5ad(h5ad_path, backed="r")
        try:
            for chunk_idx in range(n_chunks):
                start = chunk_idx * chunk_size
                end = min((chunk_idx + 1) * chunk_size, n_obs)
                chunk_prefix = f"{dataset_base}/chunk_{chunk_idx:04d}"
                obs_key = f"{chunk_prefix}/obs.parquet"
                x_key = f"{chunk_prefix}/X.h5ad"
                var_key = f"{chunk_prefix}/var.parquet"

                obs_chunk = obs_df.iloc[start:end].copy()

                # Read only one row-slice at a time from the large source h5ad.
                x_chunk_adata = backed[start:end, :].to_memory()
                x_chunk_adata.obs = pd.DataFrame(index=obs_chunk.index)
                x_chunk_adata.var = pd.DataFrame(index=var_df.index)

                obs_artifact = ln.Artifact.from_dataframe(
                    obs_chunk, key=obs_key, schema=schema
                ).save()
                var_artifact = ln.Artifact.from_dataframe(
                    var_df, key=var_key, schema=schema
                ).save()

                with tempfile.TemporaryDirectory(prefix="orion_chunk_") as tmp_dir:
                    chunk_path = Path(tmp_dir) / f"{clean_name}_chunk_{chunk_idx:04d}.h5ad"
                    x_chunk_adata.write_h5ad(chunk_path)
                    x_artifact = ln.Artifact.from_anndata(
                        str(chunk_path), key=x_key, schema=schema
                    ).save()

                x_artifact.features.set_values({"var": var_artifact})
                obs_artifact.features.set_values({"X": x_artifact})

                saved.append(
                    {
                        "dataset_name": dataset_name,
                        "chunk_index": str(chunk_idx),
                        "obs_key": obs_key,
                        "x_key": x_key,
                        "var_key": var_key,
                    }
                )
        finally:
            if backed.file is not None:
                backed.file.close()

    return saved


def run_xatlas_orion_pipeline(
    *,
    working_dir: Path = Path("data/main/xatlas_orion"),
    verify_md5: bool = True,
    article_id: int = ORION_ARTICLE_ID,
    instance: str = "laminlabs/pertdata",
    dataset_prefix: str = "xatlas/orion",
    schema_uid: str | None = None,
    schema_name: str | None = None,
    chunk_size: int = 600_000,
    overwrite: bool = False,
) -> list[dict[str, str]]:
    """Run `download -> preprocess -> save` in one call."""
    files = download_xatlas_orion(
        working_dir=working_dir,
        verify_md5=verify_md5,
        article_id=article_id,
    )
    preprocessed = preprocess_xatlas_orion(files)
    return save_xatlas_orion(
        preprocessed,
        instance=instance,
        dataset_prefix=dataset_prefix,
        schema_uid=schema_uid,
        schema_name=schema_name,
        chunk_size=chunk_size,
        overwrite=overwrite,
    )


def _fetch_expected_md5(article_id: int) -> dict[str, str]:
    """Fetch expected md5 checksums from Figshare `.md5` sidecar files."""
    expected: dict[str, str] = {}
    api_url = FIGSHARE_FILES_API.format(article_id=article_id)

    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(api_url)
        response.raise_for_status()
        for row in response.json():
            name = str(row.get("name", ""))
            if not name.endswith(".md5"):
                continue
            md5_url = str(row["download_url"])
            md5_text = client.get(md5_url).text.strip()
            if not md5_text:
                continue
            token = md5_text.split()[0]
            expected[name[: -len(".md5")]] = token

    return expected


def _md5sum(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
