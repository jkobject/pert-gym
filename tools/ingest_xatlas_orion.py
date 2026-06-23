#!/usr/bin/env python3
"""Mount-aware XAtlas/Orion backed-h5ad -> chunked Lamin triplets.

XAtlas/Orion h5ads are hundreds of GB. This script must never download them
implicitly or register the original giant h5ad as canonical ``X``. Instead it
opens an already staged local/mounted source in AnnData backed mode, copies one
row slice from ``source.X`` at a time, and writes canonical triplets:

    <prefix>/<dataset>/chunk_0000/obs.parquet -> X.h5ad -> var.parquet

The intended first smoke is HCT116 only, e.g.:

    uv run python tools/ingest_xatlas_orion.py \
      /Users/jkobject/mnt/gcs/scperturb/pert-gym/staging/data/main/xatlas_orion/raw/ndownloader.figshare.com/files/55021257 \
      --dataset hct116_filtered_dual_guide_cells \
      --dataset-prefix xatlas/orion/smoke_20260623 \
      --chunk-size 1000 --max-chunks 1
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import anndata as ad
import httpx
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

LOGGER = logging.getLogger("ingest_xatlas_orion")

ORION_ARTICLE_ID = 29190726
FIGSHARE_FILES_API = "https://api.figshare.com/v2/articles/{article_id}/files"
DEFAULT_DATASET_PREFIX = "xatlas/orion"
REQUIRED_OBS_FIELDS = [
    "dataset",
    "perturbation",
    "perturbation_type",
    "is_control",
    "cell_line",
    "organism",
    "modality",
    "assay",
]
HUGE_INGESTION_PATTERNS = (
    "ingest_prism_large_h5ad_chunks.py",
    "run_prism_ingestion_batch.py",
    "ingest_tcell_gwps_remote_chunks.py",
    "ingest_xatlas_orion.py",
)

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

    This legacy helper is kept for explicit operator use only. The CLI ingestion
    path requires a mounted/local source path and does not call this function.
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


def preprocess_xatlas_orion(files: dict[str, Path]) -> dict[str, dict[str, object]]:
    """Extract obs/var with backed-mode reads, no full-matrix load."""
    preprocessed: dict[str, dict[str, object]] = {}

    for dataset_name, path in files.items():
        source = open_backed_source(path)
        try:
            obs_df = standardize_xatlas_obs(source.obs.copy(), dataset_name)
            var_df = standardize_var_df(source.var.copy())
            log_source_summary(path, source, obs_df, var_df)
        finally:
            close_backed(source)

        preprocessed[dataset_name] = {"h5ad_path": path, "obs": obs_df, "var": var_df}

    return preprocessed


def save_xatlas_orion(
    preprocessed: dict[str, dict[str, object]],
    *,
    dataset_prefix: str = DEFAULT_DATASET_PREFIX,
    schema_uid: str | None = None,
    schema_name: str | None = None,
    chunk_size: int = 600_000,
    max_chunks: int | None = None,
    start_chunk: int = 0,
    overwrite: bool = False,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Save XAtlas/Orion triplet chunks through the project Lamin context."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if start_chunk < 0:
        raise ValueError("start_chunk must be >= 0")
    if max_chunks is not None and max_chunks <= 0:
        raise ValueError("max_chunks must be > 0 when provided")

    ensure_project_cache()
    ln = connect_pertdata()
    print_lamin_context(ln)
    if not dry_run:
        preflight_no_active_huge_ingestion()
        ln.track()
        ensure_artifact_features(ln)

    schema = None
    if schema_uid:
        schema = ln.Schema.get(uid=schema_uid)
    elif schema_name:
        schema = ln.Schema.get(name=schema_name)

    saved: list[dict[str, Any]] = []
    for dataset_name, payload in preprocessed.items():
        h5ad_path = Path(payload["h5ad_path"])
        obs_obj = payload["obs"]
        var_obj = payload["var"]

        if not isinstance(obs_obj, pd.DataFrame) or not isinstance(var_obj, pd.DataFrame):
            raise TypeError(f"Expected DataFrame obs/var for {dataset_name}")
        obs_df = cast(pd.DataFrame, obs_obj)
        var_df = cast(pd.DataFrame, var_obj)

        source = open_backed_source(h5ad_path)
        try:
            if len(obs_df) != source.n_obs or len(var_df) != source.n_vars:
                raise ValueError(
                    f"obs/var shape mismatch for {dataset_name}: "
                    f"obs={len(obs_df)} source.n_obs={source.n_obs} "
                    f"var={len(var_df)} source.n_vars={source.n_vars}"
                )
            clean_name = clean_dataset_name(dataset_name)
            dataset_base = f"{dataset_prefix.rstrip('/')}/{clean_name}"
            n_chunks_total = math.ceil(source.n_obs / chunk_size)
            end_chunk = n_chunks_total if max_chunks is None else min(n_chunks_total, start_chunk + max_chunks)
            if start_chunk >= n_chunks_total:
                raise ValueError(
                    f"start_chunk={start_chunk} is outside available chunks 0..{n_chunks_total - 1}"
                )

            for chunk_idx in range(start_chunk, end_chunk):
                start = chunk_idx * chunk_size
                end = min((chunk_idx + 1) * chunk_size, source.n_obs)
                chunk_prefix = f"{dataset_base}/chunk_{chunk_idx:04d}"
                chunk_keys = triplet_keys(chunk_prefix)
                status: str

                if triplet_exists(ln, chunk_prefix) and not overwrite:
                    raise ValueError(
                        f"Output triplet already exists for {chunk_prefix}; refusing to collide "
                        "without --overwrite."
                    )
                if any_existing_key(ln, chunk_keys.values()) and not overwrite:
                    raise ValueError(
                        f"One or more output keys already exist for {chunk_prefix}; refusing "
                        "without --overwrite."
                    )

                entry = {
                    "dataset_name": dataset_name,
                    "prefix": chunk_prefix,
                    "chunk_index": chunk_idx,
                    "start": start,
                    "end": end,
                    "n_obs": end - start,
                    "n_vars": int(source.n_vars),
                    **chunk_keys,
                }
                if dry_run:
                    print("DRY_CHUNK", json.dumps(entry, sort_keys=True), flush=True)
                    saved.append({**entry, "status": "dry_run"})
                    continue

                print("READ_X_CHUNK", chunk_prefix, start, end, flush=True)
                x_chunk = source.X[start:end, :].copy()
                x_adata = ad.AnnData(
                    X=x_chunk,
                    obs=pd.DataFrame(index=obs_df.index[start:end].copy()),
                    var=pd.DataFrame(index=var_df.index.copy()),
                )
                obs_chunk = obs_df.iloc[start:end].copy()
                var_chunk = var_df.copy()

                print(
                    "SAVE_CHUNK",
                    chunk_prefix,
                    "obs", len(obs_chunk),
                    "vars", len(var_chunk),
                    "canonical_obs", ",".join(required_obs_fields_present(obs_chunk)),
                    flush=True,
                )
                save_chunk_triplet(
                    ln,
                    chunk_prefix,
                    obs_chunk,
                    x_adata,
                    var_chunk,
                    schema=schema,
                    overwrite=overwrite,
                )
                verification = verify_triplet(ln, chunk_prefix, expected_obs=end - start, expected_vars=source.n_vars)
                clean_cache(ROOT / ".lamin-cache" / "lamindb")
                print("DONE_CHUNK", json.dumps({**entry, **verification}, sort_keys=True), flush=True)
                status = "ingested"
                saved.append({**entry, **verification, "status": status})
        finally:
            close_backed(source)

    return saved


def run_xatlas_orion_pipeline(
    *,
    working_dir: Path = Path("data/main/xatlas_orion"),
    verify_md5: bool = True,
    article_id: int = ORION_ARTICLE_ID,
    dataset_prefix: str = DEFAULT_DATASET_PREFIX,
    schema_uid: str | None = None,
    schema_name: str | None = None,
    chunk_size: int = 600_000,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """Legacy explicit-download pipeline.

    Prefer the CLI with a mounted/GCS-cached path for huge production files.
    """
    files = download_xatlas_orion(
        working_dir=working_dir,
        verify_md5=verify_md5,
        article_id=article_id,
    )
    preprocessed = preprocess_xatlas_orion(files)
    return save_xatlas_orion(
        preprocessed,
        dataset_prefix=dataset_prefix,
        schema_uid=schema_uid,
        schema_name=schema_name,
        chunk_size=chunk_size,
        overwrite=overwrite,
    )


def open_backed_source(path: Path) -> ad.AnnData:
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_file():
        raise ValueError(f"Source path is not a file: {path}")
    return ad.read_h5ad(path, backed="r")


def close_backed(source: ad.AnnData) -> None:
    if source.file is not None:
        source.file.close()


def log_source_summary(path: Path, source: ad.AnnData, obs_df: pd.DataFrame, var_df: pd.DataFrame) -> None:
    x_backing = type(source.X).__name__
    print(
        "SOURCE",
        json.dumps(
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "shape": [int(source.n_obs), int(source.n_vars)],
                "x_backing": x_backing,
                "obs_columns": list(map(str, obs_df.columns)),
                "var_columns": list(map(str, var_df.columns)),
            },
            sort_keys=True,
        ),
        flush=True,
    )


def clean_dataset_name(dataset_name: str) -> str:
    return "_".join(dataset_name.lower().replace("-", "_").split())


def infer_cell_line(dataset_name: str) -> str:
    lowered = dataset_name.lower()
    if "hct116" in lowered:
        return "HCT116"
    if "hek293" in lowered or "293t" in lowered:
        return "HEK293T"
    return "unknown"


def standardize_xatlas_obs(obs: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    obs = obs.copy()
    obs.index = obs.index.astype(str)
    obs = obs.loc[:, ~obs.columns.duplicated(keep="first")]

    rename_candidates = {
        "gene": "perturbation",
        "gene_name": "perturbation",
        "target_gene": "perturbation",
        "guide_target": "perturbation",
        "pert_gene": "perturbation",
        "perturbation_name": "perturbation",
        "crispr_type": "perturbation_type",
        "pert_type": "perturbation_type",
        "cell_line_name": "cell_line",
    }
    obs = obs.rename(columns={k: v for k, v in rename_candidates.items() if k in obs.columns and v not in obs.columns})

    if "dataset" not in obs.columns:
        obs["dataset"] = dataset_name
    if "cell_line" not in obs.columns:
        obs["cell_line"] = infer_cell_line(dataset_name)
    if "organism" not in obs.columns:
        obs["organism"] = "human"
    if "modality" not in obs.columns:
        obs["modality"] = "scRNA-seq"
    if "assay" not in obs.columns:
        obs["assay"] = "Perturb-seq"
    if "perturbation_type" not in obs.columns:
        obs["perturbation_type"] = "CRISPR"
    if "perturbation" not in obs.columns:
        obs["perturbation"] = infer_perturbation(obs)
    if "is_control" not in obs.columns:
        obs["is_control"] = infer_is_control(obs["perturbation"])
    return obs


def infer_perturbation(obs: pd.DataFrame) -> pd.Series:
    for column in ("guide", "guide_id", "sgRNA", "sgRNA_ID", "gRNA", "target"):
        if column in obs.columns:
            return obs[column].astype(str)
    return pd.Series("unknown", index=obs.index, dtype="object")


def infer_is_control(values: pd.Series) -> pd.Series:
    return values.astype(str).str.lower().str.contains(
        "control|non.target|non_target|ntc|safe_harbor|scramble|negative", na=False, regex=True
    )


def standardize_var_df(var: pd.DataFrame) -> pd.DataFrame:
    var = var.copy()
    var.index = var.index.astype(str)
    var = var.loc[:, ~var.columns.duplicated(keep="first")]
    if not var.index.is_unique:
        shell = ad.AnnData(X=None, obs=pd.DataFrame(index=[]), var=var)
        shell.var_names_make_unique()
        var = shell.var.copy()
    return var


def required_obs_fields_present(obs: pd.DataFrame) -> list[str]:
    return [field for field in REQUIRED_OBS_FIELDS if field in obs.columns]


def missing_required_obs_fields(obs: pd.DataFrame) -> list[str]:
    return [field for field in REQUIRED_OBS_FIELDS if field not in obs.columns]


def triplet_keys(prefix: str) -> dict[str, str]:
    return {
        "obs_key": f"{prefix}/obs.parquet",
        "x_key": f"{prefix}/X.h5ad",
        "var_key": f"{prefix}/var.parquet",
    }


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
    keys = triplet_keys(prefix)
    return all(ln.Artifact.filter(key=key).exists() for key in keys.values())


def any_existing_key(ln, keys) -> bool:
    return any(ln.Artifact.filter(key=key).exists() for key in keys)


def save_chunk_triplet(
    ln,
    prefix: str,
    obs_df: pd.DataFrame,
    x_adata: ad.AnnData,
    var_df: pd.DataFrame,
    *,
    schema=None,
    overwrite: bool = False,
):
    keys = triplet_keys(prefix)
    prev_obs = list(ln.Artifact.filter(key=keys["obs_key"]).all())
    prev_x = list(ln.Artifact.filter(key=keys["x_key"]).all())
    prev_var = list(ln.Artifact.filter(key=keys["var_key"]).all())

    if missing := missing_required_obs_fields(obs_df):
        raise ValueError(f"Missing canonical obs fields for {prefix}: {missing}")
    if list(x_adata.obs_names.astype(str)) != list(obs_df.index.astype(str)):
        raise ValueError(f"obs index mismatch for {prefix}")
    if list(x_adata.var_names.astype(str)) != list(var_df.index.astype(str)):
        raise ValueError(f"var index mismatch for {prefix}")

    obs_art = ln.Artifact.from_dataframe(
        obs_df,
        key=keys["obs_key"],
        schema=schema,
        revises=prev_obs[-1] if (overwrite and prev_obs) else None,
    ).save()
    x_art = ln.Artifact.from_anndata(
        x_adata,
        key=keys["x_key"],
        schema=schema,
        revises=prev_x[-1] if (overwrite and prev_x) else None,
    ).save()
    var_art = ln.Artifact.from_dataframe(
        var_df,
        key=keys["var_key"],
        schema=schema,
        revises=prev_var[-1] if (overwrite and prev_var) else None,
        skip_hash_lookup=True,
    ).save()

    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs": obs_art, "X": x_art, "var": var_art}


def verify_triplet(ln, prefix: str, *, expected_obs: int, expected_vars: int) -> dict[str, Any]:
    keys = triplet_keys(prefix)
    obs_art = ln.Artifact.get(key=keys["obs_key"])
    linked_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
    linked_var = resolve_artifact(ln, linked_x.features.get_values()["var"])
    if linked_x.key != keys["x_key"] or linked_var.key != keys["var_key"]:
        raise RuntimeError(f"Bad triplet links for {prefix}: {linked_x.key=} {linked_var.key=}")

    obs = obs_art.load()
    x = linked_x.load()
    var = linked_var.load()
    if len(obs) != expected_obs or x.n_obs != expected_obs:
        raise RuntimeError(f"Row-count mismatch for {prefix}: obs={len(obs)} X={x.n_obs}")
    if len(var) != expected_vars or x.n_vars != expected_vars:
        raise RuntimeError(f"Var-count mismatch for {prefix}: var={len(var)} X={x.n_vars}")
    if list(x.var_names.astype(str)) != list(var.index.astype(str)):
        raise RuntimeError(f"Feature index mismatch for {prefix}")
    if missing := missing_required_obs_fields(obs):
        raise RuntimeError(f"Loaded obs missing canonical fields for {prefix}: {missing}")
    return {
        "verified_obs_rows": int(len(obs)),
        "verified_x_rows": int(x.n_obs),
        "verified_var_rows": int(len(var)),
        "verified_x_vars": int(x.n_vars),
        "verified_obs_fields": required_obs_fields_present(obs),
    }


def print_lamin_context(ln) -> None:
    print(
        "LAMIN",
        ln.setup.settings.instance.slug,
        ln.setup.settings.branch.name,
        ln.setup.settings.branch.uid,
        flush=True,
    )


def current_process_family_pids() -> set[int]:
    """Return this process and its ancestors so preflight does not flag itself."""
    family = {os.getpid()}
    pid = os.getppid()
    while pid and pid not in family:
        family.add(pid)
        try:
            ppid_text = subprocess.check_output(["ps", "-o", "ppid=", "-p", str(pid)], text=True).strip()
        except Exception:
            break
        if not ppid_text or not ppid_text.isdigit():
            break
        pid = int(ppid_text)
    return family


def active_huge_ingestions() -> list[str]:
    try:
        output = subprocess.check_output(["ps", "auxww"], text=True)
    except Exception as exc:  # pragma: no cover - defensive preflight
        raise RuntimeError(f"Could not inspect running processes for huge-ingestion preflight: {exc}") from exc

    own_family = current_process_family_pids()
    matches: list[str] = []
    for line in output.splitlines():
        if not any(pattern in line for pattern in HUGE_INGESTION_PATTERNS):
            continue
        parts = line.split(None, 10)
        if len(parts) < 11 or not parts[1].isdigit():
            continue
        pid = int(parts[1])
        command = parts[10]
        if pid in own_family:
            continue
        # Ignore the command used by this preflight itself if a shell includes the pattern.
        if "ps auxww" in command or "active_huge_ingestions" in command:
            continue
        matches.append(line)
    return matches


def preflight_no_active_huge_ingestion() -> None:
    matches = active_huge_ingestions()
    if matches:
        raise RuntimeError(
            "Active huge ingestion process(es) detected; refusing Lamin writes:\n"
            + "\n".join(matches)
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="Mounted/native-cache source .h5ad; no implicit download")
    parser.add_argument("--dataset", required=True, help="Dataset name under --dataset-prefix")
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--max-chunks", type=int, default=None)
    parser.add_argument("--start-chunk", type=int, default=0)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Open/log/plan only; no Lamin writes")
    parser.add_argument("--schema-uid", default=None)
    parser.add_argument("--schema-name", default=None)
    args = parser.parse_args()

    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    if args.max_chunks is not None and args.max_chunks <= 0:
        raise ValueError("--max-chunks must be positive when provided")

    source = open_backed_source(args.path)
    try:
        obs_df = standardize_xatlas_obs(source.obs.copy(), args.dataset)
        var_df = standardize_var_df(source.var.copy())
        log_source_summary(args.path, source, obs_df, var_df)
    finally:
        close_backed(source)

    results = save_xatlas_orion(
        {args.dataset: {"h5ad_path": args.path, "obs": obs_df, "var": var_df}},
        dataset_prefix=args.dataset_prefix,
        schema_uid=args.schema_uid,
        schema_name=args.schema_name,
        chunk_size=args.chunk_size,
        max_chunks=args.max_chunks,
        start_chunk=args.start_chunk,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    print("DONE_DATASET", json.dumps(results, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
