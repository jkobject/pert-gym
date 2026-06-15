#!/usr/bin/env python3
"""Batch-ingest simple PRISM Perturb-seq h5ads one at a time.

Policy:
- process one h5ad at a time to keep disk bounded;
- inspect with backed AnnData before loading fully;
- ingest only "simple" PRISM-shaped datasets below a configurable size;
- stage every downloaded raw h5ad to GCS and delete it locally;
- clean the project-local Lamin cache after verified ingestion.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import anndata as ad
import gdown

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet  # noqa: E402
from tools.ingest_phase3_scrna import standardize_prism_obs  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402


REQUIRED_PRISM_OBS = {"perturbation_name", "condition", "crispr_type", "organism"}


def load_progress(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {
        "lamin_instance": "laminlabs/pertdata",
        "lamin_branch": {"name": "jkobject", "uid": "GCjqQtGwPzkY"},
        "ingested": [],
        "downloaded_not_ingested": [],
        "metadata_probed": [],
        "gcs_staging": {
            "bucket": "gs://scperturb",
            "prefix": "pert-gym/staging",
            "objects": [],
            "local_policy": "Large source downloads are staged to GCS and deleted locally after upload verification.",
        },
    }


def save_progress(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")


def upsert(items: list[dict], key: str, entry: dict) -> None:
    items[:] = [item for item in items if item.get(key) != entry.get(key)]
    items.append(entry)


def dataset_name(filename: str) -> str:
    return Path(filename).stem


def google_drive_file_size(file_id: str) -> int | None:
    """Return Google Drive file size when gdown can resolve metadata."""
    try:
        url = f"https://drive.google.com/uc?id={file_id}"
        parsed_url = gdown.parse_url.parse_url(url)
        resolved_id = parsed_url[1] if isinstance(parsed_url, tuple) else file_id
        return gdown.download.get_url_from_gdrive_confirmation(
            f"https://drive.google.com/uc?id={resolved_id}", quiet=True
        )[1]
    except Exception:
        return None


def triplet_exists(ln, prefix: str) -> bool:
    return all(
        ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
    )


def inspect_h5ad(path: Path) -> dict:
    adata = ad.read_h5ad(path, backed="r")
    try:
        obs_cols = set(adata.obs.columns)
        summary = {
            "n_obs": int(adata.n_obs),
            "n_vars": int(adata.n_vars),
            "obs_cols": sorted(obs_cols),
            "var_cols": list(adata.var.columns),
            "compatible": REQUIRED_PRISM_OBS.issubset(obs_cols),
            "controls": None,
            "organism": None,
            "perturbation_type": None,
            "cell_type": None,
            "cancer_type": None,
        }
        if "condition" in adata.obs:
            summary["controls"] = int(
                adata.obs["condition"].astype(str).str.lower().eq("control").sum()
            )
        if "organism" in adata.obs:
            summary["organism"] = str(adata.obs["organism"].astype(str).mode().iloc[0])
        if "crispr_type" in adata.obs:
            summary["perturbation_type"] = str(adata.obs["crispr_type"].astype(str).mode().iloc[0])
        if "cell_type" in adata.obs:
            summary["cell_type"] = "; ".join(adata.obs["cell_type"].astype(str).value_counts().head(3).index)
        if "cancer_type" in adata.obs:
            summary["cancer_type"] = "; ".join(adata.obs["cancer_type"].astype(str).value_counts().head(3).index)
        return summary
    finally:
        adata.file.close()


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


def ingest_prism_h5ad(path: Path, name: str, prefix: str, ln) -> None:
    adata = ad.read_h5ad(path)
    adata = standardize_prism_obs(adata, name)
    migrate_h5ad_to_triplet(
        adata,
        ln,
        dataset_prefix=prefix,
        replace_on_instance=False,
        storage=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listing", type=Path, default=Path("artifacts/prism_gdrive_listing.json"))
    parser.add_argument("--progress", type=Path, default=Path("artifacts/phase3_ingestion_progress.json"))
    parser.add_argument("--max-downloads", type=int, default=3)
    parser.add_argument("--max-ingest-gb", type=float, default=4.0)
    parser.add_argument("--bucket", default="scperturb")
    parser.add_argument("--gcs-prefix", default="pert-gym/staging")
    parser.add_argument(
        "--prefer-small",
        action="store_true",
        help="Probe Google Drive sizes and process smaller remaining files first.",
    )
    parser.add_argument(
        "--retry-download-failures",
        action="store_true",
        help="Retry entries that previously failed before staging to GCS.",
    )
    parser.add_argument(
        "--retry-limit",
        type=int,
        default=3,
        help="Maximum retry attempts for previous download failures.",
    )
    args = parser.parse_args()

    ensure_project_cache()
    ln = connect_pertdata()
    progress = load_progress(args.progress)
    listing = json.loads(args.listing.read_text())
    max_ingest_bytes = int(args.max_ingest_gb * 1024**3)
    processed = 0

    ingested_prefixes = {item.get("prefix") for item in progress.get("ingested", [])}
    staged_by_name = {
        item.get("dataset"): item
        for item in progress.get("downloaded_not_ingested", [])
    }
    candidates = []

    for file_id, filename, local_path in listing:
        name = dataset_name(filename)
        prefix = f"prism_collection/{name}"
        if prefix in ingested_prefixes or triplet_exists(ln, prefix):
            print(f"SKIP_INGESTED {name}")
            continue
        staged_entry = staged_by_name.get(name)
        if staged_entry:
            retries = int(staged_entry.get("retry_attempts", 0))
            can_retry = (
                args.retry_download_failures
                and not staged_entry.get("gcs_uri")
                and "Download failed" in staged_entry.get("note", "")
                and retries < args.retry_limit
            )
            if not can_retry:
                print(f"SKIP_STAGED {name}")
                continue
            print(f"RETRY_DOWNLOAD {name} attempt={retries + 1}")
        candidates.append((file_id, filename, local_path))

    if args.prefer_small:
        sized_candidates = []
        for file_id, filename, local_path in candidates:
            size = google_drive_file_size(file_id)
            name = dataset_name(filename)
            print(f"SIZE {name} {size if size is not None else 'unknown'}")
            sized_candidates.append((size is None, size or 0, file_id, filename, local_path))
        candidates = [
            (file_id, filename, local_path)
            for _, _, file_id, filename, local_path in sorted(sized_candidates)
        ]

    for file_id, filename, local_path in candidates:
        name = dataset_name(filename)
        prefix = f"prism_collection/{name}"
        if processed >= args.max_downloads:
            print(f"STOP max_downloads={args.max_downloads}")
            break

        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"DOWNLOAD {name} {file_id}")
        try:
            gdown.download(id=file_id, output=str(path), quiet=False)
        except Exception as exc:
            if path.exists():
                path.unlink()
            previous = staged_by_name.get(name, {})
            upsert(progress.setdefault("downloaded_not_ingested", []), "dataset", {
                "dataset": name,
                "path": str(path),
                "gcs_uri": None,
                "retry_attempts": int(previous.get("retry_attempts", 0)) + 1,
                "note": f"Download failed; retry later: {type(exc).__name__}: {exc}",
            })
            save_progress(args.progress, progress)
            processed += 1
            continue
        if not path.exists() or path.stat().st_size == 0:
            raise RuntimeError(f"Download failed for {name}")

        size = path.stat().st_size
        summary = inspect_h5ad(path)
        print("INSPECT", name, json.dumps({k: v for k, v in summary.items() if k not in {"obs_cols", "var_cols"}}))

        if not summary["compatible"]:
            gcs_uri = stage_to_gcs(path, args.bucket, args.gcs_prefix)
            upsert(progress.setdefault("downloaded_not_ingested", []), "dataset", {
                "dataset": name,
                "path": str(path),
                "gcs_uri": gcs_uri,
                "n_obs": summary["n_obs"],
                "n_vars": summary["n_vars"],
                "note": f"Not compatible with required PRISM obs columns: missing {sorted(REQUIRED_PRISM_OBS - set(summary['obs_cols']))}",
            })
            progress.setdefault("gcs_staging", {}).setdefault("objects", []).append(gcs_uri)
            save_progress(args.progress, progress)
            processed += 1
            continue

        if size > max_ingest_bytes:
            gcs_uri = stage_to_gcs(path, args.bucket, args.gcs_prefix)
            upsert(progress.setdefault("downloaded_not_ingested", []), "dataset", {
                "dataset": name,
                "path": str(path),
                "gcs_uri": gcs_uri,
                "n_obs": summary["n_obs"],
                "n_vars": summary["n_vars"],
                "controls": summary["controls"],
                "organism": summary["organism"],
                "perturbation_type": summary["perturbation_type"],
                "note": f"Compatible PRISM schema but raw size {size} exceeds simple ingest threshold {max_ingest_bytes}.",
            })
            progress.setdefault("gcs_staging", {}).setdefault("objects", []).append(gcs_uri)
            save_progress(args.progress, progress)
            processed += 1
            continue

        try:
            ingest_prism_h5ad(path, name, prefix, ln)
            if not triplet_exists(ln, prefix):
                raise RuntimeError(f"Triplet not found after ingest: {prefix}")
            gcs_uri = stage_to_gcs(path, args.bucket, args.gcs_prefix)
            clean_cache(ROOT / ".lamin-cache" / "lamindb")
            upsert(progress.setdefault("ingested", []), "prefix", {
                "dataset": name,
                "prefix": prefix,
                "n_obs": summary["n_obs"],
                "n_vars": summary["n_vars"],
                "controls": summary["controls"],
                "organism": summary["organism"],
                "perturbation_type": summary["perturbation_type"],
                "raw_staged_gcs": gcs_uri,
                "note": f"cell_type={summary['cell_type']}; cancer_type={summary['cancer_type']}",
            })
            progress.setdefault("gcs_staging", {}).setdefault("objects", []).append(gcs_uri)
            save_progress(args.progress, progress)
        except Exception as exc:
            gcs_uri = stage_to_gcs(path, args.bucket, args.gcs_prefix) if path.exists() else None
            clean_cache(ROOT / ".lamin-cache" / "lamindb")
            entry = {
                "dataset": name,
                "path": str(path),
                "gcs_uri": gcs_uri,
                "n_obs": summary["n_obs"],
                "n_vars": summary["n_vars"],
                "note": f"Ingestion failed: {type(exc).__name__}: {exc}",
            }
            upsert(progress.setdefault("downloaded_not_ingested", []), "dataset", entry)
            if gcs_uri:
                progress.setdefault("gcs_staging", {}).setdefault("objects", []).append(gcs_uri)
            save_progress(args.progress, progress)
            raise
        processed += 1

    save_progress(args.progress, progress)
    print(f"DONE processed={processed}")


if __name__ == "__main__":
    main()
