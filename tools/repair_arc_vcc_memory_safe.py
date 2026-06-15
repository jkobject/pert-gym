#!/usr/bin/env python3
"""Memory-safe Arc VCC triplet repair/continuation.

This complements ``tools/run_arc_vcc_ingestion.py`` after an interrupted large
chunk write. It can:

1. repair missing ``var.parquet`` artifacts and ``obs -> X -> var`` links for
   partial chunks without loading the expression matrix into memory;
2. continue a split from an explicit backed h5ad source using smaller chunks.

Use the mounted GCS object as ``--source`` when possible, e.g.:

    /mnt/gcs/scperturb/pert-gym/staging/data/main/arc_vcc/train/adata_Training.h5ad
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import anndata as ad

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402


DEFAULT_SOURCES = {
    "train": Path(
        "/mnt/gcs/scperturb/pert-gym/staging/data/main/arc_vcc/train/adata_Training.h5ad"
    ),
    "test": Path(
        "/mnt/gcs/scperturb/pert-gym/staging/data/main/arc_vcc/test/adata_Test.h5ad"
    ),
    "validation": Path(
        "/mnt/gcs/scperturb/pert-gym/staging/data/main/arc_vcc/validation/adata_Validation.h5ad"
    ),
}


def artifact_exists(ln: Any, key: str) -> bool:
    return ln.Artifact.filter(key=key).exists()


def triplet_status(ln: Any, prefix: str) -> set[str]:
    return {
        suffix
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
        if artifact_exists(ln, f"{prefix}/{suffix}")
    }


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(
                f"Feature '{name}' has dtype '{feature[0].dtype}', expected cat[Artifact]."
            )
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def standardize_arc_obs(adata: ad.AnnData, split: str) -> ad.AnnData:
    """Mirror run_arc_vcc_ingestion.standardize_arc_obs without importing Lamin code."""
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


def repair_var_link(ln: Any, prefix: str, var_df) -> bool:
    """Repair a partial chunk that has obs+X but lacks var, without loading X."""
    status = triplet_status(ln, prefix)
    if status == {"obs.parquet", "X.h5ad", "var.parquet"}:
        print(f"REPAIR_SKIP complete {prefix}", flush=True)
        return False
    if not {"obs.parquet", "X.h5ad"}.issubset(status):
        print(f"REPAIR_SKIP incomplete_without_obs_x {prefix} existing={sorted(status)}", flush=True)
        return False

    ensure_link_features(ln)
    obs_artifact = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_artifact = ln.Artifact.get(key=f"{prefix}/X.h5ad")
    prev_var = list(ln.Artifact.filter(key=f"{prefix}/var.parquet").all())
    var_artifact = ln.Artifact.from_dataframe(
        var_df,
        key=f"{prefix}/var.parquet",
        revises=prev_var[-1] if prev_var else None,
        skip_hash_lookup=True,
    ).save()
    x_artifact.features.set_values({"var": var_artifact})
    obs_artifact.features.set_values({"X": x_artifact})
    print(f"REPAIRED_VAR_LINK {prefix}", flush=True)
    return True


def resolve_artifact_value(ln: Any, value: Any):
    """Resolve Lamin feature values that may be Artifact objects, UIDs, or keys."""
    if hasattr(value, "features") and hasattr(value, "key"):
        return value
    if isinstance(value, str):
        matches = list(ln.Artifact.filter(uid=value).all())
        if matches:
            return matches[-1]
        matches = list(ln.Artifact.filter(key=value).all())
        if matches:
            return matches[-1]
    raise TypeError(f"Cannot resolve Artifact feature value {value!r} ({type(value).__name__})")


def verify_triplet(ln: Any, prefix: str, *, expected_n_obs: int | None = None, expected_n_vars: int | None = None) -> dict[str, Any]:
    """Verify obs -> X -> var links without loading the potentially large X matrix."""
    obs_artifact = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_artifact = resolve_artifact_value(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact_value(ln, x_artifact.features.get_values()["var"])
    obs = obs_artifact.load()
    var = var_artifact.load()
    result = {
        "prefix": prefix,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "x_key": x_artifact.key,
        "var_key": var_artifact.key,
        "expected_n_obs": expected_n_obs,
        "expected_n_vars": expected_n_vars,
        "ok": True,
    }
    if expected_n_obs is not None:
        result["ok"] = result["ok"] and int(obs.shape[0]) == int(expected_n_obs)
    if expected_n_vars is not None:
        result["ok"] = result["ok"] and int(var.shape[0]) == int(expected_n_vars)
    print("VERIFY " + json.dumps(result, sort_keys=True), flush=True)
    if not result["ok"]:
        raise RuntimeError(f"Triplet verification failed for {prefix}: {result}")
    return result


def load_progress(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def write_progress(path: Path, progress: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(progress, indent=2, sort_keys=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split", choices=sorted(DEFAULT_SOURCES), default="train")
    parser.add_argument("--source", type=Path, default=None)
    parser.add_argument("--chunk-size", type=int, default=25_000)
    parser.add_argument("--start", type=int, default=100_000)
    parser.add_argument("--chunk-index-start", type=int, default=1)
    parser.add_argument(
        "--repair-prefix",
        action="append",
        default=[],
        help="Partial prefix to repair; can be passed multiple times.",
    )
    parser.add_argument("--repair-known-partials", action="store_true")
    parser.add_argument("--no-continue", action="store_true")
    parser.add_argument(
        "--progress", type=Path, default=Path("artifacts/arc_vcc_ingestion_progress.json")
    )
    args = parser.parse_args()

    source = args.source or DEFAULT_SOURCES[args.split]
    if not source.exists():
        raise FileNotFoundError(source)

    ensure_project_cache()
    # connect_pertdata() verifies laminlabs/pertdata on the jkobject branch.
    ln = connect_pertdata()
    ln.track()
    from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet

    backed = ad.read_h5ad(source, backed="r")
    try:
        n_obs, n_vars = int(backed.n_obs), int(backed.n_vars)
        var_df = backed.var.copy()

        repair_prefixes = list(args.repair_prefix)
        if args.repair_known_partials:
            repair_prefixes.extend(
                [
                    "arc_vcc/2025/test/chunk_0001",
                    "arc_vcc/2025/train/chunk_0000",
                ]
            )
        seen: set[str] = set()
        repaired: list[str] = []
        for prefix in repair_prefixes:
            if prefix in seen:
                continue
            seen.add(prefix)
            if repair_var_link(ln, prefix, var_df):
                repaired.append(prefix)
                verify_triplet(ln, prefix, expected_n_vars=n_vars)
                clean_cache(ROOT / ".lamin-cache" / "lamindb")

        chunks: list[dict[str, Any]] = []
        if not args.no_continue:
            if args.start >= n_obs:
                print(f"CONTINUE_SKIP start={args.start} n_obs={n_obs}", flush=True)
            chunk_index = args.chunk_index_start
            for start in range(args.start, n_obs, args.chunk_size):
                end = min(start + args.chunk_size, n_obs)
                prefix = f"arc_vcc/2025/{args.split}/chunk_{chunk_index:04d}"
                status = triplet_status(ln, prefix)
                if status == {"obs.parquet", "X.h5ad", "var.parquet"}:
                    print(f"SKIP {prefix}", flush=True)
                    verify_triplet(ln, prefix, expected_n_obs=end - start, expected_n_vars=n_vars)
                    chunks.append(
                        {"prefix": prefix, "start": start, "end": end, "status": "exists"}
                    )
                    chunk_index += 1
                    continue
                if status:
                    print(f"REPAIR_REWRITE {prefix} existing={sorted(status)}", flush=True)
                else:
                    print(f"INGEST {prefix} start={start} end={end}", flush=True)
                chunk = backed[start:end, :].to_memory()
                chunk = standardize_arc_obs(chunk, args.split)
                migrate_h5ad_to_triplet(
                    chunk,
                    ln,
                    dataset_prefix=prefix,
                    replace_on_instance=bool(status),
                )
                verify_triplet(ln, prefix, expected_n_obs=end - start, expected_n_vars=n_vars)
                clean_cache(ROOT / ".lamin-cache" / "lamindb")
                chunks.append(
                    {"prefix": prefix, "start": start, "end": end, "status": "ingested"}
                )
                print(f"SAVED {prefix}", flush=True)
                chunk_index += 1

        progress = load_progress(args.progress)
        split_progress = progress.setdefault(args.split, {})
        split_progress["n_obs"] = n_obs
        split_progress["n_vars"] = n_vars
        split_progress["raw_gcs_uri"] = (
            "gs://scperturb/" + source.as_posix()[len("/mnt/gcs/scperturb/") :]
            if source.as_posix().startswith("/mnt/gcs/scperturb/")
            else str(source)
        )
        existing_chunks = split_progress.get("chunks", [])
        by_prefix = {entry.get("prefix"): entry for entry in existing_chunks}
        for entry in chunks:
            by_prefix[entry["prefix"]] = entry
        split_progress["chunks"] = sorted(
            by_prefix.values(), key=lambda entry: entry.get("start", -1)
        )
        if repaired:
            split_progress.setdefault("repaired", [])
            for prefix in repaired:
                if prefix not in split_progress["repaired"]:
                    split_progress["repaired"].append(prefix)
        write_progress(args.progress, progress)
        print("DONE", flush=True)
    finally:
        if backed.file:
            backed.file.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
