"""Build and optionally apply the shared-var alias policy for chunked datasets.

Default mode is metadata-only and safe: it writes a next-version manifest TSV
under artifacts/schema_audit and validates the manifest contract.  Passing
`--apply-lamin` creates one shared `<logical_dataset>/var.h5ad` artifact per
exact-hash family, relinks each chunk `X.h5ad` to that shared var, and creates a
new canonical Collection containing the manifest obs members.

The loader contract remains obs -> X -> var feature resolution; this script never
infers var from artifact-key string replacement during validation/read-back.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
from scipy import sparse

from tools.query_unified_collection import (
    build_shared_var_manifest,
    get_triplet_artifacts,
    select_latest_artifact,
    shared_var_key_for_logical_dataset,
    validate_manifest_var_policy,
    validate_triplet_var_policy,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
AUDIT_DIR = REPO_ROOT / "artifacts" / "schema_audit"
DEFAULT_MANIFEST = AUDIT_DIR / "unified_collection_manifest_20260621.tsv"
DEFAULT_CHUNK_METADATA = AUDIT_DIR / "chunking_shared_var_cleanup_plan_20260624_chunk_metadata.tsv"
DEFAULT_SHARED_CANDIDATES = AUDIT_DIR / "chunking_shared_var_cleanup_plan_20260624_shared_var_candidates.tsv"
DEFAULT_OUTPUT_MANIFEST = AUDIT_DIR / "unified_collection_manifest_20260624_shared_var.tsv"
DEFAULT_REPORT = AUDIT_DIR / "shared_var_alias_conversion_20260624.json"
DEFAULT_COLLECTION_KEY = "pert-gym/canonical/20260624-shared-var"


def _artifact_from_source_path(ln: Any, source_var: Any, *, key: str) -> Any:
    """Create a new artifact key backed by the existing source var table.

    The user-approved shared-var layout uses a dataset-level `var.h5ad` key.  We
    store the feature table in `adata.var` with zero observations, so loaders can
    recover the same DataFrame while Lamin accepts the `.h5ad` suffix.
    """
    var_df = source_var.load()
    empty_obs = pd.DataFrame(index=pd.Index([], name="obs_name"))
    empty_x = sparse.csr_matrix((0, len(var_df)))
    var_adata = ad.AnnData(X=empty_x, obs=empty_obs, var=var_df.copy())
    with tempfile.TemporaryDirectory(prefix="shared_var_alias_") as tmp_dir:
        path = Path(tmp_dir) / "var.h5ad"
        var_adata.write_h5ad(path, compression="gzip")
        return ln.Artifact.from_anndata(
            str(path),
            key=key,
            description="Shared var alias for exact-hash chunk family",
        ).save()


def _existing_artifact(ln: Any, key: str) -> Any | None:
    return select_latest_artifact(ln.Artifact.filter(key=key).all())


def _artifact_by_key(ln: Any, key: str) -> Any:
    artifact = _existing_artifact(ln, key)
    if artifact is None:
        raise KeyError(f"no Lamin artifact found for key {key!r}")
    return artifact


def _ensure_shared_var_artifact(
    ln: Any,
    logical_dataset: str,
    source_var: Any,
    *,
    reuse_existing_var: bool,
) -> tuple[Any, bool]:
    if reuse_existing_var:
        return source_var, False
    shared_key = shared_var_key_for_logical_dataset(logical_dataset)
    existing = _existing_artifact(ln, shared_key)
    if existing is not None:
        return existing, False
    shared_var = _artifact_from_source_path(ln, source_var, key=shared_key)
    return shared_var, True


def _candidate_datasets(shared_candidates: pd.DataFrame, *, limit: int | None = None, only_dataset: str | None = None) -> list[str]:
    candidate_col = "var_exactly_identical_by_hash_across_chunks"
    candidates = shared_candidates.copy()
    if candidate_col in candidates.columns:
        candidates = candidates.loc[candidates[candidate_col].astype(str).str.lower().eq("true")]
    datasets = candidates["logical_dataset"].astype(str).tolist()
    if only_dataset is not None:
        datasets = [dataset for dataset in datasets if dataset == only_dataset]
    return datasets if limit is None else datasets[:limit]


def apply_lamin_aliases(
    ln: Any,
    manifest: pd.DataFrame,
    shared_candidates: pd.DataFrame,
    *,
    collection_key: str,
    limit_families: int | None,
    relink: bool,
    reuse_existing_var: bool,
    only_dataset: str | None = None,
    skip_collection: bool = False,
) -> dict[str, Any]:
    """Create shared vars, relink X->var, create a new Collection, and read back."""
    datasets = _candidate_datasets(shared_candidates, limit=limit_families, only_dataset=only_dataset)
    report: dict[str, Any] = {
        "collection_key": collection_key,
        "families_requested": datasets,
        "families": [],
        "created_shared_var_artifacts": 0,
        "reuse_existing_var": reuse_existing_var,
        "relinked_x_artifacts": 0,
        "readback_errors": [],
        "collection_uid": None,
    }
    obs_artifacts = []
    for logical_dataset in datasets:
        print(f"[shared-var] family {logical_dataset} starting", flush=True)
        row_mask = manifest["logical_dataset"].astype(str).eq(logical_dataset)
        rows = manifest.loc[row_mask].copy()
        if rows.empty:
            continue
        first_triplet = get_triplet_artifacts(ln, rows.iloc[0])
        shared_var, created = _ensure_shared_var_artifact(
            ln,
            logical_dataset,
            first_triplet.var,
            reuse_existing_var=reuse_existing_var,
        )
        manifest.loc[row_mask, "var_key"] = shared_var.key
        manifest.loc[row_mask, "var_uid"] = shared_var.uid
        if getattr(shared_var, "hash", None):
            manifest.loc[row_mask, "var_hash"] = shared_var.hash
        if created:
            report["created_shared_var_artifacts"] += 1
        family_report = {
            "logical_dataset": logical_dataset,
            "shared_var_key": shared_var.key,
            "shared_var_uid": shared_var.uid,
            "chunks": len(rows),
            "relinked": 0,
            "readback_ok": 0,
        }
        for idx, _row in rows.iterrows():
            row = manifest.loc[idx]
            obs_artifact = _artifact_by_key(ln, str(row["artifact_key"]))
            x_artifact = obs_artifact.features.get_values()["X"]
            if isinstance(x_artifact, str):
                x_artifact = _artifact_by_key(ln, x_artifact)
            current_var = x_artifact.features.get_values().get("var")
            if isinstance(current_var, str):
                current_var = _artifact_by_key(ln, current_var)
            if relink and (current_var is None or current_var.key != shared_var.key):
                x_artifact.features.set_values({"var": shared_var})
                report["relinked_x_artifacts"] += 1
                family_report["relinked"] += 1
            readback = validate_triplet_var_policy(ln, row)
            if readback["ok"]:
                family_report["readback_ok"] += 1
            else:
                report["readback_errors"].append(readback)
            obs_artifacts.append(obs_artifact)
        print(f"[shared-var] family {logical_dataset} done: relinked={family_report['relinked']} readback_ok={family_report['readback_ok']}/{family_report['chunks']}", flush=True)
        report["families"].append(family_report)

    if obs_artifacts and not skip_collection:
        existing_collection = None
        try:
            existing_collection = ln.Collection.get(key=collection_key)
        except Exception:
            existing_collection = None
        if existing_collection is None:
            collection = ln.Collection(
                obs_artifacts,
                key=collection_key,
                description="pert-gym canonical shared-var alias policy conversion",
            ).save()
            report["collection_uid"] = collection.uid
        else:
            report["collection_uid"] = existing_collection.uid
            report["collection_already_existed"] = True
    elif skip_collection:
        report["collection_skipped"] = True
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--chunk-metadata", type=Path, default=DEFAULT_CHUNK_METADATA)
    parser.add_argument("--shared-candidates", type=Path, default=DEFAULT_SHARED_CANDIDATES)
    parser.add_argument("--output-manifest", type=Path, default=DEFAULT_OUTPUT_MANIFEST)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--collection-version", default="20260624-shared-var")
    parser.add_argument("--collection-key", default=DEFAULT_COLLECTION_KEY)
    parser.add_argument("--limit-families", type=int, default=None)
    parser.add_argument("--only-dataset", default=None)
    parser.add_argument("--apply-lamin", action="store_true")
    parser.add_argument("--skip-collection", action="store_true")
    parser.add_argument(
        "--reuse-existing-var",
        action="store_true",
        help="Reuse the first exact-hash per-chunk var artifact as the shared target instead of creating <logical_dataset>/var.h5ad artifacts.",
    )
    parser.add_argument(
        "--no-relink",
        action="store_true",
        help="With --apply-lamin, create shared vars and Collection but do not mutate X->var links.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = pd.read_csv(args.manifest, sep="\t", keep_default_na=False)
    chunk_metadata = pd.read_csv(args.chunk_metadata, sep="\t", keep_default_na=False)
    shared_candidates = pd.read_csv(args.shared_candidates, sep="\t", keep_default_na=False)
    if args.limit_families is not None or args.only_dataset is not None:
        keep = set(_candidate_datasets(shared_candidates, limit=args.limit_families, only_dataset=args.only_dataset))
        shared_candidates = shared_candidates.loc[
            shared_candidates["logical_dataset"].astype(str).isin(keep)
        ].copy()

    next_manifest = build_shared_var_manifest(
        manifest,
        chunk_metadata,
        shared_candidates,
        collection_version=args.collection_version,
    )
    violations = validate_manifest_var_policy(next_manifest)
    if not violations.empty:
        raise SystemExit("manifest var-policy violations:\n" + violations.to_string(index=False))

    report: dict[str, Any] = {
        "output_manifest": str(args.output_manifest),
        "rows": int(len(next_manifest)),
        "shared_rows": int(next_manifest["var_policy"].isin(["shared_exact_hash", "shared_alias"]).sum()),
        "shared_families": int(next_manifest.loc[next_manifest["var_policy"] != "same_prefix", "logical_dataset"].nunique()),
        "manifest_policy_valid": True,
        "apply_lamin": bool(args.apply_lamin),
    }

    if args.apply_lamin:
        from tools.lamin_context import connect_pertdata

        ln = connect_pertdata()
        ln.track(path=__file__)
        report["lamin"] = apply_lamin_aliases(
            ln,
            next_manifest,
            shared_candidates,
            collection_key=args.collection_key,
            limit_families=args.limit_families,
            relink=not args.no_relink,
            reuse_existing_var=args.reuse_existing_var,
            only_dataset=args.only_dataset,
            skip_collection=args.skip_collection,
        )

    post_violations = validate_manifest_var_policy(next_manifest)
    if not post_violations.empty:
        raise SystemExit("post-apply manifest var-policy violations:\n" + post_violations.to_string(index=False))
    args.output_manifest.parent.mkdir(parents=True, exist_ok=True)
    next_manifest.to_csv(args.output_manifest, sep="\t", index=False)

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
