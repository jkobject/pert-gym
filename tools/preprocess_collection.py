#!/usr/bin/env python3
"""Preprocess LaminDB artifacts into canonical dataset artifacts.

Key behaviors:
- Discovers datasets from `ln.Artifact` globally (no Collection dependency).
- Builds a putative grouping plan (`group`, `single`, `skip_*`) with reasons.
- Infers schema context from obs/var columns and Lamin Feature/Schema records.
- Optionally writes preprocessed artifacts and an output collection.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import lamindb as ln
import pandas as pd

try:
    import anndata as ad
except Exception:  # pragma: no cover - optional dependency at runtime
    ad = None

try:
    import mudata as mu
except Exception:  # pragma: no cover - optional dependency at runtime
    mu = None

LOGGER = logging.getLogger("preprocess_collection")

PRIMARY_SUFFIXES = {".h5ad", ".h5mu"}
MATRIX_BASENAMES = {"X.h5ad", "X.h5mu"}
SHARD_PATTERN = re.compile(r"(plate\d+|donor[_-]?\d+|day[_-]?\d+|batch[_-]?\d+|rep\d+)", re.I)
DERIVED_MARKERS = {
    "combined",
    "pseudobulk",
    "vscores",
    "normalized",
    "tutorial",
    "minified",
}


@dataclass
class DatasetUnit:
    dataset_key: str
    parent_key: str
    leaf_name: str
    artifacts: list[ln.Artifact]
    modality: str
    primary_artifact_count: int
    has_canonical_matrix_basename: bool
    source_schema_ids: list[int]
    obs_schema_ids: list[int]
    obs_columns: list[str]
    var_columns: list[str]


@dataclass
class PlanItem:
    decision: str
    target_dataset_key: str
    source_dataset_keys: list[str]
    modality: str
    reason: str
    schema_ids: list[int]
    schema_names: list[str]
    obs_feature_links: dict[str, str]
    obs_unknown_columns: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate and standardize LaminDB artifacts per dataset."
    )
    parser.add_argument(
        "--instance",
        default="laminlabs/pertdata",
        help="Lamin instance slug, e.g. laminlabs/pertdata",
    )
    parser.add_argument(
        "--output-prefix",
        default="preprocessed",
        help="Prefix for output artifact keys.",
    )
    parser.add_argument(
        "--output-collection-key",
        default="all_artifacts_preprocessed",
        help="Output collection key.",
    )
    parser.add_argument(
        "--schema-uid",
        default=None,
        help="Optional Lamin schema uid applied to all output artifacts.",
    )
    parser.add_argument(
        "--schema-name",
        default=None,
        help="Optional Lamin schema name applied to all output artifacts.",
    )
    parser.add_argument(
        "--anndata-schema-literal",
        default=None,
        choices=["ensembl_gene_ids_and_valid_features_in_obs"],
        help="Optional built-in curation schema literal for Artifact.from_anndata.",
    )
    parser.add_argument(
        "--dataset-prefix",
        default=None,
        help="Optional key prefix filter before planning, e.g. scperturb/",
    )
    parser.add_argument(
        "--include-derived",
        action="store_true",
        help="Include derived subsets (combined/pseudobulk/etc.) as processable datasets.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Only emit plan; do not create artifacts.",
    )
    parser.add_argument(
        "--write-plan",
        default=None,
        help="Optional path to save full plan JSON.",
    )
    parser.add_argument(
        "--limit-plan-items",
        type=int,
        default=None,
        help="Optional cap on number of processable plan items.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute plan and preprocessing actions without saving records.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def latest_by_key(artifacts: Iterable[ln.Artifact]) -> list[ln.Artifact]:
    latest: dict[str, ln.Artifact] = {}
    for artifact in artifacts:
        key = artifact.key or artifact.uid
        prev = latest.get(key)
        if prev is None or artifact.created_at > prev.created_at:
            latest[key] = artifact
    return list(latest.values())


def basename(artifact: ln.Artifact) -> str:
    key = artifact.key or artifact.uid
    return key.rsplit("/", 1)[-1]


def infer_modality(artifacts: list[ln.Artifact]) -> tuple[str, int, bool]:
    suffixes = Counter(a.suffix or "" for a in artifacts)
    primary_count = sum(suffixes[s] for s in PRIMARY_SUFFIXES)
    basenames = {basename(a) for a in artifacts}
    has_canonical = bool(MATRIX_BASENAMES & basenames)

    if suffixes[".h5mu"] > 0:
        return "mudata", primary_count, has_canonical
    if suffixes[".h5ad"] > 0:
        return "anndata", primary_count, has_canonical
    if suffixes[".parquet"] > 0:
        return "tabular", primary_count, has_canonical
    if any(suffixes[s] > 0 for s in [".csv", ".csv.gz", ".tsv", ".txt", ".txt.gz"]):
        return "tabular", primary_count, has_canonical
    return "other", primary_count, has_canonical


def build_dataset_units(artifacts: list[ln.Artifact]) -> dict[str, DatasetUnit]:
    grouped: dict[str, list[ln.Artifact]] = defaultdict(list)
    for artifact in artifacts:
        key = artifact.key or artifact.uid
        dataset_key = key.rsplit("/", 1)[0] if "/" in key else key
        grouped[dataset_key].append(artifact)

    units: dict[str, DatasetUnit] = {}
    for dataset_key, members in grouped.items():
        parent_key = dataset_key.rsplit("/", 1)[0] if "/" in dataset_key else ""
        leaf_name = dataset_key.rsplit("/", 1)[-1]
        modality, primary_count, has_canonical = infer_modality(members)

        source_schema_ids = sorted(
            {int(a.schema_id) for a in members if a.schema_id is not None}
        )
        obs_schema_ids = sorted(
            {
                int(a.schema_id)
                for a in members
                if basename(a) == "obs.parquet" and a.schema_id is not None
            }
        )

        obs_columns: list[str] = []
        var_columns: list[str] = []

        units[dataset_key] = DatasetUnit(
            dataset_key=dataset_key,
            parent_key=parent_key,
            leaf_name=leaf_name,
            artifacts=members,
            modality=modality,
            primary_artifact_count=primary_count,
            has_canonical_matrix_basename=has_canonical,
            source_schema_ids=source_schema_ids,
            obs_schema_ids=obs_schema_ids,
            obs_columns=obs_columns,
            var_columns=var_columns,
        )
    return units


def normalize_name(text: str) -> str:
    value = text.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(
        r"\b(weissman|tsherniak|satija|kampmann|trapnell|bock|marson|oconner|filtered|rna|protein)\b",
        " ",
        value,
    )
    return " ".join(value.split())


def likely_derived(unit: DatasetUnit) -> bool:
    text = unit.dataset_key.lower()
    return any(marker in text for marker in DERIVED_MARKERS)


def schema_lookup() -> dict[int, ln.Schema]:
    return {schema.id: schema for schema in ln.Schema.filter().all()}


def feature_dtype_lookup() -> dict[str, str]:
    df = ln.Feature.filter().to_dataframe(limit=None)
    lookup: dict[str, str] = {}
    for row in df.to_dict("records"):
        name = row.get("name")
        dtype = row.get("_dtype_str")
        if isinstance(name, str) and isinstance(dtype, str):
            lookup[name] = dtype
    return lookup


def build_schema_summary(
    unit_keys: list[str],
    units: dict[str, DatasetUnit],
    schemas: dict[int, ln.Schema],
    feature_dtypes: dict[str, str],
) -> tuple[list[int], list[str], dict[str, str], list[str]]:
    schema_ids: set[int] = set()
    obs_cols: set[str] = set()

    for key in unit_keys:
        unit = units[key]
        schema_ids.update(unit.source_schema_ids)
        obs_cols.update(unit.obs_columns)
        for sid in unit.obs_schema_ids:
            schema = schemas.get(sid)
            if schema is None:
                continue
            try:
                obs_cols.update(member.name for member in schema.members.all())
            except Exception:  # pragma: no cover - runtime data dependent
                pass

    ordered_schema_ids = sorted(schema_ids)
    schema_names: list[str] = []
    for sid in ordered_schema_ids:
        schema = schemas.get(sid)
        schema_names.append(schema.name if schema and schema.name else f"schema_id={sid}")

    obs_feature_links = {
        col: feature_dtypes[col]
        for col in sorted(obs_cols)
        if col in feature_dtypes and "cat[" in feature_dtypes[col]
    }
    obs_unknown_columns = sorted([col for col in obs_cols if col not in feature_dtypes])
    return ordered_schema_ids, schema_names, obs_feature_links, obs_unknown_columns


def propose_plan(
    units: dict[str, DatasetUnit],
    include_derived: bool,
) -> list[PlanItem]:
    schemas = schema_lookup()
    feature_dtypes = feature_dtype_lookup()

    plan: list[PlanItem] = []
    consumed: set[str] = set()

    # 1) Group sibling shards (plate/day/donor/batch-like fragments).
    siblings: dict[tuple[str, str], list[DatasetUnit]] = defaultdict(list)
    for unit in units.values():
        siblings[(unit.parent_key, unit.modality)].append(unit)

    for (_, modality), group in siblings.items():
        if modality not in {"anndata", "mudata"}:
            continue
        shard_units = [
            u
            for u in group
            if u.primary_artifact_count > 0
            and SHARD_PATTERN.search(u.leaf_name)
            and not likely_derived(u)
        ]
        if len(shard_units) < 2:
            continue

        target_key = shard_units[0].parent_key
        source_keys = sorted(u.dataset_key for u in shard_units)
        sid, sname, links, unknown = build_schema_summary(
            source_keys, units, schemas, feature_dtypes
        )
        plan.append(
            PlanItem(
                decision="group",
                target_dataset_key=target_key,
                source_dataset_keys=source_keys,
                modality=modality,
                reason=(
                    "Sibling shards detected by key pattern "
                    "(plate/day/donor/batch/replicate)."
                ),
                schema_ids=sid,
                schema_names=sname,
                obs_feature_links=links,
                obs_unknown_columns=unknown,
            )
        )
        consumed.update(source_keys)
        if target_key in units:
            consumed.add(target_key)

    # 2) Base decision for each remaining unit.
    for dataset_key in sorted(units):
        if dataset_key in consumed:
            continue
        unit = units[dataset_key]

        sid, sname, links, unknown = build_schema_summary(
            [dataset_key], units, schemas, feature_dtypes
        )

        if unit.modality not in {"anndata", "mudata"}:
            plan.append(
                PlanItem(
                    decision="skip_non_matrix",
                    target_dataset_key=dataset_key,
                    source_dataset_keys=[dataset_key],
                    modality=unit.modality,
                    reason="No primary .h5ad/.h5mu matrix artifact present.",
                    schema_ids=sid,
                    schema_names=sname,
                    obs_feature_links=links,
                    obs_unknown_columns=unknown,
                )
            )
            continue

        if "/records/" in dataset_key and unit.primary_artifact_count > 1:
            plan.append(
                PlanItem(
                    decision="skip_raw_bundle",
                    target_dataset_key=dataset_key,
                    source_dataset_keys=[dataset_key],
                    modality=unit.modality,
                    reason=(
                        "Raw record bundle containing multiple named datasets; "
                        "handled as source duplicates/subsets, not merged into one dataset."
                    ),
                    schema_ids=sid,
                    schema_names=sname,
                    obs_feature_links=links,
                    obs_unknown_columns=unknown,
                )
            )
            continue

        if likely_derived(unit) and not include_derived:
            plan.append(
                PlanItem(
                    decision="skip_derived_subset",
                    target_dataset_key=dataset_key,
                    source_dataset_keys=[dataset_key],
                    modality=unit.modality,
                    reason="Derived subset/summary dataset (combined/pseudobulk/etc.).",
                    schema_ids=sid,
                    schema_names=sname,
                    obs_feature_links=links,
                    obs_unknown_columns=unknown,
                )
            )
            continue

        if unit.primary_artifact_count > 1:
            plan.append(
                PlanItem(
                    decision="group",
                    target_dataset_key=dataset_key,
                    source_dataset_keys=[dataset_key],
                    modality=unit.modality,
                    reason="Multiple primary matrix files in one dataset folder.",
                    schema_ids=sid,
                    schema_names=sname,
                    obs_feature_links=links,
                    obs_unknown_columns=unknown,
                )
            )
            continue

        plan.append(
            PlanItem(
                decision="single",
                target_dataset_key=dataset_key,
                source_dataset_keys=[dataset_key],
                modality=unit.modality,
                reason="Single primary matrix file.",
                schema_ids=sid,
                schema_names=sname,
                obs_feature_links=links,
                obs_unknown_columns=unknown,
            )
        )

    # 3) Detect putative duplicates from scperturb records against canonical keys.
    canonical_scperturb = [
        u
        for u in units.values()
        if u.dataset_key.startswith("scperturb/")
        and "/records/" not in u.dataset_key
        and u.modality == "anndata"
    ]
    canonical_names = {u.dataset_key: normalize_name(u.leaf_name) for u in canonical_scperturb}

    for unit in units.values():
        if not unit.dataset_key.startswith("scperturb/records/"):
            continue
        duplicate_matches: list[tuple[str, str, float]] = []
        for artifact in unit.artifacts:
            if (artifact.suffix or "") != ".h5ad":
                continue
            stem = basename(artifact).removesuffix(".h5ad")
            norm_stem = normalize_name(stem)
            best_key = None
            best_score = 0.0
            for dataset_key, canonical_norm in canonical_names.items():
                score = similarity(norm_stem, canonical_norm)
                if score > best_score:
                    best_score = score
                    best_key = dataset_key
            if best_key is not None and best_score >= 0.55:
                duplicate_matches.append((basename(artifact), best_key, best_score))

        if duplicate_matches:
            sid, sname, links, unknown = build_schema_summary(
                [unit.dataset_key], units, schemas, feature_dtypes
            )
            duplicate_matches.sort(key=lambda x: x[2], reverse=True)
            examples = "; ".join(
                f"{name} -> {target} ({score:.2f})"
                for name, target, score in duplicate_matches[:5]
            )
            plan.append(
                PlanItem(
                    decision="skip_putative_duplicate",
                    target_dataset_key=unit.dataset_key,
                    source_dataset_keys=[unit.dataset_key],
                    modality=unit.modality,
                    reason=(
                        f"{len(duplicate_matches)} record files look like duplicates/subsets. "
                        f"Examples: {examples}"
                    ),
                    schema_ids=sid,
                    schema_names=sname,
                    obs_feature_links=links,
                    obs_unknown_columns=unknown,
                )
            )

    # Keep deterministic order and collapse exact duplicate plan rows.
    seen: set[tuple[str, str, tuple[str, ...], str]] = set()
    unique_plan: list[PlanItem] = []
    for item in sorted(
        plan,
        key=lambda x: (x.decision, x.target_dataset_key, ",".join(x.source_dataset_keys)),
    ):
        signature = (
            item.decision,
            item.target_dataset_key,
            tuple(item.source_dataset_keys),
            item.reason,
        )
        if signature in seen:
            continue
        seen.add(signature)
        unique_plan.append(item)

    return unique_plan


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens or not b_tokens:
        return 0.0
    jaccard = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)
    prefix_bonus = 0.2 if (a.startswith(b) or b.startswith(a)) else 0.0
    ratio = difflib.SequenceMatcher(None, a.replace(" ", ""), b.replace(" ", "")).ratio()
    return min(1.0, max(ratio, jaccard + prefix_bonus))


def find_schema(args: argparse.Namespace) -> ln.Schema | None:
    if args.schema_uid:
        return ln.Schema.get(uid=args.schema_uid)
    if args.schema_name:
        return ln.Schema.get(name=args.schema_name)
    return None


def build_output_key(output_prefix: str, dataset_key: str, suffix: str) -> str:
    return f"{output_prefix}/{dataset_key}/X{suffix}"


def read_h5ad(artifact: ln.Artifact):
    if ad is None:
        raise RuntimeError("anndata is required to process .h5ad artifacts")
    return ad.read_h5ad(artifact.cache())


def read_h5mu(artifact: ln.Artifact):
    if mu is None:
        raise RuntimeError("mudata is required to process .h5mu artifacts")
    return mu.read_h5mu(artifact.cache())


def read_parquet(artifact: ln.Artifact) -> pd.DataFrame:
    return pd.read_parquet(Path(artifact.cache()))


def aggregate_anndata(target_dataset_key: str, artifacts: list[ln.Artifact]):
    h5ad = [a for a in artifacts if (a.suffix or "") == ".h5ad"]
    if not h5ad:
        return None

    x_candidates = [a for a in h5ad if basename(a) == "X.h5ad"]
    obs_candidates = [a for a in artifacts if basename(a) == "obs.parquet"]
    var_candidates = [a for a in artifacts if basename(a) == "var.parquet"]

    if x_candidates:
        base_artifact = sorted(x_candidates, key=lambda a: a.created_at)[-1]
        adata = read_h5ad(base_artifact)

        if obs_candidates:
            obs_artifact = sorted(obs_candidates, key=lambda a: a.created_at)[-1]
            obs_df = read_parquet(obs_artifact)
            if adata.n_obs == len(obs_df):
                if not obs_df.index.equals(adata.obs_names):
                    obs_df.index = adata.obs_names
                adata.obs = obs_df
            else:
                LOGGER.warning(
                    "obs length mismatch for %s (%d vs %d)",
                    target_dataset_key,
                    adata.n_obs,
                    len(obs_df),
                )

        if var_candidates:
            var_artifact = sorted(var_candidates, key=lambda a: a.created_at)[-1]
            var_df = read_parquet(var_artifact)
            if adata.n_vars == len(var_df):
                if not var_df.index.equals(adata.var_names):
                    var_df.index = adata.var_names
                adata.var = var_df
            else:
                LOGGER.warning(
                    "var length mismatch for %s (%d vs %d)",
                    target_dataset_key,
                    adata.n_vars,
                    len(var_df),
                )
        return adata

    if len(h5ad) == 1:
        return read_h5ad(h5ad[0])

    h5ad_sorted = sorted(h5ad, key=lambda a: a.key or "")
    adatas = [read_h5ad(a) for a in h5ad_sorted]
    keys = [a.uid for a in h5ad_sorted]
    return ad.concat(adatas, axis=0, join="outer", label="source_artifact_uid", keys=keys)


def aggregate_mudata(artifacts: list[ln.Artifact]):
    h5mu = [a for a in artifacts if (a.suffix or "") == ".h5mu"]
    if not h5mu:
        return None
    if len(h5mu) == 1:
        return read_h5mu(h5mu[0])

    h5mu_sorted = sorted(h5mu, key=lambda a: a.key or "")
    mdata_list = [read_h5mu(a) for a in h5mu_sorted]
    keys = [a.uid for a in h5mu_sorted]
    return mu.concat(mdata_list, axis=0, join="outer", label="source_artifact_uid", keys=keys)


def aggregate_tabular(artifacts: list[ln.Artifact]) -> pd.DataFrame | None:
    parquet = sorted(
        [a for a in artifacts if (a.suffix or "") == ".parquet"],
        key=lambda a: a.key or "",
    )
    if not parquet:
        return None

    tables = [read_parquet(a) for a in parquet]
    if len(tables) == 1:
        return tables[0]

    colsets = {tuple(df.columns) for df in tables}
    if len(colsets) != 1:
        LOGGER.warning(
            "Skipping tabular concat for incompatible parquet schemas in %s",
            {basename(a) for a in parquet},
        )
        return None
    return pd.concat(tables, axis=0, ignore_index=False)


def artifacts_for_plan_item(item: PlanItem, units: dict[str, DatasetUnit]) -> list[ln.Artifact]:
    selected: list[ln.Artifact] = []
    for key in item.source_dataset_keys:
        selected.extend(units[key].artifacts)
    return latest_by_key(selected)


def serialize_plan(plan: list[PlanItem]) -> dict[str, Any]:
    grouped = []
    not_grouped = []

    for item in plan:
        row = {
            "decision": item.decision,
            "target_dataset_key": item.target_dataset_key,
            "source_dataset_keys": item.source_dataset_keys,
            "modality": item.modality,
            "reason": item.reason,
            "schema_ids": item.schema_ids,
            "schema_names": item.schema_names,
            "obs_feature_links": item.obs_feature_links,
            "obs_unknown_columns": item.obs_unknown_columns,
        }
        if item.decision in {"group", "single"}:
            grouped.append(row)
        else:
            not_grouped.append(row)

    return {
        "summary": {
            "n_grouped_or_single": len(grouped),
            "n_not_grouped": len(not_grouped),
        },
        "processable": grouped,
        "not_grouped": not_grouped,
    }


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    ln.connect(args.instance)
    schema_override = find_schema(args)

    artifacts = list(ln.Artifact.filter().all())
    artifacts = latest_by_key([a for a in artifacts if a.key])
    if args.dataset_prefix:
        artifacts = [a for a in artifacts if (a.key or "").startswith(args.dataset_prefix)]

    units = build_dataset_units(artifacts)
    plan = propose_plan(units, include_derived=args.include_derived)

    plan_dict = serialize_plan(plan)
    LOGGER.info(
        "Plan built: %d processable, %d not grouped",
        plan_dict["summary"]["n_grouped_or_single"],
        plan_dict["summary"]["n_not_grouped"],
    )

    if args.write_plan:
        out = Path(args.write_plan)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(plan_dict, indent=2), encoding="utf-8")
        LOGGER.info("Wrote plan to %s", out)

    if args.plan_only:
        print(json.dumps(plan_dict, indent=2))
        return

    to_process = [item for item in plan if item.decision in {"group", "single"}]
    if args.limit_plan_items is not None:
        to_process = to_process[: args.limit_plan_items]

    created_artifacts: list[ln.Artifact] = []
    processing_report: list[dict[str, Any]] = []

    for item in to_process:
        output_kind = None
        output_key = None
        output_artifact = None
        source_artifacts: list[ln.Artifact] = []

        if item.modality == "mudata":
            output_kind = "MuData"
            output_key = build_output_key(args.output_prefix, item.target_dataset_key, ".h5mu")
            if not args.dry_run:
                if mu is None:
                    raise RuntimeError(
                        "mudata is required for non-dry-run MuData preprocessing"
                    )
                source_artifacts = artifacts_for_plan_item(item, units)
                mdata = aggregate_mudata(source_artifacts)
                if mdata is not None:
                    output_artifact = ln.Artifact.from_mudata(
                        mdata,
                        key=output_key,
                        schema=schema_override,
                    ).save()
                else:
                    output_kind = None
                    output_key = None

        elif item.modality == "anndata":
            output_kind = "AnnData"
            output_key = build_output_key(args.output_prefix, item.target_dataset_key, ".h5ad")
            selected_schema: ln.Schema | str | None = schema_override
            if selected_schema is None and args.anndata_schema_literal is not None:
                selected_schema = args.anndata_schema_literal
            if not args.dry_run:
                if ad is None:
                    raise RuntimeError(
                        "anndata is required for non-dry-run AnnData preprocessing"
                    )
                source_artifacts = artifacts_for_plan_item(item, units)
                adata = aggregate_anndata(item.target_dataset_key, source_artifacts)
                if adata is not None:
                    output_artifact = ln.Artifact.from_anndata(
                        adata,
                        key=output_key,
                        schema=selected_schema,
                    ).save()
                else:
                    output_kind = None
                    output_key = None

        elif item.modality == "tabular":
            output_kind = "DataFrame"
            output_key = build_output_key(args.output_prefix, item.target_dataset_key, ".parquet")
            if not args.dry_run:
                source_artifacts = artifacts_for_plan_item(item, units)
                table = aggregate_tabular(source_artifacts)
                if table is not None:
                    output_artifact = ln.Artifact.from_df(
                        table,
                        key=output_key,
                        schema=schema_override,
                    ).save()
                else:
                    output_kind = None
                    output_key = None

        processing_report.append(
            {
                "target_dataset_key": item.target_dataset_key,
                "decision": item.decision,
                "reason": item.reason,
                "source_dataset_keys": item.source_dataset_keys,
                "modality": item.modality,
                "schema_ids": item.schema_ids,
                "schema_names": item.schema_names,
                "output_kind": output_kind,
                "output_key": output_key,
                "output_uid": None if output_artifact is None else output_artifact.uid,
            }
        )

        if output_artifact is not None:
            created_artifacts.append(output_artifact)

    if not args.dry_run and created_artifacts:
        out_collection = ln.Collection(
            created_artifacts,
            key=args.output_collection_key,
            description="Preprocessed from global Artifact inventory.",
        ).save()
        LOGGER.info(
            "Created collection %s (%s) with %d artifacts",
            out_collection.key,
            out_collection.uid,
            len(created_artifacts),
        )

    print(
        json.dumps(
            {
                "plan": plan_dict,
                "processing_report": processing_report,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
