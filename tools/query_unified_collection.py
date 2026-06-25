"""Lightweight query helpers for the pert-gym unified Lamin Collection.

The canonical unified Collection stores `obs.parquet` artifacts as members.  The
paired `X.h5ad` and `var.parquet` artifacts are resolved through Lamin feature
links (`obs.features["X"]` and `X.features["var"]`).  These helpers keep the
common discovery/filtering path manifest-backed and cheap, while returning real
Lamin artifacts when a notebook needs handles.

Examples
--------
>>> from tools.lamin_context import connect_pertdata
>>> from tools.query_unified_collection import (
...     load_unified_manifest,
...     filter_members,
...     get_triplet_artifacts,
... )
>>> ln = connect_pertdata()
>>> manifest = load_unified_manifest()
>>> prism = filter_members(manifest, source="PRISM", modality="scRNA-seq")
>>> triplet = get_triplet_artifacts(ln, prism.iloc[0].artifact_key)
>>> triplet.obs.key, triplet.x.key, triplet.var.key
('prism_collection/.../obs.parquet', 'prism_collection/.../X.h5ad', 'prism_collection/.../var.parquet')
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

DEFAULT_COLLECTION_KEY = "pert-gym/canonical/20260624-shared-var"
VAR_POLICIES = {"same_prefix", "shared_exact_hash", "shared_alias"}
SHARED_VAR_POLICIES = {"shared_exact_hash", "shared_alias"}
DEFAULT_MANIFEST_PATH = (
    Path(__file__).resolve().parents[1]
    / "artifacts"
    / "schema_audit"
    / "unified_collection_manifest_20260624_shared_var.tsv"
)
UNKNOWN_TOKENS = {"", "unknown", "nan", "none", "null", "na", "n/a"}
FILTER_COLUMNS = (
    "source",
    "modality",
    "organism",
    "cell_type",
    "cell_line",
    "perturbation_type",
    "perturbation_technology",
    "collection_category",
    "harmonization_level",
)
CONTROL_COLUMNS = (
    "is_control",
    "control",
    "is_ctrl",
    "ctrl",
    "perturbation",
    "perturbation_type",
    "target_gene",
    "gene",
)


@dataclass(frozen=True)
class TripletArtifacts:
    """Resolved Lamin artifact handles for one canonical triplet.

    Attributes are real `ln.Artifact` objects; use `.key`, `.uid`, `.path`, or
    `.load()` directly.  Loading `x` can be heavy, so callers should prefer
    metadata or backed readers unless they know the matrix is small.
    """

    obs: Any
    x: Any
    var: Any

    def keys(self) -> dict[str, str]:
        """Return artifact keys as a notebook-friendly dict."""
        return {"obs": self.obs.key, "X": self.x.key, "var": self.var.key}

    def load_var_dataframe(self) -> pd.DataFrame:
        """Load the linked var artifact as a DataFrame.

        Legacy same-prefix vars are parquet DataFrames. Dataset-level shared
        vars use the approved `<logical_dataset>/var.h5ad` key and store feature
        metadata in `AnnData.var`.
        """
        return load_var_dataframe(self.var)


def load_var_dataframe(var_artifact: Any) -> pd.DataFrame:
    """Load a linked var artifact, accepting both parquet and h5ad var aliases."""
    loaded = var_artifact.load()
    if isinstance(loaded, pd.DataFrame):
        return loaded
    if hasattr(loaded, "var"):
        return loaded.var.copy()
    raise TypeError(f"unsupported var artifact payload type: {type(loaded)!r}")


def _is_missing(value: Any) -> bool:
    if value is None or pd.isna(value):
        return True
    return str(value).strip().lower() in UNKNOWN_TOKENS


def _normalize_values(values: str | Iterable[str] | None) -> list[str] | None:
    if values is None:
        return None
    if isinstance(values, str):
        values = [values]
    normalized = [str(value).strip().lower() for value in values]
    return [value for value in normalized if value]


def select_latest_artifact(records: Iterable[Any]) -> Any | None:
    """Select one artifact deterministically from same-key Lamin records.

    Lamin can expose more than one record with the same key when branch-local
    revisions supersede an earlier record. Queryset order is not a resolution
    contract: prefer records Lamin marks latest, then break ties by creation time
    and UID so repeated resolution is stable.
    """
    candidates = list(records)
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda artifact: (
            bool(getattr(artifact, "is_latest", False)),
            str(getattr(artifact, "created_at", "")),
            str(getattr(artifact, "uid", "")),
        ),
    )[-1]


def _resolve_artifact_value(ln: Any, value: Any) -> Any:
    """Resolve a feature value that may be either an Artifact or an artifact key."""
    if value is None:
        raise KeyError("missing Lamin feature link")
    if isinstance(value, str):
        if hasattr(ln.Artifact, "filter"):
            artifact = select_latest_artifact(ln.Artifact.filter(key=value).all())
            if artifact is not None:
                return artifact
        if hasattr(ln.Artifact, "get"):
            return ln.Artifact.get(key=value)
        raise KeyError(f"no Lamin artifact found for feature-link key {value!r}")
    if getattr(value, "key", None):
        return value
    raise TypeError(f"unsupported Lamin feature-link value: {type(value)!r}")


def load_unified_manifest(path: str | Path = DEFAULT_MANIFEST_PATH) -> pd.DataFrame:
    """Load the canonical unified Collection manifest.

    Returns one row per canonical `obs.parquet` member.  Numeric counts and chunk
    indices are coerced where possible; string metadata remains plain strings so
    notebook filtering is predictable.
    """
    manifest = pd.read_csv(path, sep="\t", keep_default_na=False)
    for column in ["n_obs", "n_vars", "chunk_index", "harmonization_level_rank"]:
        if column in manifest.columns:
            manifest[column] = pd.to_numeric(manifest[column], errors="coerce")
    for column in ["has_obs_x_link", "has_x_var_link", "same_prefix_var", "link_verification_checked"]:
        if column in manifest.columns:
            manifest[column] = manifest[column].map(
                lambda value: str(value).strip().lower() == "true"
            )
    return normalize_var_policy_columns(manifest)


def expected_same_prefix_var_key(prefix: str) -> str:
    """Return the strict same-prefix var key for a triplet prefix."""
    return f"{str(prefix).rstrip('/')}/var.parquet"


def shared_var_key_for_logical_dataset(logical_dataset: str) -> str:
    """Return the canonical shared-var alias key for one logical dataset."""
    return f"{str(logical_dataset).rstrip('/')}/var.h5ad"


def normalize_var_policy_columns(manifest: pd.DataFrame) -> pd.DataFrame:
    """Ensure next-version var alias columns exist and are internally typed.

    Legacy 20260621 manifests did not include explicit `var_*` fields.  For
    backwards compatibility they are interpreted as strict same-prefix triplets
    (`var_policy == "same_prefix"`) and their `var_key` is derived from the
    existing manifest `prefix`, not from runtime artifact-key replacement.
    """
    df = manifest.copy()
    if "var_policy" not in df.columns:
        df["var_policy"] = "same_prefix"
    else:
        df["var_policy"] = df["var_policy"].astype(str).str.strip().replace("", "same_prefix")
    if "same_prefix_var" not in df.columns:
        df["same_prefix_var"] = df["var_policy"].eq("same_prefix")
    for column in ["var_key", "var_uid", "var_hash", "var_alias_group"]:
        if column not in df.columns:
            df[column] = ""
        else:
            df[column] = df[column].fillna("").astype(str)
    if "prefix" in df.columns:
        missing_same_prefix_keys = df["var_key"].eq("") & df["var_policy"].eq("same_prefix")
        df.loc[missing_same_prefix_keys, "var_key"] = df.loc[missing_same_prefix_keys, "prefix"].map(expected_same_prefix_var_key)
    return df


def build_shared_var_manifest(
    manifest: pd.DataFrame,
    chunk_metadata: pd.DataFrame,
    shared_candidates: pd.DataFrame,
    *,
    collection_version: str,
    policy: str = "shared_exact_hash",
) -> pd.DataFrame:
    """Return a next-version manifest with exact-hash chunk families aliased.

    The function is intentionally manifest-only: it does not create Lamin
    artifacts and does not mutate feature links.  Production conversion should
    first create/link the shared `var.parquet` artifact in Lamin, then validate
    the read-back `obs -> X -> var` links against the returned rows.
    """
    if policy not in SHARED_VAR_POLICIES:
        raise ValueError(f"shared policy must be one of {sorted(SHARED_VAR_POLICIES)}, got {policy!r}")
    df = normalize_var_policy_columns(manifest)
    chunks = chunk_metadata.copy()
    candidates = shared_candidates.copy()
    candidate_col = "var_exactly_identical_by_hash_across_chunks"
    if candidate_col in candidates.columns:
        mask = candidates[candidate_col].astype(str).str.lower().eq("true")
        candidate_datasets = set(candidates.loc[mask, "logical_dataset"].astype(str))
    else:
        candidate_datasets = set(candidates["logical_dataset"].astype(str))
    metadata_by_artifact = chunks.set_index("artifact_key", drop=False)
    for logical_dataset in sorted(candidate_datasets):
        row_mask = df["logical_dataset"].astype(str).eq(logical_dataset)
        if not row_mask.any():
            continue
        family_chunks = chunks.loc[chunks["logical_dataset"].astype(str).eq(logical_dataset)]
        if family_chunks.empty:
            continue
        hashes = sorted(set(family_chunks["var_hash"].dropna().astype(str)) - {""})
        if len(hashes) != 1:
            raise ValueError(f"{logical_dataset} does not have exactly one var_hash: {hashes}")
        df.loc[row_mask, "same_prefix_var"] = False
        df.loc[row_mask, "var_policy"] = policy
        df.loc[row_mask, "var_key"] = shared_var_key_for_logical_dataset(logical_dataset)
        df.loc[row_mask, "var_hash"] = hashes[0]
        df.loc[row_mask, "var_alias_group"] = logical_dataset
        # The shared artifact may be newly created, so var_uid is intentionally
        # blank until Lamin write/read-back fills it.  Preserve legacy per-chunk
        # uids only in the chunk audit TSV, not as the shared alias uid.
        df.loc[row_mask, "var_uid"] = ""
    # Fill var hashes for non-shared rows when available from the audit metadata.
    if "artifact_key" in df.columns and not metadata_by_artifact.empty:
        for idx, artifact_key in df["artifact_key"].astype(str).items():
            if artifact_key in metadata_by_artifact.index and not df.at[idx, "var_hash"]:
                df.at[idx, "var_hash"] = str(metadata_by_artifact.at[artifact_key, "var_hash"])
    df["collection_version"] = collection_version
    return df


def validate_manifest_var_policy(manifest: pd.DataFrame) -> pd.DataFrame:
    """Return row-level var-policy violations; empty means valid offline contract.

    This checks only manifest semantics.  Use `validate_triplet_var_policy()` for
    live Lamin read-back of `obs -> X -> var` feature links.
    """
    df = normalize_var_policy_columns(manifest)
    violations: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        policy = str(row.get("var_policy", "")).strip()
        same_prefix = bool(row.get("same_prefix_var", False))
        var_key = str(row.get("var_key", "")).strip()
        artifact_key = str(row.get("artifact_key", idx))
        if policy not in VAR_POLICIES:
            violations.append({"row": idx, "artifact_key": artifact_key, "reason": f"invalid var_policy {policy!r}"})
            continue
        if not bool(row.get("has_x_var_link", False)):
            violations.append({"row": idx, "artifact_key": artifact_key, "reason": "missing X->var link"})
        if policy == "same_prefix":
            if not same_prefix:
                violations.append({"row": idx, "artifact_key": artifact_key, "reason": "same_prefix policy requires same_prefix_var=True"})
            expected = expected_same_prefix_var_key(str(row.get("prefix", "")))
            if var_key and expected != var_key:
                violations.append({"row": idx, "artifact_key": artifact_key, "reason": f"same_prefix var_key mismatch: {var_key!r} != {expected!r}"})
        else:
            if same_prefix:
                violations.append({"row": idx, "artifact_key": artifact_key, "reason": f"{policy} requires same_prefix_var=False"})
            if not var_key:
                violations.append({"row": idx, "artifact_key": artifact_key, "reason": f"{policy} requires explicit var_key"})
            if not str(row.get("var_hash", "")).strip() and policy == "shared_exact_hash":
                violations.append({"row": idx, "artifact_key": artifact_key, "reason": "shared_exact_hash requires var_hash"})
    return pd.DataFrame(violations, columns=["row", "artifact_key", "reason"])


def validate_triplet_var_policy(ln: Any, artifact_key_or_row: str | pd.Series) -> dict[str, Any]:
    """Resolve live Lamin links and verify they match a manifest row's policy.

    The live var artifact is always obtained through `obs.features["X"]` then
    `X.features["var"]`; the function never infers `var` by key rewriting.
    """
    row = None if isinstance(artifact_key_or_row, str) else artifact_key_or_row
    triplet = get_triplet_artifacts(ln, artifact_key_or_row)
    result = {
        "obs_key": triplet.obs.key,
        "x_key": triplet.x.key,
        "var_key": triplet.var.key,
        "ok": True,
        "errors": [],
    }
    if row is not None:
        normalized = normalize_var_policy_columns(pd.DataFrame([row.to_dict()])).iloc[0]
        expected_key = str(normalized.get("var_key", "")).strip()
        if expected_key and triplet.var.key != expected_key:
            result["ok"] = False
            result["errors"].append(f"resolved var_key {triplet.var.key!r} != manifest var_key {expected_key!r}")
        expected_uid = str(normalized.get("var_uid", "")).strip()
        if expected_uid and getattr(triplet.var, "uid", None) != expected_uid:
            result["ok"] = False
            result["errors"].append("resolved var_uid does not match manifest var_uid")
        expected_hash = str(normalized.get("var_hash", "")).strip()
        if expected_hash and getattr(triplet.var, "hash", None) not in (None, expected_hash):
            result["ok"] = False
            result["errors"].append("resolved var_hash does not match manifest var_hash")
    return result


def get_collection(ln: Any, key: str = DEFAULT_COLLECTION_KEY) -> Any:
    """Return the real Lamin Collection record for the unified surface."""
    return ln.Collection.get(key=key)


def collection_member_keys(ln: Any, key: str = DEFAULT_COLLECTION_KEY) -> list[str]:
    """Read Collection membership from Lamin and return member artifact keys."""
    collection = get_collection(ln, key=key)
    return list(collection.artifacts.all().values_list("key", flat=True))


def filter_members(
    manifest: pd.DataFrame | None = None,
    *,
    source: str | Sequence[str] | None = None,
    modality: str | Sequence[str] | None = None,
    organism: str | Sequence[str] | None = None,
    cell_type: str | Sequence[str] | None = None,
    cell_line: str | Sequence[str] | None = None,
    perturbation_type: str | Sequence[str] | None = None,
    perturbation_technology: str | Sequence[str] | None = None,
    collection_category: str | Sequence[str] | None = None,
    harmonization_level: str | Sequence[str] | None = None,
    dataset_id_contains: str | None = None,
    prefix_contains: str | None = None,
    require_controls: bool = False,
    exclude_unknown: bool = False,
) -> pd.DataFrame:
    """Filter unified Collection members by common metadata fields.

    The return value is still a DataFrame of manifest rows, not a custom wrapper.
    This is intentional: users can inspect/sort/group with normal pandas while
    retaining Lamin artifact keys for later resolution.
    """
    df = load_unified_manifest() if manifest is None else manifest.copy()
    criteria: Mapping[str, str | Sequence[str] | None] = {
        "source": source,
        "modality": modality,
        "organism": organism,
        "cell_type": cell_type,
        "cell_line": cell_line,
        "perturbation_type": perturbation_type,
        "perturbation_technology": perturbation_technology,
        "collection_category": collection_category,
        "harmonization_level": harmonization_level,
    }
    mask = pd.Series(True, index=df.index)
    for column, values in criteria.items():
        normalized = _normalize_values(values)
        if not normalized or column not in df.columns:
            continue
        mask &= df[column].astype(str).str.lower().isin(normalized)
    if dataset_id_contains:
        mask &= df["dataset_id"].astype(str).str.contains(
            dataset_id_contains, case=False, na=False, regex=False
        )
    if prefix_contains:
        mask &= df["prefix"].astype(str).str.contains(
            prefix_contains, case=False, na=False, regex=False
        )
    if require_controls and "control_availability" in df.columns:
        mask &= ~df["control_availability"].map(_is_missing)
    result = df.loc[mask].copy()
    if exclude_unknown:
        for column in FILTER_COLUMNS:
            if column in result.columns:
                result = result.loc[~result[column].map(_is_missing)]
    return result.reset_index(drop=True)


def list_datasets(manifest: pd.DataFrame | None = None) -> pd.DataFrame:
    """Summarize logical datasets/chunk families in the unified Collection.

    Returns one row per `dataset_id`, with member counts, total observations,
    roots/source categories, and representative artifact keys.
    """
    df = load_unified_manifest() if manifest is None else manifest
    grouped = (
        df.groupby("dataset_id", dropna=False)
        .agg(
            members=("artifact_key", "count"),
            total_obs=("n_obs", "sum"),
            n_vars_min=("n_vars", "min"),
            n_vars_max=("n_vars", "max"),
            source=("source", lambda values: ", ".join(sorted(set(map(str, values))))),
            modality=("modality", lambda values: ", ".join(sorted(set(map(str, values))))),
            organism=("organism", lambda values: ", ".join(sorted(set(map(str, values))))),
            perturbation_type=(
                "perturbation_type",
                lambda values: ", ".join(sorted(set(map(str, values)))),
            ),
            collection_category=(
                "collection_category",
                lambda values: ", ".join(sorted(set(map(str, values)))),
            ),
            harmonization_level=(
                "harmonization_level",
                lambda values: ", ".join(sorted(set(map(str, values)))),
            ),
            first_artifact_key=("artifact_key", "first"),
        )
        .reset_index()
        .sort_values(["source", "dataset_id"])
    )
    return grouped.reset_index(drop=True)


def get_dataset_members(
    dataset_id: str,
    manifest: pd.DataFrame | None = None,
    *,
    exact: bool = True,
) -> pd.DataFrame:
    """Return manifest rows for one dataset or chunk family.

    `exact=False` is useful for prefix-like queries in notebooks, e.g.
    `get_dataset_members("prism_collection/GSE225775", exact=False)`.
    """
    df = load_unified_manifest() if manifest is None else manifest
    if exact:
        rows = df.loc[df["dataset_id"].astype(str) == dataset_id]
    else:
        rows = df.loc[
            df["dataset_id"].astype(str).str.contains(
                dataset_id, case=False, na=False, regex=False
            )
            | df["prefix"].astype(str).str.contains(
                dataset_id, case=False, na=False, regex=False
            )
        ]
    return rows.sort_values(["dataset_id", "chunk_index", "artifact_key"]).reset_index(
        drop=True
    )


def get_triplet_artifacts(ln: Any, artifact_key_or_row: str | pd.Series) -> TripletArtifacts:
    """Resolve obs/X/var Lamin artifacts for a manifest row or obs artifact key.

    This performs no matrix loading.  It only reads feature links and returns the
    three Lamin artifact handles.
    """
    if isinstance(artifact_key_or_row, str):
        artifact_key = artifact_key_or_row
    else:
        row = artifact_key_or_row.to_dict()
        artifact_key = str(row["artifact_key"])
    obs_artifact = ln.Artifact.get(key=artifact_key)
    x_artifact = _resolve_artifact_value(ln, obs_artifact.features.get_values()["X"])
    var_artifact = _resolve_artifact_value(ln, x_artifact.features.get_values()["var"])
    return TripletArtifacts(obs=obs_artifact, x=x_artifact, var=var_artifact)


def get_triplets_for_dataset(
    ln: Any,
    dataset_id: str,
    manifest: pd.DataFrame | None = None,
    *,
    exact: bool = True,
    limit: int | None = None,
) -> list[TripletArtifacts]:
    """Resolve triplet handles for all members of a dataset/chunk family."""
    rows = get_dataset_members(dataset_id, manifest=manifest, exact=exact)
    if limit is not None:
        rows = rows.head(limit)
    return [get_triplet_artifacts(ln, row) for _, row in rows.iterrows()]


def inspect_harmonization(manifest: pd.DataFrame | None = None) -> pd.DataFrame:
    """Count members by harmonization level and collection category."""
    df = load_unified_manifest() if manifest is None else manifest
    return (
        df.groupby(["collection_category", "harmonization_level"], dropna=False)
        .agg(
            members=("artifact_key", "count"),
            logical_datasets=("dataset_id", "nunique"),
            total_obs=("n_obs", "sum"),
        )
        .reset_index()
        .sort_values(["collection_category", "harmonization_level"])
    )


def find_control_datasets(manifest: pd.DataFrame | None = None) -> pd.DataFrame:
    """Return manifest rows that advertise known control availability.

    The current 20260621 manifest is conservative and often says `unknown`.
    For a specific dataset, use `inspect_obs_controls()` to inspect obs metadata
    without touching X.
    """
    df = load_unified_manifest() if manifest is None else manifest
    if "control_availability" not in df.columns:
        return df.iloc[0:0].copy()
    rows = df.loc[~df["control_availability"].map(_is_missing)].copy()
    return rows.reset_index(drop=True)


def inspect_obs_controls(
    obs_artifact: Any,
    *,
    max_unique_values: int = 12,
) -> dict[str, Any]:
    """Inspect control-like obs metadata for one explicit obs artifact.

    This loads only the obs parquet payload, never X.  Use it on specific rows or
    small chunks selected from the manifest; avoid broad loops over huge members.
    """
    obs = obs_artifact.load()
    columns = [column for column in CONTROL_COLUMNS if column in obs.columns]
    result: dict[str, Any] = {
        "artifact_key": obs_artifact.key,
        "n_obs": int(len(obs)),
        "control_columns": columns,
        "control_count": None,
        "examples": {},
    }
    if "is_control" in obs.columns:
        result["control_count"] = int(obs["is_control"].fillna(False).astype(bool).sum())
    for column in columns:
        counts = obs[column].astype(str).value_counts(dropna=False).head(max_unique_values)
        result["examples"][column] = {str(k): int(v) for k, v in counts.items()}
    return result
