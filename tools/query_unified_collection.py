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

DEFAULT_COLLECTION_KEY = "pert-gym/canonical/20260621"
DEFAULT_MANIFEST_PATH = (
    Path(__file__).resolve().parents[1]
    / "artifacts"
    / "schema_audit"
    / "unified_collection_manifest_20260621.tsv"
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


def _resolve_artifact_value(ln: Any, value: Any) -> Any:
    """Resolve a feature value that may be either an Artifact or an artifact key."""
    if value is None:
        raise KeyError("missing Lamin feature link")
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
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
    for column in ["has_obs_x_link", "has_x_var_link", "same_prefix_var"]:
        if column in manifest.columns:
            manifest[column] = manifest[column].map(
                lambda value: str(value).strip().lower() == "true"
            )
    return manifest


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
