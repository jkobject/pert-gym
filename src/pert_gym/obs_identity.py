"""Global observation identity helpers for canonical pert-gym obs artifacts.

Every canonical ``obs.parquet`` row should carry two identity columns:
``original_obs_index`` preserves the row/index label from the source payload, and
``obs_uuid`` is a globally unique, deterministic UUIDv5 derived from the stable
audit namespace components: dataset id, canonical prefix, and source row index.
"""

from __future__ import annotations

from collections import Counter
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import pandas as pd

OBS_UUID_COLUMN = "obs_uuid"
ORIGINAL_OBS_INDEX_COLUMN = "original_obs_index"
ORIGINAL_OBS_INDEX_DUPLICATED_COLUMN = "original_obs_index_is_duplicated"
OBS_IDENTITY_VERSION = "pert-gym.obs.v1"


def _stringify(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def _source_original_indices(obs: pd.DataFrame) -> list[str]:
    if ORIGINAL_OBS_INDEX_COLUMN in obs.columns:
        return [_stringify(value) for value in obs[ORIGINAL_OBS_INDEX_COLUMN].tolist()]
    return [_stringify(value) for value in obs.index.tolist()]


def add_obs_identity(
    obs: pd.DataFrame,
    *,
    dataset_id: str,
    prefix: str,
    source_accession: str | None = None,
    sample_column: str | None = None,
    barcode_column: str | None = None,
    chunk_id: str | None = None,
    row_kind: str | None = None,
    obs_uuid_column: str = OBS_UUID_COLUMN,
    original_obs_index_column: str = ORIGINAL_OBS_INDEX_COLUMN,
) -> pd.DataFrame:
    """Return ``obs`` with deterministic global observation identity columns.

    Parameters
    ----------
    obs:
        Source observation table. The returned table is a copy; the input is not
        mutated.
    dataset_id:
        Stable logical dataset id, e.g. ``prism_collection/GSE221321`` or
        ``depmap/avana_2024_q4``.
    prefix:
        Canonical artifact prefix for the specific obs payload/chunk.
    source_accession:
        Deprecated compatibility argument. Source accession is intentionally not
        included in the v1 UUID material.
    sample_column:
        Deprecated compatibility argument. Sample values are intentionally not
        included in the v1 UUID material.
    barcode_column:
        Deprecated compatibility argument. Barcode values are intentionally not
        included in the v1 UUID material.
    chunk_id:
        Deprecated compatibility argument. The full prefix already namespaces
        chunks in the v1 UUID material.
    row_kind:
        Deprecated compatibility argument. Row kind is intentionally not
        included in the v1 UUID material.

    UUID material follows the 2026-06-24 audit/card contract exactly:
    ``pert-gym.obs.v1:{dataset_id}:{prefix}:{original_obs_index}``.
    If the original index repeats within a payload, the zero-based row position is
    appended only for those repeated labels to keep UUIDs unique and stable.
    """

    if not dataset_id:
        raise ValueError("dataset_id is required for deterministic obs_uuid")
    if not prefix:
        raise ValueError("prefix is required for deterministic obs_uuid")

    out = obs.copy()
    original_indices = _source_original_indices(out)
    duplicate_original_index_counts = Counter(original_indices)

    uuid_values: list[str] = []
    for row_position, original_index in enumerate(original_indices):
        material = f"{OBS_IDENTITY_VERSION}:{dataset_id}:{prefix}:{original_index}"
        if duplicate_original_index_counts[original_index] > 1:
            material = f"{material}:{row_position}"
        uuid_values.append(str(uuid5(NAMESPACE_URL, material)))

    out[original_obs_index_column] = original_indices
    out[ORIGINAL_OBS_INDEX_DUPLICATED_COLUMN] = [
        duplicate_original_index_counts[original_index] > 1
        for original_index in original_indices
    ]
    out[obs_uuid_column] = uuid_values
    validate_obs_identity(
        out,
        obs_uuid_column=obs_uuid_column,
        original_obs_index_column=original_obs_index_column,
    )
    return out


def validate_obs_identity(
    obs: pd.DataFrame,
    *,
    obs_uuid_column: str = OBS_UUID_COLUMN,
    original_obs_index_column: str = ORIGINAL_OBS_INDEX_COLUMN,
) -> None:
    """Validate required global obs identity columns on an obs table.

    Raises ``ValueError`` if required columns are absent, null/empty, malformed,
    or if ``obs_uuid`` is not unique within the table being validated.
    """

    missing_columns = [
        column
        for column in (obs_uuid_column, original_obs_index_column)
        if column not in obs.columns
    ]
    if missing_columns:
        raise ValueError(
            f"missing obs identity column(s): {', '.join(missing_columns)}"
        )

    for column in (obs_uuid_column, original_obs_index_column):
        values = obs[column]
        if values.isna().any() or (values.astype(str).str.len() == 0).any():
            raise ValueError(
                f"{column} must be present and non-empty for every obs row"
            )

    if not obs[obs_uuid_column].is_unique:
        raise ValueError(f"{obs_uuid_column} must be unique within each obs artifact")

    malformed = []
    for value in obs[obs_uuid_column].astype(str).tolist():
        try:
            UUID(value)
        except ValueError:
            malformed.append(value)
    if malformed:
        preview = ", ".join(malformed[:3])
        raise ValueError(
            f"{obs_uuid_column} contains malformed UUID value(s): {preview}"
        )
