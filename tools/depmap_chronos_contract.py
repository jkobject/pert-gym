"""Release-locked schema rules for DepMap 26Q1 Chronos GeneEffect rows.

This module is intentionally local-only: it validates and annotates response
rows but neither reads nor writes Lamin artifacts. Chronos GeneEffect values are
raw scores where lower values mean stronger dependency; the policy is metadata,
not a sign transform.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

DEPMAP_26Q1_RELEASE = "DepMap Public 26Q1"
DEPMAP_26Q1_BASELINE_PREFIX = "depmap_ccle/26q1"
CHRONOS_SOURCE_ACCESSION = "31660582:62677015"
CHRONOS_RESPONSE_METRIC = "GeneEffect"
CHRONOS_SCORE_SOURCE = "Chronos"
CHRONOS_RESPONSE_TRANSFORM = "raw_Chronos_GeneEffect"
LOWER_MORE_DEPENDENT = "lower_more_dependent"

# Keep this as the schema enum rather than treating arbitrary direction strings
# as valid loader input. Existing model-ready response rows use the sensitivity
# values; Chronos extends it with lower_more_dependent.
CANONICAL_RESPONSE_DIRECTIONS = frozenset(
    {
        "higher_is_more_sensitive",
        "lower_is_more_sensitive",
        LOWER_MORE_DEPENDENT,
    }
)

CHRONOS_REQUIRED_COLUMNS = (
    "model_id",
    "depmap_id",
    "baseline_join_id",
    "response_value",
    "response_metric",
    "score_source",
    "response_transform",
    "response_direction",
)


def normalize_model_id(value: object) -> str:
    """Return the stable ModelID representation used for exact joins.

    Only surrounding whitespace introduced by table serialization is normalized.
    The identifier is not case-folded, aliased, name-matched, or otherwise
    transformed.
    """

    if value is None or pd.isna(value):
        raise ValueError("DepMap ModelID must be a non-empty value")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("DepMap ModelID must be non-empty after normalization")
    return normalized


def annotate_raw_chronos_gene_effect(rows: pd.DataFrame) -> pd.DataFrame:
    """Attach canonical Chronos metadata without altering raw GeneEffect values."""

    if "model_id" not in rows.columns or "response_value" not in rows.columns:
        raise ValueError("Chronos rows require model_id and response_value")

    annotated = rows.copy()
    raw_values = annotated["response_value"].copy()
    model_ids = annotated["model_id"].map(normalize_model_id)
    annotated["model_id"] = model_ids
    annotated["depmap_id"] = model_ids
    annotated["baseline_join_id"] = model_ids
    annotated["response_metric"] = CHRONOS_RESPONSE_METRIC
    annotated["score_source"] = CHRONOS_SCORE_SOURCE
    annotated["response_transform"] = CHRONOS_RESPONSE_TRANSFORM
    annotated["response_direction"] = LOWER_MORE_DEPENDENT

    # This assertion is intentionally value-level (not merely metadata-level):
    # a future refactor cannot silently negate the raw Chronos score.
    if not annotated["response_value"].equals(raw_values):
        raise AssertionError("Chronos raw GeneEffect values must not be sign-inverted")
    return annotated


def validate_chronos_gene_effect_rows(rows: pd.DataFrame) -> None:
    """Validate the canonical raw Chronos response-row representation."""

    missing = set(CHRONOS_REQUIRED_COLUMNS).difference(rows.columns)
    if missing:
        raise ValueError(f"Chronos rows missing required columns: {sorted(missing)}")

    directions = set(rows["response_direction"].dropna().astype(str))
    unknown_directions = directions.difference(CANONICAL_RESPONSE_DIRECTIONS)
    if unknown_directions:
        raise ValueError(
            f"Unknown response_direction values: {sorted(unknown_directions)}"
        )
    if directions != {LOWER_MORE_DEPENDENT}:
        raise ValueError("Raw Chronos GeneEffect must use lower_more_dependent")

    expected_metadata = {
        "response_metric": CHRONOS_RESPONSE_METRIC,
        "score_source": CHRONOS_SCORE_SOURCE,
        "response_transform": CHRONOS_RESPONSE_TRANSFORM,
    }
    for column, expected in expected_metadata.items():
        observed = set(rows[column].dropna().astype(str))
        if observed != {expected}:
            raise ValueError(
                f"Chronos {column} must be {expected!r}, got {sorted(observed)}"
            )

    normalized = rows.loc[:, ["model_id", "depmap_id", "baseline_join_id"]].map(
        normalize_model_id
    )
    disagreements = normalized.nunique(axis=1) != 1
    if disagreements.any():
        bad_rows = list(rows.index[disagreements])
        raise ValueError(
            "Chronos ModelID, depmap_id, and baseline_join_id must agree by exact "
            f"normalized string equality; bad rows: {bad_rows}"
        )


def apply_release_locked_baseline_policy(
    chronos_rows: pd.DataFrame,
    baseline_model_ids: Iterable[object],
    *,
    source_release: str = DEPMAP_26Q1_RELEASE,
    baseline_release: str = DEPMAP_26Q1_RELEASE,
    baseline_prefix: str = DEPMAP_26Q1_BASELINE_PREFIX,
) -> pd.DataFrame:
    """Mark exact 26Q1 baseline matches without aliasing or dropping responses.

    Unmatched source rows remain present and are explicitly excluded from
    baseline-conditioned model-ready promotion. Cross-release substitution is a
    hard error, even if identifiers happen to overlap.
    """

    if source_release != DEPMAP_26Q1_RELEASE:
        raise ValueError(f"Chronos source release must be {DEPMAP_26Q1_RELEASE!r}")
    if baseline_release != DEPMAP_26Q1_RELEASE:
        raise ValueError(
            "Cross-release baseline substitution is forbidden; expected "
            f"{DEPMAP_26Q1_RELEASE!r}, got {baseline_release!r}"
        )
    if baseline_prefix != DEPMAP_26Q1_BASELINE_PREFIX:
        raise ValueError(
            "Chronos baseline must be release-locked to "
            f"{DEPMAP_26Q1_BASELINE_PREFIX!r}, got {baseline_prefix!r}"
        )

    validate_chronos_gene_effect_rows(chronos_rows)
    baseline_ids = {normalize_model_id(value) for value in baseline_model_ids}
    result = chronos_rows.copy()
    source_ids = result["baseline_join_id"].map(normalize_model_id)
    matched = source_ids.isin(baseline_ids)
    result["baseline_release"] = baseline_release
    result["baseline_lamin_prefix"] = baseline_prefix
    result["baseline_join_status"] = matched.map(
        {True: "matched_same_release", False: "unmatched_same_release"}
    )
    result["baseline_conditioned_promotion"] = matched
    result["baseline_conditioned_exclusion_reason"] = ""
    result.loc[~matched, "baseline_conditioned_exclusion_reason"] = (
        "missing_exact_26Q1_baseline_ModelID"
    )
    return result


def write_chronos_coverage_artifact(
    source_model_ids: Iterable[object],
    baseline_model_ids: Iterable[object],
    output_path: Path,
) -> tuple[Path, Path]:
    """Write deterministic coverage JSON plus sorted unmatched-ModelID sidecar."""

    source_ids = sorted({normalize_model_id(value) for value in source_model_ids})
    baseline_ids = {normalize_model_id(value) for value in baseline_model_ids}
    unmatched_ids = [
        model_id for model_id in source_ids if model_id not in baseline_ids
    ]
    matched_ids = [model_id for model_id in source_ids if model_id in baseline_ids]
    sidecar_path = output_path.with_name(f"{output_path.stem}_unmatched_model_ids.tsv")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(
        "ModelID\treason\n"
        + "".join(
            f"{model_id}\tmissing_exact_26Q1_baseline_ModelID\n"
            for model_id in unmatched_ids
        ),
        encoding="utf-8",
    )
    payload = {
        "schema": "model_ready_v2_chronos_coverage/v1",
        "source": {
            "accession": CHRONOS_SOURCE_ACCESSION,
            "release": DEPMAP_26Q1_RELEASE,
            "score_source": CHRONOS_SCORE_SOURCE,
            "response_metric": CHRONOS_RESPONSE_METRIC,
            "response_transform": CHRONOS_RESPONSE_TRANSFORM,
            "response_direction": LOWER_MORE_DEPENDENT,
            "numeric_transform": "none",
        },
        "baseline": {
            "release": DEPMAP_26Q1_RELEASE,
            "lamin_prefix": DEPMAP_26Q1_BASELINE_PREFIX,
            "join_policy": "exact_normalized_ModelID_equality",
            "cross_release_policy": "forbidden_without_reviewed_versioned_mapping",
        },
        "coverage": {
            "source_model_ids": len(source_ids),
            "matched_model_ids": len(matched_ids),
            "unmatched_model_ids": len(unmatched_ids),
            "unmatched_model_ids_sidecar": sidecar_path.name,
        },
        "unmatched_policy": (
            "retain response-source rows; exclude from baseline-conditioned "
            "model_ready_v2 promotion with missing_exact_26Q1_baseline_ModelID"
        ),
    }
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return output_path, sidecar_path
