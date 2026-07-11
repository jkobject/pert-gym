"""Provenance-safe extraction of release-native DepMap baseline expression."""

from __future__ import annotations

import csv
import hashlib
import math
from pathlib import Path
from typing import Any, Iterable

FIXTURE_SCHEMA_VERSION = "depmap_default_model_entry_baseline.v2"
DEPMAP_26Q1_NON_EXPRESSION_COLUMNS = {
    "",
    "SequencingID",
    "ModelConditionID",
    "ModelID",
    "IsDefaultEntryForMC",
    "IsDefaultEntryForModel",
}


def _vector_sha256(expression: list[float]) -> str:
    """Return a stable content hash for an extracted expression vector."""

    payload = ",".join(repr(value) for value in expression).encode()
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path | str) -> str:
    """Return the immutable SHA256 of a local source or fixture file."""

    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_fixture_for_manifest(
    fixture: dict[str, Any], manifest: dict[str, Any]
) -> None:
    """Require a release-native default-entry fixture to match the PRISM manifest."""

    if fixture.get("schema_version") != FIXTURE_SCHEMA_VERSION:
        raise ValueError("baseline fixture lacks the provenance-safe schema version")
    expected_source = manifest.get("baseline")
    if not isinstance(expected_source, dict):
        raise ValueError("PRISM manifest lacks baseline source provenance")
    provenance = fixture.get("provenance")
    if not isinstance(provenance, dict) or not isinstance(
        provenance.get("source"), dict
    ):
        raise ValueError("baseline fixture lacks source provenance")
    source = provenance["source"]
    for key in ("uri", "generation", "sha256"):
        if str(source.get(key)) != str(expected_source.get(key)):
            raise ValueError(
                f"baseline fixture source {key} does not match PRISM manifest"
            )
    inputs = provenance.get("inputs")
    if not isinstance(inputs, dict) or not inputs.get("prism_subset_sha256"):
        raise ValueError("baseline fixture lacks PRISM subset checksum provenance")
    extraction = provenance.get("extraction")
    if not isinstance(extraction, dict) or extraction.get("selection_contract") != (
        "exactly_one_IsDefaultEntryForModel_Yes_per_requested_ModelID"
    ):
        raise ValueError("baseline fixture does not prove native default selection")
    rows = fixture.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ValueError("baseline fixture has no extracted rows")
    if extraction.get("matched_rows") != len(rows):
        raise ValueError(
            "baseline fixture matched-row provenance disagrees with payload"
        )
    model_ids = [str(row.get("depmap_id", "")) for row in rows if isinstance(row, dict)]
    if len(model_ids) != len(rows) or len(set(model_ids)) != len(model_ids):
        raise ValueError("baseline fixture rows must have unique exact ModelIDs")
    required_row_fields = {
        "source_data_row",
        "sequencing_id",
        "model_condition_id",
        "is_default_entry_for_mc",
        "is_default_entry_for_model",
        "vector_sha256",
    }
    for row in rows:
        if not isinstance(row, dict) or required_row_fields - set(row):
            raise ValueError("baseline fixture rows lack native-default provenance")
        if not isinstance(row["source_data_row"], int):
            raise ValueError("baseline fixture rows lack source-data-row provenance")
        if row["is_default_entry_for_model"] != "Yes":
            raise ValueError("baseline fixture contains a non-default ModelID row")
        expression = row.get("expression")
        if not isinstance(expression, list) or not all(
            isinstance(value, (int, float)) and math.isfinite(value)
            for value in expression
        ):
            raise ValueError("baseline fixture expression vectors must be finite")
        if row["vector_sha256"] != _vector_sha256(expression):
            raise ValueError("baseline fixture vector checksum mismatch")


def extract_exact_modelid_baseline(
    *,
    source_path: Path | str,
    requested_model_ids: Iterable[str],
    source_uri: str,
    source_generation: str,
    expected_source_sha256: str,
    extraction_command: str,
    commit: str,
) -> dict[str, Any]:
    """Select exactly one native ModelID default row per requested ModelID.

    The canonical CSV is fully scanned but only an exact
    ``IsDefaultEntryForModel == Yes`` row may supply a requested baseline.  Zero
    or multiple native defaults fail closed; ordering, averaging, and
    ``IsDefaultEntryForMC`` fallback are deliberately prohibited.
    """

    source = Path(source_path)
    source_sha256 = sha256_file(source)
    if source_sha256 != expected_source_sha256:
        raise ValueError(
            "canonical DepMap expression source SHA256 does not match the reviewed "
            f"generation: expected {expected_source_sha256}, observed {source_sha256}"
        )
    requested = {
        str(model_id).strip()
        for model_id in requested_model_ids
        if str(model_id).strip()
    }
    if not requested:
        raise ValueError("at least one exact ModelID must be requested")

    with source.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = {
            "ModelID",
            "SequencingID",
            "ModelConditionID",
            "IsDefaultEntryForMC",
            "IsDefaultEntryForModel",
        }
        if not reader.fieldnames or required_columns - set(reader.fieldnames):
            raise ValueError(
                "canonical DepMap expression CSV lacks native default-entry columns"
            )
        feature_names = [
            name
            for name in reader.fieldnames
            if name not in DEPMAP_26Q1_NON_EXPRESSION_COLUMNS
        ]
        if not feature_names:
            raise ValueError("canonical DepMap expression CSV has no expression features")

        default_candidates: dict[str, list[dict[str, Any]]] = {
            model_id: [] for model_id in requested
        }
        source_rows = 0
        for source_data_row, row in enumerate(reader, start=1):
            source_rows += 1
            model_id = str(row.get("ModelID", "")).strip()
            if not model_id:
                raise ValueError(
                    f"canonical DepMap expression row {source_data_row} has blank ModelID"
                )
            if (
                model_id not in requested
                or row["IsDefaultEntryForModel"].strip() != "Yes"
            ):
                continue
            try:
                expression = [float(row[feature]) for feature in feature_names]
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"canonical DepMap expression row {source_data_row} for {model_id} "
                    "has malformed expression values"
                ) from exc
            if not all(math.isfinite(value) for value in expression):
                raise ValueError(
                    f"canonical DepMap expression row {source_data_row} for {model_id} "
                    "has non-finite expression values"
                )
            default_candidates[model_id].append(
                {
                    "depmap_id": model_id,
                    "expression": expression,
                    "source_data_row": source_data_row,
                    "source_model_id": model_id,
                    "sequencing_id": row["SequencingID"].strip(),
                    "model_condition_id": row["ModelConditionID"].strip(),
                    "is_default_entry_for_mc": row["IsDefaultEntryForMC"].strip(),
                    "is_default_entry_for_model": row[
                        "IsDefaultEntryForModel"
                    ].strip(),
                    "vector_sha256": _vector_sha256(expression),
                }
            )

    invalid = {
        model_id: candidates
        for model_id, candidates in default_candidates.items()
        if len(candidates) != 1
    }
    if invalid:
        detail = "; ".join(
            f"{model_id} defaults={[candidate['source_data_row'] for candidate in candidates]}"
            for model_id, candidates in sorted(invalid.items())[:10]
        )
        raise ValueError(
            "each requested ModelID must have exactly one "
            f"IsDefaultEntryForModel=Yes row; {detail}"
        )
    ordered_rows = [default_candidates[model_id][0] for model_id in sorted(requested)]
    if any(
        not row["sequencing_id"] or not row["model_condition_id"]
        for row in ordered_rows
    ):
        raise ValueError("native default-entry rows must have sequencing and condition IDs")

    return {
        "schema_version": FIXTURE_SCHEMA_VERSION,
        "feature_names": feature_names,
        "rows": ordered_rows,
        "provenance": {
            "source": {
                "uri": source_uri,
                "generation": str(source_generation),
                "sha256": source_sha256,
            },
            "extraction": {
                "command": extraction_command,
                "commit": commit,
                "source_data_rows": source_rows,
                "source_feature_count": len(feature_names),
                "requested_unique_model_ids": len(requested),
                "matched_rows": len(ordered_rows),
                "selection_contract": "exactly_one_IsDefaultEntryForModel_Yes_per_requested_ModelID",
                "requested_model_ids_sha256": hashlib.sha256(
                    "\n".join(sorted(requested)).encode()
                ).hexdigest(),
            },
        },
    }
