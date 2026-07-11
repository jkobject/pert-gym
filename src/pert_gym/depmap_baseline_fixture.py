"""Provenance-safe extraction of exact DepMap ModelID baseline expression."""

from __future__ import annotations

import csv
import hashlib
import math
from pathlib import Path
from typing import Any, Iterable

FIXTURE_SCHEMA_VERSION = "depmap_exact_modelid_baseline.v1"
DEPMAP_26Q1_NON_EXPRESSION_COLUMNS = {
    "",
    "SequencingID",
    "ModelConditionID",
    "ModelID",
    "IsDefaultEntryForMC",
    "IsDefaultEntryForModel",
}


def stable_model_id(value: str) -> str:
    """Return the exact ModelID represented by a PRISM subset identifier."""

    return value.split("::", 1)[0].strip()


def requested_model_ids_from_subset(subset_path: Path | str) -> set[str]:
    """Read the immutable PRISM subset and return its stable exact ModelIDs."""

    with Path(subset_path).open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not reader.fieldnames or "depmap_id" not in reader.fieldnames:
            raise ValueError("PRISM subset TSV must have a depmap_id column")
        requested = {
            stable_model_id(str(row["depmap_id"]))
            for row in reader
            if stable_model_id(str(row["depmap_id"]))
        }
    if not requested:
        raise ValueError("PRISM subset TSV has no exact stable ModelIDs")
    return requested


def model_id_set_sha256(model_ids: Iterable[str]) -> str:
    """Hash a sorted exact ModelID set with the extraction's stable encoding."""

    return hashlib.sha256("\n".join(sorted(model_ids)).encode()).hexdigest()


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def validate_fixture_for_manifest(
    fixture: dict[str, Any], manifest: dict[str, Any], prism_subset_path: Path | str
) -> None:
    """Fail closed unless fixture payload and provenance bind to the smoke subset."""

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
        expected_value = expected_source.get(key)
        actual_value = source.get(key)
        if not isinstance(expected_value, str) or not expected_value:
            raise ValueError(f"PRISM manifest baseline source lacks {key}")
        if not isinstance(actual_value, str) or not actual_value:
            raise ValueError(f"baseline fixture source lacks {key}")
        if actual_value != expected_value:
            raise ValueError(
                f"baseline fixture source {key} does not match PRISM manifest"
            )

    extraction = provenance.get("extraction")
    if (
        not isinstance(extraction, dict)
        or extraction.get("canonical_model_id_unique") is not True
    ):
        raise ValueError("baseline fixture does not prove canonical ModelID uniqueness")
    feature_names = fixture.get("feature_names")
    if (
        not isinstance(feature_names, list)
        or not feature_names
        or any(not isinstance(name, str) or not name.strip() for name in feature_names)
        or len(set(feature_names)) != len(feature_names)
    ):
        raise ValueError("baseline fixture must have unique nonempty feature names")
    rows = fixture.get("rows")
    if (
        not isinstance(rows, list)
        or not rows
        or any(not isinstance(row, dict) for row in rows)
    ):
        raise ValueError("baseline fixture has no extracted rows")

    model_ids: list[str] = []
    source_data_rows: list[int] = []
    for row in rows:
        depmap_id = row.get("depmap_id")
        if not isinstance(depmap_id, str) or not depmap_id.strip():
            raise ValueError("baseline fixture rows must have nonempty exact depmap_id")
        if row.get("source_model_id") != depmap_id:
            raise ValueError("baseline fixture source_model_id must equal depmap_id")
        source_data_row = row.get("source_data_row")
        if not _is_positive_int(source_data_row):
            raise ValueError(
                "baseline fixture rows lack positive source-data-row provenance"
            )
        expression = row.get("expression")
        if not isinstance(expression, list) or len(expression) != len(feature_names):
            raise ValueError(
                "baseline fixture expression shape does not match features"
            )
        if any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            for value in expression
        ):
            raise ValueError(
                "baseline fixture expression values must be finite numbers"
            )
        model_ids.append(depmap_id)
        source_data_rows.append(source_data_row)
    if len(set(model_ids)) != len(model_ids):
        raise ValueError("baseline fixture rows must have unique exact ModelIDs")
    if len(set(source_data_rows)) != len(source_data_rows):
        raise ValueError("baseline fixture source data rows must be unique")

    inputs = provenance.get("inputs")
    if not isinstance(inputs, dict):
        raise ValueError("baseline fixture lacks PRISM subset provenance")
    actual_subset_sha256 = sha256_file(prism_subset_path)
    if inputs.get("prism_subset_sha256") != actual_subset_sha256:
        raise ValueError("baseline fixture subset SHA256 does not match PRISM subset")
    requested = requested_model_ids_from_subset(prism_subset_path)
    expected_requested_sha256 = model_id_set_sha256(requested)
    if extraction.get("requested_model_ids_sha256") != expected_requested_sha256:
        raise ValueError(
            "baseline fixture requested ModelID set does not match PRISM subset"
        )
    if set(model_ids) != requested:
        raise ValueError("baseline fixture rows do not exactly match the PRISM subset")

    source_data_row_count = extraction.get("source_data_rows")
    if (
        not _is_positive_int(source_data_row_count)
        or max(source_data_rows) > source_data_row_count
    ):
        raise ValueError(
            "baseline fixture source-data-row count disagrees with payload"
        )
    if extraction.get("source_feature_count") != len(feature_names):
        raise ValueError("baseline fixture source-feature count disagrees with payload")
    if extraction.get("requested_unique_model_ids") != len(requested):
        raise ValueError(
            "baseline fixture requested-row count disagrees with PRISM subset"
        )
    if extraction.get("matched_rows") != len(rows):
        raise ValueError(
            "baseline fixture matched-row provenance disagrees with payload"
        )


def sha256_file(path: Path | str) -> str:
    """Return the immutable SHA256 of a local source or fixture file."""

    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


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
    """Extract exact unique ModelID rows and retain source-row provenance.

    The canonical expression CSV is scanned completely. Any duplicate ``ModelID``
    in that source is a source-contract failure, even when the duplicated ID is
    outside the requested subset. Selecting, averaging, or last-writing duplicate
    expression rows would make a release baseline scientifically ambiguous.
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
        if not reader.fieldnames or "ModelID" not in reader.fieldnames:
            raise ValueError(
                "canonical DepMap expression CSV must have a ModelID column"
            )
        feature_names = [
            name
            for name in reader.fieldnames
            if name not in DEPMAP_26Q1_NON_EXPRESSION_COLUMNS
        ]
        if not feature_names:
            raise ValueError(
                "canonical DepMap expression CSV has no expression features"
            )

        row_numbers_by_id: dict[str, list[int]] = {}
        selected: dict[str, dict[str, Any]] = {}
        source_rows = 0
        for source_data_row, row in enumerate(reader, start=1):
            source_rows += 1
            model_id = str(row.get("ModelID", "")).strip()
            if not model_id:
                raise ValueError(
                    f"canonical DepMap expression row {source_data_row} has blank ModelID"
                )
            row_numbers_by_id.setdefault(model_id, []).append(source_data_row)
            if model_id not in requested:
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
            selected[model_id] = {
                "depmap_id": model_id,
                "expression": expression,
                "source_data_row": source_data_row,
                "source_model_id": model_id,
            }

    duplicates = {
        model_id: rows for model_id, rows in row_numbers_by_id.items() if len(rows) > 1
    }
    if duplicates:
        detail = "; ".join(
            f"{model_id} rows {rows}"
            for model_id, rows in sorted(duplicates.items())[:10]
        )
        raise ValueError(
            "canonical DepMap expression source has duplicate ModelID values; "
            f"fail closed rather than select or aggregate: {detail}"
        )
    missing = sorted(requested - set(selected))
    if missing:
        raise ValueError(
            "canonical DepMap expression source is missing requested exact ModelIDs: "
            + ", ".join(missing[:10])
        )

    ordered_rows = [selected[model_id] for model_id in sorted(requested)]
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
                "canonical_model_id_unique": True,
                "requested_model_ids_sha256": model_id_set_sha256(requested),
            },
        },
    }
