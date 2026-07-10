#!/usr/bin/env python3
"""Materialize a bounded, source-traceable Broad PRISM 24Q2 LFC projection.

This is deliberately a local, source-file-to-TSV projection. It never imports
LaminDB or writes a Lamin artifact. Run it on a worker that has the staged
release files; do not use it to bulk-read the release on a developer Mac.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
import json
import math
import sqlite3
import tempfile
from collections import Counter
from itertools import combinations
from pathlib import Path
from typing import Any

SOURCE_RELEASE = "DepMap PRISM Repurposing Public 24Q2"
LFC_URI = (
    "gs://scperturb/pert-gym/staging/data/main/broad_prism/"
    "Repurposing_Public_24Q2_LFC.csv"
)
METADATA_URI = (
    "gs://scperturb/pert-gym/staging/data/main/broad_prism/"
    "Repurposing_Public_24Q2_Treatment_Meta_Data.csv"
)
LFC_SHA256 = "824149f9b9f3821eb520b385a5976e1a9977d86b21caf5d22171763800a40523"
METADATA_SHA256 = "6be6422ba804ad0775e78b457677bdf088707b9354746a03e110ae63f5eb2061"
LFC_FIELDS = ("row_id", "profile_id", "LFC", "PASS")
METADATA_FIELDS = ("profile_id", "perturbation_type", "dose", "broad_id", "name")
OUTPUT_FIELDS = (
    "source_release",
    "source_lfc_uri",
    "source_lfc_sha256",
    "source_treatment_metadata_uri",
    "source_treatment_metadata_sha256",
    "source_row_id",
    "source_profile_id",
    "source_row_identifier",
    "source_row_chunk_index",
    "source_row_offset_in_chunk",
    "source_file_row_number",
    "perturbation",
    "perturbation_id",
    "perturbation_type",
    "organism",
    "context_id",
    "cell_line",
    "depmap_id",
    "dose",
    "dose_unit",
    "response_metric",
    "response_value",
    "response_source",
    "response_transform",
    "response_direction",
    "target_is_direct",
    "has_expression_X",
    "x_semantics",
    "model_ready_status",
    "split",
)
SPLITS = ("train", "val", "test")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_columns(
    path: Path, rows: csv.DictReader, required: tuple[str, ...]
) -> None:
    present = set(rows.fieldnames or ())
    missing = sorted(set(required) - present)
    if missing:
        raise ValueError(f"{path} is missing required columns: {missing}")


def load_metadata(path: Path) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        require_columns(path, reader, METADATA_FIELDS)
        records: dict[str, dict[str, str]] = {}
        for line_number, row in enumerate(reader, start=2):
            profile_id = (row.get("profile_id") or "").strip()
            if not profile_id:
                raise ValueError(f"{path}:{line_number} has an empty profile_id")
            if profile_id in records:
                raise ValueError(
                    f"{path}:{line_number} repeats profile_id {profile_id!r}"
                )
            records[profile_id] = row
    return records


def finite_lfc(raw: str | None) -> float | None:
    try:
        value = float(raw or "")
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def split_for_compound(broad_id: str) -> str:
    bucket = (
        int.from_bytes(
            hashlib.sha256(
                f"phase2-prism-compound-holdout-v1|{broad_id}".encode("utf-8")
            ).digest(),
            byteorder="big",
        )
        % 10
    )
    if bucket <= 1:
        return "test"
    if bucket == 2:
        return "val"
    return "train"


def selection_score(source_row_identifier: str) -> int:
    return int.from_bytes(
        hashlib.sha256(
            f"phase2-prism-real-lfc-v1|{source_row_identifier}".encode("utf-8")
        ).digest(),
        byteorder="big",
    )


def leakage(rows: list[dict[str, str]], field: str) -> dict[str, list[str]]:
    per_split = {
        split: {row[field] for row in rows if row["split"] == split} for split in SPLITS
    }
    return {
        f"{left}_{right}": sorted(per_split[left] & per_split[right])
        for left, right in combinations(SPLITS, 2)
    }


def source_row(
    raw: dict[str, str],
    metadata: dict[str, str],
    *,
    lfc_uri: str,
    lfc_sha256: str,
    metadata_uri: str,
    metadata_sha256: str,
    row_number: int,
    chunk_size_rows: int,
) -> dict[str, str]:
    row_id = (raw.get("row_id") or "").strip()
    profile_id = (raw.get("profile_id") or "").strip()
    depmap_id = row_id.split("::", maxsplit=1)[0]
    return {
        "source_release": SOURCE_RELEASE,
        "source_lfc_uri": lfc_uri,
        "source_lfc_sha256": lfc_sha256,
        "source_treatment_metadata_uri": metadata_uri,
        "source_treatment_metadata_sha256": metadata_sha256,
        "source_row_id": row_id,
        "source_profile_id": profile_id,
        "source_row_identifier": f"{row_id}|{profile_id}",
        "source_row_chunk_index": str((row_number - 1) // chunk_size_rows),
        "source_row_offset_in_chunk": str((row_number - 1) % chunk_size_rows),
        "source_file_row_number": str(row_number),
        "perturbation": (metadata.get("name") or "").strip(),
        "perturbation_id": (metadata.get("broad_id") or "").strip(),
        "perturbation_type": "drug",
        "organism": "Homo sapiens",
        "context_id": f"Homo sapiens|{depmap_id}",
        "cell_line": depmap_id,
        "depmap_id": depmap_id,
        "dose": (metadata.get("dose") or "").strip(),
        "dose_unit": "source_unit_not_specified",
        "response_metric": "lfc",
        # Preserve the source spelling after verifying its parsed numeric value
        # is finite; downstream consumers can parse this direct source field.
        "response_value": (raw.get("LFC") or "").strip(),
        "response_source": (
            "Repurposing_Public_24Q2_LFC.csv:LFC joined to Treatment_Meta_Data "
            "by profile_id"
        ),
        "response_transform": "source_lfc_vs_vehicle",
        "response_direction": "lower_more_sensitive",
        "target_is_direct": "true",
        "has_expression_X": "false",
        "x_semantics": "response_table",
        "model_ready_status": "loader_projectable_only",
        "split": split_for_compound((metadata.get("broad_id") or "").strip()),
    }


def materialize_projection(
    *,
    lfc_path: Path,
    metadata_path: Path,
    output_tsv: Path,
    output_manifest: Path,
    lfc_uri: str = LFC_URI,
    metadata_uri: str = METADATA_URI,
    selection_size: int = 128,
    chunk_size_rows: int = 250_000,
    expected_lfc_sha256: str | None = None,
    expected_metadata_sha256: str | None = None,
) -> dict[str, Any]:
    """Project source LFC rows after auditing the full source stream.

    Exact immutable source duplicates fail the run. Measurement duplicates are
    retained and reported, because source row identity is the provenance key.
    """
    if selection_size <= 0 or chunk_size_rows <= 0:
        raise ValueError("selection_size and chunk_size_rows must be positive")
    metadata = load_metadata(metadata_path)
    lfc_hash = sha256_file(lfc_path)
    metadata_hash = sha256_file(metadata_path)
    if expected_lfc_sha256 and lfc_hash != expected_lfc_sha256:
        raise ValueError("LFC source SHA-256 does not match the approved release")
    if expected_metadata_sha256 and metadata_hash != expected_metadata_sha256:
        raise ValueError(
            "treatment metadata SHA-256 does not match the approved release"
        )

    denominator = Counter()
    heap: list[tuple[int, str, dict[str, str]]] = []
    output_tsv.parent.mkdir(parents=True, exist_ok=True)
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="broad-prism-projection-") as temp_dir:
        database = sqlite3.connect(Path(temp_dir) / "seen.sqlite")
        try:
            database.execute("CREATE TABLE source_rows (identifier TEXT PRIMARY KEY)")
            database.execute(
                "CREATE TABLE measurement_rows (measurement_key TEXT PRIMARY KEY, count INTEGER NOT NULL)"
            )
            with lfc_path.open(newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                require_columns(lfc_path, reader, LFC_FIELDS)
                for row_number, raw in enumerate(reader, start=1):
                    denominator["source_rows"] += 1
                    row_id = (raw.get("row_id") or "").strip()
                    profile_id = (raw.get("profile_id") or "").strip()
                    source_identifier = f"{row_id}|{profile_id}"
                    try:
                        database.execute(
                            "INSERT INTO source_rows(identifier) VALUES (?)",
                            (source_identifier,),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise ValueError(
                            f"duplicate source_row_identifier: {source_identifier!r}"
                        ) from exc
                    lfc_value = finite_lfc(raw.get("LFC"))
                    if lfc_value is None:
                        denominator["excluded_non_finite_lfc"] += 1
                        continue
                    if (raw.get("PASS") or "").strip().lower() != "true":
                        denominator["excluded_not_pass"] += 1
                        continue
                    metadata_row = metadata.get(profile_id)
                    if metadata_row is None:
                        denominator[
                            "excluded_missing_or_unmatched_profile_metadata"
                        ] += 1
                        continue
                    if (
                        metadata_row.get("perturbation_type") or ""
                    ).strip().lower() != "trt_cp":
                        denominator["excluded_non_drug_treatment_type"] += 1
                        continue
                    broad_id = (metadata_row.get("broad_id") or "").strip()
                    name = (metadata_row.get("name") or "").strip()
                    if not broad_id or not name:
                        denominator["excluded_missing_compound_identity"] += 1
                        continue
                    denominator["eligible_rows"] += 1
                    depmap_id = row_id.split("::", maxsplit=1)[0]
                    measurement_key = "|".join((depmap_id, broad_id, profile_id))
                    database.execute(
                        "INSERT INTO measurement_rows(measurement_key, count) VALUES (?, 1) "
                        "ON CONFLICT(measurement_key) DO UPDATE SET count = count + 1",
                        (measurement_key,),
                    )
                    projected = source_row(
                        raw,
                        metadata_row,
                        lfc_uri=lfc_uri,
                        lfc_sha256=lfc_hash,
                        metadata_uri=metadata_uri,
                        metadata_sha256=metadata_hash,
                        row_number=row_number,
                        chunk_size_rows=chunk_size_rows,
                    )
                    score = selection_score(source_identifier)
                    candidate = (-score, source_identifier, projected)
                    if len(heap) < selection_size:
                        heapq.heappush(heap, candidate)
                    elif candidate > heap[0]:
                        heapq.heapreplace(heap, candidate)
            database.commit()
            duplicate_measurement_groups, duplicate_measurement_extra_rows = (
                database.execute(
                    "SELECT COUNT(*), COALESCE(SUM(count - 1), 0) FROM measurement_rows WHERE count > 1"
                ).fetchone()
            )
        finally:
            database.close()

    selected = [candidate[2] for candidate in heap]
    selected.sort(
        key=lambda row: (
            selection_score(row["source_row_identifier"]),
            row["source_row_identifier"],
        )
    )
    compound_leakage = leakage(selected, "perturbation_id")
    source_leakage = leakage(selected, "source_row_identifier")
    if any(compound_leakage.values()) or any(source_leakage.values()):
        raise AssertionError("compound or source-row leakage across splits")

    with output_tsv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(selected)
    manifest = {
        "scope": "bounded real Broad PRISM 24Q2 direct-LFC projection; no Lamin write",
        "source_release": SOURCE_RELEASE,
        "source_files": {
            "lfc": {"uri": lfc_uri, "sha256": lfc_hash},
            "treatment_metadata": {"uri": metadata_uri, "sha256": metadata_hash},
        },
        "selection": {
            "rows_requested": selection_size,
            "rows_selected": len(selected),
            "scoring": "smallest sha256(phase2-prism-real-lfc-v1|row_id|profile_id)",
            "treatment_type_policy": "trt_cp only; trt_poscon requires review",
        },
        "denominator": {
            "source_rows": denominator["source_rows"],
            "eligible_rows": denominator["eligible_rows"],
            "excluded_non_finite_lfc": denominator["excluded_non_finite_lfc"],
            "excluded_not_pass": denominator["excluded_not_pass"],
            "excluded_missing_or_unmatched_profile_metadata": denominator[
                "excluded_missing_or_unmatched_profile_metadata"
            ],
            "excluded_non_drug_treatment_type": denominator[
                "excluded_non_drug_treatment_type"
            ],
            "excluded_missing_compound_identity": denominator[
                "excluded_missing_compound_identity"
            ],
        },
        "duplicate_checks": {
            "exact_source_duplicate_count": 0,
            "exact_source_policy": "fail before emitting output",
            "measurement_duplicate_groups": duplicate_measurement_groups,
            "measurement_duplicate_extra_rows": duplicate_measurement_extra_rows,
            "measurement_duplicate_policy": "retain immutable distinct source rows and report",
        },
        "compound_holdout_leakage": {
            "broad_id": compound_leakage,
            "source_row_identifier": source_leakage,
            "context_id_report_only": leakage(selected, "context_id"),
        },
        "selected_rows": len(selected),
        "selected_split_counts": dict(
            sorted(Counter(row["split"] for row in selected).items())
        ),
    }
    output_manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lfc", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output-tsv", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--selection-size", type=int, default=128)
    parser.add_argument("--chunk-size-rows", type=int, default=250_000)
    parser.add_argument("--lfc-uri", default=LFC_URI)
    parser.add_argument("--metadata-uri", default=METADATA_URI)
    parser.add_argument("--skip-approved-hash-check", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = materialize_projection(
        lfc_path=args.lfc,
        metadata_path=args.metadata,
        output_tsv=args.output_tsv,
        output_manifest=args.output_manifest,
        lfc_uri=args.lfc_uri,
        metadata_uri=args.metadata_uri,
        selection_size=args.selection_size,
        chunk_size_rows=args.chunk_size_rows,
        expected_lfc_sha256=None if args.skip_approved_hash_check else LFC_SHA256,
        expected_metadata_sha256=None
        if args.skip_approved_hash_check
        else METADATA_SHA256,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
