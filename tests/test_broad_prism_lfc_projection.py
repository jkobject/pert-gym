from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from tools.materialize_broad_prism_lfc_projection import materialize_projection


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_materializes_only_real_eligible_rows_with_provenance_and_guards(
    tmp_path: Path,
) -> None:
    lfc = tmp_path / "lfc.csv"
    metadata = tmp_path / "metadata.csv"
    output = tmp_path / "projection.tsv"
    manifest = tmp_path / "projection.json"
    write_csv(
        metadata,
        ["profile_id", "perturbation_type", "dose", "broad_id", "name"],
        [
            {
                "profile_id": "profile-a",
                "perturbation_type": "trt_cp",
                "dose": "2.5",
                "broad_id": "BRD-A",
                "name": "compound a",
            },
            {
                "profile_id": "profile-b",
                "perturbation_type": "trt_cp",
                "dose": "5",
                "broad_id": "BRD-B",
                "name": "compound b",
            },
            {
                "profile_id": "profile-pos",
                "perturbation_type": "trt_poscon",
                "dose": "1",
                "broad_id": "BRD-P",
                "name": "positive control",
            },
            {
                "profile_id": "profile-empty",
                "perturbation_type": "trt_cp",
                "dose": "1",
                "broad_id": "",
                "name": "missing id",
            },
        ],
    )
    write_csv(
        lfc,
        ["row_id", "profile_id", "LFC", "PASS"],
        [
            {
                "row_id": "ACH-001::plate:a1",
                "profile_id": "profile-a",
                "LFC": "-0.4",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-002::plate:a2",
                "profile_id": "profile-b",
                "LFC": "0.3",
                "PASS": "false",
            },
            {
                "row_id": "ACH-003::plate:a3",
                "profile_id": "profile-a",
                "LFC": "NaN",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-004::plate:a4",
                "profile_id": "unknown",
                "LFC": "0.1",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-005::plate:a5",
                "profile_id": "profile-pos",
                "LFC": "0.2",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-006::plate:a6",
                "profile_id": "profile-empty",
                "LFC": "0.8",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-007::plate:a7",
                "profile_id": "profile-b",
                "LFC": "-0.8",
                "PASS": "TRUE",
            },
        ],
    )

    summary = materialize_projection(
        lfc_path=lfc,
        metadata_path=metadata,
        output_tsv=output,
        output_manifest=manifest,
        lfc_uri="gs://example/lfc.csv",
        metadata_uri="gs://example/metadata.csv",
        selection_size=128,
        chunk_size_rows=3,
    )

    rows = list(csv.DictReader(output.open(encoding="utf-8"), delimiter="\t"))
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert summary["selected_rows"] == 2
    assert summary["model_ready_status"] == "loader_projectable_only"
    assert payload["model_ready_status"] == "loader_projectable_only"
    assert payload["denominator"] == {
        "source_rows": 7,
        "chunk_size_rows": 3,
        "eligible_rows": 2,
        "excluded_non_finite_lfc": 1,
        "excluded_not_pass": 1,
        "excluded_missing_or_unmatched_profile_metadata": 1,
        "excluded_non_drug_treatment_type": 1,
        "excluded_missing_compound_identity": 1,
    }
    assert {row["source_row_identifier"] for row in rows} == {
        "ACH-001::plate:a1|profile-a",
        "ACH-007::plate:a7|profile-b",
    }
    assert all(
        row["source_release"] == "DepMap PRISM Repurposing Public 24Q2" for row in rows
    )
    assert all(row["source_lfc_uri"] == "gs://example/lfc.csv" for row in rows)
    assert all(row["dose_unit"] == "source_unit_not_specified" for row in rows)
    assert all(row["response_metric"] == "lfc" for row in rows)
    assert all(row["response_value"] in {"-0.4", "-0.8"} for row in rows)
    assert all(row["source_file_row_number"] for row in rows)
    assert {
        row["source_row_identifier"]: (
            row["source_row_chunk_index"],
            row["source_row_offset_in_chunk"],
        )
        for row in rows
    } == {
        "ACH-001::plate:a1|profile-a": ("0", "0"),
        "ACH-007::plate:a7|profile-b": ("2", "0"),
    }
    assert payload["duplicate_checks"]["exact_source_duplicate_count"] == 0
    assert all(
        not values
        for values in payload["compound_holdout_leakage"]["broad_id"].values()
    )
    assert all(
        not values
        for values in payload["compound_holdout_leakage"][
            "source_row_identifier"
        ].values()
    )


def test_rejects_duplicate_immutable_source_row_identifiers(tmp_path: Path) -> None:
    lfc = tmp_path / "lfc.csv"
    metadata = tmp_path / "metadata.csv"
    write_csv(
        metadata,
        ["profile_id", "perturbation_type", "dose", "broad_id", "name"],
        [
            {
                "profile_id": "profile-a",
                "perturbation_type": "trt_cp",
                "dose": "2.5",
                "broad_id": "BRD-A",
                "name": "compound a",
            }
        ],
    )
    write_csv(
        lfc,
        ["row_id", "profile_id", "LFC", "PASS"],
        [
            {
                "row_id": "ACH-001::plate:a1",
                "profile_id": "profile-a",
                "LFC": "-0.4",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-001::plate:a1",
                "profile_id": "profile-a",
                "LFC": "-0.8",
                "PASS": "TRUE",
            },
        ],
    )

    with pytest.raises(ValueError, match="duplicate source_row_identifier"):
        materialize_projection(
            lfc_path=lfc,
            metadata_path=metadata,
            output_tsv=tmp_path / "projection.tsv",
            output_manifest=tmp_path / "projection.json",
            lfc_uri="gs://example/lfc.csv",
            metadata_uri="gs://example/metadata.csv",
        )


@pytest.mark.parametrize("existing_destination", ["projection.tsv", "projection.json"])
def test_refuses_to_overwrite_existing_destination(
    tmp_path: Path, existing_destination: str
) -> None:
    lfc = tmp_path / "lfc.csv"
    metadata = tmp_path / "metadata.csv"
    output = tmp_path / "projection.tsv"
    manifest = tmp_path / "projection.json"
    write_csv(
        metadata,
        ["profile_id", "perturbation_type", "dose", "broad_id", "name"],
        [
            {
                "profile_id": "profile-a",
                "perturbation_type": "trt_cp",
                "dose": "2.5",
                "broad_id": "BRD-A",
                "name": "compound a",
            }
        ],
    )
    write_csv(
        lfc,
        ["row_id", "profile_id", "LFC", "PASS"],
        [
            {
                "row_id": "ACH-001::plate:a1",
                "profile_id": "profile-a",
                "LFC": "-0.4",
                "PASS": "TRUE",
            }
        ],
    )
    destination = tmp_path / existing_destination
    destination.write_text("prior artifact\n", encoding="utf-8")

    with pytest.raises(ValueError, match="refusing to overwrite existing output"):
        materialize_projection(
            lfc_path=lfc,
            metadata_path=metadata,
            output_tsv=output,
            output_manifest=manifest,
            lfc_uri="gs://example/lfc.csv",
            metadata_uri="gs://example/metadata.csv",
        )

    assert destination.read_text(encoding="utf-8") == "prior artifact\n"
    other_destination = manifest if destination == output else output
    assert not other_destination.exists()


def test_failed_validation_leaves_no_output_artifacts(tmp_path: Path) -> None:
    lfc = tmp_path / "lfc.csv"
    metadata = tmp_path / "metadata.csv"
    output = tmp_path / "projection.tsv"
    manifest = tmp_path / "projection.json"
    write_csv(
        metadata,
        ["profile_id", "perturbation_type", "dose", "broad_id", "name"],
        [
            {
                "profile_id": "profile-a",
                "perturbation_type": "trt_cp",
                "dose": "2.5",
                "broad_id": "BRD-A",
                "name": "compound a",
            }
        ],
    )
    write_csv(
        lfc,
        ["row_id", "profile_id", "LFC", "PASS"],
        [
            {
                "row_id": "ACH-001::plate:a1",
                "profile_id": "profile-a",
                "LFC": "-0.4",
                "PASS": "TRUE",
            },
            {
                "row_id": "ACH-001::plate:a1",
                "profile_id": "profile-a",
                "LFC": "-0.8",
                "PASS": "TRUE",
            },
        ],
    )

    with pytest.raises(ValueError, match="duplicate source_row_identifier"):
        materialize_projection(
            lfc_path=lfc,
            metadata_path=metadata,
            output_tsv=output,
            output_manifest=manifest,
            lfc_uri="gs://example/lfc.csv",
            metadata_uri="gs://example/metadata.csv",
        )

    assert not output.exists()
    assert not manifest.exists()
