import csv
import hashlib

import pytest

from pert_gym.depmap_baseline_fixture import (
    extract_exact_modelid_baseline,
    validate_fixture_for_manifest,
)


def _write_expression_csv(path, rows) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ModelID", "GENE_A", "GENE_B"])
        writer.writeheader()
        writer.writerows(rows)


def test_extract_exact_modelid_baseline_records_source_row_provenance(tmp_path) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source,
        [
            {"ModelID": "ACH-2", "GENE_A": "2.0", "GENE_B": "3.0"},
            {"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "4.0"},
            {"ModelID": "ACH-3", "GENE_A": "5.0", "GENE_B": "6.0"},
        ],
    )

    fixture = extract_exact_modelid_baseline(
        source_path=source,
        requested_model_ids={"ACH-1", "ACH-2"},
        source_uri="gs://bucket/depmap.csv",
        source_generation="123",
        expected_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
        commit="deadbeef",
    )

    assert fixture["schema_version"] == "depmap_exact_modelid_baseline.v1"
    assert fixture["feature_names"] == ["GENE_A", "GENE_B"]
    assert [row["depmap_id"] for row in fixture["rows"]] == ["ACH-1", "ACH-2"]
    assert fixture["rows"][0]["source_data_row"] == 2
    assert fixture["provenance"]["source"] == {
        "uri": "gs://bucket/depmap.csv",
        "generation": "123",
        "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
    }
    assert fixture["provenance"]["extraction"]["canonical_model_id_unique"] is True
    assert fixture["provenance"]["extraction"]["source_data_rows"] == 3
    assert fixture["provenance"]["extraction"]["matched_rows"] == 2


def test_extract_exact_modelid_baseline_rejects_duplicate_canonical_model_ids(
    tmp_path,
) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source,
        [
            {"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "2.0"},
            {"ModelID": "ACH-2", "GENE_A": "3.0", "GENE_B": "4.0"},
            {"ModelID": "ACH-1", "GENE_A": "5.0", "GENE_B": "6.0"},
        ],
    )

    with pytest.raises(ValueError, match="duplicate ModelID") as exc_info:
        extract_exact_modelid_baseline(
            source_path=source,
            requested_model_ids={"ACH-1"},
            source_uri="gs://bucket/depmap.csv",
            source_generation="123",
            expected_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
            commit="deadbeef",
        )

    assert "ACH-1" in str(exc_info.value)
    assert "rows [1, 3]" in str(exc_info.value)


def test_extract_exact_modelid_baseline_rejects_source_checksum_mismatch(
    tmp_path,
) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source, [{"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "2.0"}]
    )

    with pytest.raises(ValueError, match="SHA256"):
        extract_exact_modelid_baseline(
            source_path=source,
            requested_model_ids={"ACH-1"},
            source_uri="gs://bucket/depmap.csv",
            source_generation="123",
            expected_source_sha256="not-the-source-sha",
            extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
            commit="deadbeef",
        )


def test_extract_exact_modelid_baseline_excludes_known_non_expression_columns(
    tmp_path,
) -> None:
    source = tmp_path / "expression.csv"
    with source.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "",
                "SequencingID",
                "ModelConditionID",
                "ModelID",
                "IsDefaultEntryForMC",
                "IsDefaultEntryForModel",
                "GENE_A",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "": "0",
                "SequencingID": "CDS-001",
                "ModelConditionID": "MC-001",
                "ModelID": "ACH-1",
                "IsDefaultEntryForMC": "Yes",
                "IsDefaultEntryForModel": "Yes",
                "GENE_A": "1.5",
            }
        )

    fixture = extract_exact_modelid_baseline(
        source_path=source,
        requested_model_ids={"ACH-1"},
        source_uri="gs://bucket/depmap.csv",
        source_generation="123",
        expected_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
        commit="deadbeef",
    )

    assert fixture["feature_names"] == ["GENE_A"]
    assert fixture["rows"][0]["expression"] == [1.5]


def test_validate_fixture_for_manifest_requires_matching_source_provenance(
    tmp_path,
) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source, [{"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "2.0"}]
    )
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    fixture = extract_exact_modelid_baseline(
        source_path=source,
        requested_model_ids={"ACH-1"},
        source_uri="gs://bucket/depmap.csv",
        source_generation="123",
        expected_source_sha256=source_sha256,
        extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
        commit="deadbeef",
    )
    manifest = {
        "baseline": {
            "uri": "gs://bucket/depmap.csv",
            "generation": "123",
            "sha256": source_sha256,
        }
    }

    validate_fixture_for_manifest(fixture, manifest)

    fixture["provenance"]["source"]["generation"] = "other"
    with pytest.raises(ValueError, match="generation"):
        validate_fixture_for_manifest(fixture, manifest)
