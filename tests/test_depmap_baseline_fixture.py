import csv
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

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


def _write_prism_subset(path, model_ids: list[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["depmap_id"], delimiter="\t")
        writer.writeheader()
        writer.writerows({"depmap_id": model_id} for model_id in model_ids)


def _fixture_manifest_and_subset(tmp_path):
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source,
        [
            {"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "2.0"},
            {"ModelID": "ACH-2", "GENE_A": "3.0", "GENE_B": "4.0"},
        ],
    )
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    subset = tmp_path / "subset.tsv"
    _write_prism_subset(subset, ["ACH-1", "ACH-2"])
    fixture = extract_exact_modelid_baseline(
        source_path=source,
        requested_model_ids={"ACH-1", "ACH-2"},
        source_uri="gs://bucket/depmap.csv",
        source_generation="123",
        expected_source_sha256=source_sha256,
        extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
        commit="deadbeef",
    )
    fixture["provenance"]["inputs"] = {
        "prism_subset": str(subset),
        "prism_subset_sha256": hashlib.sha256(subset.read_bytes()).hexdigest(),
    }
    return fixture, {"baseline": dict(fixture["provenance"]["source"])}, subset


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
    fixture, manifest, subset = _fixture_manifest_and_subset(tmp_path)

    validate_fixture_for_manifest(fixture, manifest, subset)

    fixture["provenance"]["source"]["generation"] = "other"
    with pytest.raises(ValueError, match="generation"):
        validate_fixture_for_manifest(fixture, manifest, subset)


def test_validate_fixture_for_manifest_rejects_forged_row_provenance(tmp_path) -> None:
    fixture, manifest, subset = _fixture_manifest_and_subset(tmp_path)
    fixture["rows"][0]["source_model_id"] = "ACH-OTHER"

    with pytest.raises(ValueError, match="source_model_id"):
        validate_fixture_for_manifest(fixture, manifest, subset)


def test_validate_fixture_for_manifest_rejects_forged_source_data_row(tmp_path) -> None:
    fixture, manifest, subset = _fixture_manifest_and_subset(tmp_path)
    fixture["rows"][0]["source_data_row"] = 999

    with pytest.raises(ValueError, match="source-data-row count"):
        validate_fixture_for_manifest(fixture, manifest, subset)


def test_validate_fixture_for_manifest_rejects_wrong_prism_subset(tmp_path) -> None:
    fixture, manifest, subset = _fixture_manifest_and_subset(tmp_path)
    wrong_subset = tmp_path / "wrong-subset.tsv"
    _write_prism_subset(wrong_subset, ["ACH-1"])

    with pytest.raises(ValueError, match="subset SHA256"):
        validate_fixture_for_manifest(fixture, manifest, wrong_subset)


def _run_extractor(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ | {"PYTHONPATH": str(repo / "src")}
    return subprocess.run(
        [sys.executable, "tools/extract_depmap_baseline_fixture.py", *args],
        cwd=repo,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def test_extractor_rejects_identical_output_paths_without_writing(tmp_path) -> None:
    repo = Path(__file__).parents[1]
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source, [{"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "2.0"}]
    )
    subset = tmp_path / "subset.tsv"
    _write_prism_subset(subset, ["ACH-1"])
    same_path = tmp_path / "same.json"

    result = _run_extractor(
        repo,
        "--prism-subset",
        str(subset),
        "--source",
        str(source),
        "--source-uri",
        "gs://bucket/depmap.csv",
        "--source-generation",
        "123",
        "--source-sha256",
        hashlib.sha256(source.read_bytes()).hexdigest(),
        "--out",
        str(same_path),
        "--report",
        str(same_path),
    )

    assert result.returncode != 0
    assert not same_path.exists()


@pytest.mark.parametrize("existing_kind", ["out", "report"])
def test_extractor_does_not_create_other_output_when_one_exists(
    tmp_path, existing_kind: str
) -> None:
    repo = Path(__file__).parents[1]
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source, [{"ModelID": "ACH-1", "GENE_A": "1.0", "GENE_B": "2.0"}]
    )
    subset = tmp_path / "subset.tsv"
    _write_prism_subset(subset, ["ACH-1"])
    out = tmp_path / "fixture.json"
    report = tmp_path / "report.json"
    existing = out if existing_kind == "out" else report
    existing.write_text(json.dumps({"already": "exists"}))

    result = _run_extractor(
        repo,
        "--prism-subset",
        str(subset),
        "--source",
        str(source),
        "--source-uri",
        "gs://bucket/depmap.csv",
        "--source-generation",
        "123",
        "--source-sha256",
        hashlib.sha256(source.read_bytes()).hexdigest(),
        "--out",
        str(out),
        "--report",
        str(report),
    )

    assert result.returncode != 0
    assert existing.read_text() == json.dumps({"already": "exists"})
    assert not (report if existing_kind == "out" else out).exists()
