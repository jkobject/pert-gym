import csv
import hashlib
import importlib.util
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


def _write_expression_csv(path: Path, rows: list[dict[str, str]]) -> None:
    defaults = {
        "SequencingID": "CDS-default",
        "ModelConditionID": "MC-default",
        "IsDefaultEntryForMC": "Yes",
        "IsDefaultEntryForModel": "Yes",
    }
    rows = [{**defaults, **row} for row in rows]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(dict.fromkeys(k for row in rows for k in row))
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_prism_subset(path: Path, model_ids: list[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["depmap_id"], delimiter="\t")
        writer.writeheader()
        writer.writerows({"depmap_id": model_id} for model_id in model_ids)


def _extract(source: Path, requested: set[str]) -> dict:
    return extract_exact_modelid_baseline(
        source_path=source,
        requested_model_ids=requested,
        source_uri="gs://bucket/depmap.csv",
        source_generation="123",
        expected_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        extraction_command="python tools/extract_depmap_baseline_fixture.py ...",
        commit="deadbeef",
    )


def _fixture_manifest_and_subset(tmp_path: Path) -> tuple[dict, dict, Path]:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source,
        [
            {
                "ModelID": "ACH-1",
                "SequencingID": "CDS-1",
                "GENE_A": "1.0",
                "GENE_B": "2.0",
            },
            {
                "ModelID": "ACH-2",
                "SequencingID": "CDS-2",
                "GENE_A": "3.0",
                "GENE_B": "4.0",
            },
        ],
    )
    subset = tmp_path / "subset.tsv"
    _write_prism_subset(subset, ["ACH-1", "ACH-2"])
    fixture = _extract(source, {"ACH-1", "ACH-2"})
    fixture["provenance"]["inputs"] = {
        "prism_subset": str(subset),
        "prism_subset_sha256": hashlib.sha256(subset.read_bytes()).hexdigest(),
    }
    return fixture, {"baseline": dict(fixture["provenance"]["source"])}, subset


def test_extract_records_native_default_provenance(tmp_path: Path) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source,
        [
            {
                "ModelID": "ACH-1",
                "SequencingID": "CDS-nondefault",
                "IsDefaultEntryForMC": "Yes",
                "IsDefaultEntryForModel": "No",
                "GENE_A": "1.0",
            },
            {
                "ModelID": "ACH-1",
                "SequencingID": "CDS-model-default",
                "ModelConditionID": "MC-2",
                "IsDefaultEntryForMC": "No",
                "GENE_A": "2.0",
            },
            {"ModelID": "ACH-2", "SequencingID": "CDS-2", "GENE_A": "3.0"},
        ],
    )
    fixture = _extract(source, {"ACH-1", "ACH-2"})
    assert fixture["schema_version"] == "depmap_default_model_entry_baseline.v2"
    assert [row["depmap_id"] for row in fixture["rows"]] == ["ACH-1", "ACH-2"]
    assert fixture["rows"][0]["sequencing_id"] == "CDS-model-default"
    assert fixture["rows"][0]["is_default_entry_for_mc"] == "No"
    assert (
        fixture["provenance"]["extraction"]["selection_contract"]
        == "exactly_one_IsDefaultEntryForModel_Yes_per_requested_ModelID"
    )


@pytest.mark.parametrize(
    "rows",
    [
        [{"ModelID": "ACH-1", "IsDefaultEntryForModel": "No", "GENE_A": "1.0"}],
        [
            {"ModelID": "ACH-1", "GENE_A": "1.0"},
            {"ModelID": "ACH-1", "SequencingID": "CDS-2", "GENE_A": "2.0"},
        ],
    ],
)
def test_extract_rejects_zero_or_multiple_native_defaults(
    tmp_path: Path, rows: list[dict[str, str]]
) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(source, rows)
    with pytest.raises(ValueError, match="exactly one IsDefaultEntryForModel=Yes"):
        _extract(source, {"ACH-1"})


def test_extract_rejects_mc_only_fallback_and_missing_native_columns(
    tmp_path: Path,
) -> None:
    source = tmp_path / "expression.csv"
    _write_expression_csv(
        source,
        [
            {
                "ModelID": "ACH-1",
                "IsDefaultEntryForMC": "Yes",
                "IsDefaultEntryForModel": "No",
                "GENE_A": "1.0",
            }
        ],
    )
    with pytest.raises(ValueError, match="exactly one IsDefaultEntryForModel=Yes"):
        _extract(source, {"ACH-1"})
    source.write_text("ModelID,GENE_A\nACH-1,1.0\n")
    with pytest.raises(ValueError, match="native default-entry columns"):
        _extract(source, {"ACH-1"})


@pytest.mark.parametrize(
    "mutation, message",
    [
        (
            lambda fixture: fixture["rows"][0].__setitem__(
                "source_model_id", "ACH-other"
            ),
            "source_model_id",
        ),
        (
            lambda fixture: fixture["rows"][0].__setitem__("source_data_row", 0),
            "positive source-data-row",
        ),
        (
            lambda fixture: fixture["rows"][0].__setitem__("vector_sha256", "forged"),
            "vector checksum",
        ),
        (lambda fixture: fixture["rows"][0].__setitem__("expression", [1.0]), "shape"),
        (
            lambda fixture: fixture["rows"][0].__setitem__(
                "is_default_entry_for_model", "No"
            ),
            "non-default",
        ),
    ],
)
def test_validate_rejects_forged_native_provenance(
    tmp_path: Path, mutation, message: str
) -> None:
    fixture, manifest, subset = _fixture_manifest_and_subset(tmp_path)
    mutation(fixture)
    with pytest.raises(ValueError, match=message):
        validate_fixture_for_manifest(fixture, manifest, subset)


def test_validate_binds_subset_checksum_requested_ids_and_exact_rows(
    tmp_path: Path,
) -> None:
    fixture, manifest, subset = _fixture_manifest_and_subset(tmp_path)
    validate_fixture_for_manifest(fixture, manifest, subset)
    wrong_subset = tmp_path / "wrong.tsv"
    _write_prism_subset(wrong_subset, ["ACH-1"])
    with pytest.raises(ValueError, match="subset SHA256"):
        validate_fixture_for_manifest(fixture, manifest, wrong_subset)
    fixture["provenance"]["inputs"]["prism_subset_sha256"] = hashlib.sha256(
        wrong_subset.read_bytes()
    ).hexdigest()
    with pytest.raises(ValueError, match="requested ModelID set"):
        validate_fixture_for_manifest(fixture, manifest, wrong_subset)


def _run_extractor(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "tools/extract_depmap_baseline_fixture.py", *args],
        cwd=repo,
        env=os.environ | {"PYTHONPATH": str(repo / "src")},
        check=False,
        capture_output=True,
        text=True,
    )


def _extractor_args(source: Path, subset: Path, out: Path, report: Path) -> list[str]:
    return [
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
    ]


def test_extractor_rejects_output_alias_and_one_sided_existing_output(
    tmp_path: Path,
) -> None:
    repo = Path(__file__).parents[1]
    source, subset = tmp_path / "expression.csv", tmp_path / "subset.tsv"
    _write_expression_csv(source, [{"ModelID": "ACH-1", "GENE_A": "1.0"}])
    _write_prism_subset(subset, ["ACH-1"])
    same = tmp_path / "same.json"
    assert (
        _run_extractor(repo, *_extractor_args(source, subset, same, same)).returncode
        != 0
    )
    assert not same.exists()
    out, report = tmp_path / "fixture.json", tmp_path / "report.json"
    out.write_text("already exists")
    assert (
        _run_extractor(repo, *_extractor_args(source, subset, out, report)).returncode
        != 0
    )
    assert out.read_text() == "already exists"
    assert not report.exists()


def test_atomic_output_reservation_rolls_back_partial_write(
    tmp_path: Path, monkeypatch
) -> None:
    spec = importlib.util.spec_from_file_location(
        "extractor",
        Path(__file__).parents[1] / "tools/extract_depmap_baseline_fixture.py",
    )
    assert spec and spec.loader
    extractor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(extractor)
    out, report = tmp_path / "fixture.json", tmp_path / "report.json"
    original_open = Path.open

    def fail_report_write(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path == report:

            def fail_once(payload):
                raise OSError("simulated report write failure")

            handle.write = fail_once
        return handle

    monkeypatch.setattr(Path, "open", fail_report_write)
    with pytest.raises(OSError, match="simulated"):
        extractor._write_immutable_outputs(out, "fixture", report, "report")
    assert not out.exists()
    assert not report.exists()
