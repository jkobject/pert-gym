from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = (
    ROOT
    / "artifacts/dataset_completion/temporal__organoiddb_odd001154_gse194214/curate_obs_var.py"
)
SPEC = importlib.util.spec_from_file_location("odd001154_curate_obs_var", HELPER)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.fixture(autouse=True)
def use_small_obs_denominator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MODULE, "EXPECTED_SHAPE", (2, 33_694))


def baseline() -> pd.DataFrame:
    index = pd.Index(["GSM5830919:A-1", "GSM5830922:A-1"], name="cell_id")
    return pd.DataFrame(
        {
            "source_accession": ["GSE194214", "GSE194214"],
            "organoiddb_id": ["ODD001154", "ODD001154"],
            "sample_accession": ["GSM5830919", "GSM5830922"],
            "sample_title": ["Somitoid Day 1", "Somitoid Day 5"],
            "source_file": ["d1_matrix.mtx.gz", "d5_matrix.mtx.gz"],
            "source_cell_barcode": ["A-1", "A-1"],
            "source_name": ["NCRM1 human IPSC", "NCRM1 human IPSC"],
            "source_cell_type": ["Human iPSC in vitro cultures"] * 2,
            "genotype": ["wild type", "wild type"],
            "development_stage": ["Day 1", "Day 5"],
            "timepoint": [1, 5],
            "timepoint_unit": ["day", "day"],
            "organism": ["Homo sapiens", "Homo sapiens"],
            "assay": ["10x Genomics scRNA-seq"] * 2,
            "tissue": ["paraxial mesoderm organoid (somitoid)"] * 2,
            "is_control": [True, False],
            "trajectory_id": ["GSE194214:somitoid"] * 2,
            "source_matrix_semantics": ["raw UMI count matrix"] * 2,
        },
        index=index,
    )


def qc() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "n_counts": [400, 5000],
            "n_genes": [150, 2000],
            "pct_mito": [25.0, 3.0],
            "pct_ribo": [2.0, 5.0],
            "source_qc_complexity": [0.7, 0.9],
            "source_initial_qc_failure": [True, False],
        },
        index=baseline().index,
    )


def test_curate_obs_preserves_identity_and_corrects_day1_control() -> None:
    source = baseline()
    curated, receipt = MODULE.curate_obs(source, qc())

    assert curated.index.equals(source.index)
    assert curated["source_original_is_control"].tolist() == [True, False]
    assert curated["is_control"].tolist() == [False, False]
    assert curated["is_control_state"].tolist() == ["known", "known"]
    assert curated["sample"].tolist() == ["GSM5830919", "GSM5830922"]
    assert curated["cell_id"].tolist() == source.index.tolist()
    assert curated["timepoint"].tolist() == [1.0, 5.0]
    assert receipt["source_day1_control_rows_corrected"] == 1


def test_curate_obs_distinguishes_known_qc_failure_from_unresolved_quality() -> None:
    curated, receipt = MODULE.curate_obs(baseline(), qc())

    assert curated["is_low_quality"].tolist()[0] is True
    assert pd.isna(curated["is_low_quality"].iloc[1])
    assert curated["is_low_quality_state"].tolist() == ["known", "unknown"]
    assert receipt["initial_qc_failure_rows"] == 1
    assert receipt["unresolved_low_quality_rows"] == 1


def test_all_canonical_fields_have_state_and_source_columns() -> None:
    curated, _ = MODULE.curate_obs(baseline(), qc())

    for field in MODULE.CANONICAL_OBS_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated
    dispositions = MODULE.field_dispositions(curated)
    assert dispositions["cell_type"]["unknown_rows"] == 2
    assert dispositions["perturbation"]["not_applicable_rows"] == 2
