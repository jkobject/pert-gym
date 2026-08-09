from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/dataset_completion/temporal__stable_chambered_cardioids/complete_dataset.py"
)
OBS_CONTRACT = Path(__file__).parents[1] / "config/obs_completed_contract_v1.json"


def load_module():
    spec = importlib.util.spec_from_file_location("gse269572_completion", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def baseline_obs() -> pd.DataFrame:
    index = pd.Index(["row-1", "row-2", "row-3"], name="cell_id")
    return pd.DataFrame(
        {
            "source_accession": "GSE269572",
            "sample_accession": ["GSM8322368", "GSM8322371", "GSM8322374"],
            "sample_title": ["SC_2D-1", "SCPD-1", "SCWOPD-1"],
            "source_file": ["2d.mtx.gz", "with.mtx.gz", "without.mtx.gz"],
            "source_cell_barcode": ["AA-1", "BB-1", "CC-1"],
            "source_name": "H9",
            "source_cell_line": "H9",
            "treatment": ["2D culture", "with PD173074", "without PD173074"],
            "condition": ["2D culture", "with PD173074", "without PD173074"],
            "timepoint": 42.5,
            "timepoint_unit": "day in vitro",
            "development_stage": "day 42.5",
            "donor_age": pd.NA,
            "donor_sex": pd.NA,
            "donor_ethnicity": pd.NA,
            "organism": "Homo sapiens",
            "assay": "10x Chromium Single Cell 3' v2 or v3",
            "tissue": "H9-derived cardioid or matched 2D culture",
            "is_control": pd.NA,
            "trajectory_id": "GSE269572:H9:day42.5",
            "source_matrix_semantics": "author-processed Cell Ranger v7 count matrix",
        },
        index=index,
    )


def test_curate_obs_preserves_order_and_encodes_three_arm_design() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 3

    curated, receipt = module.curate_obs(baseline_obs())

    assert curated.index.tolist() == ["row-1", "row-2", "row-3"]
    assert curated["cell_id"].tolist() == [
        "GSM8322368:AA-1",
        "GSM8322371:BB-1",
        "GSM8322374:CC-1",
    ]
    assert curated["perturbation"].tolist() == ["none", "PD173074", "none"]
    assert curated["perturbation_type"].tolist() == ["none", "drug", "none"]
    assert curated["is_control"].isna().all()
    assert set(curated["is_control_state"]) == {"missing"}
    assert curated["timepoint"].isna().all()
    assert set(curated["timepoint_state"]) == {"not_applicable"}
    assert set(curated["source_original_timepoint"]) == {42.5}
    assert set(curated["source_original_timepoint_unit"]) == {"day in vitro"}
    assert "development_stage" not in curated
    assert curated["trajectory_id"].isna().all()
    assert set(curated["trajectory_id_state"]) == {"not_applicable"}
    assert curated["obs_uuid"].is_unique
    assert receipt["OBS_COMPLETED"] is True
    assert (
        receipt["scientific_modality"]
        == "single-cell expression with one drug/culture design axis"
    )
    assert receipt["temporal_verdict"]["status"] == "non_temporal_single_snapshot"
    assert receipt["experimental_axes"]["condition"]["cardinality"] == 3
    assert receipt["outcomes_endpoints"]["expression_matrix"] == "raw_counts"


def test_curate_obs_materializes_the_exact_42_field_contract() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 3
    contract = json.loads(OBS_CONTRACT.read_text())

    curated, receipt = module.curate_obs(baseline_obs())

    expected = contract["canonical_obs_columns"]
    assert list(module.OBS_FIELDS) == expected
    assert receipt["canonical_field_count"] == contract["canonical_obs_column_count"]
    assert all(
        column in curated
        for field in expected
        for column in (field, f"{field}_state", f"{field}_source")
    )
    allowed_states = set(contract["field_states"])
    assert all(
        set(curated[f"{field}_state"].astype(str)) <= allowed_states
        for field in expected
    )


def test_curate_var_adds_species_correct_stable_contract() -> None:
    module = load_module()
    module.EXPECTED_N_VARS = 2
    raw = pd.DataFrame(
        {
            "feature_id": ["ENSG00000186092", "ENSG00000284733"],
            "gene_symbol": ["OR4F5", "OR4F29"],
            "feature_type": "Gene Expression",
            "feature_namespace": "Ensembl gene ID",
            "organism": "Homo sapiens",
            "genome_build": "GRCh38",
        },
        index=pd.Index(["ENSG00000186092", "ENSG00000284733"], name="feature_id"),
    )

    curated, receipt = module.curate_var(raw)

    assert curated.index.equals(raw.index)
    assert curated["ensembl_id"].tolist() == raw["feature_id"].tolist()
    assert curated["author_gene_id"].tolist() == raw["feature_id"].tolist()
    assert curated["author_gene_symbol"].tolist() == raw["gene_symbol"].tolist()
    assert receipt["VAR_ENSEMBL_SPECIES_COMPLETED"] is True
    assert receipt["correct_species_features"] == 2


def test_ordered_sha256_is_order_and_boundary_sensitive() -> None:
    module = load_module()
    assert module.ordered_sha256(["ab", "c"]) != module.ordered_sha256(["a", "bc"])
    assert module.ordered_sha256(["a", "b"]) != module.ordered_sha256(["b", "a"])


def test_curated_revision_requires_the_exact_materialized_frame() -> None:
    module = load_module()
    prefix = "t_363b9754: source-exhaustive GSE269572 OBS"

    assert module.is_exact_curated_revision(
        f"{prefix}; frame_sha256=new; helper_sha256=helper", prefix, "new"
    )
    assert not module.is_exact_curated_revision(
        f"{prefix}; frame_sha256=stale; helper_sha256=helper", prefix, "new"
    )
