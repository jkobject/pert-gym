from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

SCRIPT = Path(
    "artifacts/dataset_completion/"
    "temporal_an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation/"
    "curate_obs_var.py"
)
SPEC = importlib.util.spec_from_file_location("alternative_cell_cycle_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def baseline(samples: list[str], *, experiment: str) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "orig.ident": samples,
            "donor_id": [f"pooled_{sample}" for sample in samples],
            "cell_type": ["basal cell"] * len(samples),
            "disease": ["normal"] * len(samples),
            "tissue_type": ["primary cell culture"] * len(samples),
            "organism": ["Mus musculus"] * len(samples),
            "nCount_RNA": [1000 + i for i in range(len(samples))],
            "nFeature_RNA": [500 + i for i in range(len(samples))],
            "percent.mt": [2.0 + i for i in range(len(samples))],
        },
        index=[f"barcode-{i}" for i in range(len(samples))],
    )
    if experiment.startswith("e2f7"):
        frame["genotype"] = [
            "E2f7_wildtype" if "wildtype" in sample else "E2f7_homozygous_knockout"
            for sample in samples
        ]
    if experiment.endswith("multiciliated_subset"):
        frame["pseudotime"] = [0.1 + i for i in range(len(samples))]
    return frame


def member(experiment: str, rows: int) -> dict:
    return {
        "dataset_id": f"fixture-{experiment}",
        "experiment": experiment,
        "n_obs": rows,
    }


def test_timecourse_curation_preserves_identity_and_maps_canonical_minutes() -> None:
    source = baseline(
        [
            "air_liquid_interface_day1",
            "air_liquid_interface_day3",
            "air_liquid_interface_day9",
            "air_liquid_interface_day36",
        ],
        experiment="timecourse_multiciliated_subset",
    )

    curated, receipt = MODULE.curate_obs(
        source, member("timecourse_multiciliated_subset", len(source))
    )

    assert curated.index.equals(source.index)
    assert curated["original_obs_index"].tolist() == source.index.tolist()
    assert curated["timepoint"].tolist() == [1440, 4320, 12960, 51840]
    assert curated["is_baseline"].tolist() == [True, False, False, False]
    assert curated["perturbation_state"].eq("not_applicable").all()
    assert curated["age"].eq("adult").all()
    assert receipt["rows"] == 4
    assert receipt["residual_unknowns"] == {"sex": 4, "pct_ribo": 4}
    assert receipt["experimental_axes"]["biological_time"] == {
        "verdict": "multitimepoint_biological_axis",
        "source_levels_minutes": [1440, 4320, 12960, 51840],
        "row_frequencies": {"12960": 1, "1440": 1, "4320": 1, "51840": 1},
        "canonical_timepoint_exposed": True,
        "level": "sample projected to cell",
    }
    assert curated["obs_uuid"].is_unique
    for field in MODULE.CANONICAL_OBS_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated
        assert curated[f"{field}_source"].astype("string").str.strip().ne("").all()


def test_ribociclib_curation_distinguishes_vehicle_unknown_dose_from_10_um() -> None:
    source = baseline(["DMSOA", "RibociclibA"], experiment="ribociclib_full")
    source["pseudotime"] = [pd.NA, 0.4]

    curated, receipt = MODULE.curate_obs(source, member("ribociclib_full", len(source)))

    assert curated["is_control"].tolist() == [True, False]
    assert curated["molecule_sequence"].tolist() == [
        MODULE.DMSO_SMILES,
        MODULE.RIBOCICLIB_SMILES,
    ]
    assert pd.isna(curated.loc["barcode-0", "dose"])
    assert curated.loc["barcode-0", "dose_state"] == "missing"
    assert curated.loc["barcode-1", "dose"] == 10.0
    assert curated.loc["barcode-1", "dose_unit"] == "micromolar"
    assert curated["timepoint"].isna().all()
    assert curated["timepoint_state"].eq("not_applicable").all()
    assert curated["source_collection_timepoint_minutes"].tolist() == [4320, 4320]
    assert receipt["experimental_axes"]["biological_time"] == {
        "verdict": "single_timepoint_non_temporal",
        "source_levels_minutes": [4320],
        "row_frequencies": {"4320": 2},
        "canonical_timepoint_exposed": False,
        "level": "sample projected to cell",
    }
    assert receipt["residual_unknowns"] == {
        "sex": 2,
        "dose": 1,
        "dose_unit": 1,
        "pct_ribo": 2,
    }


def test_e2f7_curation_maps_genotype_controls_without_per_cell_guide_claim() -> None:
    source = baseline(
        ["E2f7_wildtype_A", "E2f7_homozygous_knockout_A"],
        experiment="e2f7_multiciliated_subset",
    )

    curated, receipt = MODULE.curate_obs(
        source, member("e2f7_multiciliated_subset", len(source))
    )

    assert curated["is_control"].tolist() == [True, False]
    assert curated["perturbation"].tolist() == [
        "E2f7 wild-type control",
        "E2f7 homozygous knockout",
    ]
    assert curated["guide_sequence_state"].eq("not_applicable").all()
    assert curated["timepoint"].isna().all()
    assert curated["timepoint_state"].eq("not_applicable").all()
    assert curated["source_collection_timepoint_minutes"].tolist() == [10080, 10080]
    assert receipt["experimental_axes"]["biological_time"]["verdict"] == (
        "single_timepoint_non_temporal"
    )


def test_var_validation_requires_every_row_to_be_mouse_ensembl() -> None:
    valid = pd.DataFrame(
        {"feature_reference": ["NCBITaxon:10090", "NCBITaxon:10090"]},
        index=["ENSMUSG00000000001", "ENSMUSG00000000002"],
    )

    receipt = MODULE.verify_var(valid, 2)

    assert receipt["status"] == "PASS"
    assert receipt["stable_ensembl_id_features"] == 2
    assert receipt["correct_species_features"] == 2

    invalid = valid.copy()
    invalid.loc["ENSMUSG00000000002", "feature_reference"] = "NCBITaxon:9606"
    with pytest.raises(AssertionError, match="VAR Ensembl/species"):
        MODULE.verify_var(invalid, 2)
