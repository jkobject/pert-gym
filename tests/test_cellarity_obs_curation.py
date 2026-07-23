from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

SCRIPT = (
    Path(__file__).parents[1] / "artifacts/schema_audit/real_dataset_curation_20260723/"
    "cellarity_public_collection/t_9c09e453/curate_obs.py"
)
SPEC = importlib.util.spec_from_file_location("cellarity_obs_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)
EVIDENCE_ROOT = SCRIPT.parent


def member(kind: str, rows: int) -> dict:
    spec = next(item for item in curation.MEMBERS if item["kind"] == kind)
    return {**spec, "n_obs": rows}


def base_obs(index: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {"obs_uuid": [f"uuid-{value}" for value in index]},
        index=pd.Index(index),
    )


def test_pre_demultiplexing_raw_rows_do_not_claim_treatment() -> None:
    index = ["c1", "c2"]
    source = pd.DataFrame(
        {
            "LIBRARY_ID": ["L1", "L2"],
            "n_counts": [100.0, 200.0],
            "percent_mito": [0.1, 0.2],
            "sample_name": ["Day 1_Lib 1", "Day 2_Lib 1"],
        },
        index=index,
    )

    curated = curation.curate_obs(
        base_obs(index), source, member("gse305979_raw", len(index))
    )

    assert curated["perturbation"].isna().all()
    assert curated["perturbation_state"].eq("unknown").all()
    assert curated["is_control"].isna().all()
    assert curated["timepoint"].tolist() == [1440.0, 2880.0]
    assert curated["is_baseline"].eq(False).all()


def test_pseudobulk_distinguishes_vehicle_from_compound_dose() -> None:
    index = ["s1", "s2"]
    source = pd.DataFrame(
        {
            "bio_sample_id": ["sample-1", "sample-2"],
            "cell_id": ["A375", "A549"],
            "compound_name": ["DMSO", "Drug A"],
            "dose_uM": [pd.NA, 1.0],
            "library_id": ["L1", "L2"],
            "replicate": [1, 1],
            "timepoint_hr": [24.0, 24.0],
        },
        index=index,
    )

    curated = curation.curate_obs(
        base_obs(index), source, member("gse306429_pseudobulk", len(index))
    )

    assert curated["is_pseudobulk"].eq(True).all()
    assert curated["is_control"].tolist() == [True, False]
    assert curated["dose_state"].tolist() == ["not_applicable", "known"]
    assert pd.isna(curated.loc["s1", "dose"])
    assert curated.loc["s2", "dose"] == 1.0
    assert curated["cell_line"].tolist() == ["A375", "A549"]


def test_every_canonical_field_has_value_state_and_source_columns() -> None:
    index = ["c1"]
    source = pd.DataFrame(
        {
            "CELL_ID": ["CCL-171"],
            "CONCENTRATION_UM": [0.0],
            "LIBRARY_ID": ["L1"],
            "TIMEPOINT_HOURS": [0.0],
        },
        index=index,
    )
    curated = curation.curate_obs(
        base_obs(index), source, member("gse305979_day0_raw", 1)
    )

    for field in curation.CANONICAL_OBS_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated
    assert curated.loc["c1", "perturbation"] == "No treatment"
    assert bool(curated.loc["c1", "is_control"])
    assert bool(curated.loc["c1", "is_baseline"])


def test_source_join_requires_exact_unique_index_order() -> None:
    obs = base_obs(["a", "b"])
    source = pd.DataFrame({"LIBRARY_ID": ["L1", "L2"]}, index=["b", "a"])

    with pytest.raises(AssertionError, match="exact index join failed"):
        curation.verify_source_join(obs, source, member("gse305979_day0_raw", len(obs)))


def test_source_join_normalizes_equivalent_string_index_dtypes() -> None:
    obs = base_obs(["a", "b"])
    obs.index = pd.Index(pd.Series(["a", "b"], dtype="string"))
    source = pd.DataFrame({"LIBRARY_ID": ["L1", "L2"]}, index=["a", "b"])

    result = curation.verify_source_join(
        obs, source, member("gse305979_day0_raw", len(obs))
    )

    assert result["rows"] == 2


def test_source_join_proves_non_unique_pseudobulk_rows() -> None:
    index = ["0", "0"]
    source = pd.DataFrame(
        {
            "bio_sample_id": ["sample-1", "sample-2"],
            "cell_id": ["A375", "A549"],
            "compound_name": ["DMSO", "Drug A"],
            "dose_uM": [pd.NA, 1.0],
            "library_id": ["L1", "L2"],
            "replicate": [1, 1],
            "timepoint_hr": [24.0, 24.0],
        },
        index=index,
    )
    obs = source.copy()
    obs["cell_id"] = ["legacy-1", "legacy-2"]
    obs["cell_line"] = ["normalized-1", "normalized-2"]
    obs["original_obs_index"] = index
    obs["obs_uuid"] = ["uuid-1", "uuid-2"]

    result = curation.verify_source_join(
        obs, source, member("gse306429_pseudobulk", len(obs))
    )

    assert result["index_unique"] is False
    assert len(result["column_equalities"]) == len(source.columns)
    assert result["column_equalities"]["cell_id->cell_id"] is False
    assert "bio_sample_id" in result["row_identity_columns"]


def test_source_join_rejects_non_unique_rows_without_stable_identity() -> None:
    obs = base_obs(["0", "0"])
    source = pd.DataFrame({"LIBRARY_ID": ["L1", "L2"]}, index=["0", "0"])

    with pytest.raises(AssertionError, match="lacks exact row identity proof"):
        curation.verify_source_join(obs, source, member("gse305979_day0_raw", len(obs)))


def test_source_join_compares_author_alias_before_normalized_column() -> None:
    obs = base_obs(["a", "b"])
    obs["cell_type"] = ["ontology A", "ontology B"]
    obs["cell_type_from_author"] = ["author A", "author B"]
    source = pd.DataFrame({"cell_type": ["author A", "author B"]}, index=obs.index)

    result = curation.verify_source_join(
        obs, source, member("gse305370_citeseq", len(obs))
    )

    assert result["column_equalities"] == {"cell_type->cell_type_from_author": True}


def test_source_join_compares_cell_id_to_cell_line_alias() -> None:
    obs = base_obs(["a", "b"])
    obs["cell_line"] = ["A375", "A549"]
    source = pd.DataFrame({"cell_id": ["A375", "A549"]}, index=obs.index)

    result = curation.verify_source_join(
        obs, source, member("gse306429_vscores", len(obs))
    )

    assert result["column_equalities"] == {"cell_id->cell_line": True}


def test_frozen_bindings_and_source_manifest_cover_exact_live_identities() -> None:
    manifest = json.loads((EVIDENCE_ROOT / "source_manifest.json").read_text())
    inspection = json.loads((EVIDENCE_ROOT / "source_inspection.json").read_text())
    manifest_by_name = {
        item["filename"]: item for item in manifest["target_source_objects"]
    }

    assert len(manifest_by_name) == len(curation.MEMBERS) == 10
    assert sum(item["n_obs"] for item in manifest_by_name.values()) == 2_212_441
    for spec, inspected in zip(curation.MEMBERS, inspection["members"], strict=True):
        assert (
            manifest_by_name[spec["filename"]]["sha256"]
            == inspected["source"]["sha256"]
        )
        assert spec["x_hash"] == inspected["accepted_artifacts"]["x"]["hash"]
        assert spec["var_uid"] == inspected["accepted_artifacts"]["var"]["uid"]
    assert len(curation.load_frozen_inputs()["inputs"]) == 2


def test_gse305370_rna_uses_latest_obs_identity() -> None:
    spec = next(item for item in curation.MEMBERS if item["kind"] == "gse305370_rna")

    assert spec["before_obs_uid"] == "RJbcZEfscysBCeMj0001"
