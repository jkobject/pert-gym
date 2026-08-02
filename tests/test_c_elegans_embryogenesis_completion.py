from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd
import pytest

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/dataset_completion/temporal__c_elegans_embryogenesis/curate_obs_var.py"
)
SOURCE_MANIFEST = SCRIPT.with_name("source_manifest.json")
ACCEPTED_MANIFEST = SCRIPT.with_name("accepted_manifest.json")
SPEC = importlib.util.spec_from_file_location("c_elegans_completion", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def raw_obs() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "batch": pd.Categorical(["b1", "b1", "b2"]),
            "cell": ["cell-1", "cell-2", "cell-3"],
            "cell.subtype": pd.Categorical(["AFD", None, None]),
            "cell.type": pd.Categorical([None, "Intestine", None]),
            "plot.cell.type": pd.Categorical([None, None, "Seam_cell"]),
            "lineage": pd.Categorical(["ABa", "E", "ABp"]),
            "time.point": pd.Categorical(["300_minutes", "300_minutes", "400_minutes"]),
            "raw.embryo.time": [0, 10, 20],
            "embryo.time": [0.0, 10.0, 20.0],
            "embryo.time.bin": pd.Categorical(["< 5", "5-15", "15-25"]),
            "raw.embryo.time.bin": pd.Categorical(["< 5", "5-15", "15-25"]),
            "pseudotime": [0.0, 0.5, 1.0],
            "timepoint": [0.0, 10.0, 20.0],
            "trajectory_id": ["c_elegans_embryogenesis"] * 3,
            "is_baseline": [True, False, False],
            "n.umi": [100, 200, 300],
            "passed_initial_QC_or_later_whitelisted": [True, True, True],
            "organism": ["Caenorhabditis elegans"] * 3,
            "modality": ["scRNA-seq"] * 3,
            "raw_extra": ["retain-a", "retain-b", "retain-c"],
        },
        index=[5, 7, 11],
    )


def raw_var() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["WBGene00000001", "WBGene00000002", "WBGene00000003"],
            "gene_short_name": ["gene-a", "gene-b", "gene-c"],
            "organism": ["Caenorhabditis elegans"] * 3,
            "raw_extra": [1, 2, 3],
        },
        index=[0, 1, 2],
    )


def test_obs_curation_is_source_exhaustive_and_non_fabricating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MODULE, "EXPECTED_SHAPE", (3, 3))
    raw = raw_obs()
    curated, dispositions = MODULE.curate_obs(raw)

    assert curated.index.equals(raw.index)
    assert curated["raw_extra"].tolist() == raw["raw_extra"].tolist()
    assert curated["cell_type"].tolist() == ["AFD", "Intestine", "Seam_cell"]
    assert curated["obs_uuid"].is_unique
    assert curated["n_counts"].tolist() == [100, 200, 300]
    assert curated["sex"].isna().all()
    assert set(curated["sex__state"]) == {"unknown"}
    assert curated["perturbation"].isna().all()
    assert set(curated["perturbation__state"]) == {"not_applicable"}
    assert set(curated["x_semantics"]) == {"raw_counts"}
    assert set(dispositions) == set(MODULE.CANONICAL_OBS_FIELDS)


def test_obs_uuid_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MODULE, "EXPECTED_SHAPE", (3, 3))
    first, _ = MODULE.curate_obs(raw_obs())
    second, _ = MODULE.curate_obs(raw_obs())
    assert first["obs_uuid"].tolist() == second["obs_uuid"].tolist()


def test_var_accepts_species_native_wormbase_ensembl_metazoa_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MODULE, "EXPECTED_SHAPE", (3, 3))
    raw = raw_var()
    curated, evidence = MODULE.curate_var(raw)

    assert curated.index.equals(raw.index)
    assert curated["raw_extra"].tolist() == [1, 2, 3]
    assert curated["stable_feature_id"].tolist() == raw["id"].tolist()
    assert curated["ensembl_gene_id"].tolist() == raw["id"].tolist()
    assert set(curated["feature_namespace"]) == {"WormBase Gene ID / Ensembl Metazoa"}
    assert evidence["status"] == "pass"
    assert evidence["biological_features_total"] == 3
    assert evidence["stable_ensembl_id_features"] == 3
    assert evidence["correct_species_features"] == 3


def test_var_fails_closed_on_non_species_stable_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MODULE, "EXPECTED_SHAPE", (3, 3))
    raw = raw_var()
    raw.loc[1, "id"] = "ENSG00000123456"
    with pytest.raises(AssertionError, match="WormBase"):
        MODULE.curate_var(raw)


def test_axis_validation_binds_source_indices_and_named_id_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MODULE, "EXPECTED_SHAPE", (3, 3))
    obs = raw_obs()
    var = raw_var()
    source = ad.AnnData(
        X=np.eye(3),
        obs=pd.DataFrame({"cell": obs["cell"].to_numpy()}, index=obs.index.astype(str)),
        var=pd.DataFrame({"id": var["id"].to_numpy()}, index=var.index.astype(str)),
    )
    accepted_x = ad.AnnData(
        X=np.eye(3),
        obs=pd.DataFrame(index=obs.index.astype(str)),
        var=pd.DataFrame(index=var.index.astype(str)),
    )

    checks = MODULE.validate_axes(obs, var, source, accepted_x)

    assert all(checks.values())


def test_source_manifest_binds_figshare_file_and_primary_publication() -> None:
    manifest = json.loads(SOURCE_MANIFEST.read_text())
    assert manifest["figshare"]["article_id"] == 22491340
    assert manifest["figshare"]["file_id"] == 39943585
    assert manifest["figshare"]["file_size_bytes"] == 365_498_906
    assert manifest["figshare"]["file_md5"] == "c3a37ca238921fcec7bd5e9faa6118f1"
    assert manifest["publication"]["doi"] == "10.1126/science.aax1971"
    assert manifest["var_policy"].startswith("The 20,222 unique WBGene identifiers")


def test_sparse_array_hashes_are_frozen_from_accepted_manifest() -> None:
    manifest = json.loads(ACCEPTED_MANIFEST.read_text())
    matrix = manifest["datasets"][0]["X"]["matrix"]
    assert MODULE.EXPECTED_MATRIX_ARRAYS == {
        "data": matrix["data_sha256"],
        "indices": matrix["indices_sha256"],
        "indptr": matrix["indptr_sha256"],
    }


def test_scientific_axis_evidence_distinguishes_time_stage_and_pseudotime() -> None:
    evidence = MODULE.scientific_axis_evidence(raw_obs())

    assert evidence["classification"] == "temporal_developmental_expression_atlas"
    biological = evidence["experimental_axes"]["biological_developmental_time"]
    assert biological["verdict"] == "multitimepoint_biological_axis"
    assert biological["distinct_levels"] == 3
    assert sum(biological["row_frequencies"].values()) == 3
    assert (
        evidence["experimental_axes"]["pseudotime"]["verdict"]
        == "computed_trajectory_coordinate_not_elapsed_time"
    )
    assert evidence["outcomes_endpoints"]["verdict"] == "none"


def test_scientific_axis_evidence_rejects_inconsistent_raw_stage_bin() -> None:
    raw = raw_obs()
    raw.loc[raw.index[1], "raw.embryo.time.bin"] = "15-25"

    with pytest.raises(AssertionError, match="raw developmental time"):
        MODULE.scientific_axis_evidence(raw)


def test_collection_replacement_is_exact_and_key_preserving() -> None:
    class Artifact:
        def __init__(self, uid: str, key: str) -> None:
            self.uid = uid
            self.key = key

    old = Artifact("old", "dataset/obs.parquet")
    other = Artifact("x", "other/obs.parquet")
    new = Artifact("new", "dataset/obs.parquet")
    after = MODULE._replacement_membership([old, other], old, new)

    assert [(item.uid, item.key) for item in after] == [
        ("x", "other/obs.parquet"),
        ("new", "dataset/obs.parquet"),
    ]
    with pytest.raises(AssertionError, match="identity drift"):
        MODULE._replacement_membership([Artifact("foreign", old.key)], old, new)


def test_live_run_refuses_darwin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(MODULE.platform, "system", lambda: "Darwin")
    with pytest.raises(RuntimeError, match="EU worker"):
        MODULE.run(tmp_path)
