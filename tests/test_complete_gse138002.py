from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

SCRIPT = Path(
    "artifacts/dataset_completion/temporal__organoiddb_odd001099_gse138002/complete_dataset.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location("complete_gse138002", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def baseline_obs() -> pd.DataFrame:
    index = pd.Index(["org24", "org42", "fetal9", "adult"], name="cell_id")
    return pd.DataFrame(
        {
            "cell_id": index,
            "sample": ["24_Day", "42_Day", "Hgw9", "Adult"],
            "source_cell_type": ["RPCs", "Rods", "Neurogenic Cells", "AC/HC_Precurs"],
            "timepoint": pd.Series(
                [24.0, 42.0, 9.0, pd.NA], index=index, dtype="Float64"
            ),
            "timepoint_unit": pd.Series(
                ["day", "day", "gestational_week", pd.NA], index=index, dtype="string"
            ),
            "is_organoid": [True, True, False, False],
            "source_age_label": ["24_Day", "42_Day", "Hgw9", "Adult"],
            "source_total_mrnas": [1000, 2000, 3000, 4000],
            "source_num_genes_expressed": [500, 600, 700, 800],
            "umap1_coord": [0.1, 0.2, 0.3, 0.4],
            "umap2_coord": [1.1, 1.2, 1.3, 1.4],
            "umap3_coord": [2.1, 2.2, 2.3, 2.4],
        },
        index=index,
    )


def test_curate_obs_preserves_order_and_materializes_contract() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 4

    curated, umap, receipt = module.curate_obs(baseline_obs())

    assert curated.index.tolist() == ["org24", "org42", "fetal9", "adult"]
    assert curated["timepoint"].tolist()[:3] == [34560.0, 60480.0, 90720.0]
    assert pd.isna(curated.loc["adult", "timepoint"])
    assert curated["trajectory_id"].tolist() == [
        "retinal_organoid",
        "retinal_organoid",
        "fetal_primary_retina",
        "adult_primary_retina",
    ]
    assert curated["is_baseline"].tolist()[:3] == [True, False, True]
    assert pd.isna(curated.loc["adult", "is_baseline"])
    assert curated.loc["org24", "cell_type_ontology_term"] == "CL:0002672"
    assert pd.isna(curated.loc["fetal9", "cell_type_ontology_term"])
    assert curated["obs_uuid"].is_unique
    assert umap.shape == (4, 4)
    assert receipt["OBS_COMPLETED"] is True
    assert receipt["scientific_modality"]["perturbation"] == "none"
    age_axis = receipt["experimental_axes"]["biological_age"]
    assert age_axis["numeric_cardinality_by_trajectory"] == {
        "adult_primary_retina": 0,
        "fetal_primary_retina": 1,
        "retinal_organoid": 2,
    }
    assert receipt["outcomes_endpoints"]["scalar_response_endpoint"] == (
        "not_applicable"
    )
    for field in module.CANONICAL_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated


def test_ordered_sha256_uses_length_delimited_source_order() -> None:
    module = load_module()
    assert module.ordered_sha256(["ab", "c"]) != module.ordered_sha256(["a", "bc"])
    assert module.ordered_sha256(["a", "b"]) != module.ordered_sha256(["b", "a"])


def test_materialize_artifact_uses_lamin_cache_for_s3(
    tmp_path: Path,
) -> None:
    module = load_module()
    cached = tmp_path / "lamin-cache" / "payload.parquet"
    cached.parent.mkdir()
    cached.write_bytes(b"accepted payload")
    artifact = SimpleNamespace(
        path="s3://lamindata-eu-central-1/dataset/payload.parquet",
        cache=lambda: cached,
    )
    destination = tmp_path / "readback" / "payload.parquet"

    result = module.materialize_artifact(artifact, destination)

    assert result == destination
    assert destination.read_bytes() == b"accepted payload"
