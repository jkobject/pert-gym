from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/dataset_completion/temporal__organoiddb_odd001155_gse196799/complete_dataset.py"
)
SPEC = importlib.util.spec_from_file_location("gse196799_completion", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_curate_obs_preserves_rows_and_marks_missingness() -> None:
    sample_id = "GSM5901237"
    index = pd.Index([f"{sample_id}:A-1", f"{sample_id}:B-1"])
    baseline = pd.DataFrame(
        {
            "cell_id": index,
            "barcode": ["A-1", "B-1"],
            "source_accession": "GSE196799",
            "sample_accession": sample_id,
            "sample_title": "day_18_3D_matrigel_culture_ascorbic_acid",
            "source_name": "hiPSCs",
            "experiment": 10,
            "timepoint": 18.0,
            "timepoint_unit": "day",
            "culture_method": "3D Matrigel",
            "ascorbic_acid_from_day_12": True,
            "biosample": "SAMN25978145",
            "sra_experiment": "SRX14194764",
            "organism": "Homo sapiens",
            "assay": "10x Genomics 3' scRNA-seq",
            "n_genes_by_counts": [2500, 3000],
            "total_counts": [5000, 6000],
            "pct_counts_mt": [2.5, 3.5],
            "source_cell_call_flag_missing": True,
        },
        index=index,
    )
    sample_spec = {
        "prefix": f"data/cleaned/{sample_id}",
        "expected": {"n_obs": 2},
        "source": {
            "day": 18,
            "culture_method": "3D Matrigel",
            "ascorbic_acid_treatment": True,
        },
    }
    geo_sample = {
        "Sample_geo_accession": [sample_id],
        "Sample_title": ["day_18_3D_matrigel_culture_ascorbic_acid"],
        "Sample_treatment_protocol_ch1": ["60 ug/mL ascorbic acid"],
    }

    curated, receipt = MODULE.curate_obs(baseline, sample_id, sample_spec, geo_sample)

    assert curated.index.equals(index)
    assert curated["obs_uuid"].is_unique
    assert set(curated["cell_type_state"]) == {"unknown"}
    assert curated["cell_type"].isna().all()
    assert set(curated["perturbation"]) == {"ascorbic acid"}
    assert set(curated["dose"]) == {60.0}
    assert set(curated["timepoint"]) == {18 * 1440.0}
    assert set(curated["source_original_total_counts"]) == {5000, 6000}
    assert receipt["status"] == "PASS"


def test_curate_var_materializes_species_correct_stable_ids() -> None:
    raw = pd.DataFrame(
        {
            "feature_id": ["ENSG00000186092", "ENSG00000284733"],
            "gene_symbol": ["OR4F5", "OR4F29"],
            "feature_type": ["Gene Expression", "Gene Expression"],
        },
        index=pd.Index(["ENSG00000186092", "ENSG00000284733"]),
    )
    manifest = {"shared_var": {"n_vars": 2}}

    curated, receipt = MODULE.curate_var(raw, manifest)

    assert curated.index.equals(raw.index)
    assert set(curated["organism"]) == {"Homo sapiens"}
    assert set(curated["feature_namespace"]) == {"Ensembl Gene"}
    assert curated["stable_feature_id"].tolist() == raw["feature_id"].tolist()
    assert receipt["status"] == "PASS"
    assert receipt["stable_ensembl_id_features"] == 2
