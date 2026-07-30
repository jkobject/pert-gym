from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "artifacts/dataset_completion/temporal__zebrafish_retina_regeneration"
SCRIPT = EVIDENCE / "complete_dataset.py"
SPEC = importlib.util.spec_from_file_location("scp1973_completion", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def raw_obs() -> pd.DataFrame:
    layout = [
        ("s1", "uninjured", 2_243),
        ("s2", "44 hpl", 1_723),
        ("s3", "4 dpl", 4_899),
        ("s5", "6 dpl", 2_825),
    ]
    rows: list[dict[str, object]] = []
    index: list[str] = []
    for token, label, count in layout:
        for number in range(count):
            cell = f"{token}_cell_{number:05d}"
            index.append(cell)
            rows.append(
                {
                    "biosample_id": f"{token}_source_sample",
                    "cell_id": cell,
                    "cell_type": "Muller glia",
                    "raw_time_label": label,
                }
            )
    return pd.DataFrame(rows, index=index)


def test_source_manifest_binds_publication_geo_and_accepted_component() -> None:
    manifest = json.loads((EVIDENCE / "source_manifest.json").read_text())

    assert manifest["publication"]["doi"] == "10.7554/eLife.86507"
    assert manifest["source_accessions"] == ["SCP1973", "GSE226373", "PRJNA940076"]
    assert manifest["geo"]["feature_barcode_h5"]["sha256"] == MODULE.GEO_H5_SHA256
    assert manifest["scientific_interpretation"]["experimental_axes"][
        "elapsed_time_after_light_lesion_minutes"
    ] == [0, 2640, 5760, 8640]
    assert manifest["accepted_component"]["registered_uids"] == MODULE.ACCEPTED_UIDS


def test_obs_curation_preserves_rows_and_materializes_true_time_axis() -> None:
    raw = raw_obs()
    qc = pd.DataFrame(
        {
            "n_counts": 100.0,
            "n_genes": 50,
            "pct_mito": 1.0,
            "pct_ribo": 2.0,
        },
        index=raw.index,
    )

    result = MODULE.curate_obs(raw, qc)

    assert result.index.equals(raw.index)
    assert len(result) == MODULE.EXPECTED["n_obs"]
    assert set(MODULE.CANONICAL_FIELDS).issubset(result.columns)
    assert result["timepoint"].value_counts().sort_index().to_dict() == {
        0: 2243,
        2640: 1723,
        5760: 4899,
        8640: 2825,
    }
    assert result["is_baseline"].sum() == 2243
    assert result["is_control"].sum() == 2243
    assert result["batch"].value_counts().to_dict() == {
        "collection_day_1": 8865,
        "collection_day_2": 2825,
    }
    assert result["obs_uuid"].is_unique
    assert result["donor_id_state"].eq("missing").all()
    assert result["pseudotime_state"].eq("missing").all()
    assert result["dose_state"].eq("not_applicable").all()


def test_var_curation_accepts_species_exact_ensembl_and_reporters_only() -> None:
    stable_count = MODULE.EXPECTED["n_vars"] - 2
    ids = [f"ENSDARG{number:011d}" for number in range(stable_count)]
    raw_index = pd.Index(ids + ["EGFP", "mCherry"], name="feature")
    raw = pd.DataFrame(index=raw_index)
    geo = pd.DataFrame(
        {
            "gene_symbol": [f"gene_{number}" for number in range(stable_count)],
            "source_genome": "GRCz11_GFP_mCherry_e95",
        },
        index=pd.Index(ids, name="ensembl_gene_id"),
    )

    result = MODULE.curate_var(raw, geo)

    assert result.index.equals(raw_index)
    statuses = result["stable_feature_id_mapping_status"]
    assert statuses.str.startswith("source_exact").sum() == stable_count
    assert statuses.eq("not_applicable_synthetic_reporter").sum() == 2
    assert result["organism"].eq("Danio rerio").all()
    assert result["ensembl_gene_id"].notna().sum() == stable_count


def test_var_curation_fails_closed_on_unmapped_biological_feature() -> None:
    raw = pd.DataFrame(
        index=pd.Index(
            [f"ENSDARG{number:011d}" for number in range(MODULE.EXPECTED["n_vars"] - 1)]
            + ["unmapped_gene"],
            name="feature",
        )
    )
    geo = pd.DataFrame(
        {
            "gene_symbol": [f"gene_{number}" for number in range(len(raw) - 1)],
            "source_genome": "GRCz11_GFP_mCherry_e95",
        },
        index=pd.Index(raw.index[:-1], name="ensembl_gene_id"),
    )

    with pytest.raises(AssertionError, match="unresolved Danio rerio VAR mappings"):
        MODULE.curate_var(raw, geo)


def test_recovery_rewrites_physical_h5ad_without_changing_logical_X(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.h5ad"
    recovered = tmp_path / "recovered.h5ad"
    matrix = MODULE.ad.AnnData(
        X=sparse.csr_matrix([[1, 0, 2], [0, 3, 0]], dtype="float32"),
        obs=pd.DataFrame(index=["c1", "c2"]),
        var=pd.DataFrame(index=["g1", "g2", "g3"]),
    )
    matrix.write_h5ad(source)

    receipt = MODULE.write_recovered_x(source, recovered)

    assert receipt["source"] == receipt["recovered"]
    assert receipt["source"]["nnz"] == 3
    assert MODULE.sha256_file(source) != MODULE.sha256_file(recovered)
