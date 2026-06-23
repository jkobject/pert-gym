import pandas as pd

from tools.ingest_prism_large_h5ad_chunks import standardize_prism_obs_df


def test_standardize_prism_obs_df_preserves_cellline_as_cell_line() -> None:
    obs = pd.DataFrame(
        {
            "cellline": ["A549", "HCC827"],
            "gene": ["KRAS", "EGFR"],
        },
        index=pd.Index(["cell_a", "cell_b"]),
    )

    standardized = standardize_prism_obs_df(obs, "GSE269596")

    assert standardized["cell_line"].tolist() == ["A549", "HCC827"]
    assert "cellline" not in standardized.columns
    assert standardized["dataset"].tolist() == ["GSE269596", "GSE269596"]
