import zipfile
from pathlib import Path

import pandas as pd

from tools.sanger_score_essentiality import (
    BASELINE_JOIN,
    SANGER_SCORE_LAMIN_PREFIX,
    load_sanger_score_matrix,
    score_matrix_to_obs_var,
    write_smoke_manifest,
    write_table,
)


def make_score2_zip(path: Path) -> Path:
    raw = pd.DataFrame(
        [
            ["meta", "meta", "meta", "Model A", "Model B"],
            ["meta", "meta", "meta", "SIDM00001", "SIDM00002"],
            ["meta", "meta", "meta", "CRISPRcleanR", "CRISPRcleanR"],
            ["meta", "meta", "meta", "TRUE", "FALSE"],
            ["meta", "meta", "meta", "unused", "unused"],
            ["GENEID1", "TP53", "ENSG00000141510", "-1.25", "-0.80"],
            ["GENEID2", "A1BG", "ENSG00000121410", "0.05", "0.02"],
        ]
    )
    tsv = raw.to_csv(sep="\t", index=False, header=False)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("ProjectScore/fold_change_values.tsv", tsv)
    return path


def test_score2_zip_converts_to_obs_var_without_x(tmp_path: Path):
    source = make_score2_zip(tmp_path / "score.zip")

    matrix = load_sanger_score_matrix(source)
    obs, var = score_matrix_to_obs_var(matrix, max_models=1, max_genes=2)

    assert len(obs) == 2
    assert len(var) == 2
    assert "X" not in obs.columns
    assert "X" not in var.columns
    assert set(obs["perturbation_type"]) == {"CRISPRko"}
    assert set(obs["readout_modality"]) == {"Project_Score_CRISPR_screen"}
    assert set(obs["response_metric"]) == {"fold_change"}
    assert (
        obs.loc[obs["perturbation_gene"] == "TP53", "response_value"].iloc[0] == -1.25
    )
    assert obs.loc[obs["perturbation_gene"] == "TP53", "qc_pass"].iloc[0]
    assert (
        obs.loc[obs["perturbation_gene"] == "A1BG", "baseline_lamin_prefix"].iloc[0]
        == BASELINE_JOIN["baseline_lamin_prefix"]
    )
    assert set(var["perturbation_gene"]) == {"TP53", "A1BG"}


def test_simple_fixture_csv_uses_obs_response_columns(tmp_path: Path):
    source = tmp_path / "score.csv"
    source.write_text("sanger_model_id,TP53,A1BG\nSIDM1,-1.0,0.1\nSIDM2,-0.5,0.2\n")

    matrix = load_sanger_score_matrix(source)
    obs, var = score_matrix_to_obs_var(matrix, max_models=2, max_genes=1)

    assert len(obs) == 2
    assert len(var) == 1
    assert set(obs["response_metric"]) == {"gene_effect"}
    assert list(obs["perturbation_gene"]) == ["TP53", "TP53"]
    assert list(obs["baseline_join_id"]) == ["SIDM1", "SIDM2"]


def test_write_smoke_manifest_documents_forbidden_x(tmp_path: Path):
    source = make_score2_zip(tmp_path / "score.zip")
    matrix = load_sanger_score_matrix(source)
    obs, var = score_matrix_to_obs_var(matrix, max_models=1, max_genes=1)
    obs_path = write_table(obs, tmp_path / "obs.csv")
    var_path = write_table(var, tmp_path / "var.csv")
    manifest_path = write_smoke_manifest(
        tmp_path / "manifest.json",
        source_path=source,
        obs_path=obs_path,
        var_path=var_path,
        obs=obs,
        var=var,
    )

    payload = manifest_path.read_text()
    assert SANGER_SCORE_LAMIN_PREFIX in payload
    assert "essentiality_obs_var_only" in payload
    assert "X.h5ad" in payload
