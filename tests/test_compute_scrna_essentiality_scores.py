from __future__ import annotations

import math

import pandas as pd

from tools.compute_scrna_essentiality_scores import score_loaded_obs


def test_score_loaded_obs_uses_controls_and_gene_counts() -> None:
    obs = pd.DataFrame(
        {
            "perturbation": ["GENE1_0", "GENE1_1", "GENE2_0", "control", "control"],
            "is_control": [False, False, False, True, True],
            "perturbation_type": ["CRISPRi"] * 5,
        }
    )

    rows = score_loaded_obs(
        dataset="example",
        obs_frames=[obs],
        perturbation_type_hint="CRISPR",
        artifact_keys=["example/obs.parquet"],
        pseudocount=0.5,
    )

    by_gene = {row["perturbation_gene"]: row for row in rows}
    assert set(by_gene) == {"GENE1", "GENE2"}
    assert by_gene["GENE1"]["n_cells_perturbed"] == 2
    assert by_gene["GENE1"]["n_controls"] == 2
    assert by_gene["GENE1"]["score"] == round(math.log2(2.5 / 2.5), 6)
    assert by_gene["GENE2"]["score"] == round(math.log2(1.5 / 2.5), 6)
    assert "cell_count_log2_ratio_vs_controls" in by_gene["GENE1"]["score_method"]


def test_score_loaded_obs_refuses_missing_controls() -> None:
    obs = pd.DataFrame(
        {
            "perturbation": ["GENE1_0", "GENE2_0"],
            "perturbation_type": ["CRISPRi", "CRISPRi"],
        }
    )

    rows = score_loaded_obs(
        dataset="example",
        obs_frames=[obs],
        perturbation_type_hint="CRISPR",
        artifact_keys=["example/obs.parquet"],
        pseudocount=0.5,
    )

    assert len(rows) == 1
    assert rows[0]["score"] == "not_applicable"
    assert rows[0]["score_method"] == "not_applicable_no_identifiable_controls"
