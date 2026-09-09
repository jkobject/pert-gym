from __future__ import annotations

import pandas as pd
import pytest

from tools.curate_arc_vcc_obs_var import CANONICAL_FIELDS, MEMBERS, curate_obs


def _obs() -> pd.DataFrame:
    index = pd.Index(["cell-1", "cell-2"], name="source_cell")
    return pd.DataFrame(
        {
            "original_obs_index": index.astype(str),
            "obs_uuid": ["uuid-1", "uuid-2"],
            "dataset": ["arc_vcc_2025_test"] * 2,
            "batch": ["Flex_1_01"] * 2,
            "cell_type": ["H1 human embryonic stem cell"] * 2,
            "cell_line": ["H1_hESC"] * 2,
            "disease": ["healthy"] * 2,
            "organism": ["human"] * 2,
            "assay": ["Perturb-seq"] * 2,
            "modality": ["scRNA-seq"] * 2,
            "perturbation": ["GENE_A", "non-targeting"],
            "perturbation_type": ["CRISPRi"] * 2,
            "is_control": [False, True],
            "raw_retained": [1, 2],
        },
        index=index,
    )


def test_curate_obs_preserves_order_and_dispositions_all_contract_fields() -> None:
    source = _obs().loc[:, ["batch", "perturbation"]]
    raw = _obs()

    curated, dispositions = curate_obs(source, raw, MEMBERS[0])

    assert curated.index.equals(raw.index)
    assert curated["raw_retained"].tolist() == [1, 2]
    assert curated["cell_id"].tolist() == ["cell-1", "cell-2"]
    assert curated["technology"].eq("10x Genomics Flex").all()
    assert curated["dose"].isna().all()
    assert curated["guide_sequence"].isna().all()
    assert set(curated["dose__state"]) == {"not_applicable"}
    assert set(curated["guide_sequence__state"]) == {"unknown"}
    assert set(dispositions) == set(CANONICAL_FIELDS)
    assert all(value["total_rows"] == 2 for value in dispositions.values())


def test_curate_obs_rejects_nonexact_source_join() -> None:
    raw = _obs()
    source = _obs().loc[:, ["batch"]].iloc[::-1]

    with pytest.raises(ValueError, match="exact ordered 1:1"):
        curate_obs(source, raw, MEMBERS[0])
