from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "artifacts/dataset_completion/temporal__drosophila_dv_patterning"


def test_e_mtab_9304_live_receipt_is_bound_to_all_completion_gates() -> None:
    receipt_path = EVIDENCE / "latest_receipt.json"
    gate_path = EVIDENCE / "gate_summary.json"
    raw = receipt_path.read_bytes()
    receipt = json.loads(raw)
    summary = json.loads(gate_path.read_text())

    assert (
        summary["immutable_live_receipt"]["local_sha256"]
        == hashlib.sha256(raw).hexdigest()
    )
    assert summary["immutable_live_receipt"]["gcs_generation_uri"].endswith(
        "#1786220209678163"
    )
    assert receipt["complete"] is True
    assert all(summary["gates"].values())


def test_e_mtab_9304_exact_axes_links_species_and_payloads_pass() -> None:
    receipt = json.loads((EVIDENCE / "latest_receipt.json").read_text())
    final = receipt["final"]

    assert receipt["negative_main_equivalence"]["negative"] is True
    assert final["shape"] == [119_362, 16_936]
    assert final["obs_X_link"] is True
    assert final["X_var_link"] is True
    assert all(final["payload_exists"].values())
    assert final["obs"]["rows"] == 119_362
    assert final["obs"]["canonical_fields"] == 42
    assert final["obs"]["identity_unique"] is True
    assert final["var"]["species"] == "Drosophila melanogaster"
    assert final["var"]["namespace"] == "FlyBase"
    assert final["var"]["human_mouse_coercions"] == 0
    assert (
        final["var"]["flybase_source_exact"] + final["var"]["non_gene_not_applicable"]
        == 16_936
    )
    collection = receipt["collection"]
    assert collection["predecessor"]["target_obs_uid"] == "rt5eRz8opcJXtybp0000"
    assert collection["successor"]["target_obs_uid"] == "rt5eRz8opcJXtybp0001"
    assert collection["predecessor"]["member_count"] == 1_018
    assert collection["successor"]["member_count"] == 1_018
    assert collection["replacement_count"] == 1
    assert collection["unrelated_membership_drift"] == 0
    assert collection["replay_noop"] is True
    assert (
        collection["registry_counts_before_replay"]
        == collection["registry_counts_after_replay"]
    )


def test_e_mtab_9304_scientific_contract_has_real_axes_and_endpoints() -> None:
    receipt = json.loads((EVIDENCE / "latest_receipt.json").read_text())
    contract = receipt["scientific_contract"]

    assert contract["scientific_modality"] == "single_cell_rna_expression"
    assert contract["experimental_unit"] == "cell"
    assert contract["biological_sample_unit"] == "pooled_stage_5_embryos_by_genotype"
    axes = contract["experimental_axes"]
    assert axes["maternal_genotype"]["role"] == "perturbation"
    assert axes["maternal_genotype"]["cardinality"] > 1
    assert sum(axes["maternal_genotype"]["frequencies"].values()) == 119_362
    assert axes["collection_age"]["cardinality"] == 1
    assert axes["collection_age"]["frequencies"] == {"2.5 to 3.5 hour": 119_362}
    assert axes["developmental_stage"]["cardinality"] == 1
    assert axes["developmental_stage"]["frequencies"] == {"stage 5 embryo": 119_362}
    assert contract["temporal_verdict"] == "single_stage_single_window_non_temporal"
    assert contract["outcomes_endpoints"]["expression"]["level"] == "cell"
    assert contract["outcomes_endpoints"]["expression"]["shape"] == [
        119_362,
        16_936,
    ]
    assert contract["outcomes_endpoints"]["inferred_cell_type"]["level"] == "cell"
    assert contract["outcomes_endpoints"]["response_endpoint"] is None
