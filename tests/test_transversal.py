import csv

import pytest

from pert_gym.transversal import (
    PRISM_DATASET_TAG,
    PRISM_TASK_TAG,
    STRAND_DATASET_TAG,
    STRAND_TASK_TAG,
    load_transversal_batches,
)


def _write_tsv(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def test_tagged_transversal_contract_keeps_prism_and_strand_targets_separate(
    tmp_path,
) -> None:
    prism_path = tmp_path / "prism.tsv"
    strand_path = tmp_path / "strand.tsv"
    _write_tsv(
        prism_path,
        [
            {
                "source_row_identifier": "p1",
                "perturbation_id": "drug_a",
                "depmap_id": "ACH-1::x",
                "response_metric": "lfc",
                "response_value": "-1.0",
                "split": "train",
            },
            {
                "source_row_identifier": "p2",
                "perturbation_id": "drug_b",
                "depmap_id": "ACH-2",
                "response_metric": "lfc",
                "response_value": "0.0",
                "split": "val",
            },
            {
                "source_row_identifier": "p3",
                "perturbation_id": "drug_c",
                "depmap_id": "ACH-3",
                "response_metric": "lfc",
                "response_value": "1.0",
                "split": "test",
            },
        ],
    )
    strand_rows = []
    for split, row_id, labels in (("train", "s1", "A;B"), ("test", "s2", "C")):
        strand_rows.append(
            {
                "join_row_id": row_id,
                "perturbqa_split": split,
                "perturbqa_target_label_sample": labels,
                "guide_raw_token_count": "2",
                "guide_parsed_non_control_count": "2",
                "guide_control_token_count": "0",
                "guide_tss_proxy_true_count": "2",
                "model_ready_status": "loader_projectable_only",
            }
        )
    _write_tsv(strand_path, strand_rows)

    batches = load_transversal_batches(
        prism_subset_path=prism_path,
        prism_baseline_rows=[
            {"depmap_id": "ACH-1", "expression": [1.0, 2.0]},
            {"depmap_id": "ACH-2", "expression": [3.0, 4.0]},
            {"depmap_id": "ACH-3", "expression": [5.0, 6.0]},
        ],
        prism_baseline_feature_names=["g1", "g2"],
        strand_join_path=strand_path,
    )

    train_prism, train_strand = batches.by_split["train"]
    assert (train_prism.dataset_tag, train_prism.task_tag) == (
        PRISM_DATASET_TAG,
        PRISM_TASK_TAG,
    )
    assert train_prism.features == ((1.0, 2.0),)
    assert train_prism.numeric_targets == (-1.0,)
    assert train_prism.categorical_targets is None
    assert (train_strand.dataset_tag, train_strand.task_tag) == (
        STRAND_DATASET_TAG,
        STRAND_TASK_TAG,
    )
    assert train_strand.numeric_targets is None
    assert train_strand.categorical_targets == (("A", "B"),)
    assert train_strand.metadata["not_viability_or_survival"] is True
    assert batches.by_split["val"] == (batches.by_split["val"][0],)


def test_transversal_rejects_prism_compound_leakage(tmp_path) -> None:
    prism_path = tmp_path / "prism.tsv"
    strand_path = tmp_path / "strand.tsv"
    _write_tsv(
        prism_path,
        [
            {
                "source_row_identifier": "p1",
                "perturbation_id": "drug_a",
                "depmap_id": "ACH-1",
                "response_metric": "lfc",
                "response_value": "-1",
                "split": "train",
            },
            {
                "source_row_identifier": "p2",
                "perturbation_id": "drug_a",
                "depmap_id": "ACH-2",
                "response_metric": "lfc",
                "response_value": "0",
                "split": "val",
            },
            {
                "source_row_identifier": "p3",
                "perturbation_id": "drug_c",
                "depmap_id": "ACH-3",
                "response_metric": "lfc",
                "response_value": "1",
                "split": "test",
            },
        ],
    )
    _write_tsv(
        strand_path,
        [
            {
                "join_row_id": "s1",
                "perturbqa_split": "train",
                "perturbqa_target_label_sample": "A",
                "guide_raw_token_count": "1",
                "guide_parsed_non_control_count": "1",
                "guide_control_token_count": "0",
                "guide_tss_proxy_true_count": "1",
                "model_ready_status": "loader_projectable_only",
            }
        ],
    )

    with pytest.raises(ValueError, match="compound_ids leak"):
        load_transversal_batches(
            prism_subset_path=prism_path,
            prism_baseline_rows=[
                {"depmap_id": "ACH-1", "expression": [1.0]},
                {"depmap_id": "ACH-2", "expression": [2.0]},
                {"depmap_id": "ACH-3", "expression": [3.0]},
            ],
            prism_baseline_feature_names=["g1"],
            strand_join_path=strand_path,
        )
