import json

import pytest

from pert_gym.benchmarks import (
    BenchmarkBatch,
    adapt_model_ready_v2_rows,
    filter_expression_model_ready_members,
    load_chemcpa_drugseq_tiny,
    load_essentiality_screen_with_baseline,
    load_model_ready_v0_or_synthetic,
    load_model_ready_v2_adapters,
    load_model_ready_v2_batches,
    load_response_screen_with_baseline,
    load_scgen_viperturb_tiny,
    load_tiny_benchmark_dataset,
    model_ready_v2_batches_from_adapters,
    split_perturbation_identities,
    write_benchmark_artifacts,
)
from pert_gym.evaluate import evaluate_model
from pert_gym.models import MeanControlBaseline


def test_tiny_loader_output_shapes_and_target_contract() -> None:
    dataset = load_tiny_benchmark_dataset()

    assert dataset.split_by == "perturbation_identity"
    assert dataset.train.X
    assert dataset.val.X
    assert dataset.test.X
    assert len(dataset.train.X[0]) == 3
    assert dataset.train.target_response == dataset.train.X
    assert dataset.train.feature_names == ("gene_a", "gene_b", "gene_c")
    assert len(dataset.train.obs_covariates) == len(dataset.train.X)
    assert "cell_line" in dataset.train.obs_covariates[0]


def test_loader_split_integrity_by_non_control_perturbation_identity() -> None:
    dataset = load_tiny_benchmark_dataset()

    split_perturbations = []
    for batch in (dataset.train, dataset.val, dataset.test):
        assert batch.controls is not None
        split_perturbations.append(
            {
                perturbation
                for perturbation, is_control in zip(batch.perturbations, batch.controls)
                if not is_control
            }
        )

    assert split_perturbations[0].isdisjoint(split_perturbations[1])
    assert split_perturbations[0].isdisjoint(split_perturbations[2])
    assert split_perturbations[1].isdisjoint(split_perturbations[2])


def test_loader_copies_controls_into_each_split_for_baselines() -> None:
    dataset = load_tiny_benchmark_dataset()

    assert dataset.train.controls is not None
    assert dataset.val.controls is not None
    assert dataset.test.controls is not None
    assert any(dataset.train.controls)
    assert any(dataset.val.controls)
    assert any(dataset.test.controls)


def test_loader_output_is_evaluation_compatible() -> None:
    dataset = load_tiny_benchmark_dataset()

    result = evaluate_model(
        MeanControlBaseline(), train=dataset.train, test=dataset.test
    )

    assert result.model_name == "mean_control"
    assert result.n_obs == len(dataset.test.X)
    assert result.n_features == 3
    assert set(result.metrics) == {"mae", "rmse"}


def test_model_ready_v0_loader_reads_manifest_metadata_without_heavy_load(
    tmp_path,
) -> None:
    manifest = tmp_path / "model_ready.json"
    manifest.write_text(
        json.dumps(
            {
                "model_ready_collection": {
                    "key": "pert-gym/model-ready/test",
                    "member_count": 1,
                    "member_keys": ["tiny/obs.parquet"],
                }
            }
        )
    )

    dataset = load_model_ready_v0_or_synthetic(manifest_path=manifest)

    assert dataset.source == "model-ready-v0"
    assert dataset.metadata["fallback"] == "synthetic"
    assert dataset.metadata["model_ready_collection_key"] == "pert-gym/model-ready/test"
    assert dataset.metadata["model_ready_member_keys"] == ["tiny/obs.parquet"]


def test_model_ready_v0_loader_excludes_broad_prism_empty_response_member(
    tmp_path,
) -> None:
    manifest = tmp_path / "model_ready.json"
    manifest.write_text(
        json.dumps(
            {
                "model_ready_collection": {
                    "key": "pert-gym/model-ready/test",
                    "member_count": 2,
                    "member_keys": [
                        "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet",
                        "broad_prism_repurposing/obs.parquet",
                    ],
                    "member_metadata": {
                        "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet": {
                            "x_semantics": "expression"
                        },
                        "broad_prism_repurposing/obs.parquet": {
                            "x_semantics": "empty",
                            "modality": "response_screen",
                        },
                    },
                }
            }
        )
    )

    dataset = load_model_ready_v0_or_synthetic(manifest_path=manifest)

    assert dataset.metadata["model_ready_member_keys"] == [
        "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet"
    ]
    assert dataset.metadata["excluded_member_keys"] == [
        "broad_prism_repurposing/obs.parquet"
    ]
    assert dataset.metadata["excluded_member_reasons"] == {
        "broad_prism_repurposing/obs.parquet": "x_semantics=empty response_screen is not expression-model-ready"
    }


def test_expression_member_filter_holds_out_broad_prism_even_without_metadata() -> None:
    filtered = filter_expression_model_ready_members(
        [
            "broad_prism_repurposing/obs.parquet",
            "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet",
        ]
    )

    assert filtered.included == [
        "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet"
    ]
    assert filtered.excluded == ["broad_prism_repurposing/obs.parquet"]


def test_expression_member_filter_records_prefix_specific_broad_prism_reason() -> None:
    key = "broad_prism_repurposing/obs.parquet"

    filtered = filter_expression_model_ready_members(
        [key],
        member_metadata={
            key: {"x_semantics": "expression", "modality": "transcriptomics"}
        },
    )

    assert filtered.excluded_reasons[key] == (
        "broad_prism_repurposing is held out from expression-model-ready loaders"
    )


def test_chemcpa_drugseq_tiny_loader_uses_real_expression_and_fingerprints(
    tmp_path,
) -> None:
    artifact = tmp_path / "chemcpa_drugseq.json"
    rows = []
    for perturbation, is_control, value, fp in [
        ("dmso", True, 1.0, [1, 0, 0, 0]),
        ("dmso", True, 1.1, [1, 0, 0, 0]),
        ("drug_a", False, 2.0, [0, 1, 0, 0]),
        ("drug_b", False, 3.0, [0, 0, 1, 0]),
        ("drug_c", False, 4.0, [0, 0, 0, 1]),
        ("drug_d", False, 5.0, [1, 1, 0, 0]),
    ]:
        rows.append(
            {
                "perturbation": perturbation,
                "is_control": is_control,
                "cell_line": "U-2 OS cell",
                "assay": "DRUG-seq",
                "expression": [value, value + 0.5],
                "compound_fingerprint": fp,
            }
        )
    artifact.write_text(
        json.dumps(
            {
                "feature_names": ["gene_a", "gene_b"],
                "rows": rows,
                "source": {"dataset_prefix": "DRUG-seq/GSE120222"},
                "selection": {"n_obs": len(rows), "n_features": 2},
                "compound_metadata": {"fingerprint": {"n_bits": 4}},
            }
        )
    )

    dataset = load_chemcpa_drugseq_tiny(artifact_path=artifact)

    assert dataset.metadata["fallback"] is None
    assert dataset.metadata["loader"] == "chemcpa_drugseq_tiny"
    assert dataset.train.compound_features is not None
    assert len(dataset.train.compound_features[0]) == 4
    assert dataset.train.feature_names == ("gene_a", "gene_b")
    assert dataset.train.controls is not None
    assert any(dataset.train.controls)


def test_scgen_viperturb_tiny_loader_uses_real_expression_contract(tmp_path) -> None:
    artifact = tmp_path / "scgen_viperturb.json"
    rows = []
    for perturbation, is_control, value in [
        ("NO-TARGET", True, 1.0),
        ("NO-TARGET", True, 1.1),
        ("gene_a", False, 2.0),
        ("gene_a", False, 2.1),
        ("gene_b", False, 3.0),
        ("gene_b", False, 3.1),
        ("gene_c", False, 4.0),
        ("gene_c", False, 4.1),
        ("gene_d", False, 5.0),
        ("gene_d", False, 5.1),
    ]:
        rows.append(
            {
                "perturbation": perturbation,
                "condition": "control" if is_control else perturbation,
                "control_value": "control",
                "is_control": is_control,
                "cell_line": "unknown",
                "cell_type": "unknown",
                "assay": "VIPerturb-seq",
                "expression": [value, value + 0.25],
            }
        )
    artifact.write_text(
        json.dumps(
            {
                "feature_names": ["SAMD11", "NOC2L"],
                "rows": rows,
                "export": {"adata_path": "artifacts/model_benchmarks/tiny.h5ad"},
                "selection": {"control_value": "control"},
                "source": {
                    "dataset_prefix": "viperturb/vimentin_screen_chunk_smoke/chunk_0000"
                },
            }
        )
    )

    dataset = load_scgen_viperturb_tiny(artifact_path=artifact)

    assert dataset.metadata["fallback"] is None
    assert dataset.metadata["loader"] == "scgen_viperturb_tiny"
    assert dataset.metadata["adata_path"] == "artifacts/model_benchmarks/tiny.h5ad"
    assert dataset.train.feature_names == ("SAMD11", "NOC2L")
    assert dataset.train.controls is not None
    assert any(dataset.train.controls)
    split_non_controls = []
    for batch in (dataset.train, dataset.val, dataset.test):
        assert batch.controls is not None
        split_non_controls.append(
            {
                perturbation
                for perturbation, is_control in zip(batch.perturbations, batch.controls)
                if not is_control
            }
        )
    assert split_non_controls[0].isdisjoint(split_non_controls[1])
    assert split_non_controls[0].isdisjoint(split_non_controls[2])
    assert split_non_controls[1].isdisjoint(split_non_controls[2])


def test_response_screen_loader_joins_rows_to_baseline_by_stable_depmap_id() -> None:
    response_rows = [
        {
            "depmap_id": "ACH-000001::P103::PR500A::REP1M",
            "perturbation": "BRD-A",
            "response_metric": "lfc",
            "response_value": "-0.5",
            "is_control": False,
        },
        {
            "depmap_id": "ACH-000002",
            "perturbation": "BRD-B",
            "response_metric": "lfc",
            "response_value": "0.25",
            "is_control": False,
        },
    ]
    baseline_rows = [
        {"depmap_id": "ACH-000001", "expression": [1.0, 2.0]},
        {"depmap_id": "ACH-000002", "expression": [3.0, 4.0]},
    ]

    batch = load_response_screen_with_baseline(
        response_rows=response_rows,
        baseline_rows=baseline_rows,
        feature_names=["gene_a", "gene_b"],
    )

    assert batch.X == [[1.0, 2.0], [3.0, 4.0]]
    assert batch.target_response == [[-0.5], [0.25]]
    assert batch.obs_covariates == (
        {"depmap_id": "ACH-000001", "response_metric": "lfc"},
        {"depmap_id": "ACH-000002", "response_metric": "lfc"},
    )


def test_response_screen_loader_accepts_native_prism_lfc_rows() -> None:
    batch = load_response_screen_with_baseline(
        response_rows=[
            {
                "depmap_id": "ACH-000001",
                "broad_id": "BRD-A",
                "lfc": "-0.75",
                "is_control": False,
            }
        ],
        baseline_rows=[{"depmap_id": "ACH-000001", "expression": [1.0, 2.0]}],
        feature_names=["gene_a", "gene_b"],
    )

    assert batch.target_response == [[-0.75]]
    assert batch.perturbations == ["BRD-A"]
    assert batch.obs_covariates == (
        {"depmap_id": "ACH-000001", "response_metric": "lfc"},
    )


def test_response_screen_loader_rejects_empty_perturbation_identity() -> None:
    with pytest.raises(ValueError, match="missing perturbation/broad_id"):
        load_response_screen_with_baseline(
            response_rows=[
                {
                    "depmap_id": "ACH-000001",
                    "perturbation": "  ",
                    "response_metric": "lfc",
                    "response_value": -0.5,
                }
            ],
            baseline_rows=[{"depmap_id": "ACH-000001", "expression": [1.0, 2.0]}],
            feature_names=["gene_a", "gene_b"],
        )


def test_response_screen_loader_accepts_identical_duplicate_baselines_in_any_order() -> (
    None
):
    response_rows = [
        {
            "depmap_id": "ACH-000001::P103::PR500A::REP1M",
            "perturbation": "BRD-A",
            "response_metric": "lfc",
            "response_value": "-0.5",
        }
    ]
    baseline_rows = [
        {"depmap_id": "ACH-000001", "expression": [1.0, 2.0]},
        {"ach_id": "ACH-000001", "expression": [1.0, 2.0]},
    ]

    forward = load_response_screen_with_baseline(
        response_rows=response_rows,
        baseline_rows=baseline_rows,
        feature_names=["gene_a", "gene_b"],
    )
    reverse = load_response_screen_with_baseline(
        response_rows=response_rows,
        baseline_rows=list(reversed(baseline_rows)),
        feature_names=["gene_a", "gene_b"],
    )

    assert forward.X == [[1.0, 2.0]]
    assert reverse.X == forward.X


def test_response_screen_loader_rejects_non_identical_duplicate_baselines() -> None:
    with pytest.raises(ValueError, match="non-identical baseline RNA expression"):
        load_response_screen_with_baseline(
            response_rows=[
                {
                    "depmap_id": "ACH-000001",
                    "perturbation": "BRD-A",
                    "response_metric": "lfc",
                    "response_value": "-0.5",
                }
            ],
            baseline_rows=[
                {"depmap_id": "ACH-000001", "expression": [1.0, 2.0]},
                {"depmap_id": "ACH-000001", "expression": [3.0, 4.0]},
            ],
            feature_names=["gene_a", "gene_b"],
        )


def test_response_screen_loader_rejects_missing_or_malformed_response_semantics() -> (
    None
):
    baseline_rows = [{"depmap_id": "ACH-000001", "expression": [1.0, 2.0]}]

    with pytest.raises(ValueError, match="response_value"):
        load_response_screen_with_baseline(
            response_rows=[
                {
                    "depmap_id": "ACH-000001",
                    "perturbation": "BRD-A",
                    "response_metric": "missing",
                    "response_value": "missing",
                    "is_control": False,
                }
            ],
            baseline_rows=baseline_rows,
            feature_names=["gene_a", "gene_b"],
        )


def test_response_screen_loader_requires_separate_baseline_expression() -> None:
    with pytest.raises(ValueError, match="baseline RNA expression"):
        load_response_screen_with_baseline(
            response_rows=[
                {
                    "depmap_id": "ACH-000001",
                    "perturbation": "BRD-A",
                    "response_metric": "lfc",
                    "response_value": "1.0",
                    "is_control": False,
                }
            ],
            baseline_rows=[],
            feature_names=["gene_a", "gene_b"],
        )


def test_benchmark_artifact_summary_written(tmp_path) -> None:
    dataset = load_tiny_benchmark_dataset()

    out_path = write_benchmark_artifacts(dataset, artifact_dir=tmp_path)

    payload = json.loads(out_path.read_text())
    assert payload["split_by"] == "perturbation_identity"
    assert payload["splits"]["train"]["n_controls"] >= 1
    assert payload["splits"]["test"]["n_features"] == 3


def test_split_requires_at_least_three_non_control_identities() -> None:
    obs_rows = [
        {"perturbation": "control", "is_control": True},
        {"perturbation": "pert_a", "is_control": False},
        {"perturbation": "pert_b", "is_control": False},
    ]

    with pytest.raises(ValueError, match="three non-control perturbation"):
        split_perturbation_identities(obs_rows)


def test_benchmark_batch_validates_covariate_row_count() -> None:
    with pytest.raises(ValueError, match="obs_covariates"):
        BenchmarkBatch(
            X=[[1.0], [2.0]],
            perturbations=["control", "pert_a"],
            controls=[True, False],
            obs_covariates=[{}],
        )


@pytest.mark.parametrize(
    ("fingerprints", "message"),
    [
        ([None, [1, 0], [0, 1], [1, 1]], "must be present for every row"),
        ([[1, 0], [1, 0], [0, 1, 0], [1, 1]], "consistent width"),
    ],
)
def test_loader_rejects_mixed_compound_fingerprint_contracts(
    fingerprints, message
) -> None:
    obs_rows = []
    X = []
    for idx, (perturbation, is_control) in enumerate(
        [("control", True), ("pert_a", False), ("pert_b", False), ("pert_c", False)]
    ):
        row = {"perturbation": perturbation, "is_control": is_control}
        if fingerprints[idx] is not None:
            row["compound_fingerprint"] = fingerprints[idx]
        obs_rows.append(row)
        X.append([float(idx)])

    with pytest.raises(ValueError, match=message):
        load_tiny_benchmark_dataset(obs_rows=obs_rows, X=X, feature_names=["gene_a"])


def test_expression_member_filter_holds_out_sanger_score_dependency_member() -> None:
    filtered = filter_expression_model_ready_members(
        [
            "sanger_score_crispr/obs.parquet",
            "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet",
        ],
        member_metadata={
            "sanger_score_crispr/obs.parquet": {
                "x_semantics": "fold_change",
                "modality": "essentiality",
                "perturbation_type": "CRISPRko",
                "readout_modality": "Project_Score_CRISPR_screen",
            },
            "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet": {
                "x_semantics": "expression"
            },
        },
    )

    assert filtered.included == [
        "viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet"
    ]
    assert filtered.excluded == ["sanger_score_crispr/obs.parquet"]
    assert (
        "not expression X"
        in filtered.excluded_reasons["sanger_score_crispr/obs.parquet"]
    )


def test_essentiality_screen_loader_opts_into_sanger_score_with_baseline_expression() -> (
    None
):
    batch = load_essentiality_screen_with_baseline(
        response_rows=[
            {
                "sanger_model_id": "SIDM00001",
                "model_name": "Model A",
                "perturbation_gene": "TP53",
                "perturbation_type": "CRISPRko",
                "readout_modality": "Project_Score_CRISPR_screen",
                "response_metric": "fold_change",
                "response_value": -1.25,
                "is_control": False,
            }
        ],
        baseline_rows=[{"sanger_model_id": "SIDM00001", "expression": [0.5, 1.5]}],
        feature_names=["GAPDH", "ACTB"],
    )

    assert batch.X == [[0.5, 1.5]]
    assert batch.target_response == [[-1.25]]
    assert batch.perturbations == ["TP53"]
    assert batch.feature_names == ("GAPDH", "ACTB")
    assert batch.obs_covariates[0]["response_metric"] == "fold_change"


def test_essentiality_screen_loader_rejects_generic_crispr_semantics() -> None:
    with pytest.raises(ValueError, match="CRISPRko"):
        load_essentiality_screen_with_baseline(
            response_rows=[
                {
                    "sanger_model_id": "SIDM00001",
                    "perturbation_gene": "TP53",
                    "perturbation_type": "CRISPR",
                    "response_metric": "fold_change",
                    "response_value": -1.25,
                }
            ],
            baseline_rows=[{"sanger_model_id": "SIDM00001", "expression": [0.5, 1.5]}],
            feature_names=["GAPDH", "ACTB"],
        )


def test_model_ready_v2_adapters_separate_response_expression_image_and_mapping() -> (
    None
):
    adapters = adapt_model_ready_v2_rows(
        [
            {
                "manifest_row_id": "resp_prism",
                "source": "Broad PRISM Repurposing",
                "target_classification": "direct",
                "has_response_label": "true",
                "has_expression_X": "false",
                "sample_id": "ACH-000001::BRD-A",
                "depmap_id": "ACH-000001",
                "perturbation": "BRD-A",
                "response_metric": "auc",
                "response_value": "0.42",
                "response_direction": "lower_is_more_sensitive",
                "response_source": "PRISM",
            },
            {
                "manifest_row_id": "expr_l1000",
                "source": "LINCS L1000",
                "artifact_key": "lincs/level2/obs.parquet",
                "sample_id": "L1000:1",
                "has_expression_X": "true",
                "has_response_label": "false",
                "modality": "L1000",
                "assay": "L1000",
                "x_semantics": "normalized_expression",
                "target_task": "representation_pretraining",
                "perturbation": "trt_a",
            },
            {
                "manifest_row_id": "rxrx_payload",
                "source": "RxRx19b",
                "has_image_payload": "true",
                "has_expression_X": "false",
                "payload_artifact_keys": '["rxrx19b/X_recursion_dl_embedding.parquet"]',
                "sample_id": "rxrx19b_site_1",
                "modality": "image",
                "assay": "Cell Painting",
                "perturbation": "orf_a",
            },
            {
                "manifest_row_id": "guide_map",
                "source": "STRAND",
                "artifact_role": "guide_target_mapping",
                "artifact_key": "strand/guide_target_map.parquet",
                "guide_id": "gTP53",
                "perturbation_target": "TP53",
                "target_kind": "mapping",
            },
        ]
    )

    assert len(adapters.responses) == 1
    assert adapters.responses[0].response_value == 0.42
    assert adapters.responses[0].context["depmap_id"] == "ACH-000001"
    assert len(adapters.expressions) == 1
    assert adapters.expressions[0].x_semantics == "normalized_expression"
    assert adapters.expressions[0].role == "representation_pretraining"
    assert len(adapters.images) == 1
    assert adapters.images[0].payload_artifact_keys == (
        "rxrx19b/X_recursion_dl_embedding.parquet",
    )
    assert len(adapters.mappings) == 1
    assert adapters.mappings[0].join_fields == ("guide_id", "perturbation_target")
    assert not adapters.skipped

    batches = model_ready_v2_batches_from_adapters(adapters)
    assert {batch.modality for batch in batches} == {
        "screen",
        "L1000",
        "image",
        "mapping",
    }
    assert batches[0].target_mask is True
    assert batches[0].target_label == 0.42
    assert batches[1].features["expression_handle"] == "lincs/level2/obs.parquet"
    assert batches[2].features["payload_handles"] == (
        "rxrx19b/X_recursion_dl_embedding.parquet",
    )
    assert batches[3].organism == "not_applicable"


def test_model_ready_v2_response_adapter_rejects_fake_expression_x() -> None:
    adapters = adapt_model_ready_v2_rows(
        [
            {
                "manifest_row_id": "bad_depmap",
                "source": "DepMap Chronos/CERES dependency",
                "target_classification": "direct",
                "has_response_label": "true",
                "has_expression_X": "true",
                "response_metric": "gene_effect",
                "response_value": "-0.2",
                "response_direction": "lower_more_dependent",
            }
        ]
    )

    assert adapters.responses == ()
    assert "fake expression X" in adapters.skipped["bad_depmap"]


def test_model_ready_v2_tsv_loader_uses_tiny_local_fixture(tmp_path) -> None:
    manifest = tmp_path / "model_ready_v2.tsv"
    manifest.write_text(
        "\t".join(
            [
                "manifest_row_id",
                "source",
                "target_classification",
                "has_response_label",
                "has_expression_X",
                "sample_id",
                "perturbation",
                "response_metric",
                "response_value",
                "response_direction",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "gdsc_row",
                "Sanger/GDSC drug response",
                "direct",
                "true",
                "false",
                "SIDM00001::drug_a",
                "drug_a",
                "ln_ic50",
                "1.25",
                "lower_is_more_sensitive",
            ]
        )
        + "\n"
    )

    adapters = load_model_ready_v2_adapters(manifest_path=manifest)
    batches = load_model_ready_v2_batches(manifest_path=manifest)

    assert adapters.responses[0].sample_id == "SIDM00001::drug_a"
    assert adapters.responses[0].response_metric == "ln_ic50"
    assert batches[0].target == "ln_ic50"
    assert batches[0].target_mask is True


def test_essentiality_screen_loader_rejects_conflicting_duplicate_baselines() -> None:
    with pytest.raises(ValueError, match="non-identical baseline RNA expression"):
        load_essentiality_screen_with_baseline(
            response_rows=[
                {
                    "sanger_model_id": "SIDM00001",
                    "perturbation_gene": "TP53",
                    "perturbation_type": "CRISPRko",
                    "response_metric": "fold_change",
                    "response_value": -1.25,
                }
            ],
            baseline_rows=[
                {"sanger_model_id": "SIDM00001", "expression": [0.5, 1.5]},
                {"model_name": "SIDM00001", "expression": [9.0, 9.0]},
            ],
            feature_names=["GAPDH", "ACTB"],
        )


def test_essentiality_screen_loader_accepts_identical_duplicate_baselines() -> None:
    batch = load_essentiality_screen_with_baseline(
        response_rows=[
            {
                "sanger_model_id": "SIDM00001",
                "perturbation_gene": "TP53",
                "perturbation_type": "CRISPRko",
                "response_metric": "fold_change",
                "response_value": -1.25,
            }
        ],
        baseline_rows=[
            {"model_name": "SIDM00001", "expression": [0.5, 1.5]},
            {"sanger_model_id": "SIDM00001", "expression": [0.5, 1.5]},
        ],
        feature_names=["GAPDH", "ACTB"],
    )

    assert batch.X == [[0.5, 1.5]]


def test_essentiality_screen_loader_rejects_malformed_baseline_expression() -> None:
    with pytest.raises(ValueError, match="baseline row 0 has malformed"):
        load_essentiality_screen_with_baseline(
            response_rows=[],
            baseline_rows=[
                {"sanger_model_id": "SIDM00001", "expression": ["not-a-number"]}
            ],
            feature_names=["GAPDH"],
        )


def test_model_ready_v2_skips_malformed_expression_and_mapping_rows() -> None:
    adapters = adapt_model_ready_v2_rows(
        [
            {
                "manifest_row_id": "missing_expression_handle",
                "source": "expression-source",
                "has_expression_X": "true",
                "x_semantics": "raw_counts",
            },
            {
                "manifest_row_id": "missing_mapping_join",
                "source": "mapping-source",
                "artifact_role": "guide_target_mapping",
                "artifact_key": "mapping/guide_target.parquet",
                "target_kind": "mapping",
            },
        ]
    )

    assert not adapters.expressions
    assert not adapters.mappings
    assert adapters.skipped == {
        "missing_expression_handle": (
            "expression-like rows require a non-empty artifact_key"
        ),
        "missing_mapping_join": (
            "mapping rows require at least one explicit join field"
        ),
    }


def test_model_ready_v2_json_array_manifest_loads(tmp_path) -> None:
    manifest = tmp_path / "model_ready_v2.json"
    manifest.write_text(
        json.dumps(
            [
                {
                    "manifest_row_id": "expression",
                    "source": "expression-source",
                    "has_expression_X": True,
                    "x_semantics": "raw_counts",
                    "artifact_key": "expression/obs.parquet",
                }
            ]
        )
    )

    adapters = load_model_ready_v2_adapters(manifest_path=manifest)

    assert [sample.artifact_key for sample in adapters.expressions] == [
        "expression/obs.parquet"
    ]
    assert not adapters.skipped


def test_model_ready_v2_json_object_manifest_loads_rows(tmp_path) -> None:
    manifest = tmp_path / "model_ready_v2.json"
    manifest.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "manifest_row_id": "expression",
                        "source": "expression-source",
                        "has_expression_X": True,
                        "x_semantics": "raw_counts",
                        "artifact_key": "expression/obs.parquet",
                    }
                ]
            }
        )
    )

    adapters = load_model_ready_v2_adapters(manifest_path=manifest)

    assert [sample.artifact_key for sample in adapters.expressions] == [
        "expression/obs.parquet"
    ]


def test_model_ready_v2_expression_response_remains_expression_adapter() -> None:
    adapters = adapt_model_ready_v2_rows(
        [
            {
                "manifest_row_id": "expression_response",
                "source": "Perturb-seq",
                "has_expression_X": True,
                "has_response_label": True,
                "x_semantics": "raw_counts",
                "artifact_key": "perturb_seq/obs.parquet",
                "response_metric": "expression_fitness_proxy",
                "response_value": -0.5,
            }
        ]
    )

    assert adapters.responses == ()
    assert len(adapters.expressions) == 1
    assert adapters.expressions[0].role == "expression_response"
    assert adapters.skipped == {}


def test_model_ready_v2_skips_missing_or_malformed_image_handles() -> None:
    adapters = adapt_model_ready_v2_rows(
        [
            {
                "manifest_row_id": "missing_image_handle",
                "source": "RxRx",
                "modality": "image",
            },
            {
                "manifest_row_id": "malformed_image_handle",
                "source": "RxRx",
                "modality": "image",
                "payload_artifact_keys": "[not-json",
            },
            {
                "manifest_row_id": "invalid_native_image_handles",
                "source": "RxRx",
                "modality": "image",
                "payload_artifact_keys": ["valid.parquet", None],
            },
        ]
    )

    assert not adapters.images
    assert adapters.skipped == {
        "missing_image_handle": (
            "image rows require at least one payload artifact handle"
        ),
        "malformed_image_handle": "payload_artifact_keys contains malformed JSON",
        "invalid_native_image_handles": (
            "payload_artifact_keys arrays require non-empty string handles"
        ),
    }


def test_model_ready_v2_batch_loader_fails_on_any_skipped_row(tmp_path) -> None:
    manifest = tmp_path / "model_ready_v2.json"
    manifest.write_text(
        json.dumps(
            [
                {
                    "manifest_row_id": "missing_expression_handle",
                    "has_expression_X": True,
                    "x_semantics": "raw_counts",
                }
            ]
        )
    )

    with pytest.raises(ValueError, match="contains skipped rows"):
        load_model_ready_v2_batches(manifest_path=manifest)


def test_response_loaders_reject_non_finite_targets() -> None:
    adapters = adapt_model_ready_v2_rows(
        [
            {
                "manifest_row_id": "nan_target",
                "source": "Broad PRISM Repurposing",
                "has_response_label": True,
                "has_expression_X": False,
                "response_metric": "auc",
                "response_value": "nan",
            }
        ]
    )
    assert not adapters.responses
    assert "must be finite" in adapters.skipped["nan_target"]

    with pytest.raises(ValueError, match="malformed response_value"):
        load_response_screen_with_baseline(
            response_rows=[
                {
                    "depmap_id": "ACH-000001",
                    "broad_id": "BRD-A",
                    "lfc": "nan",
                }
            ],
            baseline_rows=[{"depmap_id": "ACH-000001", "expression": [1.0, 2.0]}],
            feature_names=["GAPDH", "ACTB"],
        )

    with pytest.raises(ValueError, match="malformed response_value"):
        load_response_screen_with_baseline(
            response_rows=[
                {
                    "depmap_id": "ACH-000001",
                    "response_metric": "auc",
                    "response_value": "inf",
                }
            ],
            baseline_rows=[{"depmap_id": "ACH-000001", "expression": [1.0, 2.0]}],
            feature_names=["GAPDH", "ACTB"],
        )

    with pytest.raises(ValueError, match="malformed response_value"):
        load_essentiality_screen_with_baseline(
            response_rows=[
                {
                    "sanger_model_id": "SIDM00001",
                    "perturbation_type": "CRISPRko",
                    "response_metric": "gene_effect",
                    "response_value": "-inf",
                }
            ],
            baseline_rows=[{"sanger_model_id": "SIDM00001", "expression": [1.0, 2.0]}],
            feature_names=["GAPDH", "ACTB"],
        )
