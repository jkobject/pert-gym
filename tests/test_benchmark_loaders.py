import json

import pytest

from pert_gym.benchmarks import (
    BenchmarkBatch,
    load_chemcpa_drugseq_tiny,
    load_model_ready_v0_or_synthetic,
    load_scgen_viperturb_tiny,
    load_tiny_benchmark_dataset,
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

    result = evaluate_model(MeanControlBaseline(), train=dataset.train, test=dataset.test)

    assert result.model_name == "mean_control"
    assert result.n_obs == len(dataset.test.X)
    assert result.n_features == 3
    assert set(result.metrics) == {"mae", "rmse"}


def test_model_ready_v0_loader_reads_manifest_metadata_without_heavy_load(tmp_path) -> None:
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


def test_chemcpa_drugseq_tiny_loader_uses_real_expression_and_fingerprints(tmp_path) -> None:
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
                "source": {"dataset_prefix": "viperturb/vimentin_screen_chunk_smoke/chunk_0000"},
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
