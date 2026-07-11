from __future__ import annotations

import json
from pathlib import Path

import nbformat

from pert_gym.processing_decisions import validate_processing_decisions_contract

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks/datasets/_template_processing_decisions.ipynb"
README = ROOT / "notebooks/datasets/README.md"


def valid_contract() -> dict[str, object]:
    return {
        "identity": {
            "dataset_id": "example",
            "source": "https://example.org/record/1",
            "version": "v1",
            "license": "CC-BY-4.0",
            "checksums": {"raw.tar": "sha256:abc"},
        },
        "delta_vs_main": {
            "branch": "jkobject",
            "artifact_count": 3,
            "logical_dataset_count": 1,
            "collection_count": 1,
            "added_artifacts": [],
            "added_collections": [],
        },
        "biological_context": {"unit": "cell", "modality": "scRNA-seq"},
        "source_payload": {"included": [], "excluded": []},
        "processing_decisions": {
            "inclusion": "documented",
            "exclusion": "documented",
            "conversion": "documented",
            "transformations": "documented",
            "quality_control": "documented",
            "obs_schema": "documented",
            "perturbation_mapping": "documented",
            "control_mapping": "documented",
            "organism_and_gene_normalization": "documented",
            "x_semantics": "documented",
            "chunk_size_policy": "documented",
            "zarr_or_h5ad": "documented",
            "shared_var_identity": "documented",
            "auxiliary_modalities": "documented",
        },
        "rejected_alternatives": [],
        "lineage": {"script": "tools/example.py", "commit": "abc", "card_id": "t_x"},
        "validation": {"readback": "pending", "denominator": "cells"},
        "collection_membership": {"collections": [], "model_ready_query": "pending"},
        "limitations_and_rollback": {"limitations": [], "rollback": "pending"},
        "temporary_gcs_dependencies": [
            {
                "uri": "gs://scperturb/pert-gym/staging/example.raw",
                "purpose": "temporary raw staging",
                "durable_replacement": "Lamin raw artifact raw/example.tar",
                "safe_to_remove_prerequisites": ["readback passed"],
            }
        ],
        "reconstruction": {
            "reproducibility_claimed": True,
            "immutable_upstream_sources": ["https://example.org/record/1"],
            "retained_lamin_raw_artifact": None,
            "safe_to_remove_gcs": False,
            "procedure": "Reacquire immutable upstream payload and rerun recorded script.",
        },
        "runtime": {
            "live_lamin_query_enabled": False,
            "allowed_live_lamin_hosts": ["pert-gym-worker-eu"],
        },
    }


def test_contract_accepts_durable_reconstruction_source() -> None:
    assert validate_processing_decisions_contract(valid_contract()) == []


def test_contract_rejects_reproducibility_claim_backed_only_by_gcs() -> None:
    contract = valid_contract()
    reconstruction = contract["reconstruction"]
    assert isinstance(reconstruction, dict)
    reconstruction["immutable_upstream_sources"] = []
    reconstruction["retained_lamin_raw_artifact"] = None

    errors = validate_processing_decisions_contract(contract)

    assert any("only an unretained GCS object" in error for error in errors)


def test_template_notebook_is_valid_and_metadata_first() -> None:
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    nbformat.validate(notebook)
    source = "\n".join(cell.source for cell in notebook.cells)

    assert notebook.metadata.kernelspec["name"] == "python3"
    assert "validate_processing_decisions_contract" in source
    assert "temporary_gcs_dependencies" in source
    assert "safe_to_remove_gcs" in source
    assert "pert-gym-worker-eu" in source
    assert all(cell.outputs == [] for cell in notebook.cells if cell.cell_type == "code")


def test_template_contract_executes_without_remote_access() -> None:
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    namespace: dict[str, object] = {"__file__": str(NOTEBOOK)}
    for cell in notebook.cells:
        if cell.cell_type == "code":
            exec(cell.source, namespace)
    assert namespace["errors"] == []


def test_readme_explains_durable_gcs_exit_contract() -> None:
    readme = README.read_text(encoding="utf-8")
    assert "safe_to_remove_gcs" in readme
    assert "immutable upstream" in readme
    assert "retained Lamin raw artifact" in readme
    assert "not proof of per-dataset coverage" in readme
