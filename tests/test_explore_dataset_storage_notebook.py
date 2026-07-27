from __future__ import annotations

import importlib.util
from pathlib import Path

import nbformat

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks/explore_dataset_storage.ipynb"
BUILDER = ROOT / "tools/build_explore_dataset_storage_notebook.py"


def load_builder():
    spec = importlib.util.spec_from_file_location(
        "dataset_storage_notebook_builder", BUILDER
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def notebook_text(notebook):
    return "\n".join(cell.source for cell in notebook.cells)


def test_notebook_is_substantial_pedagogical_and_output_free():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    nbformat.validate(notebook)
    markdown = [cell for cell in notebook.cells if cell.cell_type == "markdown"]
    code = [cell for cell in notebook.cells if cell.cell_type == "code"]
    assert len(notebook.cells) >= 40
    assert len(markdown) > len(code)
    assert all(cell.get("id") for cell in notebook.cells)
    assert all(cell.get("execution_count") is None for cell in code)
    assert all(cell.get("outputs") == [] for cell in code)


def test_notebook_explores_actual_storage_layers_not_progress_exports():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    required = [
        "scan_local_data",
        "inspect_local_payload",
        "gcloud storage ls",
        "list_gcs_level",
        "connect_pertdata",
        "ln.Collection",
        "ln.Artifact.filter",
        "storage_path",
        "features.get_values",
        "selected_obs.load()",
        "local working/download",
        "GCS processed/logical",
        "LaminDB latest artifacts",
        "raw_candidates",
        "processed_not_lamin",
        "locations_with_lamin",
        "lamin_artifact_matches",
    ]
    for marker in required:
        assert marker in text
    banned_substitutes = [
        "dataset_curation_progress.csv",
        "kanban.db",
        "owner_card_done",
        "workflow_status",
        "source_manifest_count",
    ]
    for marker in banned_substitutes:
        assert marker not in text


def test_notebook_is_read_only_and_bounds_payload_access():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    assert 'ad.read_h5ad(path, backed="r")' in text
    assert "MAX_LAMIN_METADATA_BYTES" in text
    assert "MAX_LAMIN_ROWS" in text
    assert "--recursive" not in text
    assert "to_memory(" not in text
    banned_writes = [
        "Artifact.from_",
        ".save()",
        "features.set_values",
        "gcloud storage cp",
        "gcloud storage rm",
        "gcloud storage mv",
        "gsutil cp",
        "gsutil rm",
        "subprocess.run(command" + ", check=True",  # no hidden mutation pipeline
    ]
    for marker in banned_writes:
        assert marker not in text


def test_presets_cover_multilayer_and_unpublished_examples():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    for dataset in ["SCP211", "GSE132080", "XAtlas HCT116", "GSE216481", "Artista T37"]:
        assert dataset in text
    assert "manual_downloads/2026-06-23/downloads_cleanup/SCP211/" in text
    assert "pert-gym/logical/" in text
    assert "laminlabs/pertdata" in text
    assert 'SELECTED_DATASET = "SCP211"' in text


def test_generator_matches_committed_notebook():
    module = load_builder()
    generated = module.build()
    committed = nbformat.read(NOTEBOOK, as_version=4)
    assert nbformat.writes(generated) == nbformat.writes(committed)
