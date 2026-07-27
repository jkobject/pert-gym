from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import nbformat
import pandas as pd
import pytest

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


def cell_source(notebook, cell_id):
    return next(cell.source for cell in notebook.cells if cell.id == cell_id)


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
    assert notebook.metadata.kernelspec.name == "python3"


def test_notebook_explores_actual_storage_layers_not_progress_exports():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    required = [
        "scan_local_data",
        "inspect_local_payload",
        "list_gcs_level",
        "connect_pertdata",
        "ln.Collection",
        "ln.Artifact.filter",
        "storage_path",
        "features.get_values",
        "selected_obs.load()",
        "local working/download",
        "GCS cleaned",
        "LaminDB latest artifacts",
        "raw_candidates",
        "cleaned_not_lamin",
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
    assert "MAX_LOCAL_ENTRIES" in text
    assert "entries_seen >= max_entries" in text
    assert "MAX_GCS_RESULTS" in text
    assert '"maxResults": max_results' in text
    assert "MAX_GCS_RESPONSE_BYTES + 1" in text
    assert "raise RuntimeError(f\"GCS listing failed" in text
    assert "selected_obs.size is None" in text
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


def test_local_scan_bounds_every_visited_entry(tmp_path):
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    namespace = {}
    for cell_id in ["imports", "small-helpers", "local-options"]:
        exec(cell_source(notebook, cell_id), namespace)
    scan_definition = cell_source(notebook, "local-scan").split("\n\nlocal_files =", 1)[0]
    exec(scan_definition, namespace)

    for index in range(12):
        path = tmp_path / f"irrelevant-{index}"
        path.write_text("x")
    frame = namespace["scan_local_data"]([tmp_path], max_files=100, max_entries=5)
    assert frame.attrs["entries_seen"] == 5
    assert frame.attrs["scan_truncated"] is True


def test_gcs_listing_is_server_and_response_bounded_and_fails_closed(monkeypatch):
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    namespace = {}
    for cell_id in ["imports", "small-helpers", "gcs-options", "gcs-helper"]:
        exec(cell_source(notebook, cell_id), namespace)
    namespace["gcloud_adc_token"] = lambda timeout: "test-token"

    seen = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit):
            seen["read_limit"] = limit
            return json.dumps(
                {
                    "prefixes": ["prefix/child/"],
                    "items": [{"name": "prefix/file.h5ad", "size": "7"}],
                    "nextPageToken": "more",
                }
            ).encode()

    def bounded_urlopen(request, timeout):
        seen["url"] = request.full_url
        return Response()

    namespace["urlopen"] = bounded_urlopen
    frame = namespace["list_gcs_level"]("gs://bucket/prefix/", max_results=7)
    query = parse_qs(urlparse(seen["url"]).query)
    assert query["maxResults"] == ["7"]
    assert query["delimiter"] == ["/"]
    assert query["userProject"] == [namespace["GCS_BILLING_PROJECT"]]
    assert seen["read_limit"] == namespace["MAX_GCS_RESPONSE_BYTES"] + 1
    assert frame.attrs["listing_truncated"] is True

    def denied(*args, **kwargs):
        raise PermissionError("denied")

    namespace["urlopen"] = denied
    with pytest.raises(RuntimeError, match="GCS listing failed"):
        namespace["list_gcs_level"]("gs://bucket/prefix/", max_results=7)


def test_unknown_lamin_size_refuses_load(capsys):
    notebook = nbformat.read(NOTEBOOK, as_version=4)

    class UnknownSizeArtifact:
        key = "dataset/obs.parquet"
        size = None

        def load(self):
            raise AssertionError("unknown-size payload must not be loaded")

    namespace = {
        "selected_obs": UnknownSizeArtifact(),
        "human_bytes": lambda value: str(value),
        "display": lambda value: None,
    }
    exec(cell_source(notebook, "lamin-preview"), namespace)
    assert namespace["lamin_preview"] is None
    assert "Refusing metadata load with unknown size" in capsys.readouterr().out


def test_presets_cover_multilayer_and_unpublished_examples():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    for dataset in ["SCP211", "GSE132080", "XAtlas HCT116", "GSE216481", "Artista T37"]:
        assert dataset in text
    assert "gs://scperturb/data/raw/SCP211/" in text
    assert "gs://scperturb/data/cleaned/SCP211/" in text
    assert "laminlabs/pertdata" in text
    assert 'SELECTED_DATASET = "SCP211"' in text


def test_canonical_gcs_hierarchy_is_explicit_and_legacy_is_not_canonicalized():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    for marker in [
        'EXPECTED_TOP_LEVEL = {"README.md", "data/", "other/", ".lamindb/"}',
        'EXPECTED_DATA_LEVEL = {"raw/", "cleaned/"}',
        'RAW_GCS_ROOTS = ["gs://scperturb/data/raw/"]',
        'CLEANED_GCS_ROOTS = ["gs://scperturb/data/cleaned/"]',
        'OTHER_GCS_ROOTS = ["gs://scperturb/other/"]',
        "X.h5ad",
        "X_chunk_<NNNN>.h5ad",
        "obs.parquet",
        "obs_chunk_<NNNN>.parquet",
        "var.parquet",
        "other/README.md",
        "unexpected_or_legacy",
    ]:
        assert marker in text
    assert "gs://scperturb/pert-gym/staging/" not in text


def test_notebook_reconciles_raw_cleaned_and_lamin_from_compact_receipts():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    required = [
        "cleaned_direct_datasets.json",
        "raw_plan.jsonl",
        "final_layout_readback.json",
        "final_legacy_prefixes.json",
        "lamin_key_remap_receipt.json",
        "raw_cleaned_reconciliation",
        "cleaned_without_same_name_raw",
        "raw_without_same_name_cleaned",
        "canonical_lamin_artifacts",
        "old Lamin keys remaining",
        "121 cleaned names without an exact same-name raw prefix",
        "107 raw names without an exact same-name cleaned directory",
    ]
    for marker in required:
        assert marker in text


def test_receipt_reconciliation_has_exact_review_counts():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    namespace = {}
    for cell_id in ["imports", "migration-audit-load"]:
        exec(cell_source(notebook, cell_id), namespace)

    summary = namespace["migration_summary"]
    assert summary == {
        "raw_dataset_prefixes": 111,
        "cleaned_datasets": 125,
        "cleaned_data_objects_moved": 375,
        "cleaned_readmes_added": 125,
        "cleaned_objects": 500,
        "same_name_raw_and_cleaned": 4,
        "cleaned_without_same_name_raw": 121,
        "raw_without_same_name_cleaned": 107,
        "lamin_artifacts_remapped": 69,
    }
    frame = namespace["raw_cleaned_reconciliation"]
    assert len(frame) == 232
    assert frame["dataset_name"].is_unique
    assert set(frame["relationship"]) == {
        "same name in raw and cleaned",
        "cleaned unit; no exact same-name raw prefix",
        "raw source package; no exact same-name cleaned unit",
    }
    assert set(namespace["lamin_remap"]["changes"][0]) == {"new_key", "old_key", "uid"}


def test_notebook_exposes_dataset_inventory_with_sizes_and_csv_export():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    text = notebook_text(notebook)
    for marker in [
        "dataset_inventory",
        "build_dataset_inventory",
        "export_dataset_inventory_csv",
        "raw_bytes",
        "cleaned_bytes",
        "lamin_bytes",
        "in_raw",
        "in_cleaned",
        "in_lamindb",
        "storage_membership",
    ]:
        assert marker in text


def test_dataset_inventory_aggregates_each_storage_layer_by_name():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    namespace = {}
    for cell_id in ["imports", "dataset-inventory-helper"]:
        exec(cell_source(notebook, cell_id), namespace)

    raw_plan = [
        {"action": "move", "destination": "data/raw/A/a.bin", "size": 10},
        {"action": "move", "destination": "data/raw/A/b.bin", "size": 20},
        {"action": "delete", "destination": None, "size": 999},
    ]
    cleaned_plan = [
        {"action": "move", "destination": "data/cleaned/B/X.h5ad", "size": 30},
        {"action": "move", "destination": "data/cleaned/B/obs.parquet", "size": 4},
    ]
    readmes = {"objects": [{"name": "data/cleaned/B/README.md", "size": 2}]}
    lamin = pd.DataFrame(
        [
            {"key": "data/cleaned/B/X.h5ad", "bytes": 30, "size": "30 B"},
            {"key": "data/cleaned/B/obs.parquet", "bytes": 4, "size": "4 B"},
        ]
    )

    inventory = namespace["build_dataset_inventory"](
        raw_plan, cleaned_plan, readmes, lamin
    ).set_index("dataset_name")
    assert list(inventory.index) == ["A", "B"]
    assert inventory.loc["A", ["raw_bytes", "raw_objects"]].tolist() == [30, 2]
    assert inventory.loc["B", ["cleaned_bytes", "cleaned_objects"]].tolist() == [36, 3]
    assert inventory.loc["B", ["lamin_bytes", "lamin_artifacts"]].tolist() == [34, 2]
    assert inventory.loc["A", "storage_membership"] == "raw only"
    assert inventory.loc["B", "storage_membership"] == "cleaned + LaminDB"


def test_cleaned_loader_supports_h5ad_obs_and_var_by_dataset_name():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    namespace = {}
    for cell_id in ["imports", "gcs-options", "cleaned-loader"]:
        exec(cell_source(notebook, cell_id), namespace)

    paths = namespace["cleaned_dataset_paths"]("SCP211")
    assert paths == {
        "h5ad": "gs://scperturb/data/cleaned/SCP211/X.h5ad",
        "obs": "gs://scperturb/data/cleaned/SCP211/obs.parquet",
        "var": "gs://scperturb/data/cleaned/SCP211/var.parquet",
    }
    with pytest.raises(ValueError, match="part"):
        namespace["load_cleaned_dataset"]("SCP211", part="matrix")
    with pytest.raises(ValueError, match="dataset_name"):
        namespace["cleaned_dataset_paths"]("../escape")


def test_generator_matches_committed_notebook():
    module = load_builder()
    generated = module.build()
    committed = nbformat.read(NOTEBOOK, as_version=4)
    assert nbformat.writes(generated) == nbformat.writes(committed)
