from __future__ import annotations

import gzip
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from pert_gym.logical_sparse_zarr import (
    read_logical_sparse_revision,
    shared_var_identity,
)
from pert_gym.sparse_zarr_contract import load_compatible_surface

SCRIPT = Path(__file__).parents[1] / "tools" / "build_stt0000071_component.py"


def load_module():
    spec = importlib.util.spec_from_file_location("build_stt0000071_component", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_row(section: str, filename: str, index: int) -> dict[str, str]:
    relative = f"STSA0000734/{section}/{filename}"
    return {
        "relative_path": relative,
        "sample_id": "STSA0000734",
        "timepoint": "uninjured_1",
        "section_id": section,
        "section_label": "T1_C1",
        "filename": filename,
        "gcs_uri": f"gs://scperturb/pert-gym/staging/temporal_pretraining/stt0000071_cngb_non_tiff_20260630/{relative}",
        "gcs_generation": str(1000 + index),
        "gcs_size": "10",
    }


def canonical_source_rows() -> list[dict[str, str]]:
    coverage = [
        ("STSA0000734", "uninjured_1", 3),
        ("STSA0000735", "6_hpa", 3),
        ("STSA0000736", "12_hpa", 3),
        ("STSA0000737", "1_dpa", 3),
        ("STSA0000738", "3_dpa", 3),
        ("STSA0000739", "7_dpa", 3),
        ("STSA0000740", "14_dpa", 3),
        ("STSA0000741", "28_dpa", 3),
        ("STSA0000742", "uninjured_2", 22),
    ]
    rows = []
    section_index = 0
    for sample_id, timepoint, count in coverage:
        for _ in range(count):
            section = f"STTS{section_index:07d}"
            for offset, filename in enumerate(
                (
                    f"S{section_index}.70.gem.gz",
                    f"S{section_index}.70.tsv.gz",
                    f"S{section_index}.gem.gz",
                )
            ):
                row = source_row(section, filename, section_index * 3 + offset)
                row["sample_id"] = sample_id
                row["timepoint"] = timepoint
                rows.append(row)
            section_index += 1
    return rows


def test_classify_sections_requires_exact_cellbin_metadata_and_raw(monkeypatch) -> None:
    module = load_module()
    rows = canonical_source_rows()
    sections = module.classify_sections(rows)
    assert len(sections) == 46
    assert sections[0]["cell_bin"]["filename"].endswith(".70.gem.gz")
    assert sections[0]["metadata"]["filename"].endswith(".70.tsv.gz")
    assert sections[0]["raw_unbinned"]["filename"].endswith(".gem.gz")


def test_classify_sections_rejects_incomplete_sample_timepoint_coverage() -> None:
    module = load_module()
    rows = canonical_source_rows()
    for row in rows:
        if row["sample_id"] == "STSA0000735":
            row["sample_id"] = "STSA0000734"
            row["timepoint"] = "uninjured_1"
    with pytest.raises(RuntimeError, match="sample/timepoint coverage"):
        module.classify_sections(rows)


def test_convert_section_preserves_coordinate_obs_x_parity(tmp_path: Path) -> None:
    module = load_module()
    gem = pd.DataFrame(
        {
            "geneID": ["g2", "g1", "g1"],
            "x": [10.123456789012, 10.123456789013, 20.0],
            "y": [30.0, 30.0, 40.0],
            "MIDCounts": [2, 1, 4],
        }
    )
    metadata = pd.DataFrame(
        {
            "x": [10.123456789014, 20.0],
            "y": [30.0, 40.0],
            "nGenes": [2, 1],
            "nUMI": [3, 4],
            "sid": ["s1", "s1"],
            "3D_x": [1.0, 2.0],
            "3D_y": [3.0, 4.0],
            "3D_z": [5.0, 6.0],
            "annotation": ["A", "B"],
            "cellname": ["c1", "c2"],
            "cid": ["section", "section"],
        }
    )
    gem_path = tmp_path / "section.70.gem.gz"
    metadata_path = tmp_path / "section.70.tsv.gz"
    gem.to_csv(gem_path, sep="\t", index=False, compression="gzip")
    metadata.to_csv(metadata_path, sep="\t", index=False, compression="gzip")
    section = {
        "sample_id": "STSA0000734",
        "section_id": "STTS0001152",
        "timepoint": "uninjured_1",
    }
    obs, matrix, stats = module.convert_section(
        gem_path, metadata_path, genes=["g1", "g2"], section=section
    )
    assert list(matrix.shape) == [2, 2]
    assert matrix.toarray().tolist() == [[1, 2], [4, 0]]
    assert stats == {
        "metadata_schema": "reconstructed_3d_cell_metadata/v1",
        "n_obs": 2,
        "n_vars": 2,
        "section_present_genes": 2,
        "nnz": 3,
        "sum": 7,
        "metadata_n_counts_sum": 7,
    }
    assert obs.index.tolist() == ["c1", "c2"]
    assert obs["age"].isna().all()
    assert set(obs["age_missingness_reason"]) == {"source_not_reported"}
    assert obs["timepoint"].tolist() == [0, 0]


def test_convert_early_section_uses_grid_coordinates_and_source_index(tmp_path: Path) -> None:
    module = load_module()
    gem = pd.DataFrame(
        {"geneID": ["g1", "g2"], "x": [1.25, 2.5], "y": [3.75, 4.5], "MIDCounts": [4, 5]}
    )
    source = pd.DataFrame(
        {
            "orig.ident": ["sample", "sample"],
            "nCount_Spatial": [4, 5],
            "nFeature_Spatial": [1, 1],
            "x": [100.0, 200.0],
            "y": [300.0, 400.0],
            "grid_x": [1.25, 2.5],
            "grid_y": [3.75, 4.5],
            "isolate": ["isolate", "isolate"],
            "cid": ["section", "section"],
            "time_points": ["uninjured", "uninjured"],
            "annotation": ["A", "B"],
        },
        index=["source-cell-1", "source-cell-2"],
    )
    gem_path = tmp_path / "section.70.gem.gz"
    metadata_path = tmp_path / "section.70.tsv.gz"
    gem.to_csv(gem_path, sep="\t", index=False, compression="gzip")
    source.to_csv(metadata_path, sep="\t", index=True, index_label=False, compression="gzip")
    section = {"sample_id": "STSA0000734", "section_id": "STTS0001152", "timepoint": "uninjured_1"}
    obs, matrix, stats = module.convert_section(
        gem_path, metadata_path, genes=["g1", "g2"], section=section
    )
    assert stats["metadata_schema"] == "seurat_spatial_cell_metadata/v1"
    assert obs.index.tolist() == ["source-cell-1", "source-cell-2"]
    assert obs[["x", "y"]].values.tolist() == [[1.25, 3.75], [2.5, 4.5]]
    assert obs[["3D_x", "3D_y", "3D_z"]].isna().all().all()
    assert matrix.toarray().tolist() == [[4, 0], [0, 5]]


def test_sparse_zarr_zip_roundtrip(tmp_path: Path) -> None:
    module = load_module()
    matrix = module.sparse.csr_matrix([[0, 2], [3, 0]], dtype="int64")
    path = tmp_path / "X.zarr.zip"
    module.write_sparse_zarr_zip(path, matrix)
    loaded = module.read_sparse_zarr_zip(path)
    assert loaded.shape == matrix.shape
    assert loaded.nnz == matrix.nnz
    assert (loaded != matrix).nnz == 0


def test_local_payload_resume_preserves_exact_bytes_and_rejects_drift(
    tmp_path: Path,
) -> None:
    module = load_module()
    matrix = module.sparse.csr_matrix([[0, 2], [3, 0]], dtype="int64")
    matrix_path = tmp_path / "X.zarr.zip"
    module.write_or_validate_sparse_zarr_zip(matrix_path, matrix)
    matrix_bytes = matrix_path.read_bytes()
    module.write_or_validate_sparse_zarr_zip(matrix_path, matrix)
    assert matrix_path.read_bytes() == matrix_bytes
    with pytest.raises(RuntimeError, match="local sparse-Zarr resume mismatch"):
        module.write_or_validate_sparse_zarr_zip(
            matrix_path,
            module.sparse.csr_matrix([[1, 0], [0, 0]], dtype="int64"),
        )

    obs = pd.DataFrame({"cell_id": ["c1", "c2"]}, index=["c1", "c2"])
    obs_path = tmp_path / "obs.parquet"
    module.write_or_validate_parquet(obs_path, obs)
    obs_bytes = obs_path.read_bytes()
    module.write_or_validate_parquet(obs_path, obs)
    assert obs_path.read_bytes() == obs_bytes
    with pytest.raises(RuntimeError, match="local Parquet resume mismatch"):
        module.write_or_validate_parquet(
            obs_path,
            pd.DataFrame({"cell_id": ["different"]}, index=["c1"]),
        )


def test_manifest_surface_is_accepted_by_canonical_loader() -> None:
    module = load_module()
    var = pd.DataFrame(
        {"gene_symbol": ["g1", "g2"]}, index=pd.Index(["g1", "g2"], name="var_id")
    )
    var_identity = shared_var_identity(
        var, schema_fingerprint="stt0000071-shared-var/v1"
    )
    records = [
        {
            "chunk_index": 0,
            "stats": {"n_obs": 2, "n_vars": 2, "nnz": 2},
            "dtype": "int64",
            "checksums": {
                "data_sha256": "a" * 64,
                "indices_sha256": "b" * 64,
                "indptr_sha256": "c" * 64,
            },
            "X": {"key": "sections/0000/X.zarr.zip"},
            "obs": {"key": "sections/0000/obs.parquet"},
            "source_cell_metadata": {
                "uri": "gs://source/section.metadata.tsv",
                "sha256": "d" * 64,
            },
        }
    ]
    manifest = module.canonical_surface_manifest(
        revision="stt0000071-20260720T000000Z-deadbeef",
        section_records=records,
        shared_var_key=f"vars/{var_identity.key}/var.parquet",
        var_identity=var_identity,
    )
    surface = load_compatible_surface(manifest)
    assert surface.shape == (2, 2)
    assert surface.chunks[0].obs["provenance"]["source_checksum"] == (
        "sha256-file-bytes/v1:" + "d" * 64
    )
    assert surface.chunks[0].obs["provenance"]["source_uri"] == (
        "gs://source/section.metadata.tsv"
    )
    assert surface.shared_var["frame_sha256"] == var_identity.frame_sha256


def test_canonical_loader_reads_stt_zip_chunks(tmp_path: Path) -> None:
    module = load_module()
    revision = "stt0000071-20260720T000000Z-deadbeef"
    candidate = tmp_path / module.LOGICAL_KEY / "revisions" / revision
    section = candidate / "sections" / "0000-STTS0000000"
    section.mkdir(parents=True)
    matrix = module.sparse.csr_matrix([[0, 2], [3, 0]], dtype="int64")
    matrix_path = section / "X.zarr.zip"
    module.write_sparse_zarr_zip(matrix_path, matrix)
    obs = pd.DataFrame({"cell_id": ["c1", "c2"]}, index=["c1", "c2"])
    obs.to_parquet(section / "obs.parquet")
    var = pd.DataFrame(
        {"gene_symbol": ["g1", "g2"]},
        index=pd.Index(["g1", "g2"], name="var_id"),
    )
    var_identity = shared_var_identity(
        var, schema_fingerprint="stt0000071-shared-var/v1"
    )
    shared_var_key = f"vars/{var_identity.key}/var.parquet"
    shared_var_path = tmp_path / shared_var_key
    shared_var_path.parent.mkdir(parents=True)
    var.to_parquet(shared_var_path)
    record = {
        "chunk_index": 0,
        "stats": {"n_obs": 2, "n_vars": 2, "nnz": 2},
        "dtype": "int64",
        "checksums": {
            "data_sha256": module.sha256_array(matrix.data),
            "indices_sha256": module.sha256_array(matrix.indices),
            "indptr_sha256": module.sha256_array(matrix.indptr),
        },
        "X": {"key": "sections/0000-STTS0000000/X.zarr.zip"},
        "obs": {"key": "sections/0000-STTS0000000/obs.parquet"},
        "source_cell_metadata": {
            "uri": "gs://source/input.metadata.tsv#1",
            "sha256": "d" * 64,
        },
    }
    manifest = module.canonical_surface_manifest(
        revision=revision,
        section_records=[record],
        shared_var_key=shared_var_key,
        var_identity=var_identity,
    )
    (candidate / "manifest.json").write_text(json.dumps(manifest))

    surface, loaded_matrix, loaded_obs, loaded_var = read_logical_sparse_revision(
        tmp_path, module.LOGICAL_KEY, revision
    )
    assert surface.shape == (2, 2)
    assert (loaded_matrix != matrix).nnz == 0
    assert loaded_obs.index.tolist() == ["c1", "c2"]
    assert loaded_var.equals(var)


def test_publish_output_adopts_exact_remote_object_after_prejournal_crash(
    tmp_path: Path, monkeypatch
) -> None:
    module = load_module()
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"exact payload")
    journal_path = tmp_path / "publication-journal.json"
    identity = {"revision": "r1", "staging_manifest_sha256": "a" * 64}
    remote = {
        "uri": "gs://bucket/revisions/r1/payload.bin",
        "generation": "123",
        "size": payload.stat().st_size,
        "sha256": module.sha256_file(payload),
    }

    monkeypatch.setattr(module, "upload_or_reconcile", lambda path, uri: dict(remote))
    result = module.publish_output(
        payload,
        remote["uri"],
        stage="payload",
        journal_path=journal_path,
        journal_identity=identity,
    )

    assert result == remote
    journal = json.loads(journal_path.read_text())
    assert journal["completed_stages"]["payload"] == remote
    with pytest.raises(RuntimeError, match="journal identity mismatch"):
        module.publish_output(
            payload,
            remote["uri"],
            stage="payload",
            journal_path=journal_path,
            journal_identity={"revision": "different"},
        )


def test_publication_journal_preserves_manifest_timestamp_across_resume(
    tmp_path: Path,
) -> None:
    module = load_module()
    path = tmp_path / "publication-journal.json"
    identity = {"revision": "r1"}
    first = module.load_or_create_publication_journal(path, identity)
    second = module.load_or_create_publication_journal(path, identity)
    assert isinstance(first["publication_started_at"], float)
    assert second["publication_started_at"] == first["publication_started_at"]


def test_upload_or_reconcile_reuses_exact_remote_and_rejects_drift(
    tmp_path: Path, monkeypatch
) -> None:
    module = load_module()
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"exact payload")
    remote_bytes = payload.read_bytes()
    uploads = []

    monkeypatch.setattr(
        module,
        "probe_gcs",
        lambda uri: {"uri": uri, "generation": "123", "size": len(remote_bytes)},
    )
    monkeypatch.setattr(
        module,
        "readback_object",
        lambda identity, destination: destination.write_bytes(remote_bytes),
    )
    monkeypatch.setattr(module, "run", lambda *args, **kwargs: uploads.append(args))

    identity = module.upload_or_reconcile(payload, "gs://bucket/payload.bin")
    assert identity["sha256"] == module.sha256_file(payload)
    assert uploads == []

    remote_bytes = b"drifted"
    with pytest.raises(RuntimeError, match="existing immutable object mismatch"):
        module.upload_or_reconcile(payload, "gs://bucket/payload.bin")


def test_execute_requires_complete_heavy_vm_identity_before_other_work(
    monkeypatch,
) -> None:
    module = load_module()
    monkeypatch.setattr(
        module,
        "require_heavy_vm",
        lambda: (_ for _ in ()).throw(RuntimeError("unpinned GCE identity")),
    )
    with pytest.raises(RuntimeError, match="unpinned GCE identity"):
        module.execute(SimpleNamespace(), {}, {}, [])
