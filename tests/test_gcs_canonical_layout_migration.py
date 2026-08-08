from __future__ import annotations

import pytest

from tools.migrate_gcs_canonical_layout import (
    ChecksumMismatch,
    DestinationCollision,
    GCSMover,
    PlanItem,
    RawDecision,
    build_raw_plan,
    canonical_cleaned_names,
    classify_raw_object,
    cleaned_dataset_name,
    execution_identity,
    render_dataset_readme,
)


def test_manual_download_date_is_removed_from_raw_destination() -> None:
    decision = classify_raw_object(
        "pert-gym/staging/manual_temporal/2026-06-23/GSE138002/GSE138002_RAW.tar"
    )
    assert decision == RawDecision(
        action="move",
        dataset="GSE138002",
        relative_path="GSE138002_RAW.tar",
        reason="source payload",
    )


def test_images_remain_inside_their_raw_dataset() -> None:
    decision = classify_raw_object(
        "pert-gym/staging/manual_temporal/2026-06-23/STDS0000060/images/section_01.tiff"
    )
    assert decision.action == "move"
    assert decision.dataset == "STDS0000060"
    assert decision.relative_path == "images/section_01.tiff"


def test_staging_manifests_and_execution_evidence_are_deleted() -> None:
    paths = [
        "pert-gym/staging/manual_downloads/2026-06-23/_manifests/download.tsv",
        "pert-gym/staging/data/main/prism_google_drive_datasets_20260622/DOWNLOAD_STATUS.md",
        "pert-gym/staging/data/main/strand/perturbqa_mappings_20260703/k562-de.csv",
        "pert-gym/staging/data/main/viperturb/components_smoke/x/manifest.json",
    ]
    for path in paths:
        assert classify_raw_object(path).action == "delete"


def test_accession_file_is_placed_in_accession_dataset() -> None:
    decision = classify_raw_object(
        "pert-gym/staging/data/main/prism_collection/GSE213921.h5ad"
    )
    assert decision.dataset == "GSE213921"
    assert decision.relative_path == "GSE213921.h5ad"


def test_xatlas_tmp_and_duplicate_staging_copies_are_deleted() -> None:
    assert classify_raw_object(
        "pert-gym/staging/xatlas_orion/raw/hct116_filtered_dual_guide_cells.h5ad.tmp_t_dc1f805a"
    ).action == "delete"
    assert classify_raw_object(
        "pert-gym/staging/sources/xatlas/orion/hct116/figshare-55021257-223d9171f282.h5ad"
    ).action == "delete"


def test_xatlas_source_files_share_one_dataset_without_run_ids() -> None:
    decision = classify_raw_object(
        "pert-gym/staging/data/main/xatlas_orion/raw/ndownloader.figshare.com/files/55074802"
    )
    assert decision.dataset == "xatlas_orion"
    assert decision.relative_path == "HEK293T_filtered_dual_guide_cells.h5ad"


def test_stt_sections_keep_meaningful_structure_but_not_staging_date() -> None:
    decision = classify_raw_object(
        "pert-gym/staging/temporal_pretraining/stt0000071_cngb_non_tiff_20260630/"
        "STSA0000734/STTS0001152/T1_C1.gem.gz"
    )
    assert decision.dataset == "STT0000071"
    assert decision.relative_path == "STSA0000734/STTS0001152/T1_C1.gem.gz"


def test_mouse_gastrulation_hca_is_not_mislabelled_as_emtab6967() -> None:
    hca = classify_raw_object(
        "pert-gym/staging/data/gcs_cache/mouse_gastrulation/atlas_data.tar.gz"
    )
    emtab = classify_raw_object(
        "pert-gym/staging/data/main/temporal_pretraining/E-MTAB-6967/atlas_data.tar.gz"
    )
    assert hca.dataset == "mouse_gastrulation_hca"
    assert emtab.dataset == "E-MTAB-6967"


def test_known_source_wrappers_map_without_dates_or_task_labels() -> None:
    cases = {
        "pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/filtered_objects_20260630/201218_RNA.filter.tar.gz": "GSE216481",
        "pert-gym/staging/data/main/temporal_t36_gse303344/raw_h5/sample.h5": "GSE303344",
        "pert-gym/staging/temporal_pretraining/mosta_stds0000058_20260703/stomics/sample.h5ad": "STDS0000058",
    }
    for path, expected in cases.items():
        assert classify_raw_object(path).dataset == expected


def test_google_drive_operational_scripts_are_deleted() -> None:
    assert classify_raw_object(
        "pert-gym/staging/data/main/prism_google_drive_datasets_20260622/download_drive_batch.py"
    ).action == "delete"


def test_build_plan_deduplicates_identical_destinations() -> None:
    objects = [
        {
            "name": "pert-gym/staging/data/main/temporal_pretraining/GSE325829/file.h5ad",
            "generation": "1",
            "size": 12,
            "md5Hash": "same",
            "crc32c": "crc",
        },
        {
            "name": "pert-gym/staging/manual_temporal/2026-06-23/GSE325829/file.h5ad",
            "generation": "2",
            "size": 12,
            "md5Hash": "same",
            "crc32c": "crc",
        },
    ]
    plan = build_raw_plan(objects)
    assert [item.action for item in plan].count("move") == 1
    assert [item.action for item in plan].count("delete_duplicate") == 1


def test_build_plan_accepts_equal_crc_when_one_md5_is_missing() -> None:
    objects = [
        {
            "name": "pert-gym/staging/data/temporal_pretraining/scp667_zebrafish_hindbrain/raw/SCP667/gene_sorted-matrix.mtx",
            "generation": "1",
            "size": 186037596,
            "md5Hash": "md5-only-on-first",
            "crc32c": "same-crc",
        },
        {
            "name": "pert-gym/staging/manual_downloads/2026-06-23/SCP667/gene_sorted-matrix.mtx",
            "generation": "2",
            "size": 186037596,
            "md5Hash": "",
            "crc32c": "same-crc",
        },
    ]
    plan = build_raw_plan(objects)
    assert [item.action for item in plan] == ["move", "delete_duplicate"]


def test_build_plan_rejects_matching_crc_with_divergent_md5() -> None:
    objects = [
        {
            "name": "pert-gym/staging/manual_temporal/GSE325829/file.h5ad",
            "generation": "1",
            "size": 12,
            "md5Hash": "first",
            "crc32c": "same-crc",
        },
        {
            "name": "pert-gym/staging/manual_downloads/GSE325829/file.h5ad",
            "generation": "2",
            "size": 12,
            "md5Hash": "second",
            "crc32c": "same-crc",
        },
    ]
    with pytest.raises(DestinationCollision):
        build_raw_plan(objects)


def test_build_plan_rejects_divergent_destination_collision() -> None:
    objects = [
        {
            "name": "pert-gym/staging/data/main/temporal_pretraining/GSE325829/file.h5ad",
            "generation": "1",
            "size": 12,
            "md5Hash": "first",
            "crc32c": "a",
        },
        {
            "name": "pert-gym/staging/manual_temporal/2026-06-23/GSE325829/file.h5ad",
            "generation": "2",
            "size": 13,
            "md5Hash": "second",
            "crc32c": "b",
        },
    ]
    with pytest.raises(DestinationCollision):
        build_raw_plan(objects)


class _Response:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = str(self._payload)

    def json(self) -> dict:
        return self._payload


class _Session:
    def __init__(self, responses: list[_Response]):
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def _move_item() -> PlanItem:
    return PlanItem(
        action="move",
        source="pert-gym/staging/source.bin",
        destination="data/raw/example/source.bin",
        generation="123",
        size=42,
        md5_hash="md5",
        crc32c="crc",
        reason="source payload",
    )


def test_gcs_move_rewrites_verifies_then_deletes_exact_generation() -> None:
    session = _Session(
        [
            _Response(
                200,
                {
                    "done": True,
                    "resource": {
                        "name": "data/raw/example/source.bin",
                        "size": "42",
                        "md5Hash": "md5",
                        "crc32c": "crc",
                        "generation": "456",
                    },
                },
            ),
            _Response(204),
        ]
    )
    result = GCSMover(session, bucket="scperturb", user_project="billing").apply(
        _move_item()
    )
    assert result["destination_generation"] == "456"
    assert [call[0] for call in session.calls] == ["POST", "DELETE"]
    rewrite_params = session.calls[0][2]["params"]
    assert rewrite_params["ifSourceGenerationMatch"] == "123"
    assert rewrite_params["ifGenerationMatch"] == "0"
    delete_params = session.calls[1][2]["params"]
    assert delete_params["generation"] == "123"
    assert delete_params["ifGenerationMatch"] == "123"


def test_gcs_move_resumes_from_verified_existing_destination() -> None:
    session = _Session(
        [
            _Response(412, {"error": "destination exists"}),
            _Response(
                200,
                {
                    "name": "data/raw/example/source.bin",
                    "size": "42",
                    "md5Hash": "md5",
                    "crc32c": "crc",
                    "generation": "456",
                },
            ),
            _Response(204),
        ]
    )
    result = GCSMover(session, bucket="scperturb", user_project="billing").apply(
        _move_item()
    )
    assert result["destination_generation"] == "456"
    assert [call[0] for call in session.calls] == ["POST", "GET", "DELETE"]


def test_gcs_move_resumes_when_source_is_already_deleted() -> None:
    session = _Session(
        [
            _Response(404, {"error": "source missing"}),
            _Response(
                200,
                {
                    "name": "data/raw/example/source.bin",
                    "size": "42",
                    "md5Hash": "md5",
                    "crc32c": "crc",
                    "generation": "456",
                },
            ),
            _Response(404),
        ]
    )
    result = GCSMover(session, bucket="scperturb", user_project="billing").apply(
        _move_item()
    )
    assert result["destination_generation"] == "456"
    assert [call[0] for call in session.calls] == ["POST", "GET", "DELETE"]


def test_gcs_move_retries_transient_delete_failure() -> None:
    session = _Session(
        [
            _Response(
                200,
                {
                    "done": True,
                    "resource": {
                        "name": "data/raw/example/source.bin",
                        "size": "42",
                        "md5Hash": "md5",
                        "crc32c": "crc",
                        "generation": "456",
                    },
                },
            ),
            _Response(503, {"error": "retry"}),
            _Response(204),
        ]
    )
    GCSMover(
        session, bucket="scperturb", user_project="billing", sleep=lambda _: None
    ).apply(_move_item())
    assert [call[0] for call in session.calls] == ["POST", "DELETE", "DELETE"]


def test_gcs_move_never_deletes_when_readback_identity_differs() -> None:
    session = _Session(
        [
            _Response(
                200,
                {
                    "done": True,
                    "resource": {
                        "name": "data/raw/example/source.bin",
                        "size": "41",
                        "md5Hash": "wrong",
                        "crc32c": "wrong",
                        "generation": "456",
                    },
                },
            )
        ]
    )
    with pytest.raises(ChecksumMismatch):
        GCSMover(session, bucket="scperturb", user_project="billing").apply(
            _move_item()
        )
    assert [call[0] for call in session.calls] == ["POST"]


def test_matching_crc_cannot_mask_destination_md5_mismatch() -> None:
    item = _move_item()
    with pytest.raises(ChecksumMismatch, match="MD5 mismatch"):
        GCSMover._verify_destination(
            item,
            {
                "size": str(item.size),
                "crc32c": item.crc32c,
                "md5Hash": "different-md5",
            },
        )


def test_cleaned_dataset_name_uses_biological_source_identity() -> None:
    prefix = "pert-gym/staging/pert-gym/logical/temporal/example/revisions/build/"
    assert (
        cleaned_dataset_name(
            record_id="temporal_v4_048_roadmap",
            group_parent=prefix + "datasets/002a804c-cced-43ff-80d0-9a72969fe26c",
            accepted_prefix=prefix,
        )
        == "cellxgene-002a804c-cced-43ff-80d0-9a72969fe26c"
    )
    assert (
        cleaned_dataset_name(
            record_id="temporal_v4_089_organoiddb",
            group_parent=prefix + "samples/GSM5901228",
            accepted_prefix=prefix,
        )
        == "GSM5901228"
    )
    assert (
        cleaned_dataset_name(
            record_id="lincs_level2",
            group_parent=prefix + "matrices/delta",
            accepted_prefix=prefix,
        )
        == "lincs-level2-delta"
    )


def test_cleaned_dataset_name_splits_axolotl_components_by_accession() -> None:
    prefix = "pert-gym/staging/pert-gym/logical/temporal/axolotl/revisions/build/"
    assert (
        cleaned_dataset_name(
            record_id="temporal_v4_136_transcriptomic_landscape",
            group_parent=prefix.rstrip("/"),
            accepted_prefix=prefix,
        )
        == "SCP499"
    )


def test_cleaned_dataset_name_strips_versioning_from_single_triplet() -> None:
    prefix = "pert-gym/staging/pert-gym/logical/temporal/example/revisions/build/"
    assert (
        cleaned_dataset_name(
            record_id="temporal_v4_057_c_elegans_embryogenesis",
            group_parent=prefix.rstrip("/"),
            accepted_prefix=prefix,
        )
        == "c_elegans_embryogenesis"
    )


def test_classifier_blocks_canonical_and_unscoped_paths() -> None:
    for path in [
        "data/raw/GSE123/file.h5ad",
        "pert-gym/unrelated/GSE123/file.h5ad",
        "pert-gym/unrelated/manifest.json",
        "pert-gym/unrelated/stomics/hesta/file.h5ad",
    ]:
        assert classify_raw_object(path).action == "block"


def test_mover_rejects_self_move_before_any_request() -> None:
    session = _Session([])
    mover = GCSMover(session, bucket="scperturb", user_project="billing")
    with pytest.raises(ValueError, match="self-move"):
        mover.apply(
            PlanItem(
                "move",
                "pert-gym/staging/legacy/file.h5ad",
                "pert-gym/staging/legacy/file.h5ad",
                "1",
                1,
                "m",
                "c",
                "bad plan",
            )
        )
    assert session.calls == []


def test_mover_rejects_nonlegacy_source_before_any_request() -> None:
    session = _Session([])
    mover = GCSMover(session, bucket="scperturb", user_project="billing")
    with pytest.raises(ValueError, match="legacy pert-gym/staging"):
        mover.apply(
            PlanItem(
                "move",
                "data/raw/GSE123/file.h5ad",
                "data/raw/GSE123/file.h5ad",
                "1",
                1,
                "m",
                "c",
                "bad plan",
            )
        )
    assert session.calls == []


def test_mover_rejects_unscoped_legacy_delete_before_any_request() -> None:
    session = _Session([])
    mover = GCSMover(session, bucket="scperturb", user_project="billing")
    with pytest.raises(ValueError, match="legacy pert-gym/staging"):
        mover.apply(
            PlanItem(
                "delete",
                "pert-gym/unrelated/file.h5ad",
                None,
                "1",
                1,
                "m",
                "c",
                "bad plan",
            )
        )
    assert session.calls == []


def test_mover_rejects_destination_traversal_before_any_request() -> None:
    session = _Session([])
    mover = GCSMover(session, bucket="scperturb", user_project="billing")
    with pytest.raises(ValueError, match="canonical layout"):
        mover.apply(
            PlanItem(
                "move",
                "pert-gym/staging/legacy/file.h5ad",
                "data/raw/../README.md",
                "1",
                1,
                "m",
                "c",
                "bad plan",
            )
        )
    assert session.calls == []


@pytest.mark.parametrize(
    "destination",
    [
        "data/cleaned/dataset/arbitrary.bin",
        "data/cleaned/dataset/nested/X.h5ad",
        "data/raw/dataset",
        "data/cleaned/dataset/X_chunk_1.h5ad",
        "data/cleaned/dataset/obs_chunk_0001.h5ad",
    ],
)
def test_mover_rejects_noncanonical_destination_shapes(destination: str) -> None:
    session = _Session([])
    mover = GCSMover(session, bucket="scperturb", user_project="billing")
    with pytest.raises(ValueError, match="canonical layout"):
        mover.apply(
            PlanItem(
                "move",
                "pert-gym/staging/legacy/file.h5ad",
                destination,
                "1",
                1,
                "m",
                "c",
                "bad plan",
            )
        )
    assert session.calls == []


def test_dataset_readme_is_the_single_compact_manifest() -> None:
    text = render_dataset_readme(
        "GSE123",
        {
            "record_id": "temporal_v4_001_example",
            "manifest_name": "legacy/manifest.json",
            "manifest_generation": "10",
            "manifest_sha256": "abc",
            "sources": {
                "X": {
                    "name": "legacy/X.h5ad",
                    "generation": "11",
                    "size": 100,
                    "md5Hash": "m1",
                    "crc32c": "c1",
                    "destination_generation": "21",
                },
                "obs": {
                    "name": "legacy/obs.parquet",
                    "generation": "12",
                    "size": 20,
                    "md5Hash": "m2",
                    "crc32c": "c2",
                    "destination_generation": "22",
                },
                "var": {
                    "name": "legacy/var.parquet",
                    "generation": "13",
                    "size": 3,
                    "md5Hash": "m3",
                    "crc32c": "c3",
                    "destination_generation": "23",
                },
            },
        },
    )
    assert text.startswith("# GSE123\n")
    assert "No payload transformation was performed" in text
    assert "legacy/X.h5ad#11" in text
    assert "data/cleaned/GSE123/X.h5ad#21" in text
    assert "revision" not in text.lower()
    assert "var_X" not in text


def test_execution_identity_includes_destination() -> None:
    first = PlanItem("move", "old", "data/raw/A/a", "1", 2, "m", "c", "")
    second = PlanItem("move", "old", "data/raw/B/a", "1", 2, "m", "c", "")
    assert execution_identity(first) != execution_identity(second)
    assert execution_identity(first) == execution_identity(
        {
            "action": "move",
            "source": "old",
            "source_generation": "1",
            "destination": "data/raw/A/a",
        }
    )


def test_cleaned_contract_allows_only_unchunked_or_chunk_members() -> None:
    assert canonical_cleaned_names(None) == ("X.h5ad", "obs.parquet", "var.parquet")
    with pytest.raises(ValueError):
        canonical_cleaned_names("")
    with pytest.raises(ValueError):
        canonical_cleaned_names("chunk_１２３４")
    assert canonical_cleaned_names("chunk_0007") == (
        "X_chunk_0007.h5ad",
        "obs_chunk_0007.parquet",
        "var.parquet",
    )
    for invalid in ["revision_2", "plate_1", "train", "rna", "chunk_latest"]:
        with pytest.raises(ValueError):
            canonical_cleaned_names(invalid)
