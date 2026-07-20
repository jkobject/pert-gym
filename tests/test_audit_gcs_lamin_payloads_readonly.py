from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import pert_gym_vm_runner as vm_runner

MODULE_PATH = (
    Path(__file__).parents[1] / "tools" / "audit_gcs_lamin_payloads_readonly.py"
)
SPEC = importlib.util.spec_from_file_location("gcs_audit", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _configure_approved_gce_identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    hostname: str = "pert-gym-worker-eu",
    project: str = vm_runner.EXPECTED_GCE_PROJECT,
    zone: str = vm_runner.EXPECTED_ZONE,
    instance: str = "pert-gym-worker-eu",
) -> None:
    monkeypatch.setattr(vm_runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(vm_runner.socket, "gethostname", lambda: hostname)
    metadata = {
        "project/project-id": project,
        "instance/zone": f"projects/1/zones/{zone}",
        "instance/name": instance,
    }
    monkeypatch.setattr(vm_runner, "_metadata_value", metadata.__getitem__)


@pytest.mark.parametrize(
    ("hostname", "project", "zone", "instance"),
    [
        (
            "evil-pert-gym-worker-eu-lookalike",
            vm_runner.EXPECTED_GCE_PROJECT,
            vm_runner.EXPECTED_ZONE,
            "pert-gym-worker-eu",
        ),
        (
            "pert-gym-worker-eu",
            "wrong-project",
            vm_runner.EXPECTED_ZONE,
            "pert-gym-worker-eu",
        ),
        (
            "pert-gym-worker-eu",
            vm_runner.EXPECTED_GCE_PROJECT,
            "wrong-zone",
            "pert-gym-worker-eu",
        ),
        (
            "pert-gym-worker-eu",
            vm_runner.EXPECTED_GCE_PROJECT,
            vm_runner.EXPECTED_ZONE,
            "wrong-instance",
        ),
    ],
)
def test_assert_eu_worker_rejects_unpinned_gce_identity(
    monkeypatch: pytest.MonkeyPatch,
    hostname: str,
    project: str,
    zone: str,
    instance: str,
) -> None:
    _configure_approved_gce_identity(
        monkeypatch,
        hostname=hostname,
        project=project,
        zone=zone,
        instance=instance,
    )
    monkeypatch.setattr(MODULE.socket, "gethostname", lambda: hostname)

    with pytest.raises(RuntimeError):
        MODULE.assert_eu_worker()


def test_assert_eu_worker_rejects_metadata_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_approved_gce_identity(monkeypatch)
    monkeypatch.setattr(
        vm_runner,
        "_metadata_value",
        lambda _: (_ for _ in ()).throw(RuntimeError("metadata unavailable")),
    )
    monkeypatch.setattr(MODULE.socket, "gethostname", lambda: "pert-gym-worker-eu")

    with pytest.raises(RuntimeError, match="metadata unavailable"):
        MODULE.assert_eu_worker()


def test_assert_eu_worker_accepts_exact_pinned_gce_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_approved_gce_identity(monkeypatch)
    monkeypatch.setattr(MODULE.socket, "gethostname", lambda: "pert-gym-worker-eu")

    MODULE.assert_eu_worker()


def source_row(name: str, *, size: int = 10) -> dict[str, object]:
    return {
        "name": f"pert-gym/staging/data/main/{name}",
        "uri": f"gs://scperturb/pert-gym/staging/data/main/{name}",
        "logical_prefix": "pert-gym/staging/data/main",
        "bytes": size,
        "updated": "2026-07-12T00:00:00Z",
        "storage_class": "STANDARD",
        "md5_hash": "",
        "crc32c": "crc32c-evidence",
    }


def artifact(uid: str, key: str, *, size: int | None = 10, uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        uid=uid, key=key, size=size, path=uri, url=None, storage=None
    )


def test_unknown_artifact_size_cannot_complete_exact_match() -> None:
    row = source_row("unknown-size.h5ad")
    candidate = artifact(
        "unknown-size",
        "staging/data/main/unknown-size.h5ad",
        size=None,
        uri="s3://durable/unknown-size.h5ad",
    )

    manifest = MODULE.build_manifest(
        [row], [candidate], header_reader=lambda _: (True, "ok")
    )

    assert manifest[0]["classification"] == "KEEP temporary"
    assert manifest[0]["exact_lamin_object_matches"] == 0
    assert (
        manifest[0]["source_hash_evidence"]
        == "crc32c:crc32c-evidence (MD5 unavailable)"
    )


def test_invalid_source_object_sizes_fail_closed() -> None:
    candidate = artifact(
        "durable", "staging/data/main/adversarial-size.h5ad", uri="s3://durable/file"
    )
    invalid_sizes = (None, "", False, 10.0, "10.0", " 10", "-1", "ten")

    for invalid_size in invalid_sizes:
        row = source_row("adversarial-size.h5ad", size=10)
        row["bytes"] = invalid_size
        manifest = MODULE.build_manifest(
            [row], [candidate], header_reader=lambda _: (True, "ok")
        )

        assert manifest[0]["classification"] == "KEEP temporary"
        assert manifest[0]["exact_lamin_object_matches"] == 0
        assert "invalid source object size metadata" in manifest[0]["non_safe_reasons"]

    assert MODULE.gcloud_object_size(10) == 10
    assert MODULE.gcloud_object_size("10") == 10


def test_scperturb_uri_aliases_and_ambiguous_uris_fail_closed() -> None:
    source_aliases = (
        "gs://scperturb/pert-gym/staging/data/main/alias.h5ad",
        "https://storage.googleapis.com/scperturb/pert-gym/staging/data/main/alias.h5ad",
        "https://scperturb.storage.googleapis.com/pert-gym/staging/data/main/alias.h5ad",
    )
    row = source_row("alias.h5ad")
    for index, source_alias in enumerate(source_aliases):
        candidate = artifact(
            f"source-{index}",
            "staging/data/main/alias.h5ad",
            uri=source_alias,
        )
        manifest = MODULE.build_manifest(
            [row], [candidate], header_reader=lambda _: (True, "unexpected")
        )
        assert manifest[0]["classification"] == "KEEP temporary"
        assert manifest[0]["exact_lamin_object_matches"] == 0

    for ambiguous_uri in (
        "http://storage.googleapis.com/durable/file",
        "https://example.com/durable/file",
        "https://storage.googleapis.com/durable/file?token=secret",
        "file:///durable/file",
    ):
        candidate = artifact(
            "ambiguous", "staging/data/main/alias.h5ad", uri=ambiguous_uri
        )
        manifest = MODULE.build_manifest(
            [row], [candidate], header_reader=lambda _: (True, "unexpected")
        )
        assert manifest[0]["classification"] == "KEEP temporary"
        assert manifest[0]["exact_lamin_object_matches"] == 0


def test_every_exact_candidate_requires_its_own_readback_after_old_budget() -> None:
    rows = [source_row(f"object-{index}") for index in range(25)]
    candidates = [
        artifact(
            f"uid-{index}",
            f"staging/data/main/object-{index}",
            uri=f"s3://durable/object-{index}",
        )
        for index in range(25)
    ]
    calls: list[str] = []

    def bounded_readback(uri: str) -> tuple[bool, str]:
        calls.append(uri)
        return (not uri.endswith("object-24"), f"readback:{uri.rsplit('/', 1)[-1]}")

    manifest = MODULE.build_manifest(rows, candidates, header_reader=bounded_readback)

    assert len(calls) == 25
    assert manifest[0]["classification"] == "KEEP temporary"
    assert "readback:object-24" in manifest[0]["readback_evidence"]


def test_unique_non_scperturb_target_is_preferred_deterministically() -> None:
    row = source_row("preferred.h5ad")
    source = artifact(
        "source-first",
        "staging/data/main/preferred.h5ad",
        uri="gs://scperturb/pert-gym/staging/data/main/preferred.h5ad",
    )
    durable = artifact(
        "durable-second",
        "staging/data/main/preferred.h5ad",
        uri="s3://durable/preferred.h5ad",
    )

    manifest = MODULE.build_manifest(
        [row], [source, durable], header_reader=lambda _: (True, "ok")
    )

    assert manifest[0]["classification"] == "SAFE-CANDIDATE after review"
    assert manifest[0]["lamin_uids"] == "durable-second"
    assert manifest[0]["lamin_storage_uris"] == "s3://durable/preferred.h5ad"


def test_equivalent_durable_uri_aliases_select_lowest_uid_deterministically() -> None:
    row = source_row("equivalent.h5ad")
    later = artifact(
        "z-target",
        "staging/data/main/equivalent.h5ad",
        uri="https://storage.googleapis.com/durable/equivalent.h5ad",
    )
    first = artifact(
        "a-target",
        "staging/data/main/equivalent.h5ad",
        uri="gs://durable/equivalent.h5ad",
    )

    manifest = MODULE.build_manifest(
        [row], [later, first], header_reader=lambda _: (True, "ok")
    )

    assert manifest[0]["classification"] == "SAFE-CANDIDATE after review"
    assert manifest[0]["lamin_uids"] == "a-target"


def test_multiple_non_scperturb_targets_are_ambiguous_and_not_safe() -> None:
    row = source_row("ambiguous.h5ad")
    first = artifact(
        "target-a", "staging/data/main/ambiguous.h5ad", uri="s3://first/ambiguous.h5ad"
    )
    second = artifact(
        "target-b", "staging/data/main/ambiguous.h5ad", uri="s3://second/ambiguous.h5ad"
    )

    manifest = MODULE.build_manifest(
        [row], [first, second], header_reader=lambda _: (True, "ok")
    )

    assert manifest[0]["classification"] == "KEEP temporary"
    assert manifest[0]["exact_lamin_object_matches"] == 0
    assert "ambiguous" in manifest[0]["non_safe_reasons"]


def test_all_zero_audit_remains_fail_closed() -> None:
    row = source_row("unmatched.h5ad")

    manifest = MODULE.build_manifest([row], [], header_reader=lambda _: (True, "ok"))
    summary = MODULE.summarize_manifest([row], manifest)

    assert manifest[0]["classification"] == "KEEP temporary"
    assert summary["safe_candidate_bytes"] == 0
    assert summary["unexplained_delta_bytes"] == 0
