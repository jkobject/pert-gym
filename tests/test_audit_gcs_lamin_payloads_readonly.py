from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

MODULE_PATH = (
    Path(__file__).parents[1] / "tools" / "audit_gcs_lamin_payloads_readonly.py"
)
SPEC = importlib.util.spec_from_file_location("gcs_audit", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


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
