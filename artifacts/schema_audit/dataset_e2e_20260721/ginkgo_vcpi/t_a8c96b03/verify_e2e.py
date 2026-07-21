#!/usr/bin/env python3
"""Fresh-VM verify-only adopter for the sealed Ginkgo VCPI E2E revision."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from pert_gym.logical_sparse_zarr import read_logical_sparse_revision
from tools.lamin_context import connect_pertdata

HERE = Path(__file__).resolve().parent
RUNNER_PATH = HERE / "run_e2e.py"
EXPECTED_GIT_HEAD = "ef33dc21bc50ad8d96b6f58066e5958949886b83"
SPEC = importlib.util.spec_from_file_location("ginkgo_vcpi_run_e2e", RUNNER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load exact mutation runner")
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)

STAGES = [
    "shared-var",
    "payload",
    "manifest",
    "migration-map",
    "collection-manifest",
    "collection",
    "promotion",
]
DESCRIPTIONS = {
    "shared-var": "logical sparse-Zarr shared var",
    "payload": "logical sparse-Zarr immutable payload",
    "manifest": "logical sparse-Zarr candidate manifest",
    "migration-map": "legacy to logical sparse migration map",
    "collection-manifest": "logical sparse candidate collection manifest",
    "promotion": "atomic logical sparse candidate promotion marker",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def resolve_git_state(start: Path = HERE) -> dict[str, object]:
    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(start), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    root = Path(git("rev-parse", "--show-toplevel")).resolve()
    head = git("rev-parse", "HEAD")
    dirty_paths = git("status", "--porcelain", "--untracked-files=no").splitlines()
    if head != EXPECTED_GIT_HEAD:
        raise AssertionError(
            f"verifier checkout drift: expected {EXPECTED_GIT_HEAD}, observed {head}"
        )
    if dirty_paths:
        raise AssertionError(f"verifier checkout has tracked changes: {dirty_paths}")
    return {"root": str(root), "head": head, "tracked_dirty": False}


def safely_extract_payload(payload: Path, destination: Path) -> str:
    """Hash, inspect, and extract one archive stream without link traversal."""
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    digest = hashlib.sha256()
    with payload.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
        handle.seek(0)
        with tarfile.open(fileobj=handle, mode="r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                member_path = PurePosixPath(member.name)
                if (
                    member_path.is_absolute()
                    or ".." in member_path.parts
                    or not member.name
                ):
                    raise AssertionError(f"unsafe archive member path: {member.name!r}")
                try:
                    (root / Path(*member_path.parts)).resolve().relative_to(root)
                except ValueError as error:
                    raise AssertionError(
                        f"archive member escapes extraction root: {member.name!r}"
                    ) from error
                if member.issym() or member.islnk():
                    raise AssertionError(
                        f"archive links are forbidden: {member.name!r} -> {member.linkname!r}"
                    )
                if not (member.isfile() or member.isdir()):
                    raise AssertionError(
                        f"unsupported archive member type: {member.name!r}"
                    )
            archive.extractall(root, members=members, filter="data")
    return digest.hexdigest()


def artifact_paths(shared_key: str) -> dict[str, str]:
    prefix = f"{runner.LOGICAL_KEY}/revisions/{runner.REVISION}"
    return {
        "shared-var": shared_key,
        "payload": f"{prefix}/payload.tar.gz",
        "manifest": f"{prefix}/manifest.json",
        "migration-map": f"{prefix}/migration-map.json",
        "collection-manifest": f"{prefix}/collection-manifest.json",
        "promotion": f"{runner.LOGICAL_KEY}/promotions/{runner.REVISION}.json",
    }


def verify_sealed_publication(ln: Any) -> dict[str, Any]:
    triplet_state = runner.preflight(ln)
    triplet = runner.materialize_triplet(triplet_state)
    manifest = runner.build_or_read(triplet)
    parity = runner.verify_local_parity(triplet)
    surface, _matrix, _obs, _var = read_logical_sparse_revision(
        runner.ROOT, runner.LOGICAL_KEY, runner.REVISION
    )
    manifest_path = (
        runner.ROOT
        / runner.LOGICAL_KEY
        / "revisions"
        / runner.REVISION
        / "manifest.json"
    )
    shared_key = str(manifest["shared_var"]["key"])
    paths = artifact_paths(shared_key)
    migration_map = {
        "format": "pert-gym.legacy-to-logical-map/v1",
        "logical_key": runner.LOGICAL_KEY,
        "revision": runner.REVISION,
        "source_identity": manifest.get("source_identity"),
        "chunks": [
            {
                "chunk": index,
                "start": chunk.start,
                "end": chunk.end,
                "key": chunk.key,
            }
            for index, chunk in enumerate(surface.chunks)
        ],
        "rollback": {"strategy": "append-only", "rejected_candidate_retained": True},
    }
    collection_manifest = {
        "format": "pert-gym.logical-sparse-zarr.collection/v1",
        "collection_key": runner.CANDIDATE_COLLECTION_KEY,
        "logical_key": runner.LOGICAL_KEY,
        "revision": runner.REVISION,
        "manifest_key": paths["manifest"],
        "migration_map_key": paths["migration-map"],
        "shape": list(surface.shape),
        "nnz": surface.nnz,
    }
    identity = {
        "logical_key": runner.LOGICAL_KEY,
        "revision": runner.REVISION,
        "collection_key": runner.CANDIDATE_COLLECTION_KEY,
        "manifest_sha256": sha256_file(manifest_path),
        "shared_var_key": shared_key,
        "shared_var_sha256": sha256_file(runner.ROOT / shared_key),
        "migration_map_sha256": sha256_json(migration_map),
        "collection_manifest_sha256": sha256_json(collection_manifest),
    }

    records = list(ln.Artifact.filter(key__in=list(paths.values())).all())
    by_key = {str(record.key): record for record in records}
    if len(records) != len(paths) or set(by_key) != set(paths.values()):
        raise AssertionError(
            "sealed remote publication prefix is incomplete or duplicated"
        )
    for stage, key in paths.items():
        record = by_key[key]
        if (
            record.description != DESCRIPTIONS[stage]
            or getattr(record, "revises", None) is not None
        ):
            raise AssertionError(f"sealed remote stage identity drift: {stage}")
    collections = list(ln.Collection.filter(key=runner.CANDIDATE_COLLECTION_KEY).all())
    if len(collections) != 1:
        raise AssertionError("sealed candidate Collection is absent or duplicated")
    member_keys = [str(member.key) for member in collections[0].artifacts.all()]
    expected_member_keys = [
        paths[name]
        for name in (
            "shared-var",
            "payload",
            "manifest",
            "migration-map",
            "collection-manifest",
        )
    ]
    if (
        collections[0].description
        != "append-only logical sparse-Zarr candidate metadata"
        or member_keys != expected_member_keys
        or getattr(collections[0], "revises", None) is not None
    ):
        raise AssertionError("sealed candidate Collection identity drift")

    expected_json = {
        "manifest": json.loads(manifest_path.read_text(encoding="utf-8")),
        "migration-map": migration_map,
        "collection-manifest": collection_manifest,
        "promotion": {
            "format": "pert-gym.logical-sparse-zarr.promotion/v1",
            "logical_key": runner.LOGICAL_KEY,
            "revision": runner.REVISION,
            "candidate_collection_key": runner.CANDIDATE_COLLECTION_KEY,
            "candidate_collection_uid": str(collections[0].uid),
            "manifest_key": paths["manifest"],
            "rollback_to": None,
            "state": "candidate-complete",
        },
    }
    remote_paths = {
        stage: Path(record.cache())
        for stage, record in ((stage, by_key[key]) for stage, key in paths.items())
    }
    if sha256_file(remote_paths["shared-var"]) != identity["shared_var_sha256"]:
        raise AssertionError("shared var remote readback checksum drift")
    for stage, expected in expected_json.items():
        observed = json.loads(remote_paths[stage].read_text(encoding="utf-8"))
        if observed != expected:
            raise AssertionError(f"remote JSON stage semantic drift: {stage}")

    local_files: dict[str, str] = {}
    candidate_root = runner.ROOT / runner.LOGICAL_KEY / "revisions" / runner.REVISION
    for base in (candidate_root, runner.ROOT / shared_key):
        members = (
            [base]
            if base.is_file()
            else [item for item in base.rglob("*") if item.is_file()]
        )
        for item in members:
            if item.name == "publication-journal.json":
                continue
            relative = item.relative_to(runner.ROOT).as_posix()
            local_files[relative] = sha256_file(item)
    with tempfile.TemporaryDirectory(prefix="ginkgo_vcpi_remote_payload_") as temporary:
        extract_root = Path(temporary)
        payload_sha256 = safely_extract_payload(remote_paths["payload"], extract_root)
        remote_files = {
            item.relative_to(extract_root).as_posix(): sha256_file(item)
            for item in extract_root.rglob("*")
            if item.is_file() and item.name != "publication-journal.json"
        }
        remote_journals = [
            item.relative_to(extract_root).as_posix()
            for item in extract_root.rglob("publication-journal.json")
        ]
        remote_surface, remote_matrix, remote_obs, remote_var = (
            read_logical_sparse_revision(
                extract_root, runner.LOGICAL_KEY, runner.REVISION
            )
        )
        runner.assert_frame_equal(
            remote_obs,
            runner.pd.read_parquet(triplet.obs_path),
            check_exact=True,
            check_dtype=True,
            check_names=True,
        )
        runner.assert_frame_equal(
            remote_var,
            runner.pd.read_parquet(triplet.var_path),
            check_exact=True,
            check_dtype=True,
            check_names=True,
        )
        source = runner.ad.read_h5ad(triplet.x_path, backed="r")
        remote_rows_checked = 0
        remote_nnz_checked = 0
        try:
            for block in remote_surface.chunks:
                expected = source.X[block.start : block.end].tocsr()
                observed = remote_matrix[block.start : block.end].tocsr()
                if not (
                    runner.np.array_equal(expected.indptr, observed.indptr)
                    and runner.np.array_equal(expected.indices, observed.indices)
                    and runner.np.array_equal(expected.data, observed.data)
                ):
                    raise AssertionError(
                        f"remote payload matrix parity drift at {block.start}:{block.end}"
                    )
                remote_rows_checked += block.end - block.start
                remote_nnz_checked += int(observed.nnz)
        finally:
            source.file.close()
        remote_semantic_parity = {
            "rows_checked": remote_rows_checked,
            "nnz_checked": remote_nnz_checked,
            "obs_rows_checked": len(remote_obs),
            "var_rows_checked": len(remote_var),
            "mismatch_count": 0,
        }
    if remote_files != local_files:
        missing_remote = sorted(set(local_files) - set(remote_files))
        extra_remote = sorted(set(remote_files) - set(local_files))
        mismatched = sorted(
            key
            for key in set(local_files) & set(remote_files)
            if local_files[key] != remote_files[key]
        )
        nondeterministic_zarr_bytes = all(
            ".zarr/data/" in key or ".zarr/indices/" in key for key in mismatched
        )
        if missing_remote or extra_remote or not nondeterministic_zarr_bytes:
            raise AssertionError(
                "remote payload semantic file/checksum parity drift: "
                f"missing_remote={missing_remote}, extra_remote={extra_remote}, "
                f"mismatched={mismatched}"
            )

    publication = {
        "promotion_uid": str(by_key[paths["promotion"]].uid),
        "manifest_key": paths["manifest"],
    }
    first = runner.reconcile_collections(
        ln, triplet_state, publication, allow_create=False
    )
    replay = runner.reconcile_collections(
        ln, triplet_state, publication, allow_create=False
    )
    if first["writes"] != 0 or replay["writes"] != 0:
        raise AssertionError("verify-only Collection reconciliation attempted writes")
    return {
        "format": "pert-gym.ginkgo-vcpi-fresh-readback/v1",
        "identity": identity,
        "remote_stage_uids": {
            stage: str(by_key[key].uid) for stage, key in paths.items()
        },
        "candidate_collection_uid": str(collections[0].uid),
        "parity": parity,
        "remote_payload_semantic_parity": remote_semantic_parity,
        "remote_payload_sha256": payload_sha256,
        "payload_semantic_file_count": len(local_files),
        "payload_remote_journal_members_excluded": remote_journals,
        "dataset_collection": first["dataset_collection"],
        "global_successor": first["global_successor"],
        "replay": {
            "writes": replay["writes"],
            "counts_stable": (
                replay["dataset_collection"] == first["dataset_collection"]
                and replay["global_successor"] == first["global_successor"]
            ),
        },
    }


def main() -> int:
    heartbeat = runner.ProductHeartbeat()
    heartbeat.start()
    ln = None
    try:
        git_state = resolve_git_state()
        ln = connect_pertdata()
        before = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
            "runs": ln.Run.filter().count(),
        }
        heartbeat.transition("writing")
        readback = verify_sealed_publication(ln)
        after = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
            "runs": ln.Run.filter().count(),
        }
        if before != after:
            raise AssertionError(
                f"verify-only registry counts changed: {before} -> {after}"
            )
        receipt = {
            "format": "pert-gym.ginkgo-vcpi-fresh-readback-receipt/v1",
            "task_id": runner.TASK_ID,
            "status": "PASS",
            "verifier_sha256": sha256_file(Path(__file__).resolve()),
            "mutation_helper_sha256": sha256_file(RUNNER_PATH),
            "git": git_state,
            "registry_counts_before": before,
            "registry_counts_after": after,
            "readback": readback,
        }
        receipt["canonical_sha256"] = sha256_json(receipt)
        heartbeat.transition("accepted", current=1)
        print(
            "HERMES_VERIFY_RECEIPT=" + json.dumps(receipt, sort_keys=True), flush=True
        )
        return 0
    except Exception:
        heartbeat.transition("failed")
        raise
    finally:
        heartbeat.stop()


if __name__ == "__main__":
    raise SystemExit(main())
