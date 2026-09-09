from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pytest

from tools import broad_prism_prewrite_safety as safety


def _candidate() -> dict[str, object]:
    return {
        "uid": "candidate-1",
        "key": "broad_prism_repurposing/obs.parquet",
        "sha256": "a" * 64,
        "bytes": 123,
        "path": "local://candidate",
        "rows": 4,
    }


def _state(candidate: dict[str, object] | None = None) -> dict[str, object]:
    predecessor = {
        "uid": "predecessor-1",
        "key": "broad_prism_repurposing/obs.parquet",
        "sha256": "b" * 64,
        "bytes": 120,
        "path": "local://predecessor",
        "rows": 4,
    }
    obs = candidate or predecessor
    return {
        "artifacts": [predecessor, obs],
        "triplet": {
            "obs": obs,
            "X": {
                "uid": "x-1",
                "key": "broad_prism_repurposing/X.h5ad",
                "sha256": "c" * 64,
                "bytes": 1,
                "path": "local://x",
                "n_obs": 4,
                "n_vars": 0,
            },
            "var": {
                "uid": "var-1",
                "key": "broad_prism_repurposing/var.parquet",
                "sha256": "d" * 64,
                "bytes": 1,
                "path": "local://var",
                "rows": 0,
            },
            "links": {"obs_to_x": "x-1", "x_to_var": "var-1"},
        },
        "collections": [
            {
                "uid": "collection-1",
                "name": "all",
                "version": "1",
                "hash": "e" * 64,
                "members": [
                    predecessor,
                    {
                        "uid": "member-2",
                        "key": "other/member.parquet",
                        "sha256": "h" * 64,
                    },
                ],
            }
        ],
        "unrelated": {
            "artifacts": [
                {"uid": "other-1", "key": "other/obs.parquet", "sha256": "f" * 64}
            ],
            "collections": [
                {"uid": "other-c", "hash": "g" * 64, "members": ["other-1"]}
            ],
        },
    }


def _plan(tmp_path: Path) -> dict[str, object]:
    script = tmp_path / "script.py"
    script.write_text("# local fixture\n", encoding="utf-8")
    return safety.seal_document(
        {
            "format": safety.PLAN_FORMAT,
            "task_id": "t_dad3bbb9",
            "dataset_id": "broad_prism_repurposing",
            "predecessor": _state()["triplet"]["obs"],
            "candidate": _candidate(),
            "code": {"commit": "1" * 40, "script_sha256": safety.sha256_file(script)},
            "sources": {
                "release": "PRISM 24Q2",
                "lfc": {
                    "uri": "local://lfc",
                    "generation": "1",
                    "bytes": 1,
                    "sha256": "2" * 64,
                },
            },
        }
    )


def _authorization(plan: dict[str, object]) -> dict[str, object]:
    return safety.seal_document(
        {
            "format": safety.AUTHORIZATION_FORMAT,
            "task_id": "t_dad3bbb9",
            "dataset_id": "broad_prism_repurposing",
            "plan_sha256": plan["document_sha256"],
            "approved": True,
        }
    )


def test_crash_after_candidate_save_recovers_exactly_once_without_second_save(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    authorization = _authorization(plan)
    calls = 0
    live = _state()

    def save() -> dict[str, object]:
        nonlocal calls, live
        calls += 1
        live = _state(_candidate())
        return _candidate()

    with pytest.raises(safety.CrashAfterCandidateSave):
        safety.execute_one_write_transition(
            run_root=tmp_path,
            plan=plan,
            authorization=authorization,
            candidates=lambda: [],
            save_candidate=save,
            before_state=_state(),
            fresh_state=lambda: live,
            crash_after_candidate_save=True,
        )
    assert calls == 1
    receipt = safety.execute_one_write_transition(
        run_root=tmp_path,
        plan=plan,
        authorization=authorization,
        candidates=lambda: [_candidate()],
        save_candidate=save,
        before_state=_state(),
        fresh_state=lambda: live,
    )
    assert calls == 1
    assert receipt["replay"]["writes"] == 0
    assert receipt["terminal_state"] == "sealed"


@pytest.mark.parametrize(
    "mutation", ["missing", "truncated", "digest", "state", "receipt", "terminal"]
)
def test_verify_rejects_missing_incomplete_or_tampered_evidence(
    tmp_path: Path, mutation: str
) -> None:
    plan = _plan(tmp_path)
    authorization = _authorization(plan)
    live = _state(_candidate())
    safety.execute_one_write_transition(
        run_root=tmp_path,
        plan=plan,
        authorization=authorization,
        candidates=lambda: [_candidate()],
        save_candidate=lambda: pytest.fail("must not save during recovery"),
        before_state=_state(),
        fresh_state=lambda: live,
    )
    paths = safety.evidence_paths(tmp_path)
    target = paths["receipt"] if mutation == "receipt" else paths["terminal"]
    if mutation == "missing":
        target.unlink()
    elif mutation == "truncated":
        target.write_text("{", encoding="utf-8")
    elif mutation == "digest":
        document = json.loads(target.read_text(encoding="utf-8"))
        document["dataset_id"] = "tampered"
        target.write_text(json.dumps(document), encoding="utf-8")
    elif mutation == "state":
        document = json.loads(target.read_text(encoding="utf-8"))
        document["terminal_state"] = "unsealed"
        target.write_text(json.dumps(document), encoding="utf-8")
    elif mutation == "receipt":
        document = json.loads(paths["receipt"].read_text(encoding="utf-8"))
        document["candidate"]["uid"] = "resealed-but-wrong"
        document = safety.seal_document(document)
        paths["receipt"].write_text(json.dumps(document), encoding="utf-8")
    elif mutation == "terminal":
        paths["terminal"].unlink()
    with pytest.raises(safety.EvidenceError):
        safety.verify_zero_write_evidence(tmp_path, fresh_state=lambda: live)


@pytest.mark.parametrize(
    "path",
    [
        "collection_hash",
        "collection_order",
        "collection_member",
        "unrelated_artifact",
        "unrelated_collection",
    ],
)
def test_independent_snapshot_rejects_collection_and_unrelated_drift(path: str) -> None:
    before = _state()
    after = _state(_candidate())
    if path == "collection_hash":
        after["collections"][0]["hash"] = "z" * 64
    elif path == "collection_order":
        after["collections"][0]["members"] = list(
            reversed(after["collections"][0]["members"])
        )
    elif path == "collection_member":
        after["collections"][0]["members"].append(_candidate())
    elif path == "unrelated_artifact":
        after["unrelated"]["artifacts"][0]["sha256"] = "z" * 64
    else:
        after["unrelated"]["collections"][0]["hash"] = "z" * 64
    with pytest.raises(safety.DriftError):
        safety.verify_authorized_transition(before, after, _candidate())


def test_transition_execution_rejects_drift_before_terminal_evidence(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    authorization = _authorization(plan)
    live = _state(_candidate())
    live["unrelated"]["collections"][0]["hash"] = "z" * 64
    with pytest.raises(safety.DriftError):
        safety.execute_one_write_transition(
            run_root=tmp_path,
            plan=plan,
            authorization=authorization,
            candidates=lambda: [_candidate()],
            save_candidate=lambda: pytest.fail("must not save during recovery"),
            before_state=_state(),
            fresh_state=lambda: live,
        )
    assert not safety.evidence_paths(tmp_path)["terminal"].exists()


@pytest.mark.parametrize("field", ["uid", "key", "sha256", "path", "rows"])
def test_identity_or_link_or_dimension_drift_is_rejected(field: str) -> None:
    before = _state()
    after = _state(_candidate())
    if field == "rows":
        after["triplet"]["X"]["n_obs"] = 5
    else:
        after["triplet"]["X"][field] = "wrong" if field != "rows" else 5
    with pytest.raises(safety.DriftError):
        safety.verify_authorized_transition(before, after, _candidate())
    after = _state(_candidate())
    after["triplet"]["links"]["obs_to_x"] = "wrong-x"
    with pytest.raises(safety.DriftError):
        safety.verify_authorized_transition(before, after, _candidate())


def test_ordered_uuid_identity_uses_streaming_digest_and_rejects_order_or_duplicates() -> (
    None
):
    rows = [(0, str(uuid.uuid1())), (1, str(uuid.uuid1())), (2, str(uuid.uuid1()))]
    expected = safety.ordered_obs_identity(rows)
    assert safety.ordered_obs_identity(iter(rows)) == expected
    with pytest.raises(safety.IdentityError):
        safety.verify_ordered_obs_identity(expected, [rows[1], rows[0], rows[2]])
    with pytest.raises(safety.IdentityError):
        safety.ordered_obs_identity([(0, rows[0][1]), (1, rows[0][1])])


def test_streaming_transform_has_constant_batch_envelope_at_production_denominator() -> (
    None
):
    emitted: list[tuple[int, int]] = []
    result = safety.stream_batches(
        (
            range(start, min(start + 250_000, 22_316_860))
            for start in range(0, 22_316_860, 250_000)
        ),
        batch_limit=250_000,
        emit=lambda batch: emitted.append((batch.start, batch.stop)),
    )
    assert result == {"rows": 22_316_860, "peak_rows": 250_000, "peak_batches": 1}
    assert len(emitted) == 90


@pytest.mark.parametrize("value, expected", [("TRUE", True), ("false", False)])
def test_pass_tokens_are_explicit(value: str, expected: bool) -> None:
    assert safety.parse_pass(value) is expected


@pytest.mark.parametrize("value", ["", "yes", "unknown", "0"])
def test_unknown_pass_and_treatment_values_fail_closed(value: str) -> None:
    with pytest.raises(ValueError):
        safety.parse_pass(value)
    with pytest.raises(ValueError):
        safety.parse_treatment_type(value)
    with pytest.raises(safety.ReviewRequired):
        safety.parse_treatment_type("trt_poscon")
    assert safety.parse_treatment_type("trt_cp") == "trt_cp"


def test_source_coordinates_keep_physical_csv_line_across_skips_and_chunk_boundaries() -> (
    None
):
    assert safety.source_coordinates(0, 250_000) == {
        "source_file_row_number": 2,
        "source_row_chunk_index": 0,
        "source_row_offset_in_chunk": 0,
    }
    assert (
        safety.source_coordinates(249_999, 250_000)["source_row_offset_in_chunk"]
        == 249_999
    )
    assert safety.source_coordinates(250_000, 250_000) == {
        "source_file_row_number": 250_002,
        "source_row_chunk_index": 1,
        "source_row_offset_in_chunk": 0,
    }


def test_partial_file_recovery_is_deterministic_and_ambiguous_pair_fails_closed(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "input.csv"
    destination.write_bytes(b"valid")
    digest = hashlib.sha256(b"valid").hexdigest()
    assert (
        safety.recover_file(
            destination, digest, rebuild=lambda path: pytest.fail("no rebuild")
        )
        == "reused"
    )
    destination.with_suffix(".csv.part").write_bytes(b"partial")
    with pytest.raises(safety.RecoveryError):
        safety.recover_file(destination, digest, rebuild=lambda path: None)
    destination.unlink()
    assert (
        safety.recover_file(
            destination, digest, rebuild=lambda path: path.write_bytes(b"valid")
        )
        == "rebuilt"
    )
