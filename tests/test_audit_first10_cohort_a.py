from __future__ import annotations

import pandas as pd

from tools.audit_first10_cohort_a import (
    dataframe_summary,
    exact_link,
    ordered_string_sha256,
    var_identifier_audit,
)


class _Artifact:
    def __init__(self, key: str, uid: str, values: dict | None = None):
        self.key = key
        self.uid = uid
        self.features = type(
            "Features",
            (),
            {"get_values": lambda _self: values or {}},
        )()


class _Query:
    def __init__(self, artifacts: list[_Artifact]):
        self.artifacts = artifacts

    def order_by(self, _field: str) -> _Query:
        return self

    def all(self) -> list[_Artifact]:
        return self.artifacts


class _ArtifactManager:
    def __init__(self, artifacts: list[_Artifact]):
        self.artifacts = artifacts

    def filter(self, *, is_latest: bool, key: str) -> _Query:
        assert is_latest is True
        return _Query([artifact for artifact in self.artifacts if artifact.key == key])

    def get(self, *, uid: str) -> _Artifact:
        return next(artifact for artifact in self.artifacts if artifact.uid == uid)


class _Lamin:
    def __init__(self, artifacts: list[_Artifact]):
        self.Artifact = _ArtifactManager(artifacts)


def test_ordered_string_hash_is_order_sensitive_and_framed() -> None:
    assert ordered_string_sha256(["ab", "c"]) != ordered_string_sha256(["a", "bc"])
    assert ordered_string_sha256(["a", "b"]) != ordered_string_sha256(["b", "a"])


def test_var_identifier_audit_distinguishes_reported_corruption_forms() -> None:
    index = pd.Index(["FBgn0000001\talpha", r"FBgn0000002\tbeta", "FBgn0000003/tgamma"])

    audit = var_identifier_audit(index)

    assert audit["patterns"]["actual_tab"]["count"] == 1
    assert audit["patterns"]["literal_backslash_t"]["count"] == 1
    assert audit["patterns"]["literal_slash_t"]["count"] == 1
    assert audit["tab_split_structure"]["first_field_examples"][0] == "FBgn0000001"
    assert audit["tab_split_structure"]["second_field_examples"][0] == "alpha"


def test_dataframe_summary_inventories_every_column() -> None:
    frame = pd.DataFrame(
        {
            "constant": ["human", "human", "human"],
            "partially_missing": ["a", None, "b"],
        },
        index=pd.Index(["c1", "c2", "c3"], name="cell_id"),
    )

    summary = dataframe_summary(frame)

    assert summary["shape"] == [3, 2]
    assert summary["index_name"] == "cell_id"
    assert set(summary["columns"]) == set(frame.columns)
    assert summary["columns"]["constant"]["unique_non_null"] == 1
    assert summary["columns"]["partially_missing"]["missing"] == 1


def test_exact_link_resolves_string_key_to_unique_current_uid() -> None:
    target = _Artifact("dataset/X.h5ad", "x-uid")
    source = _Artifact("dataset/obs.parquet", "obs-uid", {"X": target.key})

    link = exact_link(_Lamin([target]), source, "X")

    assert link["resolved"] is True
    assert link["key"] == target.key
    assert link["uid"] == target.uid
    assert link["latest_exact_key_candidate_count"] == 1


def test_exact_link_fails_closed_on_duplicate_current_key() -> None:
    first = _Artifact("dataset/var.parquet", "var-1")
    second = _Artifact("dataset/var.parquet", "var-2")
    source = _Artifact("dataset/X.h5ad", "x-uid", {"var": first.key})

    link = exact_link(_Lamin([first, second]), source, "var")

    assert link["resolved"] is False
    assert link["latest_exact_key_candidate_count"] == 2
