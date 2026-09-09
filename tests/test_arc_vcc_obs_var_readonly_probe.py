from types import SimpleNamespace

import pandas as pd
import pytest

from tools import arc_vcc_obs_var_readonly_probe as probe


class _Filter:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _ArtifactRegistry:
    def __init__(self, artifacts):
        self._artifacts = artifacts

    def filter(self, **lookup):
        rows = [
            artifact
            for artifact in self._artifacts
            if all(getattr(artifact, key) == value for key, value in lookup.items())
        ]
        return _Filter(rows)


def test_resolve_artifact_accepts_objects_uid_and_key() -> None:
    artifact = SimpleNamespace(uid="uid-1", key="dataset/obs.parquet")
    ln = SimpleNamespace(Artifact=_ArtifactRegistry([artifact]))

    assert probe.resolve_artifact(ln, artifact) is artifact
    assert probe.resolve_artifact(ln, artifact.uid) is artifact
    assert probe.resolve_artifact(ln, artifact.key) is artifact
    with pytest.raises(TypeError, match="cannot resolve"):
        probe.resolve_artifact(ln, "missing")


def test_frame_summary_distinguishes_substantive_values_from_placeholders() -> None:
    frame = pd.DataFrame(
        {
            "organism": ["Homo sapiens", "unknown", None],
            "batch": ["Flex_1_01", "Flex_1_01", "Flex_2_03"],
        },
        index=pd.Index(["cell-a", "cell-b", "cell-c"], name="cell_id"),
    )

    summary = probe.frame_summary(frame)

    assert summary["rows"] == 3
    assert summary["columns"] == 2
    assert summary["index_name"] == "cell_id"
    assert summary["index_unique"] is True
    assert summary["fields"]["organism"]["nonmissing"] == 2
    assert summary["fields"]["organism"]["substantive"] == 1
    assert summary["fields"]["organism"]["examples"] == ["Homo sapiens"]
    assert summary["fields"]["batch"]["unique_nonmissing"] == 2
