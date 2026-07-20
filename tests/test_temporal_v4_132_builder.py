from __future__ import annotations

import importlib.util
import io
from pathlib import Path
from typing import BinaryIO

import pytest

ROOT = Path(__file__).resolve().parents[1]
BUILDER_PATH = (
    ROOT
    / "artifacts/evidence/scp1846-rgc-survival-temporal-v4-132-t_03c886aa/build_component.py"
)


def load_builder():
    spec = importlib.util.spec_from_file_location("temporal_v4_132_builder", BUILDER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_verifier():
    path = BUILDER_PATH.with_name("verify_component.py")
    spec = importlib.util.spec_from_file_location("temporal_v4_132_verifier", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _CommitBuffer(io.BytesIO):
    def __init__(self, fs: "VersionedMemoryFS", key: str) -> None:
        super().__init__()
        self.fs = fs
        self.key = key

    def close(self) -> None:
        if not self.closed:
            self.fs.put(self.key, self.getvalue())
        super().close()


class VersionedMemoryFS:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.write_count = 0

    def put(self, key: str, value: bytes) -> None:
        self.write_count += 1
        self.objects[key] = (value, str(self.write_count))

    def exists(self, key: str) -> bool:
        return key in self.objects

    def open(self, key: str, mode: str) -> BinaryIO:
        if mode == "xb":
            if key in self.objects:
                raise FileExistsError(key)
            return _CommitBuffer(self, key)
        if mode == "rb":
            raw_key, separator, generation = key.rpartition("#")
            lookup_key = raw_key if separator else key
            value, stored_generation = self.objects[lookup_key]
            if separator and generation != stored_generation:
                raise FileNotFoundError(key)
            return io.BytesIO(value)
        raise ValueError(mode)

    def info(self, key: str) -> dict[str, object]:
        value, generation = self.objects[key]
        return {"size": len(value), "generation": generation}


def test_timepoint_is_canonical_integer_minutes_with_day_provenance() -> None:
    builder = load_builder()

    assert builder.timepoint_fields("NoCrush", 0.0) == {
        "raw_time_label": "NoCrush",
        "timepoint": 0,
        "timepoint_original_value": 0,
        "timepoint_original_unit": "day",
    }
    assert builder.timepoint_fields("ONC2D", 2.0)["timepoint"] == 2_880
    assert builder.timepoint_fields("ONC7d", 7.0)["timepoint"] == 10_080
    assert builder.timepoint_fields("ONC21d", 21.0)["timepoint"] == 30_240
    assert isinstance(builder.timepoint_fields("ONC21d", 21.0)["timepoint"], int)


def test_verifier_rejects_day_values_in_canonical_timepoint() -> None:
    verifier = load_verifier()
    corrected = verifier.pd.DataFrame(
        [
            {
                "raw_time_label": label,
                "timepoint": minutes,
                "timepoint_original_value": days,
                "timepoint_original_unit": "day",
            }
            for label, (minutes, days) in verifier.EXPECTED_TIMEPOINTS.items()
        ]
    )
    verifier.validate_canonical_timepoints(corrected)
    corrected["timepoint"] = corrected["timepoint_original_value"]

    with pytest.raises(RuntimeError, match="canonical timepoint conversion mismatch"):
        verifier.validate_canonical_timepoints(corrected)


@pytest.mark.parametrize("crash_after", [1, 2])
def test_revision_retry_adopts_identity_matching_uploaded_prefix(
    tmp_path: Path, crash_after: int
) -> None:
    builder = load_builder()
    fs = VersionedMemoryFS()
    paths = []
    for index, contents in enumerate((b"obs", b"matrix", b"var")):
        path = tmp_path / f"member-{index}"
        path.write_bytes(contents)
        paths.append(path)

    first = []
    for index, path in enumerate(paths[:crash_after]):
        first.append(
            builder.upload_or_adopt_immutable(
                fs, path, f"revision/member-{index}", role=f"member-{index}"
            )
        )
    writes_before_retry = fs.write_count

    retried = [
        builder.upload_or_adopt_immutable(
            fs, path, f"revision/member-{index}", role=f"member-{index}"
        )
        for index, path in enumerate(paths)
    ]

    assert fs.write_count == writes_before_retry + len(paths) - crash_after
    assert retried[:crash_after] == first
    assert [item["generation"] for item in retried] == ["1", "2", "3"]


def test_revision_retry_rejects_existing_stage_identity_drift(tmp_path: Path) -> None:
    builder = load_builder()
    fs = VersionedMemoryFS()
    path = tmp_path / "obs.parquet"
    path.write_bytes(b"expected")
    builder.upload_or_adopt_immutable(fs, path, "revision/obs.parquet", role="obs")
    path.write_bytes(b"drifted")
    writes_before_retry = fs.write_count

    with pytest.raises(RuntimeError, match="identity mismatch"):
        builder.upload_or_adopt_immutable(
            fs, path, "revision/obs.parquet", role="obs"
        )

    assert fs.write_count == writes_before_retry
    assert fs.objects["revision/obs.parquet"][0] == b"expected"
