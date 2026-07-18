#!/usr/bin/env python3
"""Regression checks for fail-closed Odd001137 revision recovery."""
from __future__ import annotations

import importlib.util
import io
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MemoryFS:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, str] = {}
        self.write_count = 0

    @staticmethod
    def _key(key: str) -> str:
        return key.split("#", 1)[0]

    def exists(self, key: str) -> bool:
        return self._key(key) in self.objects

    def find(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix + "/"))

    def info(self, key: str) -> dict[str, Any]:
        key = self._key(key)
        return {
            "name": key,
            "size": len(self.objects[key]),
            "generation": self.generations[key],
        }

    def open(self, key: str, mode: str):
        key = self._key(key)
        if mode == "rb":
            return io.BytesIO(self.objects[key])
        if mode != "xb" or key in self.objects:
            raise FileExistsError(key)
        fs = self

        class Writer(io.BytesIO):
            def close(self) -> None:
                if not self.closed:
                    fs.objects[key] = self.getvalue()
                    fs.generations[key] = str(len(fs.generations) + 1)
                    fs.write_count += 1
                super().close()

        return Writer()


def expect_runtime_error(fragment: str, function, *args) -> None:
    try:
        function(*args)
    except RuntimeError as exc:
        assert fragment in str(exc), str(exc)
    else:
        raise AssertionError(f"expected RuntimeError containing {fragment!r}")


def main() -> int:
    builder = load_module("row95_builder_recovery", ROOT / "build_component.py")
    prefix = "bucket/logical/revisions/rev"
    keys = builder.revision_stage_keys(prefix, "GSE158999")
    fs = MemoryFS()

    with tempfile.TemporaryDirectory(prefix="row95-recovery-") as raw_tmp:
        tmp = Path(raw_tmp)
        local_by_stage: dict[str, Path] = {}
        for index, (stage, key) in enumerate(keys.items(), start=1):
            local = tmp / f"{stage}.payload"
            payload = (
                builder.json_bytes(
                    {"provenance": builder.manifest_provenance("script-sha")}
                )
                if stage == "manifest"
                else f"identity-matching-{stage}".encode()
            )
            local.write_bytes(payload)
            local_by_stage[stage] = local
            first = builder.remote_adopt_or_upload(fs, local, key)
            assert fs.write_count == index
            adopted = builder.remote_adopt_or_upload(fs, local, key)
            assert fs.write_count == index
            assert adopted == first
            assert builder.inspect_revision_state(fs, prefix, keys) == list(keys)[:index]

        assert builder.manifest_provenance("script-sha") == builder.manifest_provenance(
            "script-sha"
        )
        assert "started_unix" not in builder.manifest_provenance("script-sha")
        assert "finished_unix" not in builder.manifest_provenance("script-sha")
        rebuilt_manifest = tmp / "rebuilt-manifest.json"
        rebuilt_manifest.write_bytes(
            builder.json_bytes(
                {"provenance": builder.manifest_provenance("script-sha")}
            )
        )
        writes_before_manifest_resume = fs.write_count
        builder.remote_adopt_or_upload(fs, rebuilt_manifest, keys["manifest"])
        assert fs.write_count == writes_before_manifest_resume

        fs.objects[keys["obs"]] = b"drifted-payload"
        expect_runtime_error(
            "existing immutable object identity mismatch",
            builder.remote_adopt_or_upload,
            fs,
            local_by_stage["obs"],
            keys["obs"],
        )
        fs.objects[keys["obs"]] = local_by_stage["obs"].read_bytes()

        fs.objects = {keys["X"]: b"x"}
        fs.generations = {keys["X"]: "1"}
        expect_runtime_error(
            "contiguous prefix", builder.inspect_revision_state, fs, prefix, keys
        )

        fs.objects = {
            keys["obs"]: local_by_stage["obs"].read_bytes(),
            prefix + "/unexpected.bin": b"x",
        }
        fs.generations = {key: str(index + 1) for index, key in enumerate(fs.objects)}
        expect_runtime_error(
            "unexpected immutable revision objects",
            builder.inspect_revision_state,
            fs,
            prefix,
            keys,
        )

    print("REVISION_RECOVERY_REGRESSION_PASS every-stage+drift+hole+extra")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
