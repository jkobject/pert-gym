#!/usr/bin/env python3
"""Regression checks for complete evidence-seal inventory verification."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def entry(root: Path, relative: str) -> dict[str, object]:
    payload = (root / relative).read_bytes()
    return {
        "path": relative,
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def expect_failure(fragment: str, function, *args) -> None:
    try:
        function(*args)
    except (AssertionError, RuntimeError) as exc:
        assert fragment in str(exc), str(exc)
    else:
        raise AssertionError(f"expected failure containing {fragment!r}")


def main() -> int:
    verifier = load_module("row95_evidence_verifier", ROOT / "verify_evidence.py")
    with tempfile.TemporaryDirectory(prefix="row95-evidence-seal-") as raw_tmp:
        packet = Path(raw_tmp)
        (packet / "a.txt").write_text("alpha\n")
        (packet / "nested").mkdir()
        (packet / "nested/b.txt").write_text("beta\n")
        inventory = {"files": [entry(packet, "a.txt"), entry(packet, "nested/b.txt")]}
        inventory_path = packet / "evidence-sha256.json"
        inventory_path.write_text(json.dumps(inventory))

        verifier.verify_inventory(packet, inventory_path)

        (packet / "a.txt").write_text("corrupt\n")
        expect_failure("identity mismatch", verifier.verify_inventory, packet, inventory_path)
        (packet / "a.txt").write_text("alpha\n")

        (packet / "nested/b.txt").unlink()
        expect_failure("missing inventory path", verifier.verify_inventory, packet, inventory_path)
        (packet / "nested/b.txt").write_text("beta\n")

        (packet / "extra.txt").write_text("extra\n")
        expect_failure("extra packet entries", verifier.verify_inventory, packet, inventory_path)

    print("EVIDENCE_SEAL_REGRESSION_PASS corruption+missing+extra")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
