#!/usr/bin/env python3
"""Fetch staged files from Google Cloud Storage using ADC credentials."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from urllib.parse import quote

import requests


CHUNK_SIZE = 8 * 1024 * 1024


def access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got {uri!r}")
    bucket, _, object_name = uri[5:].partition("/")
    if not bucket or not object_name:
        raise ValueError(f"Invalid gs:// URI: {uri!r}")
    return bucket, object_name


def download(uri: str, output: Path, token: str) -> None:
    bucket, object_name = parse_gs_uri(uri)
    url = (
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/"
        f"{quote(object_name, safe='')}?alt=media"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        stream=True,
        timeout=300,
    ) as response:
        response.raise_for_status()
        written = 0
        with output.open("wb") as handle:
            for chunk in response.iter_content(CHUNK_SIZE):
                if not chunk:
                    continue
                handle.write(chunk)
                written += len(chunk)
                print(f"{output.name}: {written}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("uri")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    download(args.uri, args.output, access_token())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
