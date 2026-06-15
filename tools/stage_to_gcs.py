#!/usr/bin/env python3
"""Stage local dataset files to Google Cloud Storage.

Uses application-default credentials through `gcloud auth application-default`.
This avoids relying on the global `gcloud` active account, which may be unset on
the shared VPS.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from urllib.parse import quote

import requests


DEFAULT_BUCKET = "scperturb"
DEFAULT_PREFIX = "pert-gym/staging"
CHUNK_SIZE = 8 * 1024 * 1024


def access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def start_resumable_upload(bucket: str, object_name: str, token: str) -> str:
    url = (
        f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"
        f"?uploadType=resumable&name={quote(object_name, safe='')}"
    )
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8",
        },
        data=json.dumps({"name": object_name}),
        timeout=60,
    )
    response.raise_for_status()
    return response.headers["Location"]


def upload_file(path: Path, bucket: str, object_name: str, token: str) -> dict:
    upload_url = start_resumable_upload(bucket, object_name, token)
    total = path.stat().st_size
    sent = 0

    with path.open("rb") as handle:
        while sent < total:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            start = sent
            end = sent + len(chunk) - 1
            sent += len(chunk)
            response = requests.put(
                upload_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{total}",
                },
                data=chunk,
                timeout=300,
            )
            if response.status_code not in {200, 201, 308}:
                raise RuntimeError(
                    f"Upload failed at {start}-{end}: "
                    f"{response.status_code} {response.text[:500]}"
                )
            print(f"{path.name}: {sent}/{total}", flush=True)

    metadata_url = (
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/"
        f"{quote(object_name, safe='')}"
    )
    metadata = requests.get(
        metadata_url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    metadata.raise_for_status()
    return metadata.json()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--delete-local", action="store_true")
    args = parser.parse_args()

    token = access_token()
    for path in args.paths:
        if not path.exists():
            raise FileNotFoundError(path)
        object_name = f"{args.prefix.rstrip('/')}/{path.as_posix()}"
        metadata = upload_file(path, args.bucket, object_name, token)
        print(f"UPLOADED gs://{args.bucket}/{object_name} size={metadata.get('size')}")
        if args.delete_local:
            path.unlink()
            print(f"DELETED_LOCAL {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
