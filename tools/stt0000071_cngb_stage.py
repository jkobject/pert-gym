#!/usr/bin/env python3
"""Enumerate and optionally stage STT0000071 CNGB non-TIFF analysis payloads.

Scope: source/staging only; no Lamin writes.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html.parser
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

BASE_URL = "https://ftp.cngb.org/pub/stomics/STT0000071/Analysis/"
RESOLVE = ["--resolve", "ftp.cngb.org:443:101.126.80.204"]
USER_AGENT = "Mozilla/5.0 pert-gym-source-probe/20260629"
TIMEPOINTS = {
    "STSA0000734": "uninjured_1",
    "STSA0000735": "6_hpa",
    "STSA0000736": "12_hpa",
    "STSA0000737": "1_dpa",
    "STSA0000738": "3_dpa",
    "STSA0000739": "7_dpa",
    "STSA0000740": "14_dpa",
    "STSA0000741": "28_dpa",
    "STSA0000742": "uninjured_2",
}


class LinkParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def run(cmd: list[str], *, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE if capture else None, check=check)


def curl_text(url: str, attempts: int = 5) -> str:
    last = None
    for attempt in range(attempts):
        try:
            cp = run(["curl", *RESOLVE, "-fsSL", "--retry", "1", "--retry-delay", "1", "--connect-timeout", "10", "--max-time", "25", "-A", USER_AGENT, url])
            return cp.stdout
        except subprocess.CalledProcessError as exc:
            last = exc
            time.sleep(1 + attempt)
    raise RuntimeError(f"curl failed for {url}: {last.stderr if last else ''}")


def list_links(url: str) -> list[str]:
    parser = LinkParser()
    parser.feed(curl_text(url))
    out = []
    for href in parser.links:
        if href in {"../", "./", "/"} or href.startswith("#") or href.startswith("javascript:"):
            continue
        full = urllib.parse.urljoin(url, href)
        if full.startswith(BASE_URL):
            out.append(full)
    return out


def enumerate_files() -> tuple[list[str], list[str]]:
    queue = [BASE_URL]
    seen_dirs: set[str] = set()
    dirs: list[str] = []
    files: list[str] = []
    while queue:
        url = queue.pop(0)
        if url in seen_dirs:
            continue
        seen_dirs.add(url)
        dirs.append(url)
        links = list_links(url)
        for full in links:
            if full.endswith("/"):
                if full not in seen_dirs:
                    queue.append(full)
            else:
                files.append(full)
        print(f"DIR {len(dirs):03d} {url} links={len(links)} files={len(files)}", file=sys.stderr)
    return dirs, files


def is_tiff(url: str) -> bool:
    name = urllib.parse.unquote(url.rsplit("/", 1)[-1]).lower()
    return ".tif" in name or name.endswith((".tif", ".tiff", ".tif.gz", ".tiff.gz"))


def allowed_non_tiff(url: str) -> bool:
    name = urllib.parse.unquote(url.rsplit("/", 1)[-1]).lower()
    if is_tiff(url):
        return False
    return name.endswith((".gem.gz", ".tsv.gz", "event.tar")) or name.startswith("readme")


def head_metadata(url: str) -> dict[str, str | None]:
    cp = run(["curl", *RESOLVE, "-fsSIL", "--retry", "1", "--retry-delay", "1", "--connect-timeout", "10", "--max-time", "30", "-A", USER_AGENT, url], check=False)
    headers = {}
    for line in cp.stdout.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    return {
        "head_status": str(cp.returncode),
        "content_length": headers.get("content-length"),
        "last_modified": headers.get("last-modified"),
        "etag": headers.get("etag"),
        "content_type": headers.get("content-type"),
        "head_error": (cp.stderr or "").strip(),
    }


def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            h.update(block)
    return h.hexdigest()


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".partial")
    if tmp.exists():
        tmp.unlink()
    run(["curl", *RESOLVE, "-fL", "--retry", "4", "--retry-delay", "5", "--max-time", "3600", "-A", USER_AGENT, "-o", str(tmp), url], capture=False)
    tmp.rename(dest)


def gcs_cp(path: Path, gcs_prefix: str, rel: str) -> str:
    uri = gcs_prefix.rstrip("/") + "/" + rel
    run(["gcloud", "storage", "cp", str(path), uri], capture=False)
    return uri


def gcs_stat(uri: str) -> dict[str, str | None]:
    cp = run(["gcloud", "storage", "objects", "describe", uri, "--format=json"], check=False)
    if cp.returncode != 0:
        return {"gcs_describe_error": cp.stderr.strip()}
    data = json.loads(cp.stdout)
    return {
        "gcs_size": str(data.get("size", "")),
        "gcs_md5_hash": data.get("md5Hash"),
        "gcs_crc32c": data.get("crc32c"),
        "gcs_generation": str(data.get("generation", "")),
    }


def metadata_from_url(url: str) -> dict[str, str | None]:
    rel = url[len(BASE_URL):]
    parts = rel.split("/")
    sample_id = parts[0] if len(parts) > 0 else ""
    section_id = parts[1] if len(parts) > 1 else ""
    filename = parts[-1]
    section_label = filename.split(".", 1)[0]
    return {
        "url": url,
        "relative_path": rel,
        "sample_id": sample_id,
        "timepoint": TIMEPOINTS.get(sample_id, ""),
        "section_id": section_id,
        "section_label": section_label,
        "filename": filename,
        "skip_tiff": str(is_tiff(url)).lower(),
        "allowed_non_tiff": str(allowed_non_tiff(url)).lower(),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="artifacts/schema_audit")
    ap.add_argument("--date", default="20260629")
    ap.add_argument("--stage", action="store_true")
    ap.add_argument("--skip-head", action="store_true", help="Skip remote HEAD probes; staging still records local size/SHA-256 and GCS metadata.")
    ap.add_argument("--local-stage-dir", default="data/staging/stt0000071_cngb_non_tiff_20260629")
    ap.add_argument("--gcs-prefix", default="gs://scperturb/pert-gym/staging/temporal_pretraining/stt0000071_cngb_non_tiff_20260629")
    ap.add_argument("--delete-local-after-upload", action="store_true", help="Remove each local payload after verified GCS describe metadata is captured.")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dirs, files = enumerate_files()
    rows = []
    for i, url in enumerate(files, 1):
        row = metadata_from_url(url)
        if args.skip_head:
            row.update({"head_status": None, "content_length": None, "last_modified": None, "etag": None, "content_type": None, "head_error": "skipped"})
        else:
            row.update(head_metadata(url))
        rows.append(row)
        if i % 20 == 0:
            print(f"HEAD {i}/{len(files)}", file=sys.stderr)

    allowed = [r for r in rows if r["allowed_non_tiff"] == "true"]
    skipped_tiff = [r for r in rows if r["skip_tiff"] == "true"]
    other = [r for r in rows if r["allowed_non_tiff"] != "true" and r["skip_tiff"] != "true"]

    if args.stage:
        to_stage = allowed[: args.limit or None]
        stage_root = Path(args.local_stage_dir)
        for i, row in enumerate(to_stage, 1):
            rel = row["relative_path"]
            dest = stage_root / rel
            print(f"STAGE {i}/{len(to_stage)} {rel}", file=sys.stderr)
            download(row["url"], dest)
            row["local_path"] = str(dest)
            row["local_size"] = str(dest.stat().st_size)
            row["sha256"] = sha256_file(dest)
            row["gcs_uri"] = gcs_cp(dest, args.gcs_prefix, rel)
            row.update(gcs_stat(row["gcs_uri"] or ""))
            if args.delete_local_after_upload:
                dest.unlink()
                row["local_deleted_after_upload"] = "true"

    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    tsv_path = out_dir / f"stt0000071_cngb_analysis_manifest_{args.date}.tsv"
    with tsv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    json_path = out_dir / f"stt0000071_cngb_analysis_manifest_{args.date}.json"
    payload = {
        "base_url": BASE_URL,
        "directories": dirs,
        "counts": {
            "directories": len(dirs),
            "files_total": len(files),
            "allowed_non_tiff": len(allowed),
            "skipped_tiff": len(skipped_tiff),
            "other_non_matching": len(other),
            "samples": len({r["sample_id"] for r in rows}),
            "sections": len({r["section_id"] for r in rows}),
        },
        "rows": rows,
    }
    json_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({"tsv": str(tsv_path), "json": str(json_path), "counts": payload["counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
