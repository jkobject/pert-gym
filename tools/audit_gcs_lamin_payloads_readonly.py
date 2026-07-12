#!/usr/bin/env python3
"""EU-only, read-only inventory of gs://scperturb/pert-gym and Lamin payloads.

Lists object metadata only and queries Lamin metadata on branch jkobject. It never
writes to Lamin or GCS and never calls a delete command. Every object proposed as
a SAFE candidate requires its own mandatory 8-byte readback from a durable,
non-source Lamin-backed storage URI.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import socket
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from tools.lamin_context import connect_pertdata, ensure_project_cache

BILLING_PROJECT = "jkobject-1549353370965"
ROOT_URI = "gs://scperturb/pert-gym"
COST_EUR_PER_GIB_MONTH = (
    0.020  # planning assumption; validate against billing SKU before deletion
)


def run(cmd: list[str]) -> str:
    return subprocess.run(
        cmd, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).stdout


def assert_eu_worker() -> None:
    if "pert-gym-worker-eu" not in socket.gethostname():
        raise RuntimeError(
            f"refusing outside pert-gym-worker-eu: {socket.gethostname()!r}"
        )


def logical_prefix(name: str) -> str:
    parts = name.split("/")
    # name begins with pert-gym. Keep an ingestion/source family together while
    # preventing one enormous staging/ bucket from becoming an unhelpful bucket.
    if len(parts) < 2:
        return name
    top = parts[1]
    if top == "staging":
        return "/".join(parts[:4]) if len(parts) >= 4 else "/".join(parts)
    if top in {
        "canonical",
        "model-ready",
        "local_archive",
        "derived",
        "sources",
        "raw",
    }:
        return "/".join(parts[:3]) if len(parts) >= 3 else "/".join(parts)
    return "/".join(parts[:3]) if len(parts) >= 3 else "/".join(parts)


def role_for(prefix: str) -> str:
    parts = prefix.split("/")
    top = parts[1] if len(parts) > 1 else ""
    if top == "staging":
        return "staging"
    if top == "local_archive":
        return "local_archive"
    if top in {"canonical", "model-ready", "derived"}:
        return "derived_or_canonical_candidate"
    if top in {"sources", "raw"}:
        return "source_immutable_candidate"
    return "unclassified"


def artifact_uri(artifact: Any) -> str:
    for attr in ("path", "url"):
        try:
            value = getattr(artifact, attr, None)
            if value:
                return str(value)
        except Exception:
            pass
    try:
        storage = getattr(artifact, "storage", None)
        root = getattr(storage, "root", "") if storage else ""
        key = getattr(artifact, "key", "") or ""
        return f"{root.rstrip('/')}/{key.lstrip('/')}" if root else ""
    except Exception:
        return ""


def normalize_storage_uri(uri: Any) -> str | None:
    """Return a canonical supported storage URI, otherwise fail closed."""
    if not isinstance(uri, str) or not uri or uri.strip() != uri:
        return None
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.username
        or parsed.password
        or port is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
    ):
        return None
    scheme = parsed.scheme.lower()
    host = parsed.netloc.lower()
    if scheme in {"gs", "s3"}:
        if not host or "/" in host or not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", host):
            return None
        return f"{scheme}://{host}{parsed.path}"
    if scheme != "https":
        return None
    if host == "storage.googleapis.com":
        bucket, separator, object_path = parsed.path.removeprefix("/").partition("/")
        if not separator or not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", bucket):
            return None
        return f"gs://{bucket}/{object_path}"
    suffix = ".storage.googleapis.com"
    if host.endswith(suffix):
        bucket = host.removesuffix(suffix)
        if (
            not bucket
            or "." in bucket
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", bucket)
        ):
            return None
        return f"gs://{bucket}{parsed.path}"
    return None


def is_scperturb_source_uri(uri: Any) -> bool:
    """Recognise every supported spelling of the source GCS bucket."""
    normalized = normalize_storage_uri(uri)
    return bool(normalized and normalized.startswith("gs://scperturb/"))


def read_header(uri: str) -> tuple[bool, str]:
    """Read at most eight bytes through fsspec, never cache/download a payload."""
    if not normalize_storage_uri(uri):
        return False, "storage URI missing or unsupported/ambiguous"
    if is_scperturb_source_uri(uri):
        return False, "storage URI points at source GCS"
    try:
        import fsspec

        with fsspec.open(uri, "rb") as handle:
            header = handle.read(8)
        return bool(header), f"header:{header.hex()}"
    except Exception as exc:  # evidence is fail-closed
        return False, f"header_error:{type(exc).__name__}:{str(exc)[:160]}"


def is_exact_known_size(value: Any, expected: int) -> bool:
    """Accept only an explicit integer size equal to the source metadata."""
    return type(value) is int and value >= 0 and value == expected


def is_non_scperturb_storage(uri: str) -> bool:
    return bool(normalize_storage_uri(uri)) and not is_scperturb_source_uri(uri)


def gcloud_object_size(value: Any) -> int | None:
    """Accept only non-negative gcloud JSON integers or ASCII integer strings."""
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
        return int(value)
    return None


def select_safe_artifact(
    matches: list[Any], expected_size: int
) -> tuple[Any | None, str]:
    """Choose exactly one durable exact-size target, otherwise fail closed."""
    exact_size = [
        artifact
        for artifact in matches
        if is_exact_known_size(getattr(artifact, "size", None), expected_size)
    ]
    if not exact_size:
        return None, "no exact known-size Lamin match"

    durable = [
        artifact
        for artifact in exact_size
        if is_non_scperturb_storage(artifact_uri(artifact))
    ]
    if not durable:
        return None, "no exact-size non-scperturb storage target"

    targets = {normalize_storage_uri(artifact_uri(artifact)) for artifact in durable}
    if len(targets) != 1:
        return None, "ambiguous exact-size non-scperturb storage targets"

    return sorted(durable, key=lambda artifact: str(getattr(artifact, "uid", "")))[
        0
    ], ""


def source_hash_evidence(row: dict[str, Any]) -> str:
    """Retain supplied CRC32C when MD5 is unavailable; never synthesize MD5."""
    md5 = str(row.get("md5_hash") or "")
    crc32c = str(row.get("crc32c") or "")
    if md5:
        return f"md5:{md5}"
    if crc32c:
        return f"crc32c:{crc32c} (MD5 unavailable)"
    return "no source checksum available"


def build_manifest(
    cleaned: list[dict[str, Any]],
    artifacts: list[Any],
    *,
    header_reader: Any = read_header,
) -> list[dict[str, Any]]:
    """Build prefix classifications using only fail-closed metadata evidence."""
    by_key: dict[str, list[Any]] = defaultdict(list)
    for artifact in artifacts:
        key = getattr(artifact, "key", None)
        if key:
            by_key[str(key)].append(artifact)

    prefix_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in cleaned:
        prefix_rows[item["logical_prefix"]].append(item)

    manifest: list[dict[str, Any]] = []
    for prefix, rows in sorted(prefix_rows.items()):
        total = sum(
            size
            for row in rows
            if (size := gcloud_object_size(row.get("bytes"))) is not None
        )
        exact: list[str] = []
        storage_ok = True
        size_ok = True
        uid_values: list[str] = []
        storage_values: list[str] = []
        headers: list[str] = []
        non_safe_reasons: list[str] = []
        for row in rows:
            relative = row["name"].removeprefix("pert-gym/")
            matches = by_key.get(relative, []) + by_key.get(row["name"], [])
            matches = list({getattr(a, "uid", id(a)): a for a in matches}.values())
            expected_size = gcloud_object_size(row.get("bytes"))
            if expected_size is None:
                storage_ok = False
                size_ok = False
                non_safe_reasons.append(
                    f"{row['name']}: invalid source object size metadata"
                )
                continue
            artifact, reason = select_safe_artifact(matches, expected_size)
            if artifact is None:
                storage_ok = False
                size_ok = False
                non_safe_reasons.append(f"{row['name']}: {reason}")
                continue

            uri = artifact_uri(artifact)
            ok, detail = header_reader(uri)
            headers.append(f"{row['name']}={detail}")
            if not ok:
                storage_ok = False
                non_safe_reasons.append(f"{row['name']}: readback failed")
            exact.append(row["name"])
            uid_values.append(str(getattr(artifact, "uid", "")))
            storage_values.append(uri)

        exact_complete = (
            bool(rows) and len(exact) == len(rows) and size_ok and storage_ok
        )
        role = role_for(prefix)
        if exact_complete:
            classification = "SAFE-CANDIDATE after review"
            rationale = "complete exact known-size key match to one non-scperturb Lamin storage target; every object bounded readback succeeded"
        elif role == "source_immutable_candidate":
            classification = "KEEP source irreplaceable"
            rationale = "source/raw path has no complete verified Lamin payload mapping"
        elif role in {"staging", "local_archive"}:
            classification = "KEEP temporary"
            rationale = (
                "staging/archive path lacks complete exact-key non-GCS Lamin evidence"
            )
        else:
            classification = "UNKNOWN-MISSING-LAMIN"
            rationale = "no complete exact-key+size+storage+readback evidence"
        manifest.append(
            {
                "logical_prefix": prefix,
                "role": role,
                "object_count": len(rows),
                "bytes": total,
                "gib": round(total / 2**30, 6),
                "oldest_updated": min((r["updated"] for r in rows), default=""),
                "newest_updated": max((r["updated"] for r in rows), default=""),
                "classification": classification,
                "exact_lamin_object_matches": len(exact),
                "lamin_uids": ";".join(sorted(set(uid_values))),
                "lamin_storage_uris": ";".join(sorted(set(storage_values))),
                "readback_evidence": ";".join(headers),
                "source_hash_evidence": ";".join(
                    source_hash_evidence(row) for row in rows
                ),
                "non_safe_reasons": ";".join(non_safe_reasons),
                "rationale": rationale,
                "proposed_delete_command_not_executed": (
                    f"gcloud storage rm --recursive --billing-project={BILLING_PROJECT} gs://scperturb/{prefix}/"
                    if classification.startswith("SAFE")
                    else ""
                ),
            }
        )
    return manifest


def summarize_manifest(
    cleaned: list[dict[str, Any]], manifest: list[dict[str, Any]]
) -> dict[str, Any]:
    total_bytes = sum(
        size
        for row in cleaned
        if (size := gcloud_object_size(row.get("bytes"))) is not None
    )
    classified_bytes = sum(r["bytes"] for r in manifest)
    safe_bytes = sum(
        r["bytes"] for r in manifest if r["classification"].startswith("SAFE")
    )
    classes: dict[str, int] = defaultdict(int)
    for row in manifest:
        classes[row["classification"]] += row["bytes"]
    return {
        "total_bytes": total_bytes,
        "classified_bytes": classified_bytes,
        "unexplained_delta_bytes": total_bytes - classified_bytes,
        "safe_candidate_bytes": safe_bytes,
        "class_bytes": dict(classes),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    assert_eu_worker()
    args.out.mkdir(parents=True, exist_ok=True)

    raw = run(
        [
            "gcloud",
            "storage",
            "objects",
            "list",
            "gs://scperturb/pert-gym/**",
            "--format=json",
            "--billing-project",
            BILLING_PROJECT,
        ]
    )
    objects = json.loads(raw)
    if isinstance(objects, dict):
        objects = objects.get("items", [])
    cleaned: list[dict[str, Any]] = []
    for item in objects:
        name = str(item.get("name", "")).lstrip("/")
        if not name.startswith("pert-gym/"):
            continue
        cleaned.append(
            {
                "name": name,
                "uri": f"gs://scperturb/{name}",
                "logical_prefix": logical_prefix(name),
                "bytes": gcloud_object_size(item.get("size")),
                "updated": item.get("update_time")
                or item.get("updateTime")
                or item.get("updated")
                or "",
                "storage_class": item.get("storage_class")
                or item.get("storageClass")
                or "",
                "md5_hash": item.get("md5_hash") or item.get("md5Hash") or "",
                "crc32c": item.get("crc32c_hash") or item.get("crc32c") or "",
            }
        )

    ensure_project_cache()
    ln = connect_pertdata()
    artifacts = list(ln.Artifact.filter().all())
    manifest = build_manifest(cleaned, artifacts)
    audit_totals = summarize_manifest(cleaned, manifest)
    total_bytes = audit_totals["total_bytes"]
    classified_bytes = audit_totals["classified_bytes"]
    safe_bytes = audit_totals["safe_candidate_bytes"]
    classes = audit_totals["class_bytes"]
    class_estimated_eur_month = {
        name: value / 2**30 * COST_EUR_PER_GIB_MONTH for name, value in classes.items()
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    object_tsv = args.out / f"gcs_pertgym_object_inventory_{stamp}_t_9fb0fd22.tsv"
    prefix_tsv = args.out / f"gcs_pertgym_lamin_manifest_{stamp}_t_9fb0fd22.tsv"
    report_json = args.out / f"gcs_pertgym_lamin_audit_{stamp}_t_9fb0fd22.json"
    report_md = args.out / f"gcs_pertgym_lamin_audit_{stamp}_t_9fb0fd22.md"
    for path, rows in ((object_tsv, cleaned), (prefix_tsv, manifest)):
        fields = sorted({key for row in rows for key in row})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "instance": "laminlabs/pertdata",
        "branch": "jkobject",
        "gcs_root": ROOT_URI,
        "requester_pays_billing_project": BILLING_PROJECT,
        "object_count": len(cleaned),
        "prefix_count": len(manifest),
        "total_bytes": total_bytes,
        "total_tib": total_bytes / 2**40,
        "classified_bytes": classified_bytes,
        "unexplained_delta_bytes": total_bytes - classified_bytes,
        "class_bytes": dict(classes),
        "safe_candidate_bytes": safe_bytes,
        "class_estimated_eur_month": class_estimated_eur_month,
        "safe_candidate_estimated_eur_month": safe_bytes
        / 2**30
        * COST_EUR_PER_GIB_MONTH,
        "cost_assumption_eur_per_gib_month": COST_EUR_PER_GIB_MONTH,
        "evidence_rule": "SAFE requires every listed object one exact known-size Lamin key match, one non-scperturb storage target, and its own successful bounded readback header; anything else fails closed. Source MD5 is retained when present, otherwise CRC32C is recorded without synthesizing MD5.",
        "files": [str(object_tsv), str(prefix_tsv), str(report_md)],
    }
    report_json.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    safe = [row for row in manifest if row["classification"].startswith("SAFE")]
    report_md.write_text(
        "\n".join(
            [
                "# GCS → Lamin payload audit (read-only)",
                "",
                f"Generated on `{socket.gethostname()}` at `{summary['generated_at']}`.",
                "",
                "## Coverage",
                "",
                f"- GCS objects: **{len(cleaned):,}**; logical prefixes: **{len(manifest):,}**.",
                f"- Accounted bytes: **{total_bytes / 2**40:.6f} TiB**; unexplained delta: **{summary['unexplained_delta_bytes']} bytes**.",
                f"- SAFE-CANDIDATE bytes: **{safe_bytes / 2**40:.6f} TiB**; planning saving: **€{summary['safe_candidate_estimated_eur_month']:.2f}/month** at €{COST_EUR_PER_GIB_MONTH:.3f}/GiB-month (validate SKU before action).",
                "",
                "## Fail-closed safety rule",
                "",
                summary["evidence_rule"],
                "No `gcloud storage rm` command was executed.",
                "",
                "## Classification totals",
                "",
                *[
                    f"- {name}: {value / 2**40:.6f} TiB; €{class_estimated_eur_month[name]:.2f}/month"
                    for name, value in sorted(classes.items())
                ],
                "",
                "## Review/delete candidates",
                "",
                *(
                    [
                        f"- `{r['logical_prefix']}` — {r['gib']:.3f} GiB; `{r['proposed_delete_command_not_executed']}`"
                        for r in safe
                    ]
                    or [
                        "- None. No prefix passed the full exact-key + non-GCS storage + bounded readback gate."
                    ]
                ),
                "",
                "## Future lifecycle policy",
                "",
                "1. Keep immutable raw/source prefixes indefinitely until independently catalogued with source checksum/license/provenance.",
                "2. Put staging prefixes behind an explicit 30-day TTL only after a manifest records source URI/checksum, target Lamin UIDs, non-GCS storage URI, and successful bounded readback; retain a 14-day quarantine tag before deletion.",
                "3. Never apply lifecycle deletion to `canonical/`, `model-ready/`, `sources/`, or `raw/` by wildcard; require prefix-level reviewer approval and a fresh audit.",
                "4. Run this metadata-only audit before each cleanup and fail closed on missing object metadata, key mismatch, external `gs://scperturb` storage, failed readback, or unexplained byte delta.",
                "",
                "## Files",
                "",
                f"- Prefix manifest: `{prefix_tsv.name}`",
                f"- Object metadata inventory: `{object_tsv.name}`",
                f"- Machine summary: `{report_json.name}`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
