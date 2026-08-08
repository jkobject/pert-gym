#!/usr/bin/env python3
"""Plan and execute the one-time canonical GCS hierarchy migration.

Planning is pure and fail-closed. Execution uses the GCS JSON rewrite API with
source/destination generation preconditions; payload bytes never transit through
the controlling machine.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import quote

_ACCESSION = re.compile(r"^(GSE\d+|SCP\d+|STDS\d+|STT\d+|E-(?:MTAB|GEOD)-\d+)", re.I)
_CHUNK = re.compile(r"^chunk_[0-9]{4,6}$")


class DestinationCollision(RuntimeError):
    """Two non-identical source objects resolve to one canonical destination."""


class ChecksumMismatch(RuntimeError):
    """A rewritten destination does not match the frozen source identity."""


@dataclass(frozen=True)
class RawDecision:
    action: str
    dataset: str | None = None
    relative_path: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class PlanItem:
    action: str
    source: str
    destination: str | None
    generation: str
    size: int
    md5_hash: str
    crc32c: str
    reason: str


def _is_canonical_destination(destination: str) -> bool:
    parts = destination.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return False
    if parts[:2] == ["data", "raw"]:
        return len(parts) >= 4
    if parts[:2] != ["data", "cleaned"] or len(parts) != 4:
        return False
    filename = parts[3]
    if filename in {"X.h5ad", "obs.parquet", "var.parquet"}:
        return True
    return bool(
        re.fullmatch(r"X_chunk_[0-9]{4}\.h5ad", filename)
        or re.fullmatch(r"obs_chunk_[0-9]{4}\.parquet", filename)
    )


def _accession_and_tail(parts: list[str]) -> tuple[str, str] | None:
    for index, part in enumerate(parts):
        match = _ACCESSION.match(part)
        if not match:
            continue
        accession = match.group(1).upper()
        # A component such as "SCP499 (axolotl)" names the dataset. If the
        # accession occurs only in a payload filename, retain that filename.
        if part.upper() == accession or part.upper().startswith(accession + " "):
            tail = parts[index + 1 :]
        else:
            tail = parts[index:]
        if not tail:
            tail = [part]
        return accession, "/".join(tail)
    return None


def _source_payload(dataset: str, relative_path: str) -> RawDecision:
    relative_path = relative_path.replace(" (1).", ".")
    return RawDecision("move", dataset, relative_path, "source payload")


def classify_raw_object(name: str) -> RawDecision:
    """Map one known legacy source object into ``data/raw/<NAME>``.

    Unknown objects are blocked rather than sent to ``other/``.
    """
    if not name.startswith("pert-gym/"):
        return RawDecision("block", reason="outside the legacy pert-gym prefix")

    parts = name.split("/")
    leaf = parts[-1]
    lower = name.lower()

    disposable_source_roots = (
        "pert-gym/staging/manual_downloads/",
        "pert-gym/staging/manual_scp/",
        "pert-gym/staging/manual_temporal/",
        "pert-gym/staging/browser_auth_scp/",
        "pert-gym/staging/data/main/",
        "pert-gym/staging/temporal_pretraining/",
    )
    in_disposable_source_scope = name.startswith(disposable_source_roots)
    delete_markers = (
        "/_manifests/",
        "/components/",
        "/components_smoke/",
        "/components_himem/",
        "/components_himem_smoke/",
        "/perturbqa_mappings_",
        "/repaired_x/",
    )
    delete_names = {
        "download_status.md",
        "download_status_live.md",
        "copy_monitor.stdout",
        "download_monitor.stdout",
        "link_monitor.stdout",
        "r_session_info.txt",
        "tcell_gwps_manifest_summary.json",
    }
    disposable_evidence = in_disposable_source_scope and (
        any(marker in lower for marker in delete_markers)
        or leaf.lower() in delete_names
        or leaf.lower().startswith("staging_manifest_")
        or leaf.lower().endswith("_validation.json")
        or leaf.lower().endswith("_validation.md")
        or (leaf.lower() == "manifest.json" and "/components" in lower)
    )
    disposable_xatlas = (
        name.startswith("pert-gym/staging/sources/xatlas/")
        or name.startswith("pert-gym/staging/xatlas_orion/")
    )
    if disposable_evidence or disposable_xatlas:
        return RawDecision("delete", reason="known non-canonical staging evidence")
    if (
        in_disposable_source_scope
        and "/api_derived/" in lower
        and leaf.lower().endswith(".json")
    ):
        return RawDecision("delete", reason="API execution evidence")

    if name.startswith("pert-gym/staging/data/main/xatlas_orion/"):
        if leaf == "55021257":
            return _source_payload("xatlas_orion", "HCT116_filtered_dual_guide_cells.h5ad")
        if leaf == "55074802":
            return _source_payload("xatlas_orion", "HEK293T_filtered_dual_guide_cells.h5ad")
        marker = "pert-gym/staging/data/main/xatlas_orion/"
        return _source_payload("xatlas_orion", name[len(marker) :].removeprefix("sidecars/"))

    named_roots = {
        "pert-gym/staging/data/main/arc_vcc/": "arc_vcc_2025",
        "pert-gym/staging/data/main/broad_prism/": "broad_prism_repurposing",
        "pert-gym/staging/data/main/depmap_ccle/": "depmap_ccle_26q1",
        "pert-gym/staging/data/main/rxrx19a/": "rxrx19a",
        "pert-gym/staging/data/main/sanger_dualguide_crc/": "sanger_dualguide_crc",
        "pert-gym/staging/data/main/sanger_score/": "sanger_score",
        "pert-gym/staging/data/main/tcell_gwps/": "tcell_gwps",
        "pert-gym/staging/data/main/viperturb/": "viperturbseq",
    }
    for root, dataset in named_roots.items():
        if name.startswith(root):
            relative = name[len(root) :]
            if dataset == "tcell_gwps":
                relative = relative.removeprefix(
                    "raw/genome-scale-tcell-perturb-seq.s3.amazonaws.com/marson2025_data/"
                )
            elif dataset == "rxrx19a":
                relative = relative.removeprefix("source/")
            return _source_payload(dataset, relative)

    if name.startswith("pert-gym/staging/data/main/strand/"):
        return RawDecision("delete", reason="QA mappings, not source payload")
    if name.startswith(
        "pert-gym/staging/data/main/prism_google_drive_datasets_20260622/"
    ):
        found = _accession_and_tail(parts)
        if found:
            return _source_payload(*found)
        return RawDecision("delete", reason="Google Drive download tooling/evidence")
    if name.startswith(
        "pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/"
    ):
        marker = "perturbase_t29/"
        return _source_payload("GSE216481", name.split(marker, 1)[1])
    if name.startswith("pert-gym/staging/data/main/temporal_t36_gse303344/"):
        marker = "temporal_t36_gse303344/"
        return _source_payload("GSE303344", name.split(marker, 1)[1])
    if name.startswith("pert-gym/staging/temporal_pretraining/mosta_stds0000058_"):
        return _source_payload("STDS0000058", name.split("/stomics/", 1)[-1])
    if name.startswith("pert-gym/staging/data/main/t37_artista/"):
        return _source_payload("STDS0000056", leaf)
    if name.startswith("pert-gym/staging/data/main/gdsc/"):
        if "GDSC1" in leaf.upper():
            return _source_payload("GDSC1", leaf)
        if "GDSC2" in leaf.upper():
            return _source_payload("GDSC2", leaf)
        return RawDecision("block", reason="GDSC object lacks GDSC1/GDSC2 identity")
    if name.startswith("pert-gym/staging/data/main/properseq/"):
        return _source_payload("GSE150818", name.split("properseq/", 1)[1].removeprefix("extracted/"))
    if name.startswith("pert-gym/staging/data/main/stomics/hesta/"):
        return _source_payload("GSE326326", leaf)

    if name.startswith(
        "pert-gym/staging/temporal_pretraining/stt0000071_cngb_non_tiff_20260630/"
    ):
        marker = "stt0000071_cngb_non_tiff_20260630/"
        return _source_payload("STT0000071", name.split(marker, 1)[1])

    fallback_source_roots = (
        "pert-gym/staging/manual_downloads/",
        "pert-gym/staging/manual_scp/",
        "pert-gym/staging/manual_temporal/",
        "pert-gym/staging/browser_auth_scp/",
        "pert-gym/staging/data/gcs_cache/",
        "pert-gym/staging/data/main/prism_collection/",
        "pert-gym/staging/data/main/temporal_pretraining/",
        "pert-gym/staging/data/temporal_pretraining/",
    )
    found = _accession_and_tail(parts) if name.startswith(fallback_source_roots) else None
    if found:
        dataset, relative = found
        # Remove wrappers that accidentally captured machine-local paths.
        if dataset == "SCP667" and "/SCP667/" in name:
            relative = name.rsplit("/SCP667/", 1)[1]
        return _source_payload(dataset, relative)

    if name.startswith("pert-gym/staging/data/gcs_cache/mouse_gastrulation/"):
        return _source_payload("mouse_gastrulation_hca", leaf)

    return RawDecision("block", reason="unclassified source object")


def _identity(item: Mapping[str, object]) -> tuple[int, str, str]:
    return (
        int(item.get("size", 0)),
        str(item.get("md5Hash", "")),
        str(item.get("crc32c", "")),
    )


def _same_identity(
    first: tuple[int, str, str], second: tuple[int, str, str]
) -> bool:
    if first[0] != second[0]:
        return False
    comparisons = []
    if first[2] and second[2]:
        comparisons.append(first[2] == second[2])
    if first[1] and second[1]:
        comparisons.append(first[1] == second[1])
    return bool(comparisons) and all(comparisons)


def build_raw_plan(objects: Iterable[Mapping[str, object]]) -> list[PlanItem]:
    plan: list[PlanItem] = []
    destination_sources: dict[str, tuple[tuple[int, str, str], int]] = {}
    for obj in objects:
        source = str(obj["name"])
        decision = classify_raw_object(source)
        destination = None
        if decision.action == "move":
            assert decision.dataset and decision.relative_path
            destination = f"data/raw/{decision.dataset}/{decision.relative_path}"
            identity = _identity(obj)
            previous = destination_sources.get(destination)
            if previous is not None:
                if not _same_identity(previous[0], identity):
                    raise DestinationCollision(destination)
                action = "delete_duplicate"
                reason = f"identical duplicate of plan item {previous[1]}"
                destination = None
            else:
                destination_sources[destination] = (identity, len(plan))
                action = "move"
                reason = decision.reason
        else:
            action = decision.action
            reason = decision.reason
        plan.append(
            PlanItem(
                action=action,
                source=source,
                destination=destination,
                generation=str(obj.get("generation", "")),
                size=int(obj.get("size", 0)),
                md5_hash=str(obj.get("md5Hash", "")),
                crc32c=str(obj.get("crc32c", "")),
                reason=reason,
            )
        )
    blocked = [item.source for item in plan if item.action == "block"]
    if blocked:
        raise ValueError(f"unclassified source objects: {blocked[:10]!r}")
    return plan


class GCSMover:
    def __init__(
        self,
        session: Any,
        *,
        bucket: str,
        user_project: str,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.session = session
        self.bucket = bucket
        self.user_project = user_project
        self.sleep = sleep
        self.base = "https://storage.googleapis.com/storage/v1"

    def _request(self, method: str, url: str, **kwargs: object) -> Any:
        response = None
        for attempt in range(6):
            response = self.session.request(method, url, **kwargs)
            if response.status_code != 429 and not 500 <= response.status_code < 600:
                return response
            if attempt < 5:
                self.sleep(float(2**attempt))
        return response

    def _delete_source(self, item: PlanItem) -> None:
        url = f"{self.base}/b/{quote(self.bucket, safe='')}/o/{quote(item.source, safe='')}"
        response = self._request(
            "DELETE",
            url,
            params={
                "generation": item.generation,
                "ifGenerationMatch": item.generation,
                "userProject": self.user_project,
            },
            timeout=120,
        )
        if response.status_code not in {200, 204, 404}:
            raise RuntimeError(f"GCS delete failed {response.status_code}: {response.text}")

    @staticmethod
    def _verify_destination(item: PlanItem, resource: Mapping[str, object]) -> None:
        if int(str(resource.get("size", "-1"))) != item.size:
            raise ChecksumMismatch(f"size mismatch for {item.destination}")
        destination_crc = str(resource.get("crc32c", ""))
        destination_md5 = str(resource.get("md5Hash", ""))
        comparisons = []
        if item.crc32c and destination_crc:
            comparisons.append(("CRC32C", item.crc32c == destination_crc))
        if item.md5_hash and destination_md5:
            comparisons.append(("MD5", item.md5_hash == destination_md5))
        if not comparisons:
            raise ChecksumMismatch(f"no comparable checksum for {item.destination}")
        mismatches = [name for name, matches in comparisons if not matches]
        if mismatches:
            raise ChecksumMismatch(
                f"{'/'.join(mismatches)} mismatch for {item.destination}"
            )

    def apply(self, item: PlanItem) -> dict[str, object]:
        if not item.source.startswith("pert-gym/staging/"):
            raise ValueError(
                "migration source must be under the legacy pert-gym/staging prefix"
            )
        if any(part in {"", ".", ".."} for part in item.source.split("/")):
            raise ValueError("migration source contains an unsafe path segment")
        if item.action in {"delete", "delete_duplicate"}:
            self._delete_source(item)
            return {"action": item.action, "source_generation": item.generation}
        if item.action != "move" or not item.destination:
            raise ValueError(f"unsupported plan action: {item.action!r}")
        if item.destination == item.source:
            raise ValueError("refusing destructive self-move")
        if not _is_canonical_destination(item.destination):
            raise ValueError("migration destination violates the canonical layout")

        source = quote(item.source, safe="")
        destination = quote(item.destination, safe="")
        url = (
            f"{self.base}/b/{quote(self.bucket, safe='')}/o/{source}/rewriteTo/"
            f"b/{quote(self.bucket, safe='')}/o/{destination}"
        )
        params = {
            "sourceGeneration": item.generation,
            "ifSourceGenerationMatch": item.generation,
            "ifGenerationMatch": "0",
            "userProject": self.user_project,
        }
        while True:
            response = self._request("POST", url, params=params, timeout=120)
            if response.status_code in {404, 412}:
                metadata_url = (
                    f"{self.base}/b/{quote(self.bucket, safe='')}/o/{destination}"
                )
                existing = self._request(
                    "GET",
                    metadata_url,
                    params={"userProject": self.user_project},
                    timeout=120,
                )
                if existing.status_code != 200:
                    raise RuntimeError(
                        "GCS rewrite precondition failed and destination cannot be read: "
                        f"{existing.status_code} {existing.text}"
                    )
                resource = existing.json()
                break
            if response.status_code != 200:
                raise RuntimeError(
                    f"GCS rewrite failed {response.status_code}: {response.text}"
                )
            payload = response.json()
            if payload.get("done"):
                resource = payload.get("resource") or {}
                break
            token = payload.get("rewriteToken")
            if not token:
                raise RuntimeError("GCS rewrite incomplete without rewriteToken")
            params = {"rewriteToken": token, "userProject": self.user_project}

        self._verify_destination(item, resource)
        self._delete_source(item)
        return {
            "action": "move",
            "source_generation": item.generation,
            "destination_generation": str(resource.get("generation", "")),
        }


def _safe_segment(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
    if not value:
        raise ValueError("empty canonical dataset name")
    return value


def cleaned_dataset_name(
    *, record_id: str, group_parent: str, accepted_prefix: str
) -> str:
    """Choose one stable dataset directory without build/version identifiers."""
    basename = group_parent.rstrip("/").rsplit("/", 1)[-1]
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", basename):
        return f"cellxgene-{basename}"
    if re.fullmatch(r"(?:GSE|GSM|SCP|STDS|STT)\d+|E-(?:MTAB|GEOD)-\d+", basename, re.I):
        return basename.upper()
    if record_id == "lincs_level2":
        return f"lincs-level2-{basename.replace('_', '-')}"
    axolotl = {
        "temporal_v4_136": "SCP499",
        "temporal_v4_137": "SCP422",
        "temporal_v4_138": "SCP500",
        "temporal_v4_139": "SCP489",
    }
    for prefix, accession in axolotl.items():
        if record_id.startswith(prefix):
            return accession
    if group_parent.rstrip("/") != accepted_prefix.rstrip("/"):
        return _safe_segment(basename)
    cleaned = re.sub(r"^temporal_v\d+_\d+_", "", record_id)
    return _safe_segment(cleaned)


def render_dataset_readme(dataset: str, info: Mapping[str, object]) -> str:
    """Render the sole compact provenance note allowed beside a cleaned triplet."""
    sources = info.get("sources")
    if not isinstance(sources, Mapping) or set(sources) != {"X", "obs", "var"}:
        raise ValueError("README requires exactly X, obs, and var source records")
    lines = [
        f"# {dataset}",
        "",
        "Canonical cleaned expression triplet for this biological dataset.",
        "No payload transformation was performed: objects were moved server-side",
        "within `gs://scperturb` and verified by size and CRC32C before deleting",
        "the exact old generations.",
        "",
        "| Role | Canonical object | Historical source | Bytes | MD5 | CRC32C |",
        "|---|---|---|---:|---|---|",
    ]
    filenames = {"X": "X.h5ad", "obs": "obs.parquet", "var": "var.parquet"}
    for role in ("X", "obs", "var"):
        source = sources[role]
        if not isinstance(source, Mapping):
            raise ValueError(f"invalid {role} source record")
        historical = f"gs://scperturb/{source['name']}#{source['generation']}"
        canonical = (
            f"gs://scperturb/data/cleaned/{dataset}/{filenames[role]}"
            f"#{source['destination_generation']}"
        )
        lines.append(
            f"| {role} | `{canonical}` | `{historical}` | {source['size']} | "
            f"`{source.get('md5Hash', '')}` | `{source.get('crc32c', '')}` |"
        )
    lines.extend(
        [
            "",
            "The ordered feature space is represented once by `var.parquet`.",
            "There are no member-specific feature files and no visible versioning.",
            "",
            "Historical acceptance record:",
            f"`gs://scperturb/{info['manifest_name']}#{info['manifest_generation']}`",
            f"(SHA-256 `{info['manifest_sha256']}`).",
            "",
        ]
    )
    return "\n".join(lines)


def canonical_cleaned_names(member: str | None) -> tuple[str, str, str]:
    if member is None:
        return "X.h5ad", "obs.parquet", "var.parquet"
    if not re.fullmatch(r"chunk_[0-9]{4}", member):
        raise ValueError("cleaned member must be chunk_NNNN; no revisions/splits/modalities")
    return f"X_{member}.h5ad", f"obs_{member}.parquet", "var.parquet"


def execution_identity(item: PlanItem | Mapping[str, object]) -> tuple[str, str, str, str]:
    """Return the full identity used to resume an execution receipt safely."""
    if isinstance(item, PlanItem):
        return (item.action, item.source, item.generation, item.destination or "")
    return (
        str(item.get("action", "")),
        str(item.get("source", "")),
        str(item.get("source_generation", "")),
        str(item.get("destination", "") or ""),
    )


def execute_plan(
    plan_path: Path,
    receipt_path: Path,
    *,
    bucket: str,
    user_project: str,
    workers: int = 8,
) -> None:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    if workers < 1:
        raise ValueError("workers must be at least 1")
    items = [PlanItem(**json.loads(line)) for line in plan_path.read_text().splitlines()]
    completed: set[tuple[str, str, str, str]] = set()
    if receipt_path.exists():
        for line in receipt_path.read_text().splitlines():
            row = json.loads(line)
            if row.get("status") == "done":
                completed.add(execution_identity(row))

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
    )
    local = threading.local()

    def apply(item: PlanItem) -> tuple[PlanItem, dict[str, object]]:
        if not hasattr(local, "mover"):
            local.mover = GCSMover(
                AuthorizedSession(credentials),
                bucket=bucket,
                user_project=user_project,
            )
        return item, local.mover.apply(item)

    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    with receipt_path.open("a", buffering=1) as receipt:
        for phase, actions in (
            ("move", {"move"}),
            ("delete", {"delete", "delete_duplicate"}),
        ):
            pending = [
                item
                for item in items
                if item.action in actions and execution_identity(item) not in completed
            ]
            print(f"phase={phase} pending={len(pending)}", flush=True)
            done_count = 0
            for start in range(0, len(pending), workers):
                batch = pending[start : start + workers]
                failures: list[tuple[PlanItem, BaseException]] = []
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=min(workers, len(batch))
                ) as pool:
                    future_to_item = {pool.submit(apply, item): item for item in batch}
                    for future in concurrent.futures.as_completed(future_to_item):
                        item = future_to_item[future]
                        try:
                            _, result = future.result()
                        except BaseException as exc:
                            failures.append((item, exc))
                            continue
                        row = {
                            "status": "done",
                            "action": item.action,
                            "source": item.source,
                            "source_generation": item.generation,
                            "destination": item.destination,
                            **result,
                        }
                        receipt.write(json.dumps(row, sort_keys=True) + "\n")
                        receipt.flush()
                        completed.add(execution_identity(item))
                        done_count += 1
                if failures:
                    detail = "; ".join(
                        f"{item.source}: {type(exc).__name__}: {exc}"
                        for item, exc in failures
                    )
                    raise RuntimeError(f"phase {phase} stopped after failed batch: {detail}")
                if done_count % 16 == 0 or start + workers >= len(pending):
                    print(
                        f"phase={phase} completed_this_run={done_count}/{len(pending)}",
                        flush=True,
                    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--bucket", default="scperturb")
    parser.add_argument("--user-project", default="jkobject-1549353370965")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    execute_plan(
        args.plan,
        args.receipt,
        bucket=args.bucket,
        user_project=args.user_project,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
