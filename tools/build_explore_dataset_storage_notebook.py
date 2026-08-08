#!/usr/bin/env python3
"""Build the live, read-only dataset storage explorer notebook."""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

OUT = Path("notebooks/explore_dataset_storage.ipynb")


def md(text: str, cell_id: str):
    cell = nbf.v4.new_markdown_cell(text.strip())
    cell["id"] = cell_id
    return cell


def code(source: str, cell_id: str):
    cell = nbf.v4.new_code_cell(source.strip())
    cell["id"] = cell_id
    cell["execution_count"] = None
    cell["outputs"] = []
    return cell


def build():
    cells = [
        md(
            """
# Explore the pert-gym storage hierarchy and actual datasets

This notebook explores **data objects and migration evidence**, not Kanban cards or progress reports.
It answers six concrete questions:

1. Which source files live under the canonical `data/raw/<NAME>/` hierarchy?
2. Which processed triplets live under `data/cleaned/<NAME>/`?
3. Which names exist in raw, cleaned, and LaminDB, and how large is each layer?
4. Why are there 125 cleaned dataset units but only 111 raw source prefixes?
5. Which GCS objects were represented in LaminDB, and were those Artifact keys remapped correctly?
6. How can one load a cleaned H5AD—or only its `obs` or `var` table—by name?

The canonical bucket contract is:

```text
gs://scperturb/
├── README.md
├── data/
│   ├── raw/
│   │   └── <NAME>/...
│   └── cleaned/
│       └── <NAME>/
│           ├── X.h5ad or X_chunk_<NNNN>.h5ad
│           ├── obs.parquet or obs_chunk_<NNNN>.parquet
│           ├── var.parquet
│           └── README.md
├── other/
│   └── README.md
└── .lamindb/  # hidden technical compatibility prefix
```

The notebook compares that target contract with the **live bucket**. Missing
canonical roots are reported as migration gaps; legacy paths are not silently
relabelled as canonical.
""",
            "title",
        ),
        md(
            """
## The storage layers

| Layer | What is there | Typical location |
|---|---|---|
| Local working/download | Raw downloads, extracted matrices, temporary caches | `data/main/`, `data/gcs_cache/`, `~/Downloads/` |
| GCS raw | Source material grouped by dataset | `gs://scperturb/data/raw/<NAME>/...` |
| GCS cleaned | One H5AD/OBS pair (or numbered chunks), one shared VAR, and one README per dataset | `gs://scperturb/data/cleaned/<NAME>/` |
| GCS other | The explicitly allowlisted auxiliary README only | `gs://scperturb/other/README.md` |
| LaminDB | Registered artifacts, triplets, feature links and versioned Collections | `laminlabs/pertdata`, branch `jkobject` |

GCS cleaned storage is not LaminDB. A cleaned GCS directory can exist without a matching
Lamin artifact. Conversely, a Lamin artifact may point to Lamin-managed S3 storage
and no longer need a project-bucket copy.
""",
            "layers",
        ),
        md(
            """
## Safety

Everything here is cloud-read-only:

- local files are opened only for metadata or backed inspection;
- GCS inventory uses bounded JSON API listings, never `mv`, `rm`, or upload;
- Lamin uses filters, Collection membership, feature links, and small metadata loads;
- the explicit cleaned loader reads one named object; H5AD reads default to a 1 GB
  safety limit and are never triggered automatically.

The notebook deliberately refuses recursive GCS listings. Navigate one level at
a time instead. It does not migrate the legacy `pert-gym/` tree into this target
layout; migration requires a separate reviewed operation.
""",
            "safety",
        ),
        md(
            """
## 1. Choose a dataset

Start with one of the presets, then edit or add a dictionary entry. Each preset
contains independent search terms because the same dataset can use different
names in a browser download, GCS path, and Lamin key.

`SCP211` is the default because it demonstrates local source files and published
Lamin triplets. Its canonical GCS raw/cleaned locations are queried directly; if
they are absent, the notebook reports that absence rather than falling back to a
legacy prefix.
""",
            "choose-explain",
        ),
        code(
            """
from pathlib import Path
import json
import os
import re
import subprocess
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen
import zipfile

import pandas as pd
from IPython.display import Markdown, display

pd.set_option("display.max_columns", 50)
pd.set_option("display.max_colwidth", 140)
""",
            "imports",
        ),
        code(
            """
PRESETS = {
    "SCP211": {
        "local_query": "SCP211",
        "gcs_raw": "gs://scperturb/data/raw/SCP211/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/SCP211/",
        "lamin_query": "scp211",
        "note": "Canonical raw/cleaned locations plus the matching Lamin artifacts.",
    },
    "GSE132080": {
        "local_query": "GSE132080",
        "gcs_raw": "gs://scperturb/data/raw/GSE132080/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/GSE132080/",
        "lamin_query": "GSE132080",
        "note": "Canonical raw/cleaned locations plus a published Lamin obs/X/var triplet.",
    },
    "XAtlas HCT116": {
        "local_query": "xatlas",
        "gcs_raw": "gs://scperturb/data/raw/xatlas_orion/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/xatlas_orion/",
        "lamin_query": "xatlas/orion/hct116",
        "note": "Canonical raw source; no conforming canonical H5AD triplet is claimed yet.",
    },
    "GSE216481": {
        "local_query": "GSE216481",
        "gcs_raw": "gs://scperturb/data/raw/GSE216481/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/GSE216481/",
        "lamin_query": "GSE216481",
        "note": "Canonical raw/cleaned locations plus currently visible Lamin chunks.",
    },
    "Artista T37": {
        "local_query": "t37_artista",
        "gcs_raw": "gs://scperturb/data/raw/STDS0000056/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/STDS0000056/",
        "lamin_query": "t37_artista",
        "note": "A useful example of working/staged data with no same-key Lamin match.",
    },
}

SELECTED_DATASET = "SCP211"  # Change this one line first.
selection = PRESETS[SELECTED_DATASET]
selection
""",
            "configuration",
        ),
        md(
            """
The three queries are intentionally visible. If a dataset uses an alias, change
`lamin_query` without changing its GCS URI. A zero same-key Lamin result means
**no matching key was found**, not proof that no aliased dataset exists.
""",
            "alias-warning",
        ),
        code(
            """
def human_bytes(value):
    if value is None:
        return ""
    value = float(value)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024 or unit == "PB":
            return f"{value:.2f} {unit}"
        value /= 1024


def as_table(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame()
""",
            "small-helpers",
        ),
        md(
            """
## 2. Local downloads and working files

This scan reads file paths and sizes only. It does not parse entire matrices. The
roots are easy to edit if another download directory is used.
""",
            "local-section",
        ),
        code(
            """
LOCAL_ROOTS = [Path("data/main"), Path("data/gcs_cache"), Path.home() / "Downloads"]
DATA_SUFFIXES = {
    ".h5ad", ".h5", ".hdf5", ".rds", ".rda", ".loom", ".mtx",
    ".parquet", ".csv", ".tsv", ".zip", ".tar", ".gz",
}
MAX_LOCAL_FILES = 10_000
MAX_LOCAL_ENTRIES = 20_000
""",
            "local-options",
        ),
        code(
            """
def scan_local_data(roots, max_files=MAX_LOCAL_FILES, max_entries=MAX_LOCAL_ENTRIES):
    rows = []
    entries_seen = 0
    truncated = False
    for root in roots:
        if not root.exists():
            continue
        pending = [root]
        while pending and not truncated:
            current = pending.pop()
            try:
                entries = os.scandir(current)
            except OSError:
                continue
            with entries:
                for entry in entries:
                    if entries_seen >= max_entries or len(rows) >= max_files:
                        truncated = True
                        break
                    entries_seen += 1
                    path = Path(entry.path)
                    path_lower = str(path).lower()
                    if "openclaw-repair" in path_lower:
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(path)
                        elif entry.is_file(follow_symlinks=False) and path.suffix.lower() in DATA_SUFFIXES:
                            size = entry.stat(follow_symlinks=False).st_size
                            rows.append({
                                "root": str(root),
                                "path": str(path),
                                "name": path.name,
                                "suffix": path.suffix.lower(),
                                "bytes": size,
                                "size": human_bytes(size),
                            })
                    except OSError:
                        continue
            if truncated:
                break
    frame = pd.DataFrame(rows)
    frame.attrs.update(entries_seen=entries_seen, scan_truncated=truncated)
    return frame


local_files = scan_local_data(LOCAL_ROOTS)
print(f"Found {len(local_files):,} local data-like files")
print(f"Inspected {local_files.attrs['entries_seen']:,} filesystem entries; truncated={local_files.attrs['scan_truncated']}")
print("Total visible bytes:", human_bytes(local_files["bytes"].sum()) if len(local_files) else "0 B")
local_files.sort_values("bytes", ascending=False).head(20)
""",
            "local-scan",
        ),
        md(
            """
### Local matches for the selected dataset

These are actual files. A local match does not imply that preprocessing is
unfinished; it may be a redundant working copy of data already published in Lamin.
The combined status later makes that distinction explicit.
""",
            "local-filter-explain",
        ),
        code(
            """
local_query = selection["local_query"].lower()
if local_files.empty:
    local_matches = local_files.copy()
else:
    local_matches = local_files[
        local_files["path"].str.lower().str.contains(local_query, regex=False)
    ].sort_values("bytes", ascending=False)
print(f"{len(local_matches):,} local matches for {local_query!r}")
local_matches.head(50)
""",
            "local-filter",
        ),
        md(
            """
### Inspect one local payload without loading it

Set `LOCAL_PREVIEW_PATH` explicitly, or leave it as `None` to inspect the first
match. H5AD/HDF5 files are opened in backed/metadata mode; MatrixMarket only reads
the header; ZIP only lists members. Other formats return file metadata.
""",
            "local-preview-explain",
        ),
        code(
            """
LOCAL_PREVIEW_PATH = None
if LOCAL_PREVIEW_PATH is None and len(local_matches):
    LOCAL_PREVIEW_PATH = local_matches.iloc[0]["path"]
LOCAL_PREVIEW_PATH
""",
            "local-preview-choice",
        ),
        code(
            """
def inspect_local_payload(path_value):
    if not path_value:
        return {"message": "No local match selected"}
    path = Path(path_value)
    result = {"path": str(path), "bytes": path.stat().st_size, "size": human_bytes(path.stat().st_size)}
    lower = path.name.lower()
    if lower.endswith(".h5ad"):
        import anndata as ad
        data = ad.read_h5ad(path, backed="r")
        try:
            result.update({
                "format": "h5ad (backed)",
                "shape": tuple(data.shape),
                "obs_columns": list(data.obs.columns[:30]),
                "var_columns": list(data.var.columns[:30]),
                "x_backing_type": type(data.X).__name__,
            })
        finally:
            if getattr(data, "file", None) is not None:
                data.file.close()
    elif lower.endswith((".h5", ".hdf5")):
        import h5py
        with h5py.File(path, "r") as handle:
            result.update({"format": "HDF5", "top_level_groups": list(handle.keys())[:50]})
    elif lower.endswith(".mtx"):
        with path.open("rt", errors="replace") as handle:
            header = []
            dimensions = None
            for _ in range(100):
                line = handle.readline()
                if not line:
                    break
                header.append(line.rstrip())
                if line and not line.startswith("%"):
                    dimensions = line.split()
                    break
        result.update({"format": "MatrixMarket", "dimensions_line": dimensions, "header": header[:8]})
    elif lower.endswith(".zip"):
        with zipfile.ZipFile(path) as archive:
            result.update({"format": "ZIP", "members": archive.namelist()[:40], "member_count": len(archive.infolist())})
    else:
        result["format"] = "metadata only"
    return result


local_preview = inspect_local_payload(LOCAL_PREVIEW_PATH)
local_preview
""",
            "local-preview",
        ),
        md(
            """
## 3. Compare the canonical hierarchy with the live bucket

The following helper lists exactly one hierarchy level. It never downloads an
object and never performs a recursive walk. This keeps the live comparison
bounded even when a dataset contains many members.
""",
            "gcs-section",
        ),
        code(
            """
GCS_BUCKET_ROOT = "gs://scperturb/"
RAW_GCS_ROOTS = ["gs://scperturb/data/raw/"]
CLEANED_GCS_ROOTS = ["gs://scperturb/data/cleaned/"]
OTHER_GCS_ROOTS = ["gs://scperturb/other/"]
EXPECTED_TOP_LEVEL = {"README.md", "data/", "other/", ".lamindb/"}
EXPECTED_DATA_LEVEL = {"raw/", "cleaned/"}
GCLOUD_TIMEOUT_SECONDS = 120
GCS_BILLING_PROJECT = "jkobject-1549353370965"
MAX_GCS_RESULTS = 500
MAX_GCS_RESPONSE_BYTES = 2 * 1024**2
""",
            "gcs-options",
        ),
        code(
            """
def gcloud_adc_token(timeout=GCLOUD_TIMEOUT_SECONDS):
    completed = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        raise RuntimeError(f"Google ADC unavailable: {completed.stderr.strip()}")
    return completed.stdout.strip()


def list_gcs_level(uri, timeout=GCLOUD_TIMEOUT_SECONDS, max_results=MAX_GCS_RESULTS):
    if not uri.startswith("gs://"):
        raise ValueError("Expected a gs:// URI")
    parsed = urlparse(uri)
    bucket = parsed.netloc
    object_name = parsed.path.lstrip("/")
    token = gcloud_adc_token(timeout)
    if object_name and not uri.endswith("/"):
        query = urlencode({"userProject": GCS_BILLING_PROJECT, "fields": "name,size,updated"})
        endpoint = (
            f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o/"
            f"{quote(object_name, safe='')}?{query}"
        )
    else:
        query = urlencode({
            "prefix": object_name,
            "delimiter": "/",
            "maxResults": max_results,
            "userProject": GCS_BILLING_PROJECT,
            "fields": "items(name,size,updated),prefixes,nextPageToken",
        })
        endpoint = f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o?{query}"
    request = Request(endpoint, headers={"Authorization": f"Bearer {token}"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read(MAX_GCS_RESPONSE_BYTES + 1)
    except Exception as error:
        raise RuntimeError(f"GCS listing failed for {uri}: {error}") from error
    if len(payload) > MAX_GCS_RESPONSE_BYTES:
        raise RuntimeError(f"GCS response exceeded {MAX_GCS_RESPONSE_BYTES} bytes for {uri}")
    document = json.loads(payload)
    rows = []
    if "name" in document:
        size = int(document["size"])
        rows.append({"kind": "object", "uri": f"gs://{bucket}/{document['name']}", "bytes": size, "size": human_bytes(size)})
    else:
        for child_prefix in document.get("prefixes", []):
            rows.append({"kind": "prefix", "uri": f"gs://{bucket}/{child_prefix}", "bytes": None, "size": ""})
        for item in document.get("items", []):
            size = int(item["size"])
            rows.append({"kind": "object", "uri": f"gs://{bucket}/{item['name']}", "bytes": size, "size": human_bytes(size)})
    frame = pd.DataFrame(rows)
    frame.attrs.update(result_limit=max_results, listing_truncated=bool(document.get("nextPageToken")))
    return frame
""",
            "gcs-helper",
        ),
        md(
            """
### Load one cleaned dataset by name

`load_cleaned_dataset(name)` downloads only that dataset's `X.h5ad` and returns an
in-memory `AnnData`. Use `part="obs"` or `part="var"` to read just the corresponding
Parquet table without downloading `X`. H5AD reads have a default 1 GB safety limit;
raise `max_h5ad_bytes` explicitly if you really want a larger matrix on this machine.
""",
            "cleaned-loader-explain",
        ),
        code(
            """
def cleaned_dataset_paths(dataset_name):
    if not isinstance(dataset_name, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", dataset_name) is None:
        raise ValueError("dataset_name must be one safe canonical path segment")
    root = f"gs://scperturb/data/cleaned/{dataset_name}"
    return {
        "h5ad": f"{root}/X.h5ad",
        "obs": f"{root}/obs.parquet",
        "var": f"{root}/var.parquet",
    }


def _cleaned_gcs_filesystem():
    import gcsfs

    return gcsfs.GCSFileSystem(
        token="google_default",
        project=GCS_BILLING_PROJECT,
        requester_pays=True,
    )


def load_cleaned_dataset(dataset_name, part="h5ad", *, max_h5ad_bytes=1_000_000_000):
    if part not in {"h5ad", "obs", "var"}:
        raise ValueError("part must be one of: 'h5ad', 'obs', 'var'")
    uri = cleaned_dataset_paths(dataset_name)[part]
    remote_key = uri.removeprefix("gs://")
    fs = _cleaned_gcs_filesystem()
    if part in {"obs", "var"}:
        with fs.open(remote_key, "rb") as handle:
            return pd.read_parquet(handle)

    import tempfile
    import anndata as ad

    remote_bytes = int(fs.info(remote_key)["size"])
    if max_h5ad_bytes is not None and remote_bytes > max_h5ad_bytes:
        raise ValueError(
            f"{uri} is {human_bytes(remote_bytes)}, above the "
            f"max_h5ad_bytes={human_bytes(max_h5ad_bytes)} safety limit"
        )
    with tempfile.NamedTemporaryFile(suffix=".h5ad", delete=False) as handle:
        local_path = Path(handle.name)
    try:
        fs.get_file(remote_key, str(local_path))
        return ad.read_h5ad(local_path)
    finally:
        local_path.unlink(missing_ok=True)
""",
            "cleaned-loader",
        ),
        md(
            """
Examples—change only the name or `part`:

```python
adata = load_cleaned_dataset("SCP1467")
obs = load_cleaned_dataset("SCP1467", part="obs")
var = load_cleaned_dataset("SCP1467", part="var")
```
""",
            "cleaned-loader-examples",
        ),
        md(
            """
### Validate the bucket shape before inspecting datasets

The expected hierarchy and the observed hierarchy are kept separate. A missing
`data/raw/`, `data/cleaned/`, `other/`, or root README is a visible migration
gap—not an empty successful dataset inventory.
""",
            "gcs-contract-explain",
        ),
        code(
            """
def child_name(uri, parent):
    return uri.removeprefix(parent).rstrip("/") + ("/" if uri.endswith("/") else "")


bucket_top = list_gcs_level(GCS_BUCKET_ROOT)
observed_top = {
    child_name(uri, GCS_BUCKET_ROOT)
    for uri in bucket_top.get("uri", pd.Series(dtype=str))
}
data_top = list_gcs_level("gs://scperturb/data/")
observed_data = {
    child_name(uri, "gs://scperturb/data/")
    for uri in data_top.get("uri", pd.Series(dtype=str))
}
hierarchy_check = pd.DataFrame([
    {
        "level": "gs://scperturb/",
        "expected": sorted(EXPECTED_TOP_LEVEL),
        "observed": sorted(observed_top),
        "missing": sorted(EXPECTED_TOP_LEVEL - observed_top),
        "unexpected_or_legacy": sorted(observed_top - EXPECTED_TOP_LEVEL),
    },
    {
        "level": "gs://scperturb/data/",
        "expected": sorted(EXPECTED_DATA_LEVEL),
        "observed": sorted(observed_data),
        "missing": sorted(EXPECTED_DATA_LEVEL - observed_data),
        "unexpected_or_legacy": sorted(observed_data - EXPECTED_DATA_LEVEL),
    },
])
hierarchy_check
""",
            "gcs-contract-check",
        ),
        md(
            """
### Canonical raw datasets

Each child of `data/raw/` is a dataset `<NAME>`. Its descendants retain the
upstream/source organization needed for reconstruction.
""",
            "gcs-raw-roots-explain",
        ),
        code(
            """
raw_root_tables = []
for root in RAW_GCS_ROOTS:
    table = list_gcs_level(root)
    if len(table):
        table.insert(0, "root", root)
        raw_root_tables.append(table)
raw_root_listing = pd.concat(raw_root_tables, ignore_index=True) if raw_root_tables else pd.DataFrame()
raw_root_listing.head(100)
""",
            "gcs-raw-roots",
        ),
        md(
            """
### Canonical cleaned datasets

Each child of `data/cleaned/` is the same dataset `<NAME>`. Members follow the
explicit `X_*.h5ad`, `var.parquet`, and `obs_*.parquet` convention. Presence here
still does not prove registration in Lamin; we check Lamin separately.
""",
            "gcs-processed-roots-explain",
        ),
        code(
            """
cleaned_root_tables = []
for root in CLEANED_GCS_ROOTS:
    table = list_gcs_level(root)
    if len(table):
        table.insert(0, "root", root)
        cleaned_root_tables.append(table)
cleaned_root_listing = pd.concat(cleaned_root_tables, ignore_index=True) if cleaned_root_tables else pd.DataFrame()
cleaned_root_listing.head(100)
""",
            "gcs-processed-roots",
        ),
        md(
            """
### Build a live inventory of meaningful local and GCS dataset entries

The children of `data/raw/` and `data/cleaned/` are already the meaningful
dataset names. The resulting table contains live paths and prefixes, not a saved
progress export or a reconstruction of the legacy `pert-gym/` tree.
""",
            "layer-inventory-explain",
        ),
        code(
            """
MEANINGFUL_GCS_ROOTS = {
    "GCS raw": RAW_GCS_ROOTS,
    "GCS cleaned": CLEANED_GCS_ROOTS,
}


def dataset_token(uri):
    name = uri.rstrip("/").split("/")[-1]
    for ending in [".tar.gz", ".csv.gz", ".tsv.gz", ".h5ad", ".parquet", ".zip", ".tar"]:
        if name.lower().endswith(ending):
            name = name[: -len(ending)]
            break
    return name


layer_rows = []
# Group local files by the first directory below each configured root.
for root in LOCAL_ROOTS:
    root_string = str(root)
    if local_files.empty:
        continue
    for path_string in local_files.loc[local_files["root"] == root_string, "path"]:
        try:
            relative = Path(path_string).relative_to(root)
        except ValueError:
            continue
        token = relative.parts[0] if len(relative.parts) > 1 else dataset_token(path_string)
        layer_rows.append({"layer": "local download/working", "dataset_token": token, "location": str(root / token)})

for layer, roots in MEANINGFUL_GCS_ROOTS.items():
    for root in roots:
        table = list_gcs_level(root)
        if table.empty or "uri" not in table:
            continue
        for uri in table.loc[table["kind"].isin(["prefix", "object"]), "uri"]:
            layer_rows.append({"layer": layer, "dataset_token": dataset_token(uri), "location": uri})

layer_inventory = pd.DataFrame(layer_rows).drop_duplicates().sort_values(["layer", "dataset_token"])
print("Actual location entries:", len(layer_inventory))
display(layer_inventory.groupby("layer").size().rename("entries"))
layer_inventory.head(200)
""",
            "layer-inventory",
        ),
        md(
            """
This is an inventory of physical/logical **locations**. We classify it against
live Lamin after connecting below. Names such as `SCP211` and `GSE132080` match
well; internal slugs can require an explicit alias and are never silently treated
as definitive absence.
""",
            "layer-inventory-caveat",
        ),
        md(
            """
### Inspect the selected dataset's GCS locations

The preset supplies exact canonical raw and cleaned dataset prefixes.
Listings remain one level deep. Change either URI to descend into a child prefix.
""",
            "gcs-selected-explain",
        ),
        code(
            """
selected_gcs_raw = list_gcs_level(selection["gcs_raw"]) if selection.get("gcs_raw") else pd.DataFrame()
selected_gcs_cleaned = (
    list_gcs_level(selection["gcs_cleaned"])
    if selection.get("gcs_cleaned")
    else pd.DataFrame()
)
print("RAW:", selection.get("gcs_raw"))
display(selected_gcs_raw.head(100))
print("CLEANED:", selection.get("gcs_cleaned"))
display(selected_gcs_cleaned.head(100))
""",
            "gcs-selected",
        ),
        md(
            """
### Navigate to another GCS prefix

Paste any prefix from the tables above. Keep it at a directory level. There is no
recursive option by design.
""",
            "gcs-navigate-explain",
        ),
        code(
            """
GCS_BROWSE_URI = selection.get("gcs_raw")
gcs_browse = list_gcs_level(GCS_BROWSE_URI) if GCS_BROWSE_URI else pd.DataFrame()
gcs_browse.head(200)
""",
            "gcs-navigate",
        ),
        md(
            """
## 4. Review the canonical migration from compact receipts

This section is **offline-first**: it reads the versioned audit package under
`artifacts/gcs_canonical_migration/`, not millions of bucket objects. The live GCS
cells above remain the bounded readback. The receipt view records:

- the 111 canonical raw source prefixes;
- the 125 accepted cleaned dataset units and their 375 data objects;
- the 125 dataset README files added afterwards, yielding 500 cleaned objects;
- the exact legacy surfaces deliberately retained;
- the Lamin Artifact keys that actually existed and were remapped.

These receipts describe the immutable migration revision. Rerun the live cells when
you need to detect drift after that revision.
""",
            "migration-audit-section",
        ),
        code(
            """
def _dataset_name_from_key(key, prefix):
    if not isinstance(key, str) or not key.startswith(prefix):
        return None
    relative = key[len(prefix):]
    if "/" not in relative:
        return None
    dataset_name = relative.split("/", 1)[0]
    return dataset_name or None


def _aggregate_inventory_rows(rows, *, prefix, key_field, action_field=None):
    totals = {}
    for row in rows:
        if action_field and row.get(action_field) != "move":
            continue
        dataset_name = _dataset_name_from_key(row.get(key_field), prefix)
        if dataset_name is None:
            continue
        entry = totals.setdefault(dataset_name, {"objects": 0, "bytes": 0})
        numeric_size = row.get("bytes")
        if numeric_size is None:
            numeric_size = row.get("size", 0)
        entry["objects"] += 1
        entry["bytes"] += int(numeric_size or 0)
    return totals


def _compact_size(value):
    value = int(value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024


def build_dataset_inventory(raw_plan, cleaned_plan, readme_receipt, lamin_artifacts):
    # Return one row per canonical name with presence, object counts, and sizes.
    raw = _aggregate_inventory_rows(
        raw_plan, prefix="data/raw/", key_field="destination", action_field="action"
    )
    cleaned = _aggregate_inventory_rows(
        cleaned_plan,
        prefix="data/cleaned/",
        key_field="destination",
        action_field="action",
    )
    readmes = _aggregate_inventory_rows(
        readme_receipt.get("objects", []), prefix="data/cleaned/", key_field="name"
    )
    for dataset_name, values in readmes.items():
        entry = cleaned.setdefault(dataset_name, {"objects": 0, "bytes": 0})
        entry["objects"] += values["objects"]
        entry["bytes"] += values["bytes"]

    lamin_rows = [] if lamin_artifacts is None else lamin_artifacts.to_dict("records")
    lamin = _aggregate_inventory_rows(
        lamin_rows, prefix="data/cleaned/", key_field="key"
    )
    records = []
    for dataset_name in sorted(set(raw) | set(cleaned) | set(lamin)):
        raw_values = raw.get(dataset_name, {"objects": 0, "bytes": 0})
        cleaned_values = cleaned.get(dataset_name, {"objects": 0, "bytes": 0})
        lamin_values = lamin.get(dataset_name, {"objects": 0, "bytes": 0})
        in_raw = bool(raw_values["objects"])
        in_cleaned = bool(cleaned_values["objects"])
        in_lamindb = bool(lamin_values["objects"])
        layers = [
            label
            for present, label in [
                (in_raw, "raw"),
                (in_cleaned, "cleaned"),
                (in_lamindb, "LaminDB"),
            ]
            if present
        ]
        records.append({
            "dataset_name": dataset_name,
            "storage_membership": " + ".join(layers) if len(layers) > 1 else f"{layers[0]} only",
            "in_raw": in_raw,
            "raw_objects": raw_values["objects"],
            "raw_bytes": raw_values["bytes"],
            "raw_size": _compact_size(raw_values["bytes"]),
            "in_cleaned": in_cleaned,
            "cleaned_objects": cleaned_values["objects"],
            "cleaned_bytes": cleaned_values["bytes"],
            "cleaned_size": _compact_size(cleaned_values["bytes"]),
            "in_lamindb": in_lamindb,
            "lamin_artifacts": lamin_values["objects"],
            "lamin_bytes": lamin_values["bytes"],
            "lamin_size": _compact_size(lamin_values["bytes"]),
        })
    return pd.DataFrame(records)
""",
            "dataset-inventory-helper",
        ),
        code(
            """
AUDIT_DIR = Path("artifacts/gcs_canonical_migration")
required_audit_files = [
    "raw_plan.jsonl",
    "cleaned_direct_plan.jsonl",
    "cleaned_direct_datasets.json",
    "cleaned_readme_receipt.json",
    "final_layout_readback.json",
    "final_legacy_prefixes.json",
    "lamin_key_remap_receipt.json",
]
missing_audit_files = [name for name in required_audit_files if not (AUDIT_DIR / name).is_file()]
if missing_audit_files:
    raise FileNotFoundError(
        "Run this notebook from the repository root; missing audit files: "
        + ", ".join(missing_audit_files)
    )

raw_plan = [json.loads(line) for line in (AUDIT_DIR / "raw_plan.jsonl").read_text().splitlines() if line]
cleaned_plan = [
    json.loads(line)
    for line in (AUDIT_DIR / "cleaned_direct_plan.jsonl").read_text().splitlines()
    if line
]
cleaned_receipt = json.loads((AUDIT_DIR / "cleaned_direct_datasets.json").read_text())
cleaned_readmes = json.loads((AUDIT_DIR / "cleaned_readme_receipt.json").read_text())
layout_readback = json.loads((AUDIT_DIR / "final_layout_readback.json").read_text())
legacy_readback = json.loads((AUDIT_DIR / "final_legacy_prefixes.json").read_text())
lamin_remap = json.loads((AUDIT_DIR / "lamin_key_remap_receipt.json").read_text())

raw_names = {
    row["destination"].split("/", 3)[2]
    for row in raw_plan
    if isinstance(row.get("destination"), str)
    and row["destination"].startswith("data/raw/")
}
cleaned_names = set(cleaned_receipt["datasets"])
same_name = raw_names & cleaned_names
cleaned_without_same_name_raw = cleaned_names - raw_names
raw_without_same_name_cleaned = raw_names - cleaned_names
cleaned_data_objects_moved = sum(
    len(metadata.get("sources", {}))
    for metadata in cleaned_receipt["datasets"].values()
)
cleaned_readmes_added = layout_readback["cleaned_objects"] - cleaned_data_objects_moved

assert len(raw_names) == layout_readback["raw_dataset_prefixes"]
assert len(cleaned_names) == layout_readback["cleaned_datasets"]
assert cleaned_data_objects_moved == 3 * len(cleaned_names)
assert cleaned_readmes_added == len(cleaned_names)

migration_summary = {
    "raw_dataset_prefixes": layout_readback["raw_dataset_prefixes"],
    "cleaned_datasets": layout_readback["cleaned_datasets"],
    "cleaned_data_objects_moved": cleaned_data_objects_moved,
    "cleaned_readmes_added": cleaned_readmes_added,
    "cleaned_objects": layout_readback["cleaned_objects"],
    "same_name_raw_and_cleaned": len(same_name),
    "cleaned_without_same_name_raw": len(cleaned_without_same_name_raw),
    "raw_without_same_name_cleaned": len(raw_without_same_name_cleaned),
    "lamin_artifacts_remapped": lamin_remap["changed"],
}

rows = []
for dataset_name in sorted(raw_names | cleaned_names):
    in_raw = dataset_name in raw_names
    in_cleaned = dataset_name in cleaned_names
    if in_raw and in_cleaned:
        relationship = "same name in raw and cleaned"
    elif in_cleaned:
        relationship = "cleaned unit; no exact same-name raw prefix"
    else:
        relationship = "raw source package; no exact same-name cleaned unit"
    cleaned_metadata = cleaned_receipt["datasets"].get(dataset_name, {})
    source_path = next(
        (item.get("name", "") for item in cleaned_metadata.get("sources", {}).values()),
        "",
    )
    if dataset_name.startswith("cellxgene-"):
        cleaned_unit_kind = "CellxGene child dataset"
    elif dataset_name.startswith("GSM"):
        cleaned_unit_kind = "sample split from a larger study"
    elif "/datasets/" in source_path:
        cleaned_unit_kind = "child dataset from an accepted logical family"
    elif in_cleaned:
        cleaned_unit_kind = "direct accepted logical dataset"
    else:
        cleaned_unit_kind = "not applicable: raw-only source package"
    rows.append({
        "dataset_name": dataset_name,
        "in_raw": in_raw,
        "in_cleaned": in_cleaned,
        "relationship": relationship,
        "cleaned_unit_kind": cleaned_unit_kind,
        "accepted_record_id": cleaned_metadata.get("record_id", ""),
        "source_path_before_migration": source_path,
    })
raw_cleaned_reconciliation = pd.DataFrame(rows)

pd.DataFrame([migration_summary])
""",
            "migration-audit-load",
        ),
        md(
            """
### Why 125 cleaned is not expected to equal 111 raw

The two folders use different units of identity:

- **`raw/<NAME>/` is a source-package inventory.** One prefix can contain an archive,
  images, or a multi-dataset upstream collection. A raw package can also remain
  unprocessed.
- **`cleaned/<NAME>/` is a publishable biological matrix unit.** One accepted source
  family can split into several child datasets or samples, each with its own
  `X`/`obs`/`var` triplet and README.
- Therefore this is not “14 unexplained extra datasets”. The exact name-set comparison
  is **4 shared names**, **121 cleaned names without an exact same-name raw prefix**, and
  **107 raw names without an exact same-name cleaned directory**.
- Most cleaned-only names are deliberate children such as CellxGene UUID datasets or
  GSM samples. Their provenance remains visible in `accepted_record_id` and
  `source_path_before_migration` below.

An exact-name mismatch is not missing provenance. It means that source-package identity
and publishable-unit identity are not a bijection. The migration receipts preserve the
accepted **logical production path**, but they do not encode a validated direct parent
edge from every cleaned unit back to one canonical `raw/<NAME>/` prefix; the notebook
therefore does not invent that join.
""",
            "raw-cleaned-explanation",
        ),
        code(
            """
relationship_counts = (
    raw_cleaned_reconciliation
    .groupby(["relationship", "cleaned_unit_kind"], dropna=False)
    .size()
    .rename("dataset_names")
    .reset_index()
    .sort_values(["relationship", "dataset_names"], ascending=[True, False])
)
display(relationship_counts)
print("Exact same-name overlap:", sorted(same_name))
""",
            "raw-cleaned-counts",
        ),
        code(
            """
# Change this filter to inspect raw-only, cleaned-only, or exact-name matches.
RECONCILIATION_FILTER = "cleaned unit; no exact same-name raw prefix"
raw_cleaned_reconciliation.loc[
    raw_cleaned_reconciliation["relationship"] == RECONCILIATION_FILTER,
    [
        "dataset_name",
        "cleaned_unit_kind",
        "accepted_record_id",
        "source_path_before_migration",
    ],
].head(200)
""",
            "raw-cleaned-review-table",
        ),
        md(
            """
### Legacy surfaces intentionally retained

Only these historical areas remain under `pert-gym/staging/`: `logical/`,
`pert-gym/logical/`, and `vars/`. They hold accepted-manifest history, sparse/Zarr or
incomplete products, and older shared feature spaces that cannot be made canonical by
renaming alone. They are review evidence and conversion backlog, not members of
`data/cleaned/`. See `docs/adr/0001-logical-sparse-zarr.md` for the sparse/Zarr rule.
""",
            "legacy-explanation",
        ),
        code(
            """
legacy_surface_rows = []
for prefix in [
    "pert-gym/staging/logical/",
    "pert-gym/staging/pert-gym/logical/",
    "pert-gym/staging/vars/",
]:
    entry = legacy_readback.get(prefix, {})
    legacy_surface_rows.append({
        "prefix": prefix,
        "immediate_children": len(entry.get("children", [])),
        "direct_objects": entry.get("direct_objects", False),
        "why_retained": {
            "pert-gym/staging/logical/": "historical logical manifests/revisions",
            "pert-gym/staging/pert-gym/logical/": "sparse/Zarr, incomplete, and historical logical products",
            "pert-gym/staging/vars/": "older shared feature spaces pending explicit decommission",
        }[prefix],
    })
pd.DataFrame(legacy_surface_rows)
""",
            "legacy-review",
        ),
        md(
            """
## 5. Query actual LaminDB objects

This connects through the repository helper to `laminlabs/pertdata`, branch
`jkobject`. It does not use a local progress file. Collection counts, artifact
keys, UIDs, sizes, observation counts, paths, and feature links come from Lamin.
""",
            "lamin-section",
        ),
        code(
            """
from tools.lamin_context import connect_pertdata

ln = connect_pertdata()
assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
assert ln.setup.settings.branch.name == "jkobject"
print("Connected:", ln.setup.settings.instance.slug, "branch", ln.setup.settings.branch.name)
""",
            "lamin-connect",
        ),
        md(
            """
### Verify the key remapping against live LaminDB

The GCS migration moved **375 data objects** (three per cleaned dataset), but only
**69 of those objects had Artifact records in this Lamin branch**. Those 69 records
were updated in place: their UIDs stayed stable while their human-readable keys moved
from historical `pert-gym/staging/...` paths to `data/cleaned/...`.

The `.lamindb/` bucket prefix is separate: it is Lamin's technical storage namespace,
not a list of canonical project keys, and it was deliberately not moved.
""",
            "lamin-remap-explain",
        ),
        code(
            """
expected_lamin_keys = {change["new_key"] for change in lamin_remap["changes"]}
old_lamin_keys = {change["old_key"] for change in lamin_remap["changes"]}
canonical_queryset = ln.Artifact.filter(
    is_latest=True,
    key__startswith="data/cleaned/",
).order_by("key")
canonical_lamin_rows = []
for artifact in canonical_queryset[:500]:
    canonical_lamin_rows.append({
        "key": artifact.key,
        "uid": artifact.uid,
        "suffix": artifact.suffix,
        "bytes": artifact.size,
        "size": human_bytes(artifact.size),
        "storage_path": str(artifact.path),
    })
canonical_lamin_artifacts = pd.DataFrame(canonical_lamin_rows)
live_canonical_keys = set(canonical_lamin_artifacts.get("key", []))
old_lamin_keys_remaining = ln.Artifact.filter(key__in=sorted(old_lamin_keys)).count()

lamin_remap_readback = pd.DataFrame([{
    "receipt_rows": len(lamin_remap["changes"]),
    "live_latest_canonical_keys": len(live_canonical_keys),
    "receipt_keys_missing_live": len(expected_lamin_keys - live_canonical_keys),
    "unexpected_live_canonical_keys": len(live_canonical_keys - expected_lamin_keys),
    "old Lamin keys remaining": old_lamin_keys_remaining,
    "uids_stable_in_receipt": len({change["uid"] for change in lamin_remap["changes"]}),
}])
display(lamin_remap_readback)
canonical_lamin_artifacts.head(100)
""",
            "lamin-remap-readback",
        ),
        md(
            """
### One inventory table across raw, cleaned, and LaminDB

`dataset_inventory` is the CSV-shaped review table: one row per canonical name, booleans
for each layer, object counts, exact bytes, and human-readable sizes. `lamin_bytes` is
**catalog coverage of cleaned objects**, not an additional physical copy to add to the
GCS total. The three filtered DataFrames below expose raw-only names, every cleaned
name, and every name represented by a canonical Lamin Artifact.
""",
            "dataset-inventory-explain",
        ),
        code(
            """
dataset_inventory = build_dataset_inventory(
    raw_plan,
    cleaned_plan,
    cleaned_readmes,
    canonical_lamin_artifacts,
)
assert len(dataset_inventory) == 232
assert dataset_inventory["in_raw"].sum() == 111
assert dataset_inventory["in_cleaned"].sum() == 125
assert dataset_inventory["in_lamindb"].sum() == 23

inventory_summary = (
    dataset_inventory
    .groupby("storage_membership", dropna=False)
    .agg(
        datasets=("dataset_name", "size"),
        raw_bytes=("raw_bytes", "sum"),
        cleaned_bytes=("cleaned_bytes", "sum"),
        lamin_bytes=("lamin_bytes", "sum"),
    )
    .reset_index()
)
for column in ["raw", "cleaned", "lamin"]:
    inventory_summary[f"{column}_size"] = inventory_summary[f"{column}_bytes"].map(_compact_size)
display(inventory_summary)
with pd.option_context("display.max_rows", None):
    display(dataset_inventory)
""",
            "dataset-inventory-build",
        ),
        code(
            """
raw_only_datasets = dataset_inventory.query("in_raw and not in_cleaned and not in_lamindb").copy()
cleaned_datasets = dataset_inventory.query("in_cleaned").copy()
lamindb_datasets = dataset_inventory.query("in_lamindb").copy()

print(
    f"raw only: {len(raw_only_datasets)} | "
    f"cleaned: {len(cleaned_datasets)} | "
    f"represented in LaminDB: {len(lamindb_datasets)}"
)
display(raw_only_datasets)
display(cleaned_datasets)
display(lamindb_datasets)
""",
            "dataset-inventory-filters",
        ),
        code(
            """
def export_dataset_inventory_csv(path="dataset_storage_inventory.csv"):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    dataset_inventory.to_csv(destination, index=False)
    return destination.resolve()


# Uncomment to export all 232 rows next to the notebook, or choose another path.
# export_dataset_inventory_csv()
""",
            "dataset-inventory-export",
        ),
        md(
            """
### Collections that actually exist

Collection membership is not the same as artifact existence. Canonical
Collections usually contain `obs.parquet` anchors; their linked `X` and `var`
artifacts form the complete triplet.
""",
            "collections-explain",
        ),
        code(
            """
MAX_COLLECTION_ROWS = 100
collection_queryset = ln.Collection.filter(key__startswith="pert-gym/").order_by("key")
collection_count = collection_queryset.count()
collection_rows = []
for collection in collection_queryset[:MAX_COLLECTION_ROWS]:
    collection_rows.append({
        "key": collection.key,
        "uid": collection.uid,
        "members": collection.artifacts.count(),
    })
collections = pd.DataFrame(collection_rows)
print(f"{collection_count:,} Collections match; showing {len(collections):,}")
collections
""",
            "collections",
        ),
        md(
            """
### Search Lamin for the selected biological dataset

This is a live substring search on artifact keys. The total count is reported
separately from the displayed rows. Raise `MAX_LAMIN_ROWS` cautiously for highly
chunked datasets such as XAtlas.
""",
            "lamin-search-explain",
        ),
        code(
            """
MAX_LAMIN_ROWS = 100
lamin_query = selection["lamin_query"]
lamin_queryset = ln.Artifact.filter(is_latest=True, key__icontains=lamin_query).order_by("key")
lamin_match_count = lamin_queryset.count()
lamin_matches = []
for artifact in lamin_queryset[:MAX_LAMIN_ROWS]:
    lamin_matches.append({
        "key": artifact.key,
        "uid": artifact.uid,
        "suffix": artifact.suffix,
        "bytes": artifact.size,
        "size": human_bytes(artifact.size),
        "n_observations": artifact.n_observations,
        "storage_path": str(artifact.path),
    })
lamin_matches = pd.DataFrame(lamin_matches)
print(f"{lamin_match_count:,} latest Lamin artifacts match {lamin_query!r}; showing {len(lamin_matches):,}")
lamin_matches.head(MAX_LAMIN_ROWS)
""",
            "lamin-search",
        ),
        md(
            """
### Classify the live location inventory into the three requested views

For every meaningful local/GCS entry, the notebook performs a live same-key
Lamin lookup. Common path aliases are explicit below. The result is deliberately
phrased as evidence:

- canonical raw location where preprocessing is not demonstrated;
- canonical cleaned GCS location without a same-key Lamin match;
- a real Lamin artifact match, while preserving any remaining raw/cleaned copy.

A same-key miss is shown as a candidate, not asserted as biological absence.
""",
            "global-classification-explain",
        ),
        code(
            """
LAMIN_ALIASES = {
    "depmap_ccle26q1": "depmap_ccle/26q1",
    "perturbase_gse216481": "GSE216481",
    "sanger_dualguide_crc": "sanger_dual_guide_crc",
    "sanger_gdsc1": "sanger_gdsc/gdsc1",
    "sanger_gdsc2": "sanger_gdsc/gdsc2",
    "virtual_cell_vcpi": "ginkgo-datapoints/vcpi",
    "tcell_d4_rest": "tcell_gwps/D4_Rest",
}
MAX_LAYER_IDENTITIES = 300


def lamin_term(token):
    accession = re.search(r"(GSE\\d+|SCP\\d+|E-MTAB-\\d+|STDS\\d+|STT\\d+)", token, re.IGNORECASE)
    if accession:
        return accession.group(1)
    return LAMIN_ALIASES.get(token.lower(), token)


classified = layer_inventory.copy().head(MAX_LAYER_IDENTITIES)
match_counts = {}
for token in classified["dataset_token"].drop_duplicates():
    term = lamin_term(token)
    if token.lower() in {"temporal", "logical_sparse_zarr"}:
        match_counts[token] = 0
    else:
        match_counts[token] = ln.Artifact.filter(is_latest=True, key__icontains=term).count()
classified["lamin_search_term"] = classified["dataset_token"].map(lamin_term)
classified["lamin_artifact_matches"] = classified["dataset_token"].map(match_counts).fillna(0).astype(int)
cleaned_tokens = set(
    classified.loc[classified["layer"] == "GCS cleaned", "dataset_token"].str.lower()
)


def evidence_category(row):
    if row["lamin_artifact_matches"] > 0:
        return "present in LaminDB (raw/cleaned GCS copy may remain)"
    if row["layer"] == "GCS cleaned" or row["dataset_token"].lower() in cleaned_tokens:
        return "GCS cleaned; no same-key Lamin match"
    return "raw/downloaded candidate; cleaning not demonstrated"


classified["evidence_category"] = classified.apply(evidence_category, axis=1)
classified.groupby("evidence_category").size().rename("location_entries")
""",
            "global-classification",
        ),
        md(
            """
#### A. Downloaded/raw candidates

These real files or GCS prefixes have no same-key processed/Lamin evidence in this
bounded live comparison. Inspect aliases before treating any row as unfinished.
""",
            "raw-candidates-explain",
        ),
        code(
            """
raw_candidates = classified[
    classified["evidence_category"] == "raw/downloaded candidate; cleaning not demonstrated"
]
raw_candidates[["dataset_token", "layer", "location", "lamin_search_term"]].head(200)
""",
            "raw-candidates",
        ),
        md(
            """
#### B. Cleaned GCS data without a same-key Lamin match

These are the closest answer to “cleaned and stored in the canonical GCS
hierarchy, but not yet in LaminDB.” Exact member identity still needs inspection
before publication.
""",
            "processed-candidates-explain",
        ),
        code(
            """
cleaned_not_lamin = classified[
    classified["evidence_category"] == "GCS cleaned; no same-key Lamin match"
]
cleaned_not_lamin[["dataset_token", "location", "lamin_search_term"]].head(200)
""",
            "processed-candidates",
        ),
        md(
            """
#### C. Present in LaminDB

These rows have at least one live latest Artifact key match. The location table
keeps local/GCS copies visible so redundant staging is not mistaken for an
unpublished dataset.
""",
            "lamin-present-explain",
        ),
        code(
            """
locations_with_lamin = classified[classified["lamin_artifact_matches"] > 0]
locations_with_lamin[
    ["dataset_token", "layer", "location", "lamin_search_term", "lamin_artifact_matches"]
].sort_values(["dataset_token", "layer"]).head(300)
""",
            "lamin-present",
        ),
        md(
            """
A Lamin path such as `s3://lamin-us-west-2/.../.lamindb/<uid>.h5ad` is the real
managed payload location. The human-readable artifact key is the stable project
handle; the UID identifies the exact registered object.
""",
            "lamin-path-explain",
        ),
        md(
            """
### Check canonical Collection membership

This tests the matching artifact UIDs against the currently dated canonical
Collection. Artifacts may exist in Lamin while not belonging to this older
Collection version, so both facts are shown.
""",
            "membership-explain",
        ),
        code(
            """
CANONICAL_COLLECTION_KEY = "pert-gym/canonical/20260621"
canonical = ln.Collection.get(key=CANONICAL_COLLECTION_KEY)
matched_uids = set(lamin_matches.get("uid", pd.Series(dtype=str)))
canonical_uids = set()
if matched_uids:
    canonical_uids = set(
        canonical.artifacts.filter(uid__in=sorted(matched_uids)).values_list(
            "uid", flat=True
        )
    )
if len(lamin_matches):
    lamin_matches["in_canonical_20260621"] = lamin_matches["uid"].isin(canonical_uids)
    display(lamin_matches.head(MAX_LAMIN_ROWS))
""",
            "membership",
        ),
        md(
            """
### Follow a real `obs → X → var` triplet

The notebook selects the first matching `obs.parquet`. Feature links are read from
Lamin rather than guessed by replacing filenames. For non-expression datasets,
links can use typed auxiliary payloads and may differ from this triplet pattern.
""",
            "links-explain",
        ),
        code(
            """
def resolve_linked_artifact(value):
    if value is None:
        return None
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


selected_obs = None
if len(lamin_matches):
    obs_rows = lamin_matches[lamin_matches["key"].str.endswith("/obs.parquet")]
    if len(obs_rows):
        selected_obs = ln.Artifact.get(uid=obs_rows.iloc[0]["uid"])

triplet = {}
if selected_obs is not None:
    values = selected_obs.features.get_values()
    selected_x = resolve_linked_artifact(values.get("X"))
    selected_var = None
    if selected_x is not None:
        selected_var = resolve_linked_artifact(selected_x.features.get_values().get("var"))
    triplet = {
        "obs_key": selected_obs.key,
        "obs_uid": selected_obs.uid,
        "X_key": getattr(selected_x, "key", None),
        "X_uid": getattr(selected_x, "uid", None),
        "var_key": getattr(selected_var, "key", None),
        "var_uid": getattr(selected_var, "uid", None),
    }
triplet
""",
            "links",
        ),
        md(
            """
### Look inside a small Lamin metadata table

This loads only the selected `obs.parquet` when it is below the explicit byte
limit. It shows actual rows and columns from the dataset. It never loads the
remote `X.h5ad` matrix.
""",
            "lamin-preview-explain",
        ),
        code(
            """
MAX_LAMIN_METADATA_BYTES = 20 * 1024**2
lamin_preview = None
if selected_obs is None:
    print("No matching obs.parquet was found")
elif selected_obs.size is None:
    print("Refusing metadata load with unknown size:", selected_obs.key)
elif selected_obs.size > MAX_LAMIN_METADATA_BYTES:
    print("Refusing metadata load:", selected_obs.key, human_bytes(selected_obs.size))
else:
    lamin_preview = selected_obs.load()
    print("Loaded actual obs table:", selected_obs.key, lamin_preview.shape)
    display(lamin_preview.head(10))
""",
            "lamin-preview",
        ),
        md(
            """
### Matrix location without matrix materialization

For the linked `X`, we display identity, remote path, registered byte size and
`n_observations`. Loading the matrix is intentionally not part of this general
Mac notebook.
""",
            "matrix-metadata-explain",
        ),
        code(
            """
if triplet.get("X_uid"):
    x_artifact = ln.Artifact.get(uid=triplet["X_uid"])
    matrix_location = {
        "key": x_artifact.key,
        "uid": x_artifact.uid,
        "path": str(x_artifact.path),
        "bytes": x_artifact.size,
        "size": human_bytes(x_artifact.size),
        "n_observations": x_artifact.n_observations,
    }
else:
    matrix_location = {"message": "No linked X artifact resolved"}
matrix_location
""",
            "matrix-metadata",
        ),
        md(
            """
## 6. Combined location/status view

This summary is deliberately multi-valued. It does not erase raw copies merely
because Lamin exists, and it does not call a raw download "processed".
""",
            "combined-section",
        ),
        code(
            """
local_present = bool(len(local_matches))
gcs_raw_present = bool(len(selected_gcs_raw))
gcs_cleaned_present = bool(len(selected_gcs_cleaned))
lamin_present = lamin_match_count > 0

if lamin_present:
    primary_state = "present in LaminDB"
elif gcs_cleaned_present:
    primary_state = "cleaned on canonical GCS; no matching Lamin key found"
elif local_present or gcs_raw_present:
    primary_state = "raw/local only; no canonical cleaned GCS or matching Lamin key found"
else:
    primary_state = "not found with the configured canonical locations/aliases"

location_summary = pd.DataFrame([
    {"layer": "local working/download", "present": local_present, "location": ", ".join(str(p) for p in LOCAL_ROOTS), "matches": len(local_matches)},
    {"layer": "GCS raw", "present": gcs_raw_present, "location": selection.get("gcs_raw"), "matches": len(selected_gcs_raw)},
    {"layer": "GCS cleaned", "present": gcs_cleaned_present, "location": selection.get("gcs_cleaned"), "matches": len(selected_gcs_cleaned)},
    {"layer": "LaminDB latest artifacts", "present": lamin_present, "location": "laminlabs/pertdata / jkobject", "matches": lamin_match_count},
])
print(SELECTED_DATASET, "→", primary_state)
display(location_summary)
""",
            "combined-status",
        ),
        md(
            """
### How to interpret overlap

- **Local + GCS raw + Lamin**: data are published, but redundant source/working
  copies may still exist.
- **GCS cleaned + no Lamin match**: a canonical cleaned-data candidate, subject
  to checking aliases and exact member identity.
- **Local/GCS raw only**: downloaded/source data exist, but cleaning/publication
  is not demonstrated by these locations.
- **Lamin only**: durable managed data exist; canonical GCS paths may not yet be
  populated or may no longer be needed.

A substring miss is not a deletion claim. Adjust aliases before concluding that a
dataset is absent. The hierarchy check above is the authority for migration gaps.
""",
            "overlap-interpretation",
        ),
        md(
            """
## 7. Try the useful examples

Change only `SELECTED_DATASET` near the top and rerun all cells:

- `SCP211`: canonical raw/cleaned paths plus Lamin triplets;
- `GSE132080`: canonical dataset paths plus a Lamin triplet;
- `XAtlas HCT116`: canonical dataset paths plus many Lamin chunks;
- `GSE216481`: canonical dataset paths plus Lamin chunks under a different key shape;
- `Artista T37`: canonical paths and an example with no same-key Lamin match.

Then add the dataset you care about to `PRESETS`. Keeping aliases explicit makes
the result auditable and easy to correct.
""",
            "examples",
        ),
        md(
            """
## 8. What this notebook does not claim

- Presence in Lamin is not the same as acceptance into the latest versioned
  Collection or full `DATASET_E2E_V3` completion.
- A GCS `logical/` prefix can hold an incomplete, superseded, candidate, or
  rollback revision; inspect its immediate children and exact revision.
- Local files can be stale duplicates.
- No matrix-scale computation is performed here.
- This notebook locates and safely opens datasets; it does not mutate, clean, or
  delete any storage layer.
""",
            "limitations",
        ),
        md(
            """
## Recap

You now have direct handles to the data:

- local file paths and backed/header inspection;
- live GCS source and processed prefixes with object sizes;
- the compact migration receipts, exact raw/cleaned set differences, and retained legacy surfaces;
- a live readback of canonical Lamin Artifact keys against the remap receipt;
- live Lamin Collection keys, Artifact keys/UIDs, remote paths and feature links;
- a combined view that preserves every physical layer.

The next question is no longer “which progress file says done?” but “for this
biological dataset, which real payloads exist in each storage system?”
""",
            "recap",
        ),
    ]

    notebook = nbf.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {
                "display_name": "Python 3 (pert-gym project environment)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.11"},
        },
    )
    nbf.validate(notebook)
    return notebook


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(build(), OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
