# ADR 0001: Versioned logical sparse Zarr surfaces

- Status: Accepted for new logical surfaces
- Date: 2026-07-11
- Policy: `config/logical_sparse_zarr_policy.v1.json`

## Context

The existing triplet contract (`obs.parquet -> X.h5ad -> var.parquet`) is the
compatibility baseline. It is correct for small and legacy datasets, but makes
large backed sources operationally expensive: XAtlas/Orion (HCT116 and HEK293T),
large PRISM sources, and the ~22M cell T-cell GWPS source currently use fixed
row chunks and duplicate `var.parquet` per chunk. Temporal/spatial datasets and
PerturbAI are heterogeneous and must not silently lose modality/provenance
information when their matrices become large.

This ADR does **not** migrate, revise, replace, or delete any current Lamin
artifact. It defines a new, immutable logical surface for future writes.

## Decision

### 1. Logical surface and versioning

A logical sparse surface is an immutable `manifest.json` with:

- `format: "pert-gym.logical-sparse-zarr"` and integer `version: 1`;
- global `shape`, `nnz`, sparse orientation (`csr` or `csc`), dtype and codec;
- row-major, half-open chunk intervals `[start, end)`; and
- exactly one `obs.parquet` for each X chunk plus one shared `var.parquet`.

A consumer resolves a promoted manifest revision rather than listing an object
prefix. Unknown versions fail closed. The normalizer in
`src/pert_gym/sparse_zarr_contract.py` supports this v1 contract and legacy
`pert-gym.triplet-h5ad` metadata. Legacy triplets are exposed as one logical
chunk and are never rewritten in place.

### 2. Adaptive chunk size and balanced tails

Chunking is row-major for the high-throughput CSR writers. The initial target
rows are derived from `n_obs`, `n_vars`, `nnz`, and the configured RSS limit:

```
floor((max_rss_bytes - n_vars * 8) /
      ((max(1, nnz / n_obs) * bytes_per_nnz + 8) * materialization_factor))
```

The result is clamped to policy min/max rows. The writer measures the first
chunk's peak RSS and reduces later chunks before they exceed the limit. It must
not retain materialized prior chunks. The final plan uses
`ceil(n_obs / target_rows)` chunks and distributes the remainder, so chunks
have sizes differing by at most one instead of an undersized final tail.

CSC is supported for a producer/read workload that needs it, but it still uses
logical obs intervals and must meet the same parity rules. Matrix orientation
is recorded in the manifest and is never inferred from a key name.

### 3. Shared var identity

A var object can be shared only when all identity components match:

1. `var_index_sha256`: SHA-256 of exact UTF-8 var-index strings in order,
   separated by LF and terminated by LF;
2. `var_frame_sha256`: SHA-256 of canonical JSON Lines (sorted var column
   names, rows in index order, normalized JSON scalar values, terminating LF);
3. `schema_fingerprint`: the feature-schema identity.

A matching row count, a filename, or an opaque dataframe hash is not enough.
The manifest stores the shared var key and hashes; readers reject a mismatch.
HCT116 and HEK293T therefore remain separate until their complete identities
match, and PRISM shares only after normalization proves identity.

### 4. Obs provenance

Every chunk records its exact shape, dtype, and exact 64-hex (case-insensitive)
SHA-256 checksums for `data`, `indices`, and `indptr`; it has exactly one unique,
nonempty obs-sidecar key. Each obs sidecar records source URI, a versioned raw-file
checksum (`sha256-file-bytes/v1:<64-hex-digest>`), a source-row interval with the
same row count as its X chunk, ingestion run identifier, and writer version. The
shared var identity additionally requires a nonempty `schema_fingerprint` and
exact SHA-256 values. The obs sidecar is authoritative for row order. No
concatenate/reorder operation is permitted without an explicit new manifest
revision and provenance update.

### 5. Exact integrity denominator and promotion

The denominator is exact, not approximate:

- chunk intervals tile `[0, shape[0])` without a gap or overlap;
- `sum(end - start) == shape[0]`;
- `sum(chunk.nnz) == manifest.nnz`; and
- each chunk records shape, nnz, dtype and checksums for sparse arrays.

Before promotion, deterministic probes include the first/last row and probes
from every chunk. Source and readback must agree on row/obs identity, shape,
nnz, indices, and values. A failed candidate cannot become the active alias.

### 6. Rollback

Writes are append-only revisions. Promotion atomically points a logical alias
to a fully verified immutable manifest. Rollback repoints to the last promoted
manifest and retains rejected candidates (with a reason) for audit. It never
deletes a candidate or mutates a current artifact.

## Dataset-family policy

| Family | Surface | Var sharing | Special rule |
| --- | --- | --- | --- |
| XAtlas Orion HCT116 | CSR logical Zarr | Same source identity only | Backed row reads and adaptive RSS gate. |
| XAtlas Orion HEK293T | CSR logical Zarr | Same source identity only | Separate from HCT116 unless all identity hashes match. |
| PRISM Perturb-seq | CSR for large/chunked sources | Per normalized feature identity | Each accession remains its own logical dataset. |
| T-cell GWPS | CSR logical Zarr | Per source-matrix identity | Preserve source intervals for all 22M cells. |
| Temporal/spatial | CSR expression plus explicit sidecars | Identical modality/feature identity only | Preserve coordinates, images and time annotations as declared sidecars. |
| PerturbAI | CSR only when sparse expression is supplied | Producer-specific until canonical schema is proven | Preserve producer manifest; unknown versions fail closed. |

## Consequences

New large write paths must use the machine-readable policy and manifest
validator before a remote write. Existing chunkers are not altered by this ADR;
a follow-up implementation can adopt the writer contract without changing their
legacy outputs. The configuration defines a VM-only benchmark for 5k/10k/25k
representative CSR/CSC surfaces and records explicit case-local RSS baseline,
peak, peak delta, and a `case_rss_peak_measurement` declaration. The current
benchmark isolates each case in its own process and reports the operating
system's high-water RSS over matrix generation and write/readback. It also
records wall time, bytes, separate matrix/obs/source-row parity, and a total
wall time.
