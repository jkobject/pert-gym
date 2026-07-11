# Agent context

Use this page as a concise routing guide; detailed migration rules are in
[migration-reproducibility-and-gcs-exit.md](migration-reproducibility-and-gcs-exit.md).

- `laminlabs/pertdata` is the durable system of record. Connect only with
  `tools.lamin_context.connect_pertdata()` and preserve the branch policy.
- Treat project GCS as temporary staging. Large payload/GCS/Lamin work belongs
  on the warm EU VM, never the Mac; the Mac may perform metadata-first control
  work only.
- Keep artifacts, Collection members/chunks, logical datasets, and model-ready
  datasets as distinct denominators. Use an accepted live inventory as the
  authority for each count.
- The target is adaptive sparse-Zarr/shared-var with legacy-triplet adaptation.
  Preserve legacy data until an accepted replacement has parity/readback and a
  rollback identity.
- One writer publishes a logical dataset through the append-only,
  crash-recoverable journal/recovery workflow. Do not infer a completed dataset
  from a stage or a running card.
- Each added logical dataset requires an executable processing-decisions notebook
  that can reconstruct from immutable upstream sources/checksums or a retained
  Lamin raw artifact. GCS removal requires reviewed `GCS_DECOMMISSION_READY`.

Status labels are normative: **ACCEPTED** has a merged/independent gate;
**CURRENT** is live but not necessarily complete; **PENDING** is not a completed
claim.
