# CELLxGENE writer authorization manifest runbook

## Boundary

`writer-authorization-manifest.json` is the reviewed authorization set for the
CELLxGENE logical component writer. It replaces runtime selection of a
per-dataset authorization file. The detached
`writer-authorization-manifest.sha256` is the reviewable content-hash signature
for the exact manifest bytes.

The manifest authorizes no discovery. Each entry is copied from a frozen,
reviewed publication/source packet and binds:

- exact catalogue record and config bytes;
- collection, collection-version, dataset, dataset-version, and asset IDs;
- immutable HTTP HEAD identity (final URL, length, ETag, last-modified, and
  object version where supplied);
- expected shape, species, and assays;
- an explicit missingness policy that always forbids invented values;
- writer, contract, parity-helper, and live-ledger-helper bytes;
- exact task, revision, family lease, parent/correction review provenance, and
  expiry.

Runtime validates the detached manifest digest, expiry and review state before
it resolves exactly one record. It then validates the config hash and every
intrinsic identity, followed by the existing live source API/HEAD drift checks,
writer host/memory gate, global/family/legacy leases, and live accepted-component
ledger. A missing, duplicate, conflicting, stale, expired, or unreviewed entry
fails before external I/O.

This mechanism is authorization only. Metadata completeness remains in the
config's OBS predicates and semantic evidence. Unknown metadata can be allowed
only by naming the field in `missingness_policy.allowed_unknown_fields`; this
does not add, infer, or invent a value.

## Bounded batch update

1. Freeze each candidate publication/source packet as a sorted JSON config.
   Do not use mutable discovery output as authorization input.
2. For each config, add one manifest entry containing the exact identities
   listed above. `config_sha256` and `source_packet_sha256` must both equal the
   SHA-256 of the frozen config bytes.
3. Keep record IDs and config hashes unique across the whole manifest. Set
   `execution_authorized` only after the packet's independent review provenance
   is complete.
4. Update the shared writer-contract hashes from the exact reviewed files.
5. Serialize with sorted keys and a trailing newline, then regenerate the
   detached digest:

   ```bash
   shasum -a 256 writer-authorization-manifest.json \
     > writer-authorization-manifest.sha256
   ```

   The digest line must name exactly `writer-authorization-manifest.json`.
6. Run the focused tests:

   ```bash
   uv run pytest -q \
     tests/test_cellxgene_authorization_manifest.py \
     tests/test_cellxgene_writer_contract.py \
     tests/test_temporal_v4_099_review_artifact.py
   ```

7. Review the config rows, manifest rows, detached digest, and test output in
   one bounded PR. Adding a dataset changes data rows and hashes, not writer or
   validator code.

## Invocation

```bash
python write_component.py \
  --config row-N-config.json \
  --authorization-manifest writer-authorization-manifest.json \
  --authorization-manifest-sha256 writer-authorization-manifest.sha256
```

The legacy `--authorization` argument remains only for replaying the frozen
row-99 review fixture and historical fail-closed regression tests. Production
execution uses the manifest arguments above.

## Current migration

Rows 7, 55, and 111 are represented as three entries in the initial manifest.
Their prior reviewed packet identities and provenance are preserved exactly.
No VM, GCS, Lamin, candidate, publication, promotion, cleanup, or deletion is
performed by manifest generation or validation.
