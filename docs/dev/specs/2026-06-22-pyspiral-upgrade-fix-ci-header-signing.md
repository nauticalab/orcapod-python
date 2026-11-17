# Spec: Fix SpiralDB CI Test Failure via pyspiral Upgrade

**Date:** 2026-06-22
**Issue:** PLT-1773 — Fix ongoing Spiral database test failure in CI
**Status:** Approved

---

## Problem

All three `TestSpiralDBConnectorIntegration` tests have been failing in CI on every push to
`extension-type-system` and `main` since approximately June 15, 2026 (between 04:04 and 20:57
UTC). The error is identical across all tests:

```
Exception: Vortex error: Io: HttpRequest error: https://t3.storage.dev/...vortex?
  X-Amz-SignedHeaders=host%3Bif-none-match: 400 Bad Request.
  <Error><Code>AccessDenied</Code>
  <Message>There were headers present in the request which were not signed</Message>
  ...
```

### Root Cause

`pyspiral 0.11.7` (the version pinned in `uv.lock`) uses an embedded Rust HTTP client to
fetch Vortex columnar files from `t3.storage.dev` (SpiralDB's S3-compatible object store).
When the client fetches a file, it:

1. Generates a presigned S3 URL signing only `host` and `if-none-match` headers
   (`X-Amz-SignedHeaders=host%3Bif-none-match`).
2. Makes the GET request while the Rust HTTP runtime also sends additional headers not
   included in the signature.

Around June 15, 2026, `t3.storage.dev` tightened enforcement: every header present in the
request must now appear in `X-Amz-SignedHeaders`. Requests with unsigned headers are rejected
with `AccessDenied`. The last green CI run used the same runner image version
(`20260607.184.1`) as the first red run — confirming this is a server-side enforcement
change, not a runner or code regression.

**This is a pyspiral library bug fixed in newer releases.** pyspiral 0.14.x rewrote the
HTTP/auth stack to use Python-level `httpx` instead of the embedded Rust client, which
provides proper control over signed headers.

---

## Fix

### 1. Upgrade `pyspiral` lock to `0.14.9`

Run `uv lock --upgrade-package pyspiral`. This updates `uv.lock` from `0.11.7` to `0.14.9`.

### 2. Bump `pyproject.toml` minimum constraint

Change `pyspiral>=0.11.0` to `pyspiral>=0.14.0` in the `spiraldb` optional-dependency
group. This documents the effective floor (0.11.x is broken) and prevents accidental
downgrade.

### 3. No connector code changes required

Full API compatibility check between 0.11.7 and 0.14.9:

| API call (in `SpiralDBConnector`) | Status |
|---|---|
| `sp.Spiral(overrides=...)` | ✅ unchanged |
| `spiral.project(project_id)` | ✅ unchanged |
| `project.list_tables()` → `.dataset`, `.table` | ✅ `TableResource` still exposes both attrs |
| `project.table(table_id)` | ✅ unchanged |
| `table.schema().to_arrow()` | ✅ `Schema` still has `to_arrow()` |
| `table.key_schema.names` | ✅ `key_schema → Schema`; `Schema.names` present |
| `spiral.scan(tbl.select()).to_record_batches()` | ✅ returns `RecordBatchReader` (iterable) |
| `spiral.scan(tbl.select()).to_table()` | ✅ unchanged |
| `tbl.write(records)` | ✅ unchanged |
| `project.create_table(id, key_schema=[...], exist_ok=True)` | ✅ `key_schema` kwarg-only, same types |
| `project.drop_table(table_id)` | ✅ unchanged |

Unit tests mock the `sp` module entirely and are unaffected by the version change.

### 4. Add breadcrumb comments

- **`tests/test_databases/test_spiraldb_connector_integration.py`** — extend the module
  docstring to document the t3.storage.dev enforcement change and explain that pyspiral
  must be kept up to date.
- **`DESIGN_ISSUES.md`** — add a new entry recording the root cause, the symptom, and
  the resolution for future reference.

---

## Testing

- **Local unit tests** (`uv run pytest tests/test_databases/test_spiraldb_connector.py -v`)
  pass with no changes (mocks unaffected by version).
- **CI integration tests** (`spiral-integration` job on push to `extension-type-system`
  branch) must go green — that is the definitive verification that the fix resolves the
  header-signing rejection from `t3.storage.dev`.

---

## Scope boundaries

**In scope:**
- `pyproject.toml` minimum version bump
- `uv.lock` update
- Breadcrumb documentation

**Out of scope:**
- Changes to `SpiralDBConnector` logic (API is compatible — no edits needed)
- Pinning the runner image or GitHub Actions environment
- Any SpiralDB connector refactor

---

## Note on pyspiral version management

pyspiral is in active development and has released frequently (0.8.x through 0.14.x in
roughly six months). The project should expect to upgrade pyspiral periodically —
sometimes in direct response to SpiralDB server-side changes. Keeping the lock pinned
to a recent specific version (`0.14.9`) satisfies reproducibility; frequent bumps to
track the upstream are expected and normal until pyspiral reaches a stable release
cadence.
