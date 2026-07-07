# Design: `op.File` URL-form identity audit (ITL-474)

**Date:** 2026-07-07
**Issue:** ITL-474
**Status:** Approved

---

## Overview

Companion audit to the ENIGMA File System Spec (engmfs) project. `op.File` is
`UPath`-based, so registering `engmfs` as an fsspec protocol makes
`op.File("engm://ephys/x.bin")` work transparently. This spec records the audit
findings and specifies the regression tests that lock in URL-form identity preservation.

---

## Audit Findings

The audit examined every layer where `op.File` could eagerly resolve a URL-form path
to a host-specific concrete backend path. **No issues were found.** URL-form identity is
already preserved throughout.

### Construction (`File.__init__`)

`File.__init__` calls only `.is_symlink()`, `.exists()`, `.is_dir()`, and `.is_file()`
on `self.__wrapped__` — all are read-only filesystem checks that go through the fsspec
backend without resolving the URL to a concrete local path. No `.resolve()` or
`.absolute()` is called.

### `str()`

`ProxyUPath.__str__` delegates to `UPath.__str__()`, which returns the URL form
verbatim. `str(File("engm://ns/x.bin"))` = `"engm://ns/x.bin"`.

### Python `hash()`

`ProxyUPath.__hash__` delegates to `UPath.__hash__()`:

```python
def __hash__(self) -> int:
    return hash((self.protocol, self.__vfspath__()))
```

For `File("engm://ns/x.bin")` this is `hash(("engm", "/ns/x.bin"))` — deterministic,
URL-based, and host-independent.

### `LogicalFile` serialisation

`python_to_storage` stores `json.dumps({"path": str(value)})`, which encodes the URL
form. `storage_to_python` reconstructs via `File(path)` — the URL string round-trips
intact.

### Semantic hashing (`FileHandler`)

`FileHandler.handle()` extracts `self.__wrapped__` (the raw `UPath`) and passes it to
`FileHasher.hash_file()`, which reads file bytes and returns a SHA-256 content hash.
The hash is based on file content, not the path string, making it inherently portable
across hosts.

### `CachedFileHasher`

`CachedFileHasher.hash_file()` calls `path.resolve()` before building the `FileHashKey`
cache key. `UPath.resolve()` only normalises `.` and `..` path components — it does not
call any fsspec backend resolution. For a URL like `engm://ns/x.bin` (no `.`/`..`),
`resolve()` returns the path unchanged. The `FileHashKey.path` therefore retains the
URL form.

---

## What Changes

**No production code changes.** The audit confirms the implementation is already correct.

The deliverable is a suite of regression tests that:

1. Verify URL-form identity preservation so it cannot silently regress.
2. Serve as executable documentation of the audit findings.

---

## Regression Test Design

### Stub filesystem

Tests use the `memory://` fsspec protocol as a stand-in for `engm://`. It is always
available (bundled with fsspec), has identical non-local-protocol semantics, and requires
no mocking infrastructure beyond writing a file to the in-memory filesystem.

### Test locations

| Test class | File | What it covers |
|---|---|---|
| `TestURLFormIdentity` | `tests/test_extension_types/test_file_type.py` | `str()`, `hash()`, `LogicalFile` round-trip |
| `TestCachedFileHasherURLKey` (new method in existing class) | `tests/test_hashing/test_file_hashers.py` | `CachedFileHasher` cache-key URL preservation |

### `TestURLFormIdentity` — five tests

**Fixture:** Creates a file at `memory://ns/x.bin` with known content before the test
class runs; tears it down after.

| Test | Assertion |
|---|---|
| `test_str_preserves_url_form` | `str(File("memory://ns/x.bin")) == "memory://ns/x.bin"` |
| `test_hash_is_stable` | `hash(File("memory://ns/x.bin")) == hash(File("memory://ns/x.bin"))` |
| `test_hash_equals_upath_protocol_tuple` | `hash(File(...))` equals `hash(("memory", "/ns/x.bin"))` — confirms it is the URL-tuple hash, not any resolved value |
| `test_logical_file_storage_encodes_url` | `json.loads(python_to_storage(file))["path"] == "memory://ns/x.bin"` |
| `test_logical_file_round_trip_preserves_url` | `str(storage_to_python(python_to_storage(file))) == "memory://ns/x.bin"` |

### `CachedFileHasher` URL cache-key test

Added to `tests/test_hashing/test_file_hashers.py` as a new method on the existing
`TestCachedFileHasher` class (or as a standalone test if no such class exists).

| Test | Assertion |
|---|---|
| `test_cache_key_preserves_url_form` | After hashing `memory://ns/x.bin`, the `FileHashKey.path` in the cache has `str(key.path) == "memory://ns/x.bin"` |
