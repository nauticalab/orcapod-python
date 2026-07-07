# op.File URL-form Identity — Regression Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add regression tests that lock in URL-form identity preservation for `op.File` across construction, `str()`, `hash()`, `LogicalFile` serialisation, and `CachedFileHasher` cache-key behaviour.

**Architecture:** Two test additions — a new `TestURLFormIdentity` class in the existing `test_file_type.py` (covering `File` and `LogicalFile`) and one new method in the existing `TestCachedFileHasher` class in `test_file_hashers.py`. No production code changes. The `memory://` fsspec protocol (always bundled with fsspec) stands in for `engm://`.

**Tech Stack:** pytest, fsspec (memory filesystem), upath.UPath, orcapod.extension_types.file_type.File/LogicalFile, orcapod.hashing.file_hashers.CachedFileHasher/FileHasher/FileHashKey, orcapod.hashing.hash_cachers.InMemoryHashCacher

---

### Task 1: Add `TestURLFormIdentity` to `test_file_type.py`

**Files:**
- Modify: `tests/test_extension_types/test_file_type.py` (append new class at end of file)

- [ ] **Step 1: Append the new test class**

Open `tests/test_extension_types/test_file_type.py` and append the following block at the **end of the file**, after the last existing class:

```python
class TestURLFormIdentity:
    """Regression tests: op.File preserves URL-form identity for non-local protocols.

    Uses ``memory://`` as a stable, always-available stand-in for ``engm://``.
    The same invariants hold for any non-local fsspec protocol.
    """

    @pytest.fixture(autouse=True)
    def memory_file(self):
        """Create ``memory://ns/x.bin`` with known content; clean up after."""
        import fsspec
        fs = fsspec.filesystem("memory")
        fs.mkdir("/ns", exist_ok=True)
        with fs.open("/ns/x.bin", "wb") as fh:
            fh.write(b"url-identity-test-content")
        yield
        fs.rm("/ns/x.bin")

    def test_str_preserves_url_form(self):
        f = File("memory://ns/x.bin")
        assert str(f) == "memory://ns/x.bin", (
            f"Expected URL form 'memory://ns/x.bin', got {str(f)!r}"
        )

    def test_hash_is_stable(self):
        h1 = hash(File("memory://ns/x.bin"))
        h2 = hash(File("memory://ns/x.bin"))
        assert h1 == h2, "hash() must be identical across two constructions of the same URL"

    def test_hash_equals_upath_protocol_tuple(self):
        # UPath.__hash__ = hash((protocol, vfspath))
        # For memory://ns/x.bin: protocol="memory", vfspath="/ns/x.bin"
        # This test pins the exact hash contract so any regression is immediately visible.
        expected = hash(("memory", "/ns/x.bin"))
        actual = hash(File("memory://ns/x.bin"))
        assert actual == expected, (
            f"hash(File('memory://ns/x.bin')) should equal hash(('memory', '/ns/x.bin')), "
            f"got {actual} vs {expected}"
        )

    def test_logical_file_storage_encodes_url(self):
        f = File("memory://ns/x.bin")
        lt = LogicalFile()
        storage = lt.python_to_storage(f)
        data = json.loads(storage)
        assert data["path"] == "memory://ns/x.bin", (
            f"python_to_storage must encode URL form; got path={data['path']!r}"
        )

    def test_logical_file_round_trip_preserves_url(self):
        f = File("memory://ns/x.bin")
        lt = LogicalFile()
        recovered = lt.storage_to_python(lt.python_to_storage(f))
        assert str(recovered) == "memory://ns/x.bin", (
            f"storage_to_python(python_to_storage(f)) must preserve URL form; "
            f"got {str(recovered)!r}"
        )
```

- [ ] **Step 2: Run the new tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_file_type.py::TestURLFormIdentity -v
```

Expected output (all five pass):
```
PASSED tests/test_extension_types/test_file_type.py::TestURLFormIdentity::test_str_preserves_url_form
PASSED tests/test_extension_types/test_file_type.py::TestURLFormIdentity::test_hash_is_stable
PASSED tests/test_extension_types/test_file_type.py::TestURLFormIdentity::test_hash_equals_upath_protocol_tuple
PASSED tests/test_extension_types/test_file_type.py::TestURLFormIdentity::test_logical_file_storage_encodes_url
PASSED tests/test_extension_types/test_file_type.py::TestURLFormIdentity::test_logical_file_round_trip_preserves_url
5 passed
```

- [ ] **Step 3: Run the full `test_file_type.py` to confirm nothing regressed**

```bash
uv run pytest tests/test_extension_types/test_file_type.py -v
```

Expected: all pre-existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_file_type.py
git commit -m "test(file_type): add TestURLFormIdentity regression tests (ITL-474)"
```

---

### Task 2: Add URL cache-key test to `TestCachedFileHasher`

**Files:**
- Modify: `tests/test_hashing/test_file_hashers.py` (append one method to `TestCachedFileHasher`)

- [ ] **Step 1: Append the new test method to `TestCachedFileHasher`**

Open `tests/test_hashing/test_file_hashers.py` and append the following method at the **end of the `TestCachedFileHasher` class** (after `test_clear_cache_forces_rehash`):

```python
    def test_cache_key_preserves_url_form(self):
        """CachedFileHasher must not resolve URL-form paths to concrete backend paths.

        UPath.resolve() only normalises ``.``/``..`` components — it does not call
        any fsspec backend resolution. The FileHashKey.path must therefore retain
        the original URL string for non-local protocols.
        """
        import fsspec
        from upath import UPath

        fs = fsspec.filesystem("memory")
        fs.mkdir("/ns", exist_ok=True)
        with fs.open("/ns/cache_key_test.bin", "wb") as fh:
            fh.write(b"cache-key-url-test")

        try:
            inner = FileHasher(algorithm="sha256")
            cacher = InMemoryHashCacher()
            cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

            cached.hash_file(UPath("memory://ns/cache_key_test.bin"))

            keys = list(cacher._cache.keys())
            assert len(keys) == 1, f"Expected 1 cache entry, got {len(keys)}"
            key = keys[0]
            assert str(key.path) == "memory://ns/cache_key_test.bin", (
                f"Cache key path must preserve URL form; got {str(key.path)!r}"
            )
        finally:
            fs.rm("/ns/cache_key_test.bin")
```

- [ ] **Step 2: Run the new test to confirm it passes**

```bash
uv run pytest tests/test_hashing/test_file_hashers.py::TestCachedFileHasher::test_cache_key_preserves_url_form -v
```

Expected:
```
PASSED tests/test_hashing/test_file_hashers.py::TestCachedFileHasher::test_cache_key_preserves_url_form
1 passed
```

- [ ] **Step 3: Run the full `test_file_hashers.py` to confirm nothing regressed**

```bash
uv run pytest tests/test_hashing/test_file_hashers.py -v
```

Expected: all pre-existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_hashing/test_file_hashers.py
git commit -m "test(file_hashers): add URL cache-key preservation test for CachedFileHasher (ITL-474)"
```

---

### Task 3: Full test suite verification and PR

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass, no new failures.

- [ ] **Step 2: Push the branch**

```bash
git push -u origin eywalker/itl-474-opfile-audit-url-form-identity-preservation-for-engmfs
```

- [ ] **Step 3: Open a PR against `main`**

```bash
gh pr create \
  --base main \
  --title "test(file_type): add URL-form identity regression tests (ITL-474)" \
  --body "$(cat <<'EOF'
## Summary

Audit result for ITL-474: `op.File` already preserves URL-form identity correctly at every layer — no production code changes needed.

This PR adds regression tests that lock in that behaviour:

- `TestURLFormIdentity` (5 tests) in `tests/test_extension_types/test_file_type.py`: covers `str()`, `hash()` stability, hash equals `hash((protocol, vfspath))`, `LogicalFile.python_to_storage()` encoding, and `LogicalFile` round-trip.
- `test_cache_key_preserves_url_form` in `tests/test_hashing/test_file_hashers.py`: confirms `CachedFileHasher` does not resolve URL-form paths to concrete backend paths when building the `FileHashKey`.

Both test classes use `memory://` (always-available fsspec protocol) as a stand-in for `engm://`.

Closes ITL-474
EOF
)"
```
