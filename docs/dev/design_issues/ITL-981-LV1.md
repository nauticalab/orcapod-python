## `src/orcapod/logical_types/ignore_list.py`, `src/orcapod/logical_types/numpy_type.py` — library value types

### LV1 — Library value types: storage and hash decided separately, strict hash, per-type ignore list
**Status:** resolved
**Severity:** high
**Issue:** ITL-981 (supersedes the storage and hash of ITL-888, entry AR1)

The first v0.3 array format (ITL-888, never released) tied storage to the hash: an array was
stored in Orcapod's own canonical struct (C order, little-endian, packed fields) and hashed
as that row. So an array did not read back as it was given, and the hash silently ignored
memory order and byte order, with no way to say otherwise. The same question was open for
torch tensors, Polars and pandas data frames.

Developer's decisions (November 12, 2026): storage is the library's own serialization and a
value reads back as given; the hash is defined by Orcapod on the content, never on the stored
bytes; the hash is strict by default (anything about a value that can be changed and that
storage keeps is part of its identity); what the hash ignores is a per-type list of property
names saved in the extension metadata, so two lists are two logical types. A property storage
cannot restore never counts, because a value read back must hash like the value written.

**Fix:** `logical_types/ignore_list.py` holds the shared mechanism: the metadata shape
`{"ignore":[...],"version":N}`, `IgnoreListLogicalType`, `IgnoreListHandler`, one Arrow
extension class per list. The data context declares each type's list once, in `hash_ignore`
of `contexts/logical_types.json`. The Arrow hasher hashes a cell under the list of its own
column. `numpy.ndarray` is stored as a `.npy` file (format 3.0, no pickle) under
`orcapod.numpy.ndarray` and hashed in two steps: the element digest of dtype, shape and bits,
then the type name with memory order, byte order and structured layout, each ignorable.
Design note: `superpowers/specs/2026-11-12-itl-981-library-value-types-design.md`.

Known limits, by decision: a data context has one list per type and refuses a column stored
under another (the Python schema carries only `np.ndarray`, so a per-column list needs a user
API, which v0.3 does not have); a strided view is stored and hashed as its C-contiguous copy;
unit-less datetimes and field titles stay refused (ITL-911) although `.npy` could store them.
