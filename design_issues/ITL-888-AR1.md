## `src/orcapod/logical_types/numpy_type.py`, `src/orcapod/logical_types/array_format.py` — array format

### AR1 — `numpy.ndarray` stored as a `.npy` file and hashed by file bytes
**Status:** in progress
**Severity:** high
**Issue:** ITL-888 (absorbs ITL-883; design ITL-874)

Arrays were stored as the exact `np.save` file in a `large_binary` column under extension name
`numpy.ndarray`, and hashed as SHA-256 of that file under the bare label `sha256`. The hash
followed the file, not the value: equal arrays in C and Fortran order, in little- and big-endian
byte order, or with structured fields at other offsets hashed differently. Masked arrays lost
their mask silently. `StringDType` failed with a misleading pickle error. A pod declared
`-> np.ndarray` could return a NumPy scalar unchecked.

v0.3 moves to the value-based format of `superpowers/specs/2026-11-10-array-format-v0.3.md`:
extension `orcapod.ndarray` over `struct<dtype, shape, data>` (little-endian, C order), hashed
by value under the label `ndarray_v1` (developer's decisions, November 10 and 11, 2026). Old
data is neither detected nor migrated in v0.3; migration is deferred to ITL-889. Accepting
`longdouble`/`clongdouble`, unit-less datetimes and titled structured dtypes, refused in v0.3,
is reconsidered in ITL-911. The new logical type name moves the table paths of nodes with an
array column and the pipeline and operator tables of every node downstream of one (pipeline
hashes are Merkle-chained); an array-free pod downstream keeps its result table and reuses its
results.

Known compromise: the struct's children are nullable at the Arrow level, because Polars drops
nested non-null flags on its way back to Arrow (ITL-842). The logical type enforces non-null
instead. Tighten under a new storage version once nested nullability survives Polars.

**Superseded (ITL-981, November 12, 2026):** the struct storage and the layout-independent hash
described above were replaced before any release by `.npy` storage under
`orcapod.numpy.ndarray` and a strict value hash with a per-type ignore list; see entry LV1
(`design_issues/ITL-981-LV1.md`). The refusals, the declared-output check and the v0.2
statements of this entry still hold.
