## `src/orcapod/logical_types/`

### NC1 — A `set[T]` of a primitive was stored, read back and hashed as a list
**Status:** resolved
**Severity:** high
**Issue:** ITL-611

`set[T]` where `T` has no logical type (`set[int]`, `set[str]`, `set[bytes]`,
`set[datetime]`) was stored as a plain `large_list<T>`, the same Arrow type as `list[T]`. The
set identity was in no stored table and in no row hash: a downstream pod annotated
`set[int]` received a `list` even inside the job that computed it, `node.as_source()` read a
`list` under a schema that said `set[int]`, a job loaded in another process said `list[int]`,
and the row `{1, 2, 3}` as `set[int]` hashed exactly like `[1, 2, 3]` as `list[int]`. A
`set[T]` of a logical type (`set[Path]`) was already the native set container, but a job
loaded in a fresh process could not restore it (`parse_arrow_type_string` resolved only
extension names already registered in the process) and read its raw storage, with a row hash
that differed from the in-process one.

**Fix:** the developer approved (November 14, 2026) storing a set of a primitive as the
native set container: `ListLogicalType` over an `ArrowElement`, extension name
`set[<element Arrow type in its large variants>]` (`set[int64]`), metadata
`{"category": "set", "element_kind": "arrow"}` (developer, November 15, 2026: the name
holds the element as it is and is the registry key; the metadata does not repeat it, and a
reader takes the element from the storage, refusing a storage that is not a list of the
element the name names), no version key, elements in the
canonical set order of `canonical_order.sort_set_elements` (unchanged, so the stored values
are as before and only the type identity moves). `list[T]` of a primitive stays a plain
`large_list`. A container's elements are value-hashed only when its innermost element is a
logical type (a `set[bytes]` is hashed as stored, as a `list[bytes]` column is). A saved
schema rebuilds an unregistered native container from its identifier through the registered
factory, so a loaded job restores `set[int]` and `set[Path]` alike. The row hash of every row
holding a set of a primitive moves (documented; no golden value moved, entries for
`set[int]`, `set[str]` and `set[bytes]` were added). Known limits: inside a dict value or a
tuple field a set of a primitive keeps its plain storage (Arrow has no extension type in a
struct field); `frozenset[T]` is refused, its support to be evaluated in ITL-1140; a store
written by a development build of v0.3 that holds a set of a primitive as a plain list is not
refused on read (it serves lists, and a run that adds a row to it raises when the two forms
are concatenated): clear such nodes.
