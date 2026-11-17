# ITL-905: dict insertion order changes the row hash — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Equal dicts and sets built from Python values give equal row hashes on every path (nested, sync and async, fresh and cached), because a `dict` and a `set` are order independent (developer, November 11, 2026). A key/value list that arrives already in Arrow form is an ordered list and hashes as stored (ruling below).

**Architecture (final):** One ordering rule, defined once on Arrow *storage* values (`semantic_types/canonical_order.py`). It is applied in two places: (1) the converter sorts dict entries and plain `set[T]` elements when a Python value becomes Arrow; (3) a datagram built from Python values holds its dict values in the same canonical order, so a fresh value and a cached read-back value iterate identically. Fix (2), sorting dict-encoded lists at hash time, was implemented and then REMOVED by the developer's ruling: hashing an Arrow table never sorts anything. Sections 2, 4 and 5 below are the original proposal; "Outcome" at the end is authoritative.

**Tech Stack:** Python, PyArrow, starfix 0.4 (unchanged), pytest via `uv run`.

**Status (November 11, 2026, final design):** implemented on the branch; see "Outcome" at
the end, which supersedes sections 2, 4 and 5 where they differ.

> **Developer rulings (November 11, 2026):** Orcapod sorts dict and set values before storing
> them into Arrow; this is not starfix's concern. Hashing an Arrow table NEVER sorts anything
> (starfix ignores only column order and struct field order): a `list<struct<key, value>>`
> that arrives in Arrow form is an ordered list and its order is part of its hash. So fix 2
> (hash-time sorting) was implemented and then REMOVED. Plain `set[T]` was folded in (separate
> commit). In-memory canonical copy (fix 3) and raising on NaN/duplicate keys are kept,
> decided by the developer on November 12, 2026.

---

## 0. Findings from the investigation (main `b197544e`)

Reproduced: `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` give different row data hashes through `DictSource`.

Extra observations:

* An `ArrowTableSource` holding `large_list<struct<key, value>>` gets Python type `dict[K, V]`, and the two entry orders also hash differently. So the Arrow-native path is affected independently of the converter.
* Duplicate keys in an Arrow-native key/value list are accepted silently: `[{a,1},{a,2}]` hashes both entries but `as_dict()` returns `{'a': 2}`. The hash and the Python value disagree.
* `map<K, V>` columns are rejected by the hasher today (`NotImplementedError: Unsupported leaf type: map<...>`), so they are not a path to fix here.
* A Python `TypedDict` with fields `key`/`value` is not a supported type, and no other supported Python type maps to `list<struct<key, value>>`. In Orcapod's type system that layout always reads back as `dict[K, V]` (`arrow_type_to_python_type`, `_create_arrow_to_python_converter`).
* **Out of scope, but serious (reported, not fixed):** a plain `set[str]` value is stored as `list<large_string>` in Python set iteration order, which depends on `PYTHONHASHSEED`. The same set gives a different row hash in every process (verified with seeds 1, 2, 3). `set[<logical type>]` already sorts (`ListLogicalType`), plain `set[T]` does not.
* Out of scope: `record_id_column` on a dict-valued column builds its token from `str(dict)`, which is insertion ordered.

## 1. Every place a dict becomes Arrow (brief item a)

Python → Arrow, all through `UniversalTypeConverter.get_python_to_arrow_converter` → `_create_python_to_arrow_converter`, `origin is dict` branch (`universal_converter.py:1596-1606`):

| Path | Entry point |
|---|---|
| `DictSource`, `ListSource`, `DataFrameSource`, `PollingSource` | `python_dicts_to_arrow_table` |
| `Datagram` / `Data` / `Tag` from a Python dict (incl. function pod outputs, sync and async) | `Datagram._ensure_data_table` → `python_dicts_to_arrow_table` |
| `FunctionPodStream` / `FunctionNode` output tables | `python_dicts_to_struct_dicts` |
| dict nested in list / dict / tuple / Optional | the converter's own recursion |
| dict inside a dataclass / pydantic field | `DataclassLogicalType.python_to_storage` → `converter.python_to_storage` → same branch |
| dict as an element of `list[<logical>]` / `set[<logical>]` | not possible: a dict is not a logical type |

So fix (1) in that one branch covers every Python-origin path.

Arrow → hash without the converter: every `content_hash` of a table goes through `StarfixArrowHasher.hash_table` → `_process_table_columns`, which already walks every list/struct column value by value with `SemanticHashingVisitor`. Paths that reach it with entries in whatever order the table has: `ArrowTableSource` / `DeltaTableSource` / `CSVSource` input tables, rows read back from a result or pipeline store (incl. v0.2.0 stores), `DerivedSource`, and operators that rebuild tables (`Batch`, `GroupBy`, `MergeJoin`, `Join`, Polars round trips — none of them reorder list contents, but they pass through whatever order they were given). Fix (2) in the visitor covers all of them, sync and async (async operators hash through the same hasher).

## 2. Canonical form (brief item b)

**Rule:** entries of a dict are sorted ascending by key; the order is defined on the *converted (storage) key*, i.e. the value that is stored in Arrow and read back by `to_pylist()`.

Why the storage key and not the Python key:
* The hasher only has storage values (fix 2). Defining the order on storage values gives one function used by both the converter and the hasher, so the converter's order and the hasher's order cannot drift.
* Python keys that convert to the same storage value (naive datetime under `coerce_utc` vs the same UTC-aware datetime) must collide, not sort apart.
* Converted datetimes are all timezone-aware, so they are mutually comparable; raw Python datetimes may mix naive and aware and raise.

Sort key `dict_entry_sort_key(storage_key)`, total and independent of process, locale and platform:

| Key type (storage) | Order |
|---|---|
| `None` (key type `K \| None`) | first |
| `bool` | `False < True` |
| `int` (any width) | numeric |
| `float` | numeric; `-0.0` and `0.0` compare equal (they are one Python dict key); **NaN rejected** |
| `str` | Unicode code point (Python `str` comparison; equals UTF-8 byte order; never locale) |
| `bytes` | unsigned bytewise lexicographic |
| `date`, `datetime` (aware after conversion), `time`, `timedelta`, `Decimal` | natural order (datetime by instant) |
| `tuple[...]` (stored as `struct<f0, f1, ...>`) | lexicographic over fields in field order, each field by this table |

* NaN keys: NaN is not equal to itself, so equality of dicts with NaN keys is identity based and has no canonical form. Raise `ValueError` naming the column and the key.
* Keys whose storage values are not mutually comparable (anything outside the table above): raise `TypeError` naming the column and the key type. Logical/extension key types are already rejected when the type is registered (ET1), so in practice this is a guard.
* **Duplicate keys** after conversion (Arrow-native duplicates, or two Python keys that convert to one storage value): raise `ValueError` naming the column and key. A dict cannot hold two entries for one key, and silently keeping both (hash) while `as_dict()` keeps one is the current inconsistency.
* Nested dicts: values are canonicalised recursively first (the converter recurses into values before sorting; the visitor visits elements before sorting), then the outer entries are sorted by key only. Values never take part in the order (keys are unique).

**Why sorted dicts keep their hash:** canonicalisation only permutes entries; for a dict whose insertion order already equals the sorted order the stored list and the hashed bytes are byte-identical, so its hash does not move. This holds for str keys inserted in code-point order, ints ascending, etc. The golden suite proves it (section 5).

**Alternative considered — order-independent hash in starfix (stored order left alone).** starfix would need a way to hash a list column as a multiset/map: a per-field option (e.g. field metadata `orcapod:dict` or an `unordered_list` flag) that combines per-entry digests order-independently (sort entry digests, or a commutative combine). That is a cross-language spec change in the Rust crate plus `starfix-python`, a release, and a dependency bump, and ITL-898 is adding the opposite option (order-sensitive columns/struct fields) right now. It would also leave stored and read-back values in insertion order, so a fresh value and a cached value of the same graph still iterate differently (item c). **Recommendation: canonical sort in Orcapod (fixes 1+2+3), no starfix change.** Hash-time sorting reuses the visitor's existing per-value walk, so its cost is one sort per dict value.

## 3. What a pod reads back (brief item c)

With sort-on-write alone, a cached/read-back value is key-sorted while a fresh in-memory value keeps the user's insertion order (`Data.as_dict()` returns the dict the function returned). `==` agrees, iteration order and `repr` do not.

**Recommendation: canonicalise in memory too (fix 3).** When a `Datagram` is built from Python values, each field whose Python type contains a `dict` (directly or inside `list`/`tuple`/`dict`/`Optional`) is replaced by a **new** canonically ordered copy (no mutation of the user's object). Values inside dataclass/pydantic objects are not rebuilt (that would replace the user's object); their hash is canonical through fix 1, and this residual difference is documented on the hashing page. Then fresh and cached runs hand the downstream pod dicts that iterate identically.

## 4. Tables that already hold unsorted entries (brief item d)

* **On hash: yes** (fix 2). **On read: no.** Orcapod does not rewrite a user's Arrow table or stored rows (no mutation, no silent repair). A value read from an unsorted Arrow source or a v0.2.0 store keeps its stored order in `as_dict()`; only its hash is canonical. (Optionally a later issue can canonicalise at `ArrowTableSource` ingest; not proposed here.)
* **Is `list<struct<key, value>>` always a dict?** Yes in Orcapod's type system: the converter maps exactly that layout (a list — large, regular or fixed-size — whose element is a struct with exactly the two fields `key` and `value`, in either field order) to `dict[K, V]`, and no supported Python type produces it otherwise. A user who needs an ordered list of pairs uses `list[tuple[K, V]]` (stored `struct<f0, f1>`, never sorted) or other field names. The detection becomes one helper, `is_dict_arrow_type`, used by the converter's two `arrow → python` branches and by the hasher, so "what is a dict" has a single definition. Any other list of structs (other field names, or extra fields) is never sorted. The hashing reference page will state this rule.

## 5. Migration assessment (brief item e)

Hashes that move — **only for values containing a dict whose stored entry order was not already key-sorted**:

| Hash | Moves when |
|---|---|
| row data content hash (`Data.content_hash`, `INPUT_DATA_HASH_COL`, `OUTPUT_DATA_HASH_COL` of new rows) | the row holds such a dict at any depth |
| function pod cache key / result-store lookup | its input row holds such a dict |
| pipeline record id / entry id (system tags + input data hash) | same |
| source table hash → default `source_id` → per-row record ids (`_tag::source...`) | a source built without an explicit `source_id` holds such a dict in any row |
| stream / node `content_hash` that commit to the above | transitively |

Hashes that do **not** move: every schema hash, every `pipeline_hash` (schema + topology only), `source_id` given explicitly, any value with no dict or with dicts already in key order, all semantic-hasher hashes (it already sorts dict keys).

What a v0.2.0 store sees (pipeline path is unchanged because `pipeline_hash` is unchanged, so the same tables are read):
* Inputs whose dicts were key-sorted: same input hash → cache hit, same record id, served as before.
* Inputs with an unsorted dict: new input hash → result-cache miss → the pod recomputes once and a new row is written next to the old one. Old rows are never deleted or rewritten, so nothing looks dropped. A store-wide read (`get_all_records`, `FunctionJobNode` hot-load from the DB) shows **both** the old entry and the new one for such an input — duplicated, not dropped. This will be stated in the CHANGELOG with the remedy (drop the old rows, or start a fresh store). No automatic repair (standing rule).
* `EmptyData` replay of an old pipeline entry carries its stored (pre-fix) output hash to the downstream lookup; that is self-consistent with the downstream entries computed in the same old store, so nothing is mis-paired; it is replaced once the upstream entry is recomputed.
* Old stored rows with duplicate keys (only possible through an Arrow-native source) now raise on hashing, naming the column. Flagged for the coordinator: acceptable, or warn instead?

Golden suite (`tests/test_hashing/golden`):
* **No existing entry changes.** The only dict values in the golden cases are `datagram/nested` (`{"a": 1}`, `{"b": 2, "c": 3}` — already key-sorted) and `semantic/dict.ab`/`dict.ba` (semantic hasher, unaffected). The arrow group has no key/value list tables. The unchanged suite passing after the fix is the proof that sorted dicts keep their hash.
* **Added entries only** (regenerate, check the diff only adds lines): `datagram/dict_order` (rows `{"b": 2, "a": 1}` and `{"a": 1, "b": 2}` → equal hashes), nested `dict[str, dict[str, int]]` reordered, `dict[int, str]` with keys `10, 9`, a tuple-key dict; `arrow/table` `kv.sorted` and `kv.unsorted` (must be equal), each pinning the canonical value.

---

## File structure

* Create `src/orcapod/semantic_types/dict_order.py` — `is_dict_arrow_type(arrow_type)`, `dict_entry_sort_key(storage_key)`, `sort_dict_entries(entries, column)` (sort + NaN / incomparable / duplicate checks). Single definition of the rule.
* Modify `src/orcapod/semantic_types/universal_converter.py` — `origin is dict` converter branch sorts via `sort_dict_entries`; the two `arrow → python` dict detections use `is_dict_arrow_type`.
* Modify `src/orcapod/hashing/visitors.py` — `SemanticHashingVisitor.visit_list`: after visiting elements, if `is_dict_arrow_type(list_type)`, sort the processed entries with `sort_dict_entries` (column path from `_current_field_path`).
* Modify `src/orcapod/core/datagrams/datagram.py` — dict-backed init canonicalises dict-containing fields into new objects (fix 3), via a helper `canonicalize_dict_values(value, python_type, converter)` in `dict_order.py`.
* Tests: `tests/test_hashing/test_dict_order_hash.py` (public interface), golden additions in `tests/test_hashing/golden/cases.py` (new case functions only; no existing fixture moved or renamed).
* Docs: `docs/reference/hashing.md` (dict canonical order, what is a dict, duplicate/NaN errors), `CHANGELOG.md` `[0.3.0] — unreleased` (hash change + migration note), `DESIGN_ISSUES.md` (one entry at the end).

## Tasks (STEP 2, after approval)

### Task 1: failing public-interface regression tests

**Files:** Create `tests/test_hashing/test_dict_order_hash.py`

- [ ] Write tests (all through public API: sources, `Data.content_hash()`, `as_dict()`, `FunctionPod`/`CachedFunctionPod.process(stream).iter_data()`, `async_process_data` for the async pod path, databases' `get_all_records`):
  * `DictSource` inferred schema: `{"a":1,"b":2}` vs `{"b":2,"a":1}` → equal `content_hash`.
  * `DictSource` with `data_schema={"m": dict[str, int]}` → equal.
  * function pod `-> dict[str, int]` returning the two orders → equal output hashes; sync and async.
  * nested `dict[str, dict[str, int]]`, `list[dict[str, int]]`, `dict[str, list[dict[str,int]]]` reordered at inner and outer level → equal.
  * `ArrowTableSource` with `large_list<struct<key,value>>` in both orders → different, because Arrow lists hash as stored. Only the key-ordered list matches the `DictSource` hash.
  * key types: int (`{10:..., 9:...}`), bool, bytes, float incl. `-0.0`, date, aware datetime, tuple keys, Optional key with `None` → reorder gives equal hash.
  * NaN key → `ValueError` naming the column; Arrow-native duplicate key → `ValueError` naming the column.
  * `list[tuple[str,int]]` (ordered pairs) in two orders → hashes **differ** (non-dict lists are never sorted).
  * cached pod hit: `CachedFunctionPod` over `InMemoryArrowDatabase` and `DeltaTableDatabase(tmp_path)`; process `{"b":2,"a":1}` then `{"a":1,"b":2}` → one stored record, second run is a hit.
  * fresh vs cached values: the pod output dict read via `as_dict()` iterates in key order on the fresh run and on the cached run.
  * a dict already in key order keeps the exact hash it had before (assert against the hash pinned by the existing golden `datagram/nested` entry by running the same rows).
- [ ] Run `uv run pytest tests/test_hashing/test_dict_order_hash.py -v`; expected: reorder/Arrow/cache/NaN/duplicate tests FAIL, the `list[tuple]` and already-sorted tests PASS. Paste output into the PR description.
- [ ] Commit `test(hashing): failing regression tests for dict insertion order (ITL-905)`.

### Task 2: the ordering rule

**Files:** Create `src/orcapod/semantic_types/dict_order.py`; unit tests in `tests/test_semantic_types/test_dict_order.py` (these are about the helper itself).

```python
def is_dict_arrow_type(arrow_type: pa.DataType) -> bool:
    """True for a list (any kind) whose element is struct{key, value}, exactly those fields."""

def dict_entry_sort_key(storage_key: Any) -> tuple:
    # None first; struct (dict from to_pylist) → tuple of field keys in field order;
    # float NaN → ValueError; otherwise (1, value).

def sort_dict_entries(entries: list[dict], column: str) -> list[dict]:
    # Returns a new list sorted by dict_entry_sort_key(entry["key"]); raises ValueError on
    # NaN or duplicate keys and TypeError on incomparable keys, naming ``column``.
```

- [ ] Write the helper tests (ordering table from section 2, NaN, duplicates, incomparable), see them fail, implement, see them pass, commit.

### Task 3: converter sorts on write (fix 1)

**Files:** Modify `src/orcapod/semantic_types/universal_converter.py:1596-1606`, `1390-1401`, `1681-1702`.

- [ ] In the `origin is dict` converter: build `[{"key": key_converter(k), "value": value_converter(v)} ...]`, then return `sort_dict_entries(entries, column=...)`. Replace the two `set(field_names) == {"key", "value"}` checks with `is_dict_arrow_type`.
- [ ] Run Task 1 tests (Python-origin cases now pass; Arrow-native still fail) and `uv run pytest tests/test_semantic_types -q`. Commit.

### Task 4: hasher sorts dict entries (fix 2) — SUPERSEDED, do not implement

> Rejected by the developer (November 11, 2026) and reverted: hashing an Arrow table never sorts. `SemanticHashingVisitor.visit_list` stays unchanged.

**Files:** Modify `src/orcapod/hashing/visitors.py` (`SemanticHashingVisitor.visit_list`).

- [x] ~~After `_visit_list_elements`, if `is_dict_arrow_type(list_type)`, return `sort_dict_entries(processed, column=".".join(self._current_field_path))`. Run Task 1 tests (Arrow-native, nested, store cases pass) and `uv run pytest tests/test_hashing -q` (golden must pass unchanged). Commit.~~ (reverted)

### Task 5: canonical in-memory values (fix 3)

**Files:** Modify `src/orcapod/core/datagrams/datagram.py` dict-backed init; add `canonicalize_dict_values` to `dict_order.py`.

- [ ] Walk the Python type: `dict[K, V]` → new dict sorted by `dict_entry_sort_key(key_converter(k))` with values canonicalised; `list`/`tuple`/`Optional` → rebuilt only if a child changed; anything else returned as is. Run the fresh-vs-cached test. Commit.

### Task 6: golden additions

- [ ] Add the case functions listed in section 5 to `tests/test_hashing/golden/cases.py` (new functions only), regenerate with `uv run python tests/test_hashing/golden/generate_golden_hashes.py`, check `git diff tests/test_hashing/golden/data` only adds lines. Commit.

### Task 7: docs, CHANGELOG, DESIGN_ISSUES

- [ ] `docs/reference/hashing.md`: section "Dict values are order independent" (rule table, what is a dict, NaN/duplicate errors, read-back order, dataclass caveat). Runs under `tests/test_docs`.
- [ ] `CHANGELOG.md` `[0.3.0] — unreleased`: Fixed + "Hash changes" note with the migration text of section 5.
- [ ] `DESIGN_ISSUES.md`: one entry appended at the end, status resolved with **Fix:**.
- [ ] Full checks: `uv run pytest tests/`, `uv run --with ruff ruff check .`, `uv run --with ruff ruff format --check .`, `uv run --with basedpyright basedpyright` (exit code). Commit, merge origin/main, push, PR.

## Open questions for the coordinator

1. Approve canonical sort in Orcapod (converter + hasher + in-memory) over a starfix order-independent hash?
2. Approve fix 3 (in-memory canonical copy of dict values in plain containers; dataclass/pydantic internals left as given)?
3. NaN keys and duplicate keys raise (including when hashing a v0.2.0 stored row that has duplicates, only possible via Arrow-native input) — OK?
4. The `set[T]` per-process hash bug: separate issue (recommended), or fold into this PR (same mechanism: sort plain `set[T]` on write)?

---

## Outcome (as implemented)

Commits on `eywalker/itl-905-a-dict-values-insertion-order-changes-its`:
1. tests first (failing) incl. a Delta store written by v0.2.0 (`tests/test_hashing/data/`).
2. converter sorts dict entries (`semantic_types/canonical_order.py`, `CanonicalOrderError`).
3. hash-time sort (later reverted by ruling).
4. `Datagram` in-memory canonical copy.
5. golden: re-pin `pipeline:index_pick.index.row[s1|s2].data_hash` (approved), then additions only
   (`arrow:dict_order`, `datagram:dict_order`).
6. plain `set[T]` sorted at conversion (+ module/error rename).
7. revert hash-time sort; Arrow-native unsorted vs sorted pinned as DIFFERENT.
8. v0.2.0 fixture extended with a dict-returning pod: v0.3 does not serve v0.2.0 rows. Every input is recomputed, and the v0.2.0 tables remain unchanged.
9. docs (hashing reference 1c), CHANGELOG `[0.3.0]`, DESIGN_ISSUES DO1.

Correction to section 5: the golden suite DID hold one out-of-order dict
(`pipeline:index_pick`, `meta={"k":..,"j":..}`); my first survey truncated grep output. An AST
walk over every dict literal in `cases.py` found no other dict reaching Arrow out of key order
(`datagram:nested` is key-sorted; `semantic:*` dicts go to the semantic hasher only).

Performance: the Arrow hasher is byte-for-byte unchanged from `main`, so hashing tables with no
dict columns costs nothing extra. Conversion adds one sort per dict or set value.

---

## Review round 1 (CodeRabbit on `2994073e`) and merge of `main` `c8efc092`

Worker 2, November 11, 2026.

* Merge `origin/main` (`c8efc092`): conflicts only in `CHANGELOG.md` and `DESIGN_ISSUES.md`,
  resolved additively (one `[0.3.0]` section with both sides; DO1 and NP1 both kept).
  Golden diff vs main: 2 changed lines (the approved `index_pick` s1/s2 data hashes), 40 added.
* Findings and actions:
  1. `hashing.md` signed zero: asked coordinator (proposal: document, no normalisation).
  2. `hashing.md` fresh vs cached: qualify for `0.2.0` cache hits; same in CHANGELOG and DO1.
  3. `canonical_order._key_rank` NaN: reject every NaN-capable non-integral real
     (`numpy.float32`) and Decimal NaN, not only `float`. Failing test first.
  4. `ListLogicalType` set NaN (pre-existing code): asked coordinator (proposal: decline, report).
  5. Plan Task 4: marked superseded (this commit).
  6-9. Docstrings on new test helpers/classes/methods and golden case functions.
  10. `tarfile.extractall(filter="data")` needs 3.11.4: use the filter when available, else a
      member check (regular files and dirs only, no absolute or `..` paths).
  11. (body) `Datagram.update` / `with_columns` dict-only branches: apply the canonical copy.
      Failing test first.
  12. (body nits) `itertools.pairwise`, `datetime.UTC`: apply. ast-grep CWE-78 on a constant
      subprocess script and basedpyright "partially unknown" warnings from `DataValue`: decline.
