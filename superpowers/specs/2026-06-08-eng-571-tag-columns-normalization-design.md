# Design: `tag_columns` bare-string normalization (ENG-571)

**Date:** 2026-06-08
**Linear issue:** [ENG-571](https://linear.app/enigma-metamorphic/issue/ENG-571/source-tag-columns-should-accept-a-bare-string-as-a-single-column-name)

---

## Overview

Source classes that accept `tag_columns` in `__init__` currently expect an
iterable of column names.  Passing a bare string (e.g. `tag_columns="session_id"`)
silently iterates over its characters, producing `["s","e","s","s","i","o","n","_","i","d"]`
instead of `["session_id"]`.  This change adds a shared normalization helper and
applies it at every entry point where a user-supplied `tag_columns` value first
touches library code.

---

## Goals & Success Criteria

* `tag_columns="session_id"` and `tag_columns=["session_id"]` produce identical
  behaviour in every built-in source class.
* Lists, tuples, and other iterables of strings continue to work as today.
* Non-string, non-iterable values (e.g. `int`, `float`) raise a `TypeError` with
  a clear, actionable message.
* Iterables that contain non-string elements raise `TypeError` with element-type
  detail.
* All affected sources have their parameter annotation updated to
  `str | Collection[str]` (or `str | Collection[str] | None` where `None` means
  "use primary key").
* Unit tests cover: bare string, single-element list, multi-element list, tuple,
  invalid input, and the helper in isolation.

---

## Scope

**In scope:**

* `_normalize_column_list()` helper in `src/orcapod/utils/schema_utils.py`.
* All call sites where a user-supplied `tag_columns` value is first consumed:
  - `SourceStreamBuilder.build()` — defense-in-depth for any current or future
    source that passes `tag_columns` directly to the builder.
  - `DBTableSource.__init__` — replaces `list(tag_columns)`.
  - `SQLiteTableSource.__init__` — replaces `list(tag_columns)`.
  - `SpiralDBTableSource.__init__` — replaces `list(tag_columns)`.
  - `DataFrameSource.__init__` — replaces the inline `isinstance(tag_columns, str)`
    check.
  - `ArrowTableSource.__init__`, `CSVSource.__init__`, `DictSource.__init__`,
    `DeltaTableSource.__init__` — add normalization before passing to builder.
* Type annotation updates at all affected `__init__` signatures.
* New test file `tests/test_core/sources/test_tag_columns_normalization.py`.

**Out of scope:**

* `system_tag_columns` — a separate, less footgun-prone parameter; out of scope
  per the Linear issue.
* `column_config`, `all_info`, and any other unrelated init args.
* `ListSource` — its `tag_columns` is derived internally, not accepted from the
  user.
* Renaming `tag_columns` or related parameters.

---

## Architecture

### The normalization helper

Added to `src/orcapod/utils/schema_utils.py`:

```python
def _normalize_column_list(value: Any) -> list[str]:
    """Normalize a column-list argument to a plain list of strings.

    Accepts a bare string (wraps it in a list), any iterable of strings
    (converts to list), or raises ``TypeError`` for non-string non-iterable
    inputs or iterables that contain non-string elements.

    Args:
        value: A single column name (``str``) or an iterable of column names.

    Returns:
        A list of column name strings.

    Raises:
        TypeError: If ``value`` is not a ``str`` or iterable, or if any
            element of the iterable is not a ``str``.
    """
    if isinstance(value, str):
        return [value]
    try:
        result = list(value)
    except TypeError:
        raise TypeError(
            f"tag_columns must be a string or iterable of strings, "
            f"got {type(value).__name__!r}"
        )
    bad = [x for x in result if not isinstance(x, str)]
    if bad:
        raise TypeError(
            f"All tag_columns elements must be strings; "
            f"got {[type(x).__name__ for x in bad]!r}"
        )
    return result
```

The helper is private (`_` prefix) — it is an implementation detail of the
sources package, not a public API.

### Two distinct flows — both fixed

There are two paths through which a user-supplied `tag_columns` reaches
`SourceStreamBuilder.build()`:

**Flow 1 — Direct pass** (`ArrowTableSource`, `CSVSource`, `DictSource`,
`DeltaTableSource`):
```
__init__(tag_columns=...) ──► builder.build(tag_columns=tag_columns)
```
Fix: normalize in `__init__` before the builder call **and** in
`SourceStreamBuilder.build()` as defense-in-depth.

**Flow 2 — Pre-processed** (`DBTableSource`, `SQLiteTableSource`,
`SpiralDBTableSource`, `PostgreSQLTableSource`):
```
__init__(tag_columns=...) ──► list(tag_columns) ──► super().__init__(...) ──► builder.build(...)
```
Fix: replace `list(tag_columns)` with `_normalize_column_list(tag_columns)` at
each call site.  The builder fix also applies here as defense-in-depth.

### `SourceStreamBuilder.build()` change

The method normalizes `tag_columns` at entry before converting to a tuple:

```python
def build(self, table, tag_columns, ...):
    tag_columns = _normalize_column_list(tag_columns)
    tag_columns_tuple = tuple(tag_columns)
    ...
```

The method's type annotation for `tag_columns` is updated to
`str | Collection[str]`.

### Type annotation updates

| File | Parameter | Old type | New type |
|---|---|---|---|
| `arrow_table_source.py` | `tag_columns` | `Collection[str]` | `str \| Collection[str]` |
| `csv_source.py` | `tag_columns` | `Collection[str]` | `str \| Collection[str]` |
| `dict_source.py` | `tag_columns` | `Collection[str]` | `str \| Collection[str]` |
| `delta_table_source.py` | `tag_columns` | `Collection[str]` | `str \| Collection[str]` |
| `data_frame_source.py` | `tag_columns` | already `str \| Collection[str]` | no change |
| `db_table_source.py` | `tag_columns` | `Collection[str] \| None` | `str \| Collection[str] \| None` |
| `sqlite_table_source.py` | `tag_columns` | `Collection[str] \| None` | `str \| Collection[str] \| None` |
| `postgresql_table_source.py` | `tag_columns` | `Collection[str] \| None` | `str \| Collection[str] \| None` |
| `spiraldb_table_source.py` | `tag_columns` | `Collection[str] \| None` | `str \| Collection[str] \| None` |
| `stream_builder.py` | `tag_columns` | `Collection[str]` | `str \| Collection[str]` |

---

## Error handling

| Input | Result |
|---|---|
| `"session_id"` | `["session_id"]` |
| `["session_id"]` | `["session_id"]` |
| `("a", "b")` | `["a", "b"]` |
| `[]` | `[]` |
| `42` | `TypeError: tag_columns must be a string or iterable of strings, got 'int'` |
| `[1, "b"]` | `TypeError: All tag_columns elements must be strings; got ['int']` |

`None` is **not** passed to `_normalize_column_list` — sources that accept
`None` to mean "use primary key" handle the `None` check before calling the
helper.

---

## Testing

New file: `tests/test_core/sources/test_tag_columns_normalization.py`

### Helper unit tests (`_normalize_column_list`)
* Bare string → single-element list
* Single-element list → same list
* Multi-element list → same list
* Tuple of strings → list
* Empty list → empty list
* Integer → `TypeError`
* Float → `TypeError`
* List containing integer elements → `TypeError` with element-type detail

### Per-source integration tests (parametrize over source types)
Sources covered: `DataFrameSource`, `ArrowTableSource`, `CSVSource`, `DictSource`,
`DeltaTableSource`, `SQLiteTableSource` (`:memory:` + known PK).

For each:
* `tag_columns="col_name"` (bare string) → same `keys()` as `tag_columns=["col_name"]`
* `tag_columns=["col_name"]` (single-element list) → baseline
* `tag_columns=["a", "b"]` (multi-element list) → baseline
* `tag_columns=("col_name",)` (tuple) → same as list
* `tag_columns=42` → `TypeError`

`PostgreSQLTableSource` and `SpiralDBTableSource` require live databases; bare-string
normalization is validated indirectly through `DBTableSource` tests using the
`MockDBConnector` (already used in `test_db_table_source.py`).

---

## Implementation checklist

1. Add `_normalize_column_list()` to `src/orcapod/utils/schema_utils.py`.
2. Update `SourceStreamBuilder.build()`: normalize `tag_columns` at entry, update type hint.
3. Update `DataFrameSource.__init__`: replace inline check with `_normalize_column_list()`.
4. Update `ArrowTableSource.__init__`: add normalization before builder call, update type hint.
5. Update `CSVSource.__init__`: add normalization, update type hint.
6. Update `DictSource.__init__`: add normalization, update type hint.
7. Update `DeltaTableSource.__init__`: add normalization, update type hint.
8. Update `DBTableSource.__init__`: replace `list(tag_columns)` with helper, update type hint.
9. Update `SQLiteTableSource.__init__`: replace `list(tag_columns)` with helper, update type hint.
10. Update `SpiralDBTableSource.__init__`: replace `list(tag_columns)` with helper, update type hint.
11. Update `PostgreSQLTableSource.__init__`: update type hint only (delegates to `DBTableSource`).
12. Write `tests/test_core/sources/test_tag_columns_normalization.py`.
13. Run full test suite; confirm no regressions.
