# ITL-432: Pydantic/Dataclass Models as Pipeline Columns

**Issue:** ITL-432  
**Date:** 2026-06-27

## Overview

Pydantic models and dataclasses cannot flow through Orcapod pipelines as column
types. Two independent defects in the Arrow/Polars extension type machinery are
responsible. Both bugs are self-contained and addressed by surgical changes to
the hashing handler registry and Polars extension type construction.

---

## Bug A — Extension type reaching `ArrowDigester`

### Symptom

```
TypeError: unhashable type: '_ArrowExt___main___Cfg'
```

### Root cause

No semantic handlers are registered for `pydantic.BaseModel` subclasses or
dataclasses. In `SemanticHashingVisitor.visit_extension()`, when
`type_handler_registry.has_handler(python_type)` returns False, the visitor
returns the extension type and its storage value unchanged (the passthrough
path). That extension-typed column flows directly into `ArrowDigester.hash_table`.

`ArrowDigester` is a pure-Python starfix implementation. Its
`_primitive_data_type_string` function builds a lookup dict with Arrow
primitive type instances as keys (`if dt in _simple:`). Performing a dict
membership test on an unhashable extension type raises `TypeError`. Even if
`__hash__` were added to the extension type, `ArrowDigester` would raise
`NotImplementedError` because it has no handler for extension types — the
only correct fix is ensuring extension types never reach it.

### Fix: register semantic handlers for pydantic models and dataclasses

**`PythonTypeHandlerProtocol` — add optional `supports_type`**

Add an optional `supports_type(target_type: type) -> bool` method,
mirroring the `supports_class` pattern already used by
`LogicalTypeFactoryProtocol`. When defined, the handler registry calls it
after finding a handler via MRO walk; if it returns False the walk continues.
Handlers without `supports_type` are treated as unconditional matches
(existing behaviour unchanged).

**`PythonTypeHandlerRegistry` — respect `supports_type` in MRO walk**

Update `get_handler_for_type` to apply `supports_type` at every lookup point —
both the initial exact-match check and the MRO walk:

```python
def _try_handler(handler, target_type):
    """Return handler if it accepts target_type, else None."""
    if handler is None:
        return None
    if hasattr(handler, "supports_type") and not handler.supports_type(target_type):
        return None
    return handler

# exact match
handler = _try_handler(self._handlers.get(target_type), target_type)
if handler is not None:
    return handler
# MRO walk
for base in target_type.__mro__[1:]:
    handler = _try_handler(self._handlers.get(base), target_type)
    if handler is not None:
        return handler
return None
```

`get_handler(obj)` delegates to `get_handler_for_type(type(obj))` and
`has_handler(target_type)` delegates to `get_handler_for_type`, so both
inherit the fix automatically.

**New handlers in `builtin_handlers.py`**

```python
class PydanticModelHandler:
    """Handler for pydantic BaseModel instances — delegates to model_dump()."""

    def handle(self, obj: Any, hasher: SemanticHasherProtocol) -> Any:
        return obj.model_dump()


class DataclassModelHandler:
    """Handler for dataclass instances — delegates to dataclasses.asdict()."""

    def supports_type(self, target_type: type) -> bool:
        import dataclasses
        return dataclasses.is_dataclass(target_type)

    def handle(self, obj: Any, hasher: SemanticHasherProtocol) -> Any:
        import dataclasses
        return dataclasses.asdict(obj)
```

`model_dump()` and `dataclasses.asdict()` both return plain dicts that
accurately reflect the model's content. The recursive semantic hasher hashes
the returned dict, producing a stable content-based hash.

**Registration in `register_builtin_python_type_handlers`**

```python
from pydantic import BaseModel
registry.register(BaseModel, PydanticModelHandler())

import dataclasses as _dc
registry.register(object, DataclassModelHandler())
```

`PydanticModelHandler` is registered against `pydantic.BaseModel` — MRO
lookup finds it for any subclass. `DataclassModelHandler` is registered
against `object` (matching the pattern used by `DataclassLogicalTypeFactory`
in `v0.1.json`) and gated by `supports_type`, which returns True only for
actual dataclass types.

---

## Bug B — Metadata loss on Polars round-trip

### Symptom

```
ValueError: Arrow extension type '__main__.Cfg': expected metadata
b'{"category": "orcapod.pydantic"}' but got b''
```

### Root cause

`PydanticLogicalType.__init__` (line 93) and `DataclassLogicalType.__init__`
(line 93) both call `make_polars_extension_type(logical_name, storage_type)`
without passing `metadata`. The Arrow extension type is built with category
metadata (`b'{"category": "orcapod.pydantic"}'`), but the Polars extension
type carries no metadata.

When `pl.DataFrame(table).to_arrow()` reconstructs the Arrow column, Polars
calls `__arrow_ext_deserialize__` with the Polars extension's metadata — which
is empty bytes (`b''`). The strict equality check in `_deserialize` fails
because `b'' != b'{"category": "orcapod.pydantic"}'`.

### Fix: pass category metadata to `make_polars_extension_type`

In `PydanticLogicalType.__init__`:

```python
# Before:
self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)

# After:
self._polars_ext_class = make_polars_extension_type(
    logical_name,
    storage_type,
    metadata=json.dumps({"category": PYDANTIC_CATEGORY}),
)
```

Same change in `DataclassLogicalType.__init__`, using `DATACLASS_CATEGORY`.

After this fix, `pl.DataFrame(table).to_arrow()` passes
`b'{"category": "orcapod.pydantic"}'` to `__arrow_ext_deserialize__`, which
matches `_metadata` and succeeds.

---

## Files changed

| File | Change |
|------|--------|
| `src/orcapod/protocols/hashing_protocols.py` | Add optional `supports_type` to `PythonTypeHandlerProtocol` docstring and protocol stub |
| `src/orcapod/hashing/semantic_hashing/type_handler_registry.py` | Update `get_handler_for_type` to call `supports_type` when present; `get_handler` inherits the fix via delegation |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Add `PydanticModelHandler`, `DataclassModelHandler`; register both in `register_builtin_python_type_handlers` |
| `src/orcapod/extension_types/pydantic_logical_type_factory.py` | Pass `metadata` to `make_polars_extension_type()` in `PydanticLogicalType.__init__` |
| `src/orcapod/extension_types/dataclass_logical_type_factory.py` | Pass `metadata` to `make_polars_extension_type()` in `DataclassLogicalType.__init__` |
| `tests/test_hashing/test_pydantic_dataclass_hashing.py` | New regression tests (see below) |

---

## Tests

New file: `tests/test_hashing/test_pydantic_dataclass_hashing.py`

**Bug A regression — pydantic:**
Build a table with a pydantic model column (registered via the default
context), call `arrow_hasher.hash_table(table)`. Assert no `TypeError` is
raised and a `ContentHash` is returned.

**Bug A regression — dataclass:**
Same as above with a dataclass column.

**Bug B regression — pydantic Polars round-trip:**
Build a table with a pydantic model column, round-trip via
`pl.DataFrame(table).to_arrow()`, call `arrow_hasher.hash_table(round_tripped)`.
Assert no `ValueError` is raised and the hash equals that of the original table.

**Bug B regression — dataclass Polars round-trip:**
Same as above with a dataclass column.

**Handler unit tests:**
- `PydanticModelHandler.handle` returns `model.model_dump()` for a flat model
- `DataclassModelHandler.handle` returns `dataclasses.asdict(obj)` for a flat dataclass
- `DataclassModelHandler.supports_type` returns True for dataclasses, False for pydantic models and plain classes
- Registry MRO walk respects `supports_type`: registering `DataclassModelHandler` against `object` does not intercept non-dataclass lookups

---

## Out of scope

- Adding `__hash__` to synthesized extension types (tracked as a follow-up)
- Schema cleaner changes (no longer needed — the Polars metadata fix resolves the underlying cause)
- Deserialization relaxation (no backward-compatibility shims; greenfield project)
