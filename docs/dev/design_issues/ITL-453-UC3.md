## `src/orcapod/semantic_types/universal_converter.py`

### UC3 — Function signature hash order-dependent for union-typed annotations
**Status:** resolved
**Severity:** high
**Issue:** ITL-453

``get_function_signature()`` in ``hash_utils.py`` used ``str(param)`` to build
the signature string. For union type annotations, ``str(param)`` calls
``inspect.formatannotation``, which falls through to ``repr(annotation)`` —
reflecting declaration order. Two semantically identical signatures like
``foo(x: str | Path)`` and ``foo(x: Path | str)`` therefore produced different
``_function_signature_hash`` values, silently breaking content addressability
for any pipeline that refactored union member order.

**Fix:** Added ``_is_union_annotation`` and ``_canonical_annotation_str``
helpers to ``hash_utils.py``. Union members are now sorted byte-wise by their
``inspect.formatannotation`` string before being embedded in the signature.
Applied the same fix to ``FunctionSignatureExtractor.extract_function_info``.
Also added ``eval_str=True`` to ``inspect.signature()`` so that string
annotations produced by ``from __future__ import annotations`` (PEP 563) are
resolved to live type objects before union detection. Non-union annotations are
byte-for-byte unchanged.
