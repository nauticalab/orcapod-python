# PostRunPayload InvocationContext Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `invocation_context: InvocationContext | None` to `PostRunPayload` so post-run hook authors have access to the same deterministic invocation hash, `format_id()`, and `pipeline_run_id` that `SideEffectPod` functions already receive.

**Architecture:** Extract `InvocationContext`/`InvocationHashConfig` from `side_effects.py` into a new stdlib-only leaf module `invocation.py`. Add the field to `PostRunPayload` (with `= None` default for backward compat). Thread `run_id` into `_build_post_run_payload` and call `_build_invocation_context()` there so every hook payload carries a fully-populated context.

**Tech Stack:** Python 3.11+, PyArrow (for preimage hashing in `_build_invocation_context`), pytest + uv.

**Issue:** ITL-531  
**Spec:** `superpowers/specs/2026-07-21-postrunpayload-invocation-context-design.md`  
**Branch:** `eywalker/itl-531-align-postrunpayload-with-sideeffectpod-invocationcontext`

---

## File Map

| Action | File | Purpose |
|--------|------|---------|
| **Create** | `src/orcapod/invocation.py` | New leaf module: `InvocationHashConfig`, `_serialize_component`, `InvocationContext` |
| **Modify** | `src/orcapod/side_effects.py` | Remove moved definitions; import from `invocation.py` |
| **Modify** | `src/orcapod/hooks.py` | Import `InvocationContext`; add `invocation_context` field to `PostRunPayload` |
| **Modify** | `src/orcapod/core/function_pod.py` | Thread `run_id` into `_build_post_run_payload`; build and attach `InvocationContext` |
| **Modify** | `src/orcapod/core/cached_function_pod.py` | Pass `run_id` to `_build_post_run_payload` in overridden hook methods |
| **Modify** | `src/orcapod/__init__.py` | Import `InvocationContext`/`InvocationHashConfig` from `.invocation` instead of `.side_effects` |
| **Modify** | `tests/test_core/function_pod/test_post_run_hooks.py` | Add `TestInvocationContextOnPayload` test class |

---

## Task 1: Create `src/orcapod/invocation.py` leaf module

**Files:**
- Create: `src/orcapod/invocation.py`

This task extracts the three invocation-identity items from `side_effects.py` into a new module with no orcapod-internal runtime imports. No behavioral change — only relocation.

- [ ] **Step 1: Create `src/orcapod/invocation.py`**

```python
# src/orcapod/invocation.py
"""Invocation identity types for orcapod pipeline elements.

Provides ``InvocationHashConfig``, ``InvocationContext``, and the internal
``_serialize_component`` helper. These types describe how a single pod
invocation is identified — independent of whether the pod is a side-effect
pod or a function pod. Extracted here so both ``hooks.py`` and
``side_effects.py`` can import them without circular-import risk.
"""

from __future__ import annotations

import base64
import dataclasses
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# InvocationHashConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    """Controls how ``InvocationContext.invocation_hash`` is serialized.

    Args:
        encoding: Output encoding — ``"hex"`` (default) or ``"base64"``.
        component_length: Bytes of raw digest to use per component. ``None``
            means full digest length. Applied identically to every
            ``::``-separated component.
    """

    encoding: Literal["hex", "base64"] = "hex"
    component_length: int | None = None


def _serialize_component(content_hash: ContentHash, config: InvocationHashConfig) -> str:
    """Serialize one ``ContentHash`` component per ``InvocationHashConfig``.

    The method name is always included as a prefix (e.g. ``"arrow_v2.1:abcd1234"``).
    Only the digest bytes are subject to truncation via ``component_length``.

    Args:
        content_hash: The hash to serialize.
        config: Encoding and truncation config.

    Returns:
        A string of the form ``"{method}:{encoded_digest}"`` where the digest
        is optionally truncated then encoded as hex or base64.
    """
    raw = content_hash.digest
    if config.component_length is not None:
        raw = raw[:config.component_length]
    if config.encoding == "base64":
        encoded = base64.b64encode(raw).decode("ascii")
    else:
        encoded = raw.hex()
    return f"{content_hash.method}:{encoded}"


# ---------------------------------------------------------------------------
# InvocationContext
# ---------------------------------------------------------------------------


class InvocationContext:
    """Per-invocation context describing a single pod call.

    Carries a deterministic ``invocation_hash`` and metadata about the
    current delivery. ``invocation_hash`` is a computed property that
    delegates to ``format_id()`` with the pod's default
    ``InvocationHashConfig``. ``format_id()`` can be called with a custom
    config to re-serialize without recomputation.

    Public fields are read-only by convention (no public setters).

    Available on:
    - Side-effect pod functions (injected as ``ctx`` argument).
    - Function pod post-run hooks (via ``PostRunPayload.invocation_context``).

    Args:
        pod_name: ``pod.label`` of the invoking pod.
        pipeline_run_id: The current pipeline run identifier, or ``None``
            for standalone / lazy pipelines.
    """

    def __init__(
        self,
        pod_name: str,
        pipeline_run_id: str | None,
        _pipeline_hash_ch: ContentHash,
        _record_id_hash_ch: ContentHash,
        _hash_config: InvocationHashConfig,
        _track_completion: bool,
    ) -> None:
        self.pod_name = pod_name
        self.pipeline_run_id = pipeline_run_id
        self._pipeline_hash_ch = _pipeline_hash_ch
        self._record_id_hash_ch = _record_id_hash_ch
        self._hash_config = _hash_config
        self._track_completion = _track_completion

    @property
    def invocation_hash(self) -> str:
        """Serialized invocation hash — delegates to ``format_id()``."""
        return self.format_id()

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return the invocation hash string with an optional format override.

        Serializes the stored ``ContentHash`` components. Uses ``config``
        if supplied, otherwise the pod's own ``InvocationHashConfig``.

        Args:
            config: Optional encoding/truncation override.

        Returns:
            A string of the form ``"{component1}::{component2}"``
            (two components when ``track_completion=True``) or
            ``"{c1}::{c2}::{run_id}"`` (three components when
            ``track_completion=False`` and ``pipeline_run_id`` is not ``None``).
            Each component is ``"{method}:{encoded_digest}"``.
        """
        cfg = config or self._hash_config
        c1 = _serialize_component(self._pipeline_hash_ch, cfg)
        c2 = _serialize_component(self._record_id_hash_ch, cfg)
        if not self._track_completion and self.pipeline_run_id is not None:
            return f"{c1}::{c2}::{self.pipeline_run_id}"
        return f"{c1}::{c2}"
```

- [ ] **Step 2: Update `side_effects.py` — remove moved definitions, import from `invocation.py`**

In `src/orcapod/side_effects.py`, replace the three blocks (lines 50–185, which define `InvocationHashConfig`, `_serialize_component`, and `InvocationContext`) with a single import. The replacement goes right after the `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` constant definition.

Remove these sections entirely from `side_effects.py`:
- The `InvocationHashConfig` dataclass (lines ~51–63)
- The `_serialize_component` function (lines ~65–86)
- The `InvocationContext` class (lines ~126–185)
- Their section comment headers (`# InvocationHashConfig`, `# InvocationContext`)

Add this import block after the `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` line:

```python
from orcapod.invocation import (
    InvocationContext,
    InvocationHashConfig,
    _serialize_component,
)
```

- [ ] **Step 3: Update `function_pod.py` — switch local import to `invocation.py`**

In `src/orcapod/core/function_pod.py`, find the local import block inside `_build_invocation_context` (around line 185–189):

```python
        from orcapod.side_effects import (
            InvocationContext,
            InvocationHashConfig,
            _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
        )
```

Replace with:

```python
        from orcapod.invocation import InvocationContext, InvocationHashConfig
        from orcapod.side_effects import _SIDE_EFFECT_RECOMPUTATION_INDEX_COL
```

- [ ] **Step 4: Update `__init__.py` — import from `.invocation` instead of `.side_effects`**

In `src/orcapod/__init__.py`, change:

```python
from .side_effects import (
    InvocationContext,
    InvocationHashConfig,
    SideEffectPod,
    SideEffectPodConfig,
    side_effect_pod,
    sink_pod,
    tap_pod,
)
```

To:

```python
from .invocation import (
    InvocationContext,
    InvocationHashConfig,
)
from .side_effects import (
    SideEffectPod,
    SideEffectPodConfig,
    side_effect_pod,
    sink_pod,
    tap_pod,
)
```

- [ ] **Step 5: Run existing tests to verify nothing broke**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests pass. If any fail, they indicate a missed import update — fix before proceeding.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/invocation.py src/orcapod/side_effects.py src/orcapod/core/function_pod.py src/orcapod/__init__.py
git commit -m "refactor(invocation): extract InvocationContext/InvocationHashConfig to leaf module (ITL-531)"
```

---

## Task 2: Write failing tests for `PostRunPayload.invocation_context`

**Files:**
- Modify: `tests/test_core/function_pod/test_post_run_hooks.py`

- [ ] **Step 1: Add `TestInvocationContextOnPayload` test class**

Append this class to the end of `tests/test_core/function_pod/test_post_run_hooks.py`. Add `from orcapod import InvocationHashConfig` to the existing imports at the top of the file.

```python
# ---------------------------------------------------------------------------
# InvocationContext on PostRunPayload (ITL-531)
# ---------------------------------------------------------------------------


class TestInvocationContextOnPayload:
    def test_invocation_context_always_present(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert payloads[0].invocation_context is not None

    def test_invocation_hash_is_nonempty_string(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        ctx = payloads[0].invocation_context
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0

    def test_format_id_matches_invocation_hash(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        ctx = payloads[0].invocation_context
        assert ctx.format_id() == ctx.invocation_hash

    def test_format_id_base64_differs_from_hex(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        ctx = payloads[0].invocation_context
        hex_id = ctx.invocation_hash
        b64_id = ctx.format_id(InvocationHashConfig(encoding="base64"))
        assert hex_id != b64_id

    def test_pipeline_run_id_is_none_standalone(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert payloads[0].invocation_context.pipeline_run_id is None

    def test_invocation_hash_deterministic(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        # Process the same input stream twice
        stream1 = pod.process(_make_stream(n=1))
        list(stream1.iter_data())
        stream2 = pod.process(_make_stream(n=1))
        list(stream2.iter_data())

        assert len(payloads) == 2
        assert (
            payloads[0].invocation_context.invocation_hash
            == payloads[1].invocation_context.invocation_hash
        )

    def test_error_payload_has_invocation_context(self):
        def explodes(x: int) -> int:
            raise RuntimeError("boom")

        pf = PythonDataFunction(explodes, output_keys="result")
        pod = FunctionPod(pf)
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(HookConfig(fn=payloads.append, on_error="log"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(RuntimeError, match="boom"):
            list(stream.iter_data())

        assert len(payloads) == 1
        assert payloads[0].invocation_context is not None
        assert isinstance(payloads[0].invocation_context.invocation_hash, str)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload -v
```

Expected: All 7 tests FAIL. The first failure should be `AttributeError: 'PostRunPayload' object has no attribute 'invocation_context'` (or similar). If any pass unexpectedly, investigate before proceeding.

---

## Task 3: Add `invocation_context` field to `PostRunPayload`

**Files:**
- Modify: `src/orcapod/hooks.py`

- [ ] **Step 1: Add import and field to `PostRunPayload`**

In `src/orcapod/hooks.py`, add the import after the existing imports:

```python
from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Literal

from orcapod.invocation import InvocationContext

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
```

Then update `PostRunPayload` to add the new field. The field goes after `pod` with a default of `None` so existing positional and keyword construction still works:

```python
@dataclasses.dataclass(frozen=True)
class PostRunPayload:
    """Payload passed to every post-run hook after a pod invocation.

    Attributes:
        record_id_hash: String form of ``output.datagram_uuid`` — the same UUID
            used as the primary key when the result is stored in a backing database.
            ``None`` when ``output`` is ``None`` (filtered row or error).
        tag: The input tag for this invocation. Treat as read-only.
        input: The input data for this invocation. Treat as read-only.
        output: The output data; ``None`` if the function filtered the row out or raised.
        stats: Timing and status bundle.
        pod: Identity of the pod that produced this result.
        invocation_context: Deterministic invocation identity for this row.
            Carries ``invocation_hash`` (input-keyed, encoding-configurable),
            ``format_id(config)`` for custom serialization, and
            ``pipeline_run_id``. ``None`` only when ``PostRunPayload`` is
            constructed directly without providing this argument (e.g. in
            tests); always populated when built by the pod itself.
    """

    record_id_hash: str | None
    tag: TagProtocol
    input: DataProtocol
    output: DataProtocol | None
    stats: RunStats
    pod: PodContext
    invocation_context: InvocationContext | None = None
```

- [ ] **Step 2: Run the new tests — expect partial failure**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload -v
```

Expected: Tests fail because `invocation_context` is always `None` (not yet populated by the pod). The assertion `assert payloads[0].invocation_context is not None` should now be the failure point rather than an `AttributeError`.

- [ ] **Step 3: Run the full existing test suite to confirm no regressions**

```bash
uv run pytest tests/ -x -q --ignore=tests/test_core/function_pod/test_post_run_hooks.py
```

Expected: All pass. `PostRunPayload`'s `= None` default means no existing construction sites break.

---

## Task 4: Thread `run_id` into `_build_post_run_payload` and populate `invocation_context`

**Files:**
- Modify: `src/orcapod/core/function_pod.py`

- [ ] **Step 1: Update `_build_post_run_payload` signature and body**

Find `_build_post_run_payload` in `_FunctionPodBase` (around line 352). Replace the entire method:

```python
    def _build_post_run_payload(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        output_data: DataProtocol | None,
        started_at: datetime,
        finished_at: datetime,
        status: InvocationStatus,
        exc: Exception | None,
        run_id: str | None = None,
    ) -> PostRunPayload:
        """Build a ``PostRunPayload`` from invocation results.

        Args:
            tag: The input tag.
            data: The input data.
            output_data: The output data, or ``None`` if filtered or errored.
            started_at: UTC timestamp when the invocation started.
            finished_at: UTC timestamp when compute-or-lookup completed.
            status: Invocation status (``COMPUTED``, ``HIT``, or ``ERROR``).
            exc: The exception raised, if ``status == ERROR``; ``None`` otherwise.
            run_id: Pipeline run identifier, or ``None`` in standalone mode.
                Forwarded to ``InvocationContext.pipeline_run_id``.

        Returns:
            A ``PostRunPayload`` ready to pass to registered hooks.
        """
        record_id = (
            str(output_data.datagram_uuid) if output_data is not None else None
        )
        invocation_context = self._build_invocation_context(tag, data, run_id=run_id)
        return PostRunPayload(
            record_id_hash=record_id,
            tag=tag,
            input=data,
            output=output_data,
            stats=RunStats(
                duration_ms=(finished_at - started_at).total_seconds() * 1000,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                error=exc,
            ),
            pod=PodContext(
                label=self.label,
                pod_hash=self.content_hash().to_string(),
            ),
            invocation_context=invocation_context,
        )
```

- [ ] **Step 2: Update `_invoke_with_hooks` to pass `run_id` to `_build_post_run_payload`**

Find `_invoke_with_hooks` in `_FunctionPodBase` (around line 397). There are two calls to `_build_post_run_payload` inside it — the error path and the success path. Update both to pass `run_id=run_id`:

```python
    def _invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Call ``process_data``, time it, and fire post-run hooks.

        When ``_post_run_hooks`` is empty, delegates directly to
        ``process_data`` with zero overhead. Override in subclasses (e.g.
        ``CachedFunctionPod``) to supply a different ``InvocationStatus``.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger forwarded to ``process_data``.
            run_id: Pipeline run identifier forwarded to ``process_data`` and
                ``PostRunPayload.invocation_context``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return self.process_data(tag, data, logger=logger, run_id=run_id)

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = self.process_data(tag, data, logger=logger, run_id=run_id)
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc, run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at,
                InvocationStatus.COMPUTED, None, run_id=run_id,
            )
        )
        return out_tag, output_data
```

- [ ] **Step 3: Update `_async_invoke_with_hooks` to pass `run_id` to `_build_post_run_payload`**

Find `_async_invoke_with_hooks` in `_FunctionPodBase` (around line 448). Update both `_build_post_run_payload` calls inside it to pass `run_id=run_id`:

```python
    async def _async_invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_invoke_with_hooks``.

        When ``_post_run_hooks`` is empty, delegates directly to
        ``async_process_data`` with zero overhead.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger forwarded to
                ``async_process_data``.
            run_id: Pipeline run identifier forwarded to ``async_process_data``
                and ``PostRunPayload.invocation_context``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return await self.async_process_data(tag, data, logger=logger, run_id=run_id)

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = await self.async_process_data(
                tag, data, logger=logger, run_id=run_id
            )
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc, run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at,
                InvocationStatus.COMPUTED, None, run_id=run_id,
            )
        )
        return out_tag, output_data
```

- [ ] **Step 4: Run the new test class — all 7 tests should now pass**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload -v
```

Expected output:
```
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_invocation_context_always_present PASSED
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_invocation_hash_is_nonempty_string PASSED
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_format_id_matches_invocation_hash PASSED
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_format_id_base64_differs_from_hex PASSED
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_pipeline_run_id_is_none_standalone PASSED
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_invocation_hash_deterministic PASSED
tests/test_core/function_pod/test_post_run_hooks.py::TestInvocationContextOnPayload::test_error_payload_has_invocation_context PASSED

7 passed
```

- [ ] **Step 5: Update `CachedFunctionPod._invoke_with_hooks` to pass `run_id`**

`CachedFunctionPod` (in `src/orcapod/core/cached_function_pod.py`) overrides
`_invoke_with_hooks` and `_async_invoke_with_hooks`, calling `_build_post_run_payload`
without `run_id`. With the new `run_id` keyword argument added in Step 1, the code
still compiles (default `None`), but `pipeline_run_id` would silently be `None`
even when a run_id is in scope. Fix both calls in each override.

In `CachedFunctionPod._invoke_with_hooks`, update the two `_build_post_run_payload` calls:

```python
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc, run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at, status, None,
                run_id=run_id,
            )
        )
```

In `CachedFunctionPod._async_invoke_with_hooks`, make the same two changes:

```python
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc, run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at, status, None,
                run_id=run_id,
            )
        )
```

- [ ] **Step 6: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: All tests pass. Fix any failures before committing.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/hooks.py src/orcapod/core/function_pod.py src/orcapod/core/cached_function_pod.py tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "feat(hooks): add invocation_context to PostRunPayload (ITL-531)"
```

---

## Task 5: Export `InvocationContext` from public API and finalize

**Files:**
- Modify: `src/orcapod/__init__.py` (verify — already done in Task 1)
- Modify: `tests/test_core/function_pod/test_post_run_hooks.py` (add public API import check)

- [ ] **Step 1: Verify `InvocationContext` is still exported from `orcapod`**

```bash
uv run python -c "from orcapod import InvocationContext, InvocationHashConfig; print('OK')"
```

Expected: `OK`

- [ ] **Step 2: Add public API import assertion to the existing `test_public_api_imports` test**

In `tests/test_core/function_pod/test_post_run_hooks.py`, find `test_public_api_imports` in `TestDecoratorConvenience` and add two assertions:

```python
    def test_public_api_imports(self):
        import orcapod
        assert hasattr(orcapod, "PostRunPayload")
        assert hasattr(orcapod, "HookConfig")
        assert hasattr(orcapod, "InvocationStatus")
        assert hasattr(orcapod, "RunStats")
        assert hasattr(orcapod, "PodContext")
        assert hasattr(orcapod, "InvocationContext")    # ITL-531
        assert hasattr(orcapod, "InvocationHashConfig") # ITL-531
```

- [ ] **Step 3: Run the updated test**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestDecoratorConvenience::test_public_api_imports -v
```

Expected: PASS

- [ ] **Step 4: Run the complete test suite one final time**

```bash
uv run pytest tests/ -q
```

Expected: All tests pass, no warnings about import paths.

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "test(hooks): verify InvocationContext/InvocationHashConfig in public API (ITL-531)"
```

---

## Task 6: Push branch and open PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin eywalker/itl-531-align-postrunpayload-with-sideeffectpod-invocationcontext
```

- [ ] **Step 2: Open PR against `main`**

```bash
gh pr create \
  --title "feat(hooks): align PostRunPayload with InvocationContext (ITL-531)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Extracts `InvocationContext` and `InvocationHashConfig` out of `side_effects.py` into a new stdlib-only leaf module `src/orcapod/invocation.py`
- Adds `invocation_context: InvocationContext | None = None` to `PostRunPayload` — populated on every payload built by the pod
- Threads `run_id` through `_build_post_run_payload` so `pipeline_run_id` is accessible via the context

Closes ITL-531

## Test plan

- [ ] All existing tests pass unmodified (additive `= None` default)
- [ ] `TestInvocationContextOnPayload` (7 new tests) all pass
- [ ] `test_public_api_imports` verifies `InvocationContext` and `InvocationHashConfig` remain in the public API

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
