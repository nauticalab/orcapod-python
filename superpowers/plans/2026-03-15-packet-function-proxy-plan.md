# PacketFunctionProxy Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable cached data access from loaded pipelines when the original packet function is unavailable, by introducing `PacketFunctionProxy`.

**Architecture:** `PacketFunctionProxy` implements `PacketFunctionProtocol` using stored metadata from the serialized config. It plugs into the existing `FunctionPod` → `FunctionNode` → `CachedFunctionPod` chain unchanged, so cached DB reads work. Invocation raises `PacketFunctionUnavailableError`. Late binding via `bind()` allows attaching a real function with identity validation.

**Tech Stack:** Python, PyArrow, orcapod internals (PacketFunctionBase, FunctionPod, FunctionNode, CachedFunctionPod, Pipeline serialization)

**Spec:** `superpowers/specs/2026-03-15-stub-packet-function-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/orcapod/errors.py` | Modify | Add `PacketFunctionUnavailableError` |
| `src/orcapod/core/packet_function_proxy.py` | Create | `PacketFunctionProxy` class |
| `src/orcapod/pipeline/serialization.py` | Modify | `fallback_to_proxy` in `resolve_packet_function_from_config` |
| `src/orcapod/core/function_pod.py` | Modify | `fallback_to_proxy` in `FunctionPod.from_config` |
| `src/orcapod/pipeline/graph.py` | Modify | `_load_function_node` proxy support; upstream usability relaxation |
| `tests/test_core/packet_function/test_packet_function_proxy.py` | Create | Proxy unit tests |
| `tests/test_pipeline/test_serialization.py` | Modify | Pipeline load integration tests |

---

## Chunk 1: PacketFunctionProxy Core

### Task 1: Add `PacketFunctionUnavailableError`

**Files:**
- Modify: `src/orcapod/errors.py`

- [ ] **Step 1: Add the error class**

Add at the end of `src/orcapod/errors.py`:

```python
class PacketFunctionUnavailableError(RuntimeError):
    """Raised when a packet function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original packet function is not available. Only cached results can
    be accessed.
    """
```

- [ ] **Step 2: Verify import works**

Run: `uv run python -c "from orcapod.errors import PacketFunctionUnavailableError; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/errors.py
git commit -m "feat(errors): add PacketFunctionUnavailableError"
```

---

### Task 2: Create `PacketFunctionProxy` — construction and metadata

**Files:**
- Create: `src/orcapod/core/packet_function_proxy.py`
- Create: `tests/test_core/packet_function/test_packet_function_proxy.py`

**Context:** `PacketFunctionProxy` subclasses `PacketFunctionBase` (from `src/orcapod/core/packet_function.py`). It needs to:
- Accept a config dict (same format as `PythonPacketFunction.to_config()` output)
- Extract version from `config["config"]["version"]` and pass to `super().__init__(version=...)`
- Eagerly deserialize schemas via `deserialize_schema()` from `pipeline.serialization`
- Store the original config, pre-computed hashes, and a `_uri` override
- Override `uri` property to return stored value (avoiding semantic hasher recomputation)
- Override `content_hash()` and `pipeline_hash()` to return stored values

The config format from `PythonPacketFunction.to_config()` is:
```python
{
    "packet_function_type_id": "python.function.v0",
    "config": {
        "module_path": "some.module",
        "callable_name": "my_func",
        "version": "v0.1",
        "input_packet_schema": {"age": "int64", "name": "large_string"},
        "output_packet_schema": {"result": "float64"},
        "output_keys": ["result"],
    },
}
```

The `uri` comes from the parent `FunctionPod.to_config()` which wraps the above:
```python
{
    "uri": ["my_func", "<schema_hash>", "v0", "python.function.v0"],
    "packet_function": { ... the above ... },
    "node_config": None,
}
```

So the proxy receives the `packet_function` sub-dict as `config`, and receives
`uri` separately (or from the parent `function_pod` config in `_load_function_node`).

- [ ] **Step 1: Write failing tests for construction and metadata**

Create `tests/test_core/packet_function/test_packet_function_proxy.py`:

```python
"""Tests for PacketFunctionProxy."""

from __future__ import annotations

import pytest

from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.packet_function_proxy import PacketFunctionProxy
from orcapod.errors import PacketFunctionUnavailableError
from orcapod.types import ContentHash, Schema


def _make_sample_function():
    """Create a simple packet function for testing."""

    def double_age(age: int) -> int:
        return age * 2

    return PythonPacketFunction(
        double_age,
        output_keys="doubled_age",
        version="v1.0",
    )


def _make_proxy_from_function(pf: PythonPacketFunction) -> PacketFunctionProxy:
    """Create a proxy from a live packet function's config."""
    config = pf.to_config()
    return PacketFunctionProxy(
        config=config,
        uri=tuple(pf.uri),
        content_hash_str=pf.content_hash().to_string(),
        pipeline_hash_str=pf.pipeline_hash().to_string(),
    )


class TestPacketFunctionProxyConstruction:
    def test_construction_from_config(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy is not None

    def test_canonical_function_name(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.canonical_function_name == pf.canonical_function_name

    def test_major_version(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.major_version == pf.major_version

    def test_minor_version_string(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.minor_version_string == pf.minor_version_string

    def test_packet_function_type_id(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.packet_function_type_id == pf.packet_function_type_id

    def test_input_packet_schema(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert dict(proxy.input_packet_schema) == dict(pf.input_packet_schema)

    def test_output_packet_schema(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert dict(proxy.output_packet_schema) == dict(pf.output_packet_schema)

    def test_uri_returns_stored_value(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.uri == pf.uri

    def test_content_hash_returns_stored_value(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.content_hash().to_string() == pf.content_hash().to_string()

    def test_pipeline_hash_returns_stored_value(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.pipeline_hash().to_string() == pf.pipeline_hash().to_string()

    def test_to_config_returns_original(self):
        pf = _make_sample_function()
        config = pf.to_config()
        proxy = PacketFunctionProxy(
            config=config,
            uri=tuple(pf.uri),
            content_hash_str=pf.content_hash().to_string(),
            pipeline_hash_str=pf.pipeline_hash().to_string(),
        )
        assert proxy.to_config() == config

    def test_executor_returns_none(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.executor is None

    def test_executor_setter_is_noop(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.executor = None  # should not raise
        assert proxy.executor is None

    def test_is_bound_initially_false(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.is_bound is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py -v`
Expected: All FAIL (ModuleNotFoundError for `packet_function_proxy`)

- [ ] **Step 3: Implement `PacketFunctionProxy`**

Create `src/orcapod/core/packet_function_proxy.py`:

```python
"""PacketFunctionProxy — stands in for an unavailable packet function."""

from __future__ import annotations

from typing import Any

from orcapod.core.packet_function import PacketFunctionBase
from orcapod.errors import PacketFunctionUnavailableError
from orcapod.protocols.core_protocols import (
    PacketFunctionExecutorProtocol,
    PacketFunctionProtocol,
    PacketProtocol,
)
from orcapod.types import ContentHash, Schema


class PacketFunctionProxy(PacketFunctionBase):
    """Proxy for a packet function that is not available in this environment.

    Carries all identifying metadata (schemas, URI, hashes) from the
    serialized config so that ``FunctionPod``, ``FunctionNode``, and
    ``CachedFunctionPod`` can be fully constructed. Invocation methods
    raise ``PacketFunctionUnavailableError`` unless a real function has
    been bound via ``bind()``.

    Args:
        config: The packet function config dict as produced by
            ``PythonPacketFunction.to_config()``.
        uri: The original URI tuple from the ``FunctionPod.to_config()``
            output.  Stored directly and returned from the ``uri``
            property to avoid semantic hasher recomputation.
        content_hash_str: Pre-computed content hash string from the
            pipeline descriptor.
        pipeline_hash_str: Pre-computed pipeline hash string from the
            pipeline descriptor.
    """

    def __init__(
        self,
        config: dict[str, Any],
        uri: tuple[str, ...],
        content_hash_str: str | None = None,
        pipeline_hash_str: str | None = None,
    ) -> None:
        self._original_config = config
        inner = config.get("config", config)

        # Extract version for PacketFunctionBase.__init__
        version = inner.get("version", "v0.0")

        super().__init__(version=version)

        # Store identity metadata
        self._proxy_type_id = config.get(
            "packet_function_type_id", "python.function.v0"
        )
        self._proxy_function_name = inner.get("callable_name", "unknown")
        self._stored_uri = uri
        self._stored_content_hash = content_hash_str
        self._stored_pipeline_hash = pipeline_hash_str

        # Eagerly deserialize schemas
        from orcapod.pipeline.serialization import deserialize_schema

        raw_input = inner.get("input_packet_schema", {})
        raw_output = inner.get("output_packet_schema", {})
        self._input_schema = Schema(deserialize_schema(raw_input))
        self._output_schema = Schema(deserialize_schema(raw_output))

        # Late binding state
        self._bound_function: PacketFunctionProtocol | None = None

    # ==================== Identity & Metadata ====================

    @property
    def packet_function_type_id(self) -> str:
        return self._proxy_type_id

    @property
    def canonical_function_name(self) -> str:
        return self._proxy_function_name

    @property
    def input_packet_schema(self) -> Schema:
        return self._input_schema

    @property
    def output_packet_schema(self) -> Schema:
        return self._output_schema

    @property
    def uri(self) -> tuple[str, ...]:
        """Return the stored original URI, avoiding recomputation."""
        return self._stored_uri

    # ==================== Hashing ====================

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the stored content hash."""
        if self._stored_content_hash is not None:
            return ContentHash.from_string(self._stored_content_hash)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the stored pipeline hash."""
        if self._stored_pipeline_hash is not None:
            return ContentHash.from_string(self._stored_pipeline_hash)
        return super().pipeline_hash(hasher)

    # ==================== Variation / Execution Data ====================

    def get_function_variation_data(self) -> dict[str, Any]:
        if self._bound_function is not None:
            return self._bound_function.get_function_variation_data()
        return {}

    def get_execution_data(self) -> dict[str, Any]:
        if self._bound_function is not None:
            return self._bound_function.get_execution_data()
        return {}

    # ==================== Executor ====================

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        if self._bound_function is not None:
            return self._bound_function.executor
        return None

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        if self._bound_function is not None:
            self._bound_function.executor = executor
        # No-op when unbound — avoids breaking _apply_execution_engine

    # ==================== Invocation ====================

    def _raise_unavailable(self) -> None:
        raise PacketFunctionUnavailableError(
            f"Packet function '{self._proxy_function_name}' is not available "
            f"in this environment. Only cached results can be accessed."
        )

    def call(self, packet: PacketProtocol) -> PacketProtocol | None:
        if self._bound_function is not None:
            return self._bound_function.call(packet)
        self._raise_unavailable()

    async def async_call(self, packet: PacketProtocol) -> PacketProtocol | None:
        if self._bound_function is not None:
            return await self._bound_function.async_call(packet)
        self._raise_unavailable()

    def direct_call(self, packet: PacketProtocol) -> PacketProtocol | None:
        if self._bound_function is not None:
            return self._bound_function.direct_call(packet)
        self._raise_unavailable()

    async def direct_async_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        if self._bound_function is not None:
            return await self._bound_function.direct_async_call(packet)
        self._raise_unavailable()

    # ==================== Serialization ====================

    def to_config(self) -> dict[str, Any]:
        """Return the original config dict (round-trip preservation)."""
        return self._original_config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PacketFunctionProxy":
        """Construct a proxy from a packet function config dict.

        Args:
            config: A dict as produced by ``PythonPacketFunction.to_config()``.

        Returns:
            A new ``PacketFunctionProxy`` instance.
        """
        # URI not available from packet_function config alone — caller
        # must provide it separately via the constructor.  from_config
        # builds a proxy without URI/hash overrides.
        inner = config.get("config", config)
        # Build a placeholder URI from available metadata
        type_id = config.get("packet_function_type_id", "python.function.v0")
        name = inner.get("callable_name", "unknown")
        version = inner.get("version", "v0.0")
        import re

        match = re.match(r"\D*(\d+)\.(.*)", version)
        major = int(match.group(1)) if match else 0
        uri = (name, "", f"v{major}", type_id)

        return cls(config=config, uri=uri)

    # ==================== Binding ====================

    @property
    def is_bound(self) -> bool:
        """Return whether a real packet function has been bound."""
        return self._bound_function is not None

    def bind(self, packet_function: PacketFunctionProtocol) -> None:
        """Bind a real packet function to this proxy.

        Validates that the provided function matches the proxy's stored
        identity before accepting it.

        Args:
            packet_function: The real packet function to bind.

        Raises:
            ValueError: If the provided function's identifying information
                does not match the proxy's stored identity.
        """
        mismatches: list[str] = []

        if packet_function.canonical_function_name != self.canonical_function_name:
            mismatches.append(
                f"canonical_function_name: expected {self.canonical_function_name!r}, "
                f"got {packet_function.canonical_function_name!r}"
            )
        if packet_function.major_version != self.major_version:
            mismatches.append(
                f"major_version: expected {self.major_version}, "
                f"got {packet_function.major_version}"
            )
        if packet_function.packet_function_type_id != self.packet_function_type_id:
            mismatches.append(
                f"packet_function_type_id: expected {self.packet_function_type_id!r}, "
                f"got {packet_function.packet_function_type_id!r}"
            )
        if dict(packet_function.input_packet_schema) != dict(self.input_packet_schema):
            mismatches.append(
                f"input_packet_schema: expected {dict(self.input_packet_schema)}, "
                f"got {dict(packet_function.input_packet_schema)}"
            )
        if dict(packet_function.output_packet_schema) != dict(
            self.output_packet_schema
        ):
            mismatches.append(
                f"output_packet_schema: expected {dict(self.output_packet_schema)}, "
                f"got {dict(packet_function.output_packet_schema)}"
            )
        if packet_function.uri != self.uri:
            mismatches.append(
                f"uri: expected {self.uri}, got {packet_function.uri}"
            )
        if (
            self._stored_content_hash is not None
            and packet_function.content_hash().to_string()
            != self._stored_content_hash
        ):
            mismatches.append(
                f"content_hash: expected {self._stored_content_hash}, "
                f"got {packet_function.content_hash().to_string()}"
            )

        if mismatches:
            raise ValueError(
                f"Cannot bind packet function: identity mismatch.\n"
                + "\n".join(f"  - {m}" for m in mismatches)
            )

        self._bound_function = packet_function

    def unbind(self) -> None:
        """Remove the bound packet function, reverting to proxy-only mode."""
        self._bound_function = None
```

**Important note about `call()` and `async_call()`:** These are inherited from
`PacketFunctionBase` which routes through `direct_call()` / `direct_async_call()`
(via the executor if set, or directly). Since the proxy overrides `direct_call()`
and `direct_async_call()` to raise `PacketFunctionUnavailableError` (when unbound)
or delegate (when bound), the inherited `call()` / `async_call()` will do the
right thing automatically. Check `PacketFunctionBase.call()` implementation to
confirm this routing — if it calls `self.direct_call()`, we're good. If it has
its own logic, we may need to override `call()` too.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/packet_function_proxy.py tests/test_core/packet_function/test_packet_function_proxy.py
git commit -m "feat(core): add PacketFunctionProxy with metadata and identity"
```

---

### Task 3: Test invocation behavior (unbound raises, bound delegates)

**Files:**
- Modify: `tests/test_core/packet_function/test_packet_function_proxy.py`

- [ ] **Step 1: Write failing tests for invocation**

Append to the test file:

```python
class TestPacketFunctionProxyInvocation:
    def test_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        # Create a minimal packet to pass
        from orcapod.core.datagrams.tag_packet import Packet

        packet = Packet({"age": 25})
        with pytest.raises(PacketFunctionUnavailableError, match="double_age"):
            proxy.call(packet)

    @pytest.mark.asyncio
    async def test_async_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        from orcapod.core.datagrams.tag_packet import Packet

        packet = Packet({"age": 25})
        with pytest.raises(PacketFunctionUnavailableError, match="double_age"):
            await proxy.async_call(packet)

    def test_direct_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        from orcapod.core.datagrams.tag_packet import Packet

        packet = Packet({"age": 25})
        with pytest.raises(PacketFunctionUnavailableError):
            proxy.direct_call(packet)

    def test_variation_data_empty_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.get_function_variation_data() == {}

    def test_execution_data_empty_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.get_execution_data() == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py::TestPacketFunctionProxyInvocation -v`
Expected: FAIL (depending on whether `call()` routes through `direct_call()`)

- [ ] **Step 3: Fix any test issues**

The `call()`, `async_call()`, `direct_call()`, and `direct_async_call()`
overrides are already in the Task 2 implementation. They raise
`PacketFunctionUnavailableError` when unbound and delegate when bound.
If tests fail, check that the error message includes the function name.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/packet_function/test_packet_function_proxy.py src/orcapod/core/packet_function_proxy.py
git commit -m "test(proxy): add invocation behavior tests for PacketFunctionProxy"
```

---

### Task 4: Test `bind()`, `unbind()`, and identity mismatch rejection

**Files:**
- Modify: `tests/test_core/packet_function/test_packet_function_proxy.py`

- [ ] **Step 1: Write failing tests for bind/unbind**

Append to the test file:

```python
class TestPacketFunctionProxyBinding:
    def test_bind_succeeds_with_matching_function(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        assert proxy.is_bound is True

    def test_call_delegates_after_bind(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        from orcapod.core.datagrams.tag_packet import Packet

        packet = Packet({"age": 25})
        result = proxy.call(packet)
        assert result is not None
        assert result.as_dict()["doubled_age"] == 50

    def test_variation_data_delegates_after_bind(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        assert proxy.get_function_variation_data() == pf.get_function_variation_data()

    def test_execution_data_delegates_after_bind(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        assert proxy.get_execution_data() == pf.get_execution_data()

    def test_unbind_reverts_to_proxy_mode(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        assert proxy.is_bound is True
        proxy.unbind()
        assert proxy.is_bound is False
        from orcapod.core.datagrams.tag_packet import Packet

        packet = Packet({"age": 25})
        with pytest.raises(PacketFunctionUnavailableError):
            proxy.call(packet)

    def test_bind_rejects_mismatched_function_name(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def triple_age(age: int) -> int:
            return age * 3

        other_pf = PythonPacketFunction(
            triple_age, output_keys="tripled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="canonical_function_name"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_version(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(age: int) -> int:
            return age * 2

        other_pf = PythonPacketFunction(
            double_age, output_keys="doubled_age", version="v2.0"
        )
        with pytest.raises(ValueError, match="major_version"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_output_schema(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(age: int) -> str:
            return str(age * 2)

        other_pf = PythonPacketFunction(
            double_age, output_keys="doubled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="output_packet_schema"):
            proxy.bind(other_pf)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py::TestPacketFunctionProxyBinding -v`
Expected: FAIL

- [ ] **Step 3: Fix any issues in implementation**

The `bind()` and `unbind()` implementations are already in the Task 2 code.
If `call()` delegation doesn't work, ensure the bound function's `call()` is
used (not `direct_call()`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/packet_function/test_packet_function_proxy.py src/orcapod/core/packet_function_proxy.py
git commit -m "test(proxy): add bind/unbind and identity validation tests"
```

---

### Task 5: Test `FunctionPod` with proxy

**Files:**
- Modify: `tests/test_core/packet_function/test_packet_function_proxy.py`

- [ ] **Step 1: Write failing tests for FunctionPod integration**

Append to the test file:

```python
class TestFunctionPodWithProxy:
    def test_function_pod_constructs_with_proxy(self):
        from orcapod.core.function_pod import FunctionPod

        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        pod = FunctionPod(packet_function=proxy)
        assert pod is not None
        assert pod.packet_function is proxy

    def test_function_pod_output_schema(self):
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.sources.dict_source import DictSource

        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        pod = FunctionPod(packet_function=proxy)
        source = DictSource({"age": [10, 20, 30]})
        tag_schema, packet_schema = pod.output_schema(source)
        assert "doubled_age" in packet_schema

    def test_function_pod_process_packet_raises(self):
        from orcapod.core.datagrams.tag_packet import Packet, Tag
        from orcapod.core.function_pod import FunctionPod

        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        pod = FunctionPod(packet_function=proxy)
        tag = Tag({})
        packet = Packet({"age": 25})
        with pytest.raises(PacketFunctionUnavailableError):
            pod.process_packet(tag, packet)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py::TestFunctionPodWithProxy -v`
Expected: FAIL

- [ ] **Step 3: Fix any issues**

The `FunctionPod` constructor calls `_FunctionPodBase.uri` which accesses
`self.packet_function.output_packet_schema`. Since the proxy's `uri` is
overridden, we need to check if `_FunctionPodBase.uri` is even called during
construction. If `_output_schema_hash` is set lazily (only when `uri` is
accessed), construction should work. If `CachedFunctionPod` init triggers
`uri` access, we need to ensure the proxy's override takes precedence.

**Key insight:** `_FunctionPodBase.uri` is on the *pod*, not the packet
function. The pod's `uri` calls `self.packet_function.canonical_function_name`,
`self.data_context.semantic_hasher.hash_object(self.packet_function.output_packet_schema)`,
etc. But the *proxy's* `uri` property (on `PacketFunctionBase`) is a different
property. The pod accesses `self.packet_function.uri` only in
`identity_structure()` and `pipeline_identity_structure()`, which return
`self.packet_function.identity_structure()` → `self.uri` on the proxy. So the
proxy's `uri` override IS used for identity/hashing, which is correct.

The pod's own `uri` (on `_FunctionPodBase`) is used for `CachedFunctionPod`
record path. This pod-level `uri` computes `semantic_hasher.hash_object(
self.packet_function.output_packet_schema)`. Since the proxy's
`output_packet_schema` is eagerly deserialized, this should work — but the
resulting hash may differ from the original. This is why the spec says to store
the URI and use it. However, the pod-level `uri` is separate from the proxy's
`uri`.

If the pod-level `uri` hash differs, `CachedFunctionPod` will look in the wrong
DB path. **Solution**: In `_load_function_node`, after constructing the
`FunctionNode`, override `_cached_function_pod._cache.record_path` with the
stored `result_record_path` from the descriptor. OR store the
`_output_schema_hash` on the FunctionPod from the stored URI.

This is a subtlety the implementation will need to handle. The test will reveal
if it's a problem.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_proxy.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/packet_function/test_packet_function_proxy.py src/orcapod/core/packet_function_proxy.py
git commit -m "test(proxy): add FunctionPod integration tests"
```

---

## Chunk 2: Serialization and Pipeline Loading

### Task 6: Add `fallback_to_proxy` to serialization resolver

**Files:**
- Modify: `src/orcapod/pipeline/serialization.py`
- Modify: `tests/test_pipeline/test_serialization_helpers.py`

- [ ] **Step 1: Write failing test for fallback**

Add to `tests/test_pipeline/test_serialization_helpers.py`:

```python
def test_resolve_packet_function_fallback_to_proxy():
    """When the module can't be imported, fallback_to_proxy returns a proxy."""
    from orcapod.core.packet_function_proxy import PacketFunctionProxy
    from orcapod.pipeline.serialization import resolve_packet_function_from_config

    config = {
        "packet_function_type_id": "python.function.v0",
        "config": {
            "module_path": "nonexistent.module.that.does.not.exist",
            "callable_name": "some_func",
            "version": "v1.0",
            "input_packet_schema": {"x": "int64"},
            "output_packet_schema": {"y": "float64"},
            "output_keys": ["y"],
        },
    }

    # Without fallback, should raise
    with pytest.raises(Exception):
        resolve_packet_function_from_config(config)

    # With fallback, should return proxy
    result = resolve_packet_function_from_config(config, fallback_to_proxy=True)
    assert isinstance(result, PacketFunctionProxy)
    assert result.canonical_function_name == "some_func"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py::test_resolve_packet_function_fallback_to_proxy -v`
Expected: FAIL (TypeError — `fallback_to_proxy` not recognized)

- [ ] **Step 3: Implement fallback in `resolve_packet_function_from_config`**

In `src/orcapod/pipeline/serialization.py`, modify `resolve_packet_function_from_config`:

```python
def resolve_packet_function_from_config(
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> Any:
    """Reconstruct a packet function from a config dict.

    Args:
        config: Dict with at least a ``"packet_function_type_id"`` key matching
            a registered packet function type.
        fallback_to_proxy: If ``True`` and reconstruction fails, return a
            ``PacketFunctionProxy`` preserving identity from the config.

    Returns:
        A new packet function instance constructed from the config.

    Raises:
        ValueError: If the type ID is missing or unknown (and fallback is off).
    """
    _ensure_registries()
    type_id = config.get("packet_function_type_id")
    if type_id not in PACKET_FUNCTION_REGISTRY:
        if fallback_to_proxy:
            return _packet_function_proxy_from_config(config)
        raise ValueError(
            f"Unknown packet function type: {type_id!r}. "
            f"Known types: {sorted(PACKET_FUNCTION_REGISTRY.keys())}"
        )
    cls = PACKET_FUNCTION_REGISTRY[type_id]
    try:
        return cls.from_config(config)
    except Exception:
        if fallback_to_proxy:
            logger.warning(
                "Could not reconstruct packet function from config; "
                "returning PacketFunctionProxy."
            )
            return _packet_function_proxy_from_config(config)
        raise


def _packet_function_proxy_from_config(config: dict[str, Any]) -> Any:
    """Create a ``PacketFunctionProxy`` from a packet function config.

    Args:
        config: Packet function config dict.

    Returns:
        A ``PacketFunctionProxy`` preserving the original identity.
    """
    from orcapod.core.packet_function_proxy import PacketFunctionProxy

    return PacketFunctionProxy.from_config(config)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py::test_resolve_packet_function_fallback_to_proxy -v`
Expected: PASS

- [ ] **Step 5: Run full serialization helper tests**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py -v`
Expected: All PASS (no regressions)

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/serialization.py tests/test_pipeline/test_serialization_helpers.py
git commit -m "feat(serialization): add fallback_to_proxy for packet function resolution"
```

---

### Task 7: Add `fallback_to_proxy` to `FunctionPod.from_config`

**Files:**
- Modify: `src/orcapod/core/function_pod.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_pipeline/test_serialization_helpers.py`:

```python
def test_function_pod_from_config_fallback_to_proxy():
    """FunctionPod.from_config with fallback_to_proxy returns pod with proxy."""
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.packet_function_proxy import PacketFunctionProxy

    config = {
        "uri": ["some_func", "hash123", "v1", "python.function.v0"],
        "packet_function": {
            "packet_function_type_id": "python.function.v0",
            "config": {
                "module_path": "nonexistent.module",
                "callable_name": "some_func",
                "version": "v1.0",
                "input_packet_schema": {"x": "int64"},
                "output_packet_schema": {"y": "float64"},
                "output_keys": ["y"],
            },
        },
        "node_config": None,
    }

    # Without fallback, should raise
    with pytest.raises(Exception):
        FunctionPod.from_config(config)

    # With fallback, should return FunctionPod with proxy
    pod = FunctionPod.from_config(config, fallback_to_proxy=True)
    assert isinstance(pod.packet_function, PacketFunctionProxy)
    assert pod.packet_function.canonical_function_name == "some_func"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py::test_function_pod_from_config_fallback_to_proxy -v`
Expected: FAIL

- [ ] **Step 3: Modify `FunctionPod.from_config`**

In `src/orcapod/core/function_pod.py`, change `FunctionPod.from_config` (around line 297):

```python
@classmethod
def from_config(
    cls,
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> "FunctionPod":
    """Reconstruct a FunctionPod from a config dict.

    Args:
        config: A dict as produced by :meth:`to_config`.
        fallback_to_proxy: If ``True`` and the packet function cannot be
            reconstructed, use a ``PacketFunctionProxy`` instead.

    Returns:
        A new ``FunctionPod`` instance.
    """
    from orcapod.pipeline.serialization import resolve_packet_function_from_config

    pf_config = config["packet_function"]

    if fallback_to_proxy:
        from orcapod.core.packet_function_proxy import PacketFunctionProxy

        packet_function = resolve_packet_function_from_config(
            pf_config, fallback_to_proxy=True
        )
        # If we got a proxy, inject the URI from the parent config
        if isinstance(packet_function, PacketFunctionProxy):
            uri_list = config.get("uri")
            if uri_list is not None:
                packet_function._stored_uri = tuple(uri_list)
    else:
        packet_function = resolve_packet_function_from_config(pf_config)

    node_config = None
    if config.get("node_config") is not None:
        node_config = NodeConfig(**config["node_config"])

    return cls(packet_function=packet_function, node_config=node_config)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py::test_function_pod_from_config_fallback_to_proxy -v`
Expected: PASS

- [ ] **Step 5: Run full test suite for regressions**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/function_pod.py tests/test_pipeline/test_serialization_helpers.py
git commit -m "feat(function_pod): add fallback_to_proxy to FunctionPod.from_config"
```

---

### Task 8: Update `Pipeline.load` — upstream usability and function node loading

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`

This is the integration point. Two changes:

1. Relax upstream usability check to accept `READ_ONLY`
2. Modify `_load_function_node` to use `fallback_to_proxy=True` and set `load_status`

- [ ] **Step 1: Modify upstream usability check in `Pipeline.load`**

In `src/orcapod/pipeline/graph.py`, find the upstream usability check for function
nodes (around line 722-726):

Change:
```python
upstream_usable = (
    upstream_node is not None
    and hasattr(upstream_node, "load_status")
    and upstream_node.load_status == LoadStatus.FULL
)
```

To:
```python
upstream_usable = (
    upstream_node is not None
    and hasattr(upstream_node, "load_status")
    and upstream_node.load_status
    in (LoadStatus.FULL, LoadStatus.READ_ONLY)
)
```

Do the same for the operator node `all_upstreams_usable` check (around line 744-751):

Change:
```python
all_upstreams_usable = (
    all(
        hasattr(n, "load_status") and n.load_status == LoadStatus.FULL
        for n in upstream_nodes
    )
    if upstream_nodes
    else False
)
```

To:
```python
all_upstreams_usable = (
    all(
        hasattr(n, "load_status")
        and n.load_status in (LoadStatus.FULL, LoadStatus.READ_ONLY)
        for n in upstream_nodes
    )
    if upstream_nodes
    else False
)
```

- [ ] **Step 2: Modify `_load_function_node` to use proxy fallback**

In `src/orcapod/pipeline/graph.py`, replace `_load_function_node` (around line 851-900):

```python
@staticmethod
def _load_function_node(
    descriptor: dict[str, Any],
    mode: str,
    upstream_node: Any | None,
    upstream_usable: bool,
    databases: dict[str, Any],
) -> FunctionNode:
    """Reconstruct a FunctionNode from a descriptor.

    Args:
        descriptor: The serialized node descriptor.
        mode: Load mode.
        upstream_node: The reconstructed upstream node, or ``None``.
        upstream_usable: Whether the upstream can provide data.
        databases: Database role mapping.

    Returns:
        A ``FunctionNode`` instance.
    """
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.packet_function_proxy import PacketFunctionProxy

    if mode != "read_only" and upstream_usable:
        try:
            pod = FunctionPod.from_config(
                descriptor["function_pod"], fallback_to_proxy=True
            )
            node = FunctionNode.from_descriptor(
                descriptor,
                function_pod=pod,
                input_stream=upstream_node,
                databases=databases,
            )
            # Set load_status based on whether packet function is a proxy
            if isinstance(pod.packet_function, PacketFunctionProxy):
                node._load_status = LoadStatus.READ_ONLY
                # Override CachedFunctionPod's record_path with the stored
                # value from the descriptor.  The pod-level URI computation
                # (which hashes output_packet_schema via semantic_hasher)
                # may produce a different hash after schema round-trip,
                # causing DB lookups to miss.  The descriptor stores the
                # original record_path that was used when data was written.
                stored_result_path = tuple(
                    descriptor.get("result_record_path", ())
                )
                if stored_result_path and node._cached_function_pod is not None:
                    # NOTE: This reaches into private fields. Logged as a
                    # design issue — CachedFunctionPod / ResultCache should
                    # expose a public API for record_path override.
                    node._cached_function_pod._cache._record_path = (
                        stored_result_path
                    )
            else:
                node._load_status = LoadStatus.FULL
            return node
        except Exception:
            logger.warning(
                "Failed to reconstruct function node %r, "
                "falling back to read-only.",
                descriptor.get("label"),
            )

    # Fall through: upstream not usable, read_only mode, or reconstruction failed
    if not upstream_usable and mode != "read_only":
        logger.warning(
            "Upstream for function node %r is not usable, "
            "falling back to read-only.",
            descriptor.get("label"),
        )

    return FunctionNode.from_descriptor(
        descriptor,
        function_pod=None,
        input_stream=None,
        databases=databases,
    )
```

- [ ] **Step 3: Run existing serialization tests to check for regressions**

Run: `uv run pytest tests/test_pipeline/test_serialization.py -v`
Expected: All PASS (existing behavior preserved)

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/graph.py
git commit -m "feat(pipeline): use PacketFunctionProxy in load, relax upstream usability"
```

---

### Task 9: Integration test — pipeline load with unavailable function

**Files:**
- Modify: `tests/test_pipeline/test_serialization.py`

- [ ] **Step 1: Write integration test**

Add to `tests/test_pipeline/test_serialization.py`:

```python
class TestPipelineLoadWithUnavailableFunction:
    """Tests for loading a pipeline when the packet function is not importable."""

    def _build_and_save_pipeline(self, tmp_path):
        """Build a pipeline with a function pod, run it, and save to JSON."""
        from orcapod.core.function_pod import FunctionPod, function_pod
        from orcapod.core.sources.dict_source import DictSource
        from orcapod.databases.delta_lake_databases import DeltaTableDatabase
        from orcapod.pipeline import Pipeline

        db_path = str(tmp_path / "pipeline_db")
        db = DeltaTableDatabase(db_path)

        source = DictSource(
            {"name": ["alice", "bob"], "age": [30, 25]},
            tag_columns=["name"],
        )

        @function_pod(output_keys="doubled_age", version="v1.0")
        def double_age(age: int) -> int:
            return age * 2

        with Pipeline("test_pipeline", pipeline_database=db) as p:
            stream = source
            stream = double_age.pod(stream)

        p.run()

        save_path = str(tmp_path / "pipeline.json")
        p.save(save_path)
        return save_path, db_path

    def test_load_with_unavailable_function_has_read_only_status(self, tmp_path):
        """Function node should be READ_ONLY when function can't be loaded."""
        import json

        from orcapod.pipeline import LoadStatus, Pipeline

        save_path, _ = self._build_and_save_pipeline(tmp_path)

        # Corrupt the module path to simulate unavailable function
        with open(save_path) as f:
            data = json.load(f)
        for node in data["nodes"].values():
            if node.get("node_type") == "function":
                pf_config = node["function_pod"]["packet_function"]["config"]
                pf_config["module_path"] = "nonexistent.module.path"
        with open(save_path, "w") as f:
            json.dump(data, f)

        loaded = Pipeline.load(save_path)

        # Find the function node
        fn_node = None
        for node in loaded.compiled_nodes.values():
            if node.node_type == "function":
                fn_node = node
                break

        assert fn_node is not None
        assert fn_node.load_status == LoadStatus.READ_ONLY

    def test_load_with_unavailable_function_can_get_all_records(self, tmp_path):
        """get_all_records() should return cached data."""
        import json

        from orcapod.pipeline import Pipeline

        save_path, _ = self._build_and_save_pipeline(tmp_path)

        with open(save_path) as f:
            data = json.load(f)
        for node in data["nodes"].values():
            if node.get("node_type") == "function":
                pf_config = node["function_pod"]["packet_function"]["config"]
                pf_config["module_path"] = "nonexistent.module.path"
        with open(save_path, "w") as f:
            json.dump(data, f)

        loaded = Pipeline.load(save_path)
        fn_node = None
        for node in loaded.compiled_nodes.values():
            if node.node_type == "function":
                fn_node = node
                break

        records = fn_node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_load_with_unavailable_function_iter_packets_yields_cached(
        self, tmp_path
    ):
        """iter_packets() should yield cached results from Phase 1."""
        import json

        from orcapod.pipeline import Pipeline

        save_path, _ = self._build_and_save_pipeline(tmp_path)

        with open(save_path) as f:
            data = json.load(f)
        for node in data["nodes"].values():
            if node.get("node_type") == "function":
                pf_config = node["function_pod"]["packet_function"]["config"]
                pf_config["module_path"] = "nonexistent.module.path"
        with open(save_path, "w") as f:
            json.dump(data, f)

        loaded = Pipeline.load(save_path)
        fn_node = None
        for node in loaded.compiled_nodes.values():
            if node.node_type == "function":
                fn_node = node
                break

        packets = list(fn_node.iter_packets())
        assert len(packets) == 2
        # Verify data content
        values = sorted(p.as_dict()["doubled_age"] for _, p in packets)
        assert values == [50, 60]
```

- [ ] **Step 2: Run integration tests**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineLoadWithUnavailableFunction -v`
Expected: Tests may FAIL if there are issues with the pod-level `uri` hash
divergence (see Task 5 notes). Debug and fix as needed.

- [ ] **Step 3: Fix any issues discovered**

The most likely issue: `CachedFunctionPod.record_path` computed from
`_FunctionPodBase.uri` (which hashes `output_packet_schema`) may differ from
the original `result_record_path` stored in the descriptor. If so, after
constructing the `FunctionNode` in `_load_function_node`, override the record
path:

```python
# In _load_function_node, after constructing the node with proxy:
if isinstance(pod.packet_function, PacketFunctionProxy):
    stored_result_path = tuple(descriptor.get("result_record_path", ()))
    if stored_result_path and node._cached_function_pod is not None:
        node._cached_function_pod._cache._record_path = stored_result_path
```

Check if `ResultCache` has `_record_path` as a settable attribute or if
`record_path` is a property. Adjust accordingly.

- [ ] **Step 4: Run integration tests again**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineLoadWithUnavailableFunction -v`
Expected: All PASS

- [ ] **Step 5: Run full serialization test suite**

Run: `uv run pytest tests/test_pipeline/test_serialization.py -v`
Expected: All PASS (no regressions)

- [ ] **Step 6: Commit**

```bash
git add tests/test_pipeline/test_serialization.py src/orcapod/pipeline/graph.py
git commit -m "test(pipeline): add integration tests for loading with unavailable function"
```

---

### Task 10: Integration test — downstream computation from cached data

**Files:**
- Modify: `tests/test_pipeline/test_serialization.py`

- [ ] **Step 1: Write downstream operator test**

Add to `TestPipelineLoadWithUnavailableFunction`:

```python
    def test_downstream_operator_computes_from_cached(self, tmp_path):
        """An operator downstream of a proxy-backed node should work."""
        import json

        from orcapod.core.operators import SelectPacketColumns
        from orcapod.core.function_pod import function_pod
        from orcapod.core.sources.dict_source import DictSource
        from orcapod.databases.delta_lake_databases import DeltaTableDatabase
        from orcapod.pipeline import LoadStatus, Pipeline

        db_path = str(tmp_path / "pipeline_db")
        db = DeltaTableDatabase(db_path)

        source = DictSource(
            {"name": ["alice", "bob"], "age": [30, 25]},
            tag_columns=["name"],
        )

        @function_pod(output_keys=("doubled_age", "original_age"), version="v1.0")
        def transform(age: int) -> tuple[int, int]:
            return age * 2, age

        select_op = SelectPacketColumns(columns=["doubled_age"])

        with Pipeline("test_downstream", pipeline_database=db) as p:
            stream = source
            stream = transform.pod(stream)
            stream = select_op(stream)

        p.run()

        save_path = str(tmp_path / "pipeline.json")
        p.save(save_path)

        # Corrupt function module path
        with open(save_path) as f:
            data = json.load(f)
        for node in data["nodes"].values():
            if node.get("node_type") == "function":
                pf_config = node["function_pod"]["packet_function"]["config"]
                pf_config["module_path"] = "nonexistent.module.path"
        with open(save_path, "w") as f:
            json.dump(data, f)

        loaded = Pipeline.load(save_path)

        # Find the operator node
        op_node = None
        fn_node = None
        for node in loaded.compiled_nodes.values():
            if node.node_type == "operator":
                op_node = node
            elif node.node_type == "function":
                fn_node = node

        assert fn_node is not None
        assert fn_node.load_status == LoadStatus.READ_ONLY

        assert op_node is not None
        assert op_node.load_status == LoadStatus.FULL

        # The operator should be able to compute from cached data
        table = op_node.as_table()
        assert table is not None
        assert "doubled_age" in table.column_names
        assert table.num_rows == 2
```

- [ ] **Step 2: Run test**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineLoadWithUnavailableFunction::test_downstream_operator_computes_from_cached -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_pipeline/test_serialization.py
git commit -m "test(pipeline): add downstream operator test for cached data flow"
```

---

### Task 11: Run full test suite and final cleanup

**Files:**
- All modified files

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest tests/ -v --timeout=120`
Expected: All PASS

- [ ] **Step 2: Fix any failures**

Address any test failures discovered.

- [ ] **Step 3: Final commit if any fixes were needed**

```bash
git add -u
git commit -m "fix: address test failures from PacketFunctionProxy integration"
```
