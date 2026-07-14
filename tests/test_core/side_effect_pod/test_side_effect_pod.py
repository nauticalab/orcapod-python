# tests/test_core/side_effect_pod/test_side_effect_pod.py
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.streams import ArrowTableStream
from orcapod.core.datagrams import Data, Tag


def _make_stream(n: int = 3) -> ArrowTableStream:
    """Simple stream: tag=id (int), data=value (int)."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {"id": list(range(n)), "value": list(range(n))},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


# ---------------------------------------------------------------------------
# Task 1 tests
# ---------------------------------------------------------------------------

class TestInvocationHashConfig:
    def test_defaults(self):
        from orcapod.side_effects import InvocationHashConfig
        cfg = InvocationHashConfig()
        assert cfg.encoding == "hex"
        assert cfg.component_length is None

    def test_custom(self):
        from orcapod.side_effects import InvocationHashConfig
        cfg = InvocationHashConfig(encoding="base64", component_length=8)
        assert cfg.encoding == "base64"
        assert cfg.component_length == 8

    def test_frozen(self):
        from orcapod.side_effects import InvocationHashConfig
        cfg = InvocationHashConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.encoding = "base64"  # type: ignore[misc]


class TestSideEffectPodConfig:
    def test_defaults(self):
        from orcapod.side_effects import SideEffectPodConfig
        cfg = SideEffectPodConfig()
        assert cfg.track_completion is True
        assert cfg.drop_on_failure is True
        assert cfg.on_error == "raise"

    def test_custom(self):
        from orcapod.side_effects import SideEffectPodConfig
        cfg = SideEffectPodConfig(track_completion=False, drop_on_failure=False)
        assert cfg.track_completion is False
        assert cfg.drop_on_failure is False


# ---------------------------------------------------------------------------
# Task 3 tests
# ---------------------------------------------------------------------------

class TestSideEffectInvocation:
    def test_construction(self):
        from orcapod.pipeline.pod_invocation import SideEffectInvocation
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(data)

        pod = SideEffectPod(fn)
        stream = _make_stream()
        inv = SideEffectInvocation(pod=pod, input_streams=(stream,))
        assert inv.pod is pod
        assert inv.input_streams == (stream,)

    def test_requires_exactly_one_stream(self):
        from orcapod.pipeline.pod_invocation import SideEffectInvocation
        from orcapod.side_effects import SideEffectPod

        def fn(data, ctx): pass
        pod = SideEffectPod(fn)
        stream = _make_stream()

        with pytest.raises(ValueError):
            SideEffectInvocation(pod=pod, input_streams=())

        with pytest.raises(ValueError):
            SideEffectInvocation(pod=pod, input_streams=(stream, stream))
