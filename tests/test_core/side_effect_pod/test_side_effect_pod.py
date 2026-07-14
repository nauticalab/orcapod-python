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


# ---------------------------------------------------------------------------
# Task 5 tests — standalone / lazy mode (no DB)
# ---------------------------------------------------------------------------


class TestSideEffectPodStandalone:
    """T1–T4, T8–T10: standalone execution via SideEffectPodStream."""

    def test_t1_passthrough_drop_on_failure_false(self):
        """T1: All rows emitted when drop_on_failure=False and no errors."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(dict(data))

        pod = SideEffectPod(fn, config=SideEffectPodConfig(drop_on_failure=False))
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3
        assert len(calls) == 3

    def test_t2_passthrough_drop_on_failure_true_no_errors(self):
        """T2: All rows emitted when drop_on_failure=True and no errors."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(dict(data))

        pod = SideEffectPod(fn)  # default: drop_on_failure=True
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3
        assert len(calls) == 3

    def test_t3_invocation_context_always_passed(self):
        """T3: InvocationContext always constructed and passed."""
        from orcapod.side_effects import SideEffectPod, InvocationContext

        received = []
        def fn(data, ctx):
            received.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert len(received) == 1
        ctx = received[0]
        assert isinstance(ctx, InvocationContext)
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0
        assert ctx.format_id().startswith("orcapod-")

    def test_t4_invocation_context_ignored_by_callee(self):
        """T4: Pod works fine when callee ignores ctx."""
        from orcapod.side_effects import SideEffectPod

        calls = []
        def fn(data, _ctx):
            calls.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(2)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 2
        assert len(calls) == 2

    def test_t8_on_error_raise(self):
        """T8: on_error='raise' propagates the exception."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(data, ctx):
            raise RuntimeError("boom")

        pod = SideEffectPod(fn, config=SideEffectPodConfig(on_error="raise"))
        stream = _make_stream(1)

        with pytest.raises(RuntimeError, match="boom"):
            list(pod.process(stream).iter_data())

    def test_t9_on_error_log_drop_on_failure_true(self):
        """T9: on_error='log' + drop_on_failure=True drops failed rows."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(data, ctx):
            raise RuntimeError("oops")

        pod = SideEffectPod(
            fn,
            config=SideEffectPodConfig(on_error="log", drop_on_failure=True),
        )
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 0  # all rows dropped

    def test_t10_on_error_log_drop_on_failure_false(self):
        """T10: on_error='log' + drop_on_failure=False passes through despite failure."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(data, ctx):
            raise RuntimeError("oops")

        pod = SideEffectPod(
            fn,
            config=SideEffectPodConfig(on_error="log", drop_on_failure=False),
        )
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3  # all rows still emitted


class TestDecorators:
    """T16–T18: @sink_pod, @tap_pod, @side_effect_pod."""

    def test_t16_sink_pod(self):
        """T16: @sink_pod sets track_completion=True, drop_on_failure=True."""
        from orcapod.side_effects import sink_pod, SideEffectPod

        @sink_pod
        def my_sink(data, ctx):
            pass

        assert isinstance(my_sink, SideEffectPod)
        assert my_sink.pod_config.track_completion is True
        assert my_sink.pod_config.drop_on_failure is True

    def test_t17_tap_pod(self):
        """T17: @tap_pod sets track_completion=False, drop_on_failure=False."""
        from orcapod.side_effects import tap_pod, SideEffectPod

        @tap_pod
        def my_tap(data, ctx):
            pass

        assert isinstance(my_tap, SideEffectPod)
        assert my_tap.pod_config.track_completion is False
        assert my_tap.pod_config.drop_on_failure is False

    def test_t18_side_effect_pod_config_combinations(self):
        """T18: @side_effect_pod(config=...) all four combinations."""
        from orcapod.side_effects import side_effect_pod, SideEffectPodConfig

        for tc in [True, False]:
            for dof in [True, False]:
                cfg = SideEffectPodConfig(track_completion=tc, drop_on_failure=dof)

                @side_effect_pod(config=cfg)
                def fn(data, ctx):
                    pass

                assert fn.pod_config.track_completion is tc
                assert fn.pod_config.drop_on_failure is dof

    def test_sink_pod_parameterised(self):
        """@sink_pod(config=...) with explicit config override."""
        from orcapod.side_effects import sink_pod, SideEffectPodConfig

        cfg = SideEffectPodConfig(on_error="log")

        @sink_pod(config=cfg)
        def my_sink(data, ctx):
            pass

        assert my_sink.pod_config.on_error == "log"
        assert my_sink.pod_config.track_completion is True
        assert my_sink.pod_config.drop_on_failure is True


# ---------------------------------------------------------------------------
# Task 7 tests — DB-backed execution via SideEffectJobNode
# ---------------------------------------------------------------------------


def _make_in_memory_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    return InMemoryArrowDatabase()


class TestSideEffectJobNodeSync:
    """T5–T7, T11–T12: DB-backed sync execution."""

    def _make_node_with_db(self, fn, config=None):
        """Helper: build a SideEffectJobNode with an in-memory DB attached."""
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode

        pod = SideEffectPod(fn, config=config)
        stream = _make_stream(3)
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        db = _make_in_memory_db()
        node.attach_databases(pipeline_database=db)
        return node, stream, db

    def test_t5_invocation_log_written_on_success(self):
        """T5: DB row written with status='success'."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode

        calls = []
        def fn(data, ctx):
            calls.append(True)

        node, stream, db = self._make_node_with_db(fn)
        results = node.execute(stream)
        assert len(results) == 3
        assert len(calls) == 3

        # Read log table
        table_path = (node.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert all(df["status"] == "success")
        assert "full_input_packet_hash" in df.columns
        assert "invocation_hash" not in df.columns  # never stored

    def test_t6_track_completion_skips_on_rerun(self):
        """T6: Second run skips re-delivery; skipped row still emitted."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(True)

        cfg = SideEffectPodConfig(track_completion=True)
        pod = SideEffectPod(fn, config=cfg)
        stream = _make_stream(2)
        db = _make_in_memory_db()

        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=db)
        results1 = node1.execute(stream)
        assert len(results1) == 2
        assert len(calls) == 2

        # Second run with same DB
        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=db)
        results2 = node2.execute(stream)
        assert len(results2) == 2  # rows still emitted (pass-through)
        assert len(calls) == 2  # fn NOT called again

        # Check log has skipped rows
        table_path = (node1.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert "skipped" in df["status"].to_list()

    def test_t7_no_track_completion_reruns_delivery(self):
        """T7: track_completion=False always re-delivers."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(True)

        cfg = SideEffectPodConfig(track_completion=False)
        pod = SideEffectPod(fn, config=cfg)
        stream = _make_stream(2)
        db = _make_in_memory_db()

        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=db)
        node1.execute(stream)

        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=db)
        node2.execute(stream)

        assert len(calls) == 4  # called twice per run

        table_path = (node1.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert len(df) == 4  # two rows × two runs
        assert all(df["status"] == "success")

    def test_t11_invocation_hash_determinism(self):
        """T11: Identical inputs produce identical invocation_hash."""
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, InvocationContext

        ctx_list: list[InvocationContext] = []
        def fn(data, ctx):
            ctx_list.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)

        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=_make_in_memory_db())
        node1.execute(stream)
        hash1 = ctx_list[0].invocation_hash
        ctx_list.clear()

        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=_make_in_memory_db())
        node2.execute(stream)
        hash2 = ctx_list[0].invocation_hash

        assert hash1 == hash2

    def test_t12_format_id_base64_override(self):
        """T12: format_id with base64 encoding returns valid compound."""
        from orcapod.side_effects import (
            SideEffectPod, SideEffectJobNode, InvocationHashConfig, InvocationContext
        )

        ctx_list: list[InvocationContext] = []
        def fn(data, ctx):
            ctx_list.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=_make_in_memory_db())
        node.execute(stream)

        ctx = ctx_list[0]
        override = InvocationHashConfig(encoding="base64", component_length=8)
        fid = ctx.format_id(override)

        assert fid.startswith("orcapod-")
        # Two base64-encoded components of 8 bytes each (11 chars each in base64)
        parts = fid[len("orcapod-"):].split("::")
        assert len(parts) == 2
        import base64
        for part in parts:
            decoded = base64.b64decode(part)
            assert len(decoded) == 8
