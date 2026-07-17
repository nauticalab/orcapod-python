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
        def fn(value, ctx):
            calls.append(value)

        pod = SideEffectPod(fn)
        stream = _make_stream()
        inv = SideEffectInvocation(pod=pod, input_streams=(stream,))
        assert inv.pod is pod
        assert inv.input_streams == (stream,)

    def test_requires_exactly_one_stream(self):
        from orcapod.pipeline.pod_invocation import SideEffectInvocation
        from orcapod.side_effects import SideEffectPod

        def fn(value, ctx): pass
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
        def fn(value, ctx):
            calls.append({"value": value})

        pod = SideEffectPod(fn, config=SideEffectPodConfig(drop_on_failure=False))
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3
        assert len(calls) == 3

    def test_t2_passthrough_drop_on_failure_true_no_errors(self):
        """T2: All rows emitted when drop_on_failure=True and no errors."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(value, ctx):
            calls.append({"value": value})

        pod = SideEffectPod(fn)  # default: drop_on_failure=True
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3
        assert len(calls) == 3

    def test_t3_invocation_context_always_passed(self):
        """T3: InvocationContext always constructed and passed."""
        from orcapod.side_effects import SideEffectPod, InvocationContext

        received = []
        def fn(value, ctx):
            received.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert len(received) == 1
        ctx = received[0]
        assert isinstance(ctx, InvocationContext)
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0
        assert "::" in ctx.format_id()

    def test_t4_invocation_context_ignored_by_callee(self):
        """T4: Pod works fine when callee ignores ctx."""
        from orcapod.side_effects import SideEffectPod

        calls = []
        def fn(value, ctx):
            calls.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(2)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 2
        assert len(calls) == 2

    def test_t8_on_error_raise(self):
        """T8: on_error='raise' propagates the exception."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(value, ctx):
            raise RuntimeError("boom")

        pod = SideEffectPod(fn, config=SideEffectPodConfig(on_error="raise"))
        stream = _make_stream(1)

        with pytest.raises(RuntimeError, match="boom"):
            list(pod.process(stream).iter_data())

    def test_t9_on_error_log_drop_on_failure_true(self):
        """T9: on_error='log' + drop_on_failure=True drops failed rows."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(value, ctx):
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

        def fn(value, ctx):
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
        def my_sink(value, ctx):
            pass

        assert isinstance(my_sink, SideEffectPod)
        assert my_sink.pod_config.track_completion is True
        assert my_sink.pod_config.drop_on_failure is True

    def test_t17_tap_pod(self):
        """T17: @tap_pod sets track_completion=False, drop_on_failure=False."""
        from orcapod.side_effects import tap_pod, SideEffectPod

        @tap_pod
        def my_tap(value, ctx):
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
                def fn(value, ctx):
                    pass

                assert fn.pod_config.track_completion is tc
                assert fn.pod_config.drop_on_failure is dof

    def test_sink_pod_parameterised(self):
        """@sink_pod(config=...) with explicit config override."""
        from orcapod.side_effects import sink_pod, SideEffectPodConfig

        cfg = SideEffectPodConfig(on_error="log")

        @sink_pod(config=cfg)
        def my_sink(value, ctx):
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
        """T5: DB row written for each successful delivery."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode

        calls = []
        def fn(value, ctx):
            calls.append(True)

        node, stream, db = self._make_node_with_db(fn)
        results = node.execute(stream)
        assert len(results) == 3
        assert len(calls) == 3

        # Read log table
        table_path = node._table_path
        records = db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert "record_id_hash" in df.columns
        assert "executed_at" in df.columns
        assert "status" not in df.columns  # only success records; no status column
        assert "invocation_hash" not in df.columns  # never stored

    def test_t6_track_completion_skips_on_rerun(self):
        """T6: Second run skips re-delivery; skipped row still emitted."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        calls = []
        def fn(value, ctx):
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
        assert len(calls) == 2  # fn NOT called again — completion check triggered

        # Log still has exactly 2 rows (one per unique input); no duplicate writes
        table_path = node1._table_path
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert len(df) == 2  # one success record per unique input, not re-written

    def test_t7_no_track_completion_reruns_delivery(self):
        """T7: track_completion=False always re-delivers."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        calls = []
        def fn(value, ctx):
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

        table_path = node1._table_path
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        # track_completion=False → same record_id written twice; skip_duplicates=True
        # means second write is silently dropped, so we still see 2 unique rows.
        assert len(df) == 2
        assert "record_id_hash" in df.columns
        assert "status" not in df.columns

    def test_failure_does_not_write_log_so_next_run_retries(self):
        """Delivery failure writes nothing to the log; the next run retries the row.

        With ``track_completion=True``, the completion check uses ``get_record_by_id``.
        A failed delivery never writes a success record, so the record_id is absent
        from the DB and the next run sees no prior completion — it calls fn again.
        """
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        call_count = 0

        def fn(value, ctx):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("always fails")

        cfg = SideEffectPodConfig(
            track_completion=True, on_error="log", drop_on_failure=False
        )
        pod = SideEffectPod(fn, config=cfg)
        stream = _make_stream(1)
        db = _make_in_memory_db()

        # Run 1: fn raises → nothing written to DB.
        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=db)
        results1 = node1.execute(stream)
        assert len(results1) == 1  # row still emitted (drop_on_failure=False)
        assert call_count == 1     # fn was invoked

        table_path = node1._table_path
        assert db.get_all_records(table_path) is None  # no success record in DB

        # Run 2: no record found → fn is called again, still fails.
        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=db)
        results2 = node2.execute(stream)
        assert len(results2) == 1
        assert call_count == 2  # retried — fn called a second time

        assert db.get_all_records(table_path) is None  # still nothing in DB

    def test_fail_then_succeed_then_skip(self):
        """Fail → success → skip: the full idempotency lifecycle.

        Run 1: fn fails → no DB record → row emitted (drop_on_failure=False).
        Run 2: no record found → fn retried → succeeds → DB record written.
        Run 3: record found → fn NOT called → row re-emitted from cache.
        """
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        call_count = 0

        def fn(value, ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient failure on first call")
            # call 2+: succeed

        cfg = SideEffectPodConfig(
            track_completion=True, on_error="log", drop_on_failure=False
        )
        pod = SideEffectPod(fn, config=cfg)
        stream = _make_stream(1)
        db = _make_in_memory_db()

        # Run 1: fn raises → no DB record.
        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=db)
        results1 = node1.execute(stream)
        assert len(results1) == 1  # emitted despite failure
        assert call_count == 1

        table_path = node1._table_path
        assert db.get_all_records(table_path) is None  # failure ≠ completion

        # Run 2: no record → fn retried → succeeds → record written.
        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=db)
        results2 = node2.execute(stream)
        assert len(results2) == 1
        assert call_count == 2  # fn called exactly once more

        records = db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 1  # one success record now in DB

        # Run 3: record found → fn NOT called → row re-emitted from cache.
        node3 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node3.attach_databases(pipeline_database=db)
        results3 = node3.execute(stream)
        assert len(results3) == 1
        assert call_count == 2  # still 2 — fn was skipped entirely

        # DB still has exactly one record (no duplicate writes).
        assert len(pl.from_arrow(db.get_all_records(table_path))) == 1

    def test_t11_invocation_hash_determinism(self):
        """T11: Identical inputs produce identical invocation_hash."""
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, InvocationContext

        ctx_list: list[InvocationContext] = []
        def fn(value, ctx):
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
        def fn(value, ctx):
            ctx_list.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=_make_in_memory_db())
        node.execute(stream)

        ctx = ctx_list[0]
        override = InvocationHashConfig(encoding="base64", component_length=8)
        fid = ctx.format_id(override)

        # Two components of the form "{method}:{base64_digest}", separated by "::"
        parts = fid.split("::")
        assert len(parts) == 2
        import base64
        for part in parts:
            # Each component is "{method}:{encoded_digest}"; split on the first ":"
            assert ":" in part
            _method, encoded = part.split(":", 1)
            decoded = base64.b64decode(encoded)
            assert len(decoded) == 8


# ---------------------------------------------------------------------------
# Task 8 tests — async execution
# ---------------------------------------------------------------------------


class TestSideEffectJobNodeAsync:
    """T13–T14: async_execute via channels."""

    def test_t13_async_execute_basic(self):
        """T13: async_execute processes all rows and writes log."""
        import asyncio
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode
        from orcapod.channels import Channel

        calls = []
        def fn(value, ctx):
            calls.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(3)
        db = _make_in_memory_db()
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=db)

        async def _run():
            ch_in = Channel(buffer_size=10)
            ch_out = Channel(buffer_size=10)

            async def feed():
                for tag, data in stream.iter_data():
                    await ch_in.writer.send((tag, data))
                await ch_in.writer.close()

            await asyncio.gather(
                feed(),
                node.async_execute(
                    [ch_in.reader], ch_out.writer, run_id="test-run-1"
                ),
            )
            return await ch_out.reader.collect()

        results = asyncio.run(_run())
        assert len(results) == 3
        assert len(calls) == 3

        table_path = node._table_path
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert "record_id_hash" in df.columns
        assert "status" not in df.columns  # only success records; no status column

    def test_t14_async_execute_parallel(self):
        """T14: Concurrent delivery via max_concurrency — all rows complete."""
        import asyncio
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode
        from orcapod.channels import Channel

        results_store = []
        def fn(value, ctx):
            results_store.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(10)
        db = _make_in_memory_db()
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=db)

        async def _run():
            ch_in = Channel(buffer_size=20)
            ch_out = Channel(buffer_size=20)

            async def feed():
                for tag, data in stream.iter_data():
                    await ch_in.writer.send((tag, data))
                await ch_in.writer.close()

            await asyncio.gather(
                feed(),
                node.async_execute([ch_in.reader], ch_out.writer),
            )
            return await ch_out.reader.collect()

        out = asyncio.run(_run())
        assert len(out) == 10
        assert len(results_store) == 10


# ---------------------------------------------------------------------------
# Task 9 tests — full pipeline integration
# ---------------------------------------------------------------------------


class TestSideEffectPodPipelineIntegration:
    """T15: Side-effect pod inside a PipelineJob."""

    def test_t15_pipeline_composition_mid_pipeline(self):
        """T15: Pod runs mid-pipeline; delivery log written to DB."""
        import polars as pl
        from orcapod.pipeline.job import PipelineJob
        from orcapod.side_effects import SideEffectPod
        from orcapod.core.sources.dict_source import DictSource

        delivery_log = []

        def log_delivery(value, ctx):
            delivery_log.append({"value": value})

        log_pod = SideEffectPod(log_delivery)

        db = _make_in_memory_db()

        with PipelineJob(name="test_pipeline", store=db) as job:
            source = DictSource(
                [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                tag_columns=["id"],
            )
            log_pod.process(source)

        job.run()

        # Delivery log was populated
        assert len(delivery_log) == 2

        # Find the side_effect node in the compiled graph
        side_effect_nodes = [
            n for n in job.dag.nodes()
            if getattr(n, "node_type", None) == "side_effect"
        ]
        assert len(side_effect_nodes) == 1
        node = side_effect_nodes[0]

        # Invocation log written to DB (pipeline_db is scoped to the pipeline name)
        pipeline_db = db.at("test_pipeline")
        table_path = node._table_path
        records = pipeline_db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 2
        assert "record_id_hash" in df.columns
        assert "status" not in df.columns  # only success records; no status column


# ---------------------------------------------------------------------------
# New call style + ctx_arg_name tests
# ---------------------------------------------------------------------------


class TestSideEffectPodNewCallStyle:
    """SEP-UPDATE: Data fields passed as kwargs, not as DataProtocol."""

    def test_sep_update_kwargs_passed_to_fn(self):
        """Data fields unpacked as kwargs; ctx passed by ctx_arg_name."""
        from orcapod.side_effects import SideEffectPod

        received: dict = {}

        def fn(value, ctx):
            received["value"] = value
            received["ctx"] = ctx

        pod = SideEffectPod(fn)
        stream = _make_stream(1)  # stream has 'value' data column (int64)
        list(pod.process(stream).iter_data())

        assert "value" in received
        assert isinstance(received["value"], int)
        assert received["ctx"] is not None


class TestSideEffectPodCtxArgName:
    """SEP-CTX-NAME: ctx_arg_name routes InvocationContext to correct parameter."""

    def test_sep_ctx_name_custom(self):
        """ctx_arg_name='context' injects InvocationContext under that name."""
        from orcapod.side_effects import SideEffectPod, InvocationContext

        received: dict = {}

        def fn(value, context):
            received["context"] = context

        pod = SideEffectPod(fn, ctx_arg_name="context")
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert isinstance(received["context"], InvocationContext)

    def test_sep_ctx_name_collision_raises(self):
        """ctx_arg_name colliding with a data column name raises ValueError."""
        from orcapod.side_effects import SideEffectPod

        def fn(value, ctx):
            pass

        # _make_stream has 'value' as the data column; use ctx_arg_name='value' to force collision
        pod = SideEffectPod(fn, ctx_arg_name="value")
        stream = _make_stream(1)
        with pytest.raises(ValueError, match="collides"):
            list(pod.process(stream).iter_data())
