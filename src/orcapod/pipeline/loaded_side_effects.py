"""Side-effect nodes restored from a saved pipeline or job file, read-only (ITL-1094).

A saved file records a side-effect node's label, content hash, pipeline hash, output
schema, node uri, data context and Orcapod configuration. It records neither the
side-effect function nor the pod configuration (``on_error``, ``track_completion``,
``drop_on_failure``, ``max_concurrency``). ``Pipeline.load`` and ``PipelineJob.load``
restore the node from that record, in its saved place in the graph, with no pod: it
never delivers and never writes its invocation log.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from orcapod.core.nodes.function_node import run_job_node
from orcapod.side_effects import SideEffectJobNode, SideEffectNode

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.config import OrcapodConfig
    from orcapod.contexts import DataContext
    from orcapod.core.streams.view import StreamView
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
    )
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.side_effects import SideEffectPod
    from orcapod.types import ContentHash


class SavedSideEffectNode:
    """What a saved pipeline or job file records for one side-effect node.

    Args:
        descriptor: The node's entry in the saved file (``node_type``
            ``"side_effect"``).

    Raises:
        ValueError: If the descriptor lacks ``content_hash``, ``pipeline_hash`` or
            ``node_uri``.
    """

    def __init__(self, descriptor: dict[str, Any]) -> None:
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig
        from orcapod.contexts import resolve_saved_context
        from orcapod.types import ContentHash

        missing = [
            name
            for name in ("content_hash", "pipeline_hash", "node_uri")
            if not descriptor.get(name)
        ]
        if missing:
            raise ValueError(
                f"The saved side-effect node {descriptor.get('label')!r} has no "
                f"{', '.join(missing)}."
            )
        self.descriptor: dict[str, Any] = descriptor
        self.label: str | None = descriptor.get("label")
        self.content_hash: ContentHash = ContentHash.from_string(
            descriptor["content_hash"]
        )
        self.pipeline_hash: ContentHash = ContentHash.from_string(
            descriptor["pipeline_hash"]
        )
        self.node_uri: tuple[str, ...] = tuple(descriptor["node_uri"])
        self.data_context: DataContext = resolve_saved_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        self.orcapod_config: OrcapodConfig = (
            OrcapodConfig.from_dict(config_dict)
            if config_dict is not None
            else DEFAULT_CONFIG
        )


def cannot_deliver_error(label: str | None) -> RuntimeError:
    """Return the error for asking a loaded side-effect node to deliver."""
    return RuntimeError(
        f"Side-effect node {label!r} was loaded from a saved file, which holds no "
        "side-effect function, so it cannot deliver. Build the job from its code to "
        "run its side effects."
    )


class LoadedSideEffectNode(SideEffectNode):
    """A side-effect node of a loaded pipeline blueprint: no pod, never delivers.

    Built by ``Pipeline.load``, and by ``as_pipeline()`` of a loaded job. Its identity
    and node uri are the saved ones. It passes its input's columns through, as every
    side-effect node does: its schema under any column configuration is the restored
    upstream's. A blueprint side-effect node delivers when it is read, so every read
    raises.

    Args:
        descriptor: The node's entry in the saved file.
        input_stream: The restored node upstream of it.

    Raises:
        ValueError: As for ``SavedSideEffectNode``.
    """

    def __init__(
        self, descriptor: dict[str, Any], input_stream: StreamProtocol
    ) -> None:
        saved = SavedSideEffectNode(descriptor)
        super().__init__(
            # A loaded node has no pod: every method that would use it is overridden.
            side_effect_pod=None,  # pyright: ignore[reportArgumentType]
            input_stream=input_stream,
            label=saved.label,
        )
        self._saved: SavedSideEffectNode = saved
        self._descriptor: dict[str, Any] = descriptor
        self._data_context = saved.data_context
        self._orcapod_config = saved.orcapod_config

    @property
    @override
    def producer(self) -> SideEffectPod | None:
        """``None``: a loaded node has no pod."""
        return None

    @override
    def identity_structure(self) -> Any:
        """The saved content hash."""
        return self._saved.content_hash

    @override
    def pipeline_identity_structure(self) -> Any:
        """The saved pipeline hash."""
        return self._saved.pipeline_hash

    @override
    def content_hash(self, hasher: Any = None) -> ContentHash:
        """The saved content hash."""
        return self._saved.content_hash

    @override
    def pipeline_hash(self, hasher: Any = None) -> ContentHash:
        """The saved pipeline hash."""
        return self._saved.pipeline_hash

    @override
    def computed_label(self) -> str | None:
        return None

    @override
    def _rows_data_context(self) -> DataContext:
        return self._saved.data_context

    @property
    @override
    def node_uri(self) -> tuple[str, ...]:
        """The saved node uri."""
        return self._saved.node_uri

    @override
    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise: a blueprint node delivers when read, and this one cannot deliver."""
        raise cannot_deliver_error(self.label)


class LoadedSideEffectJobNode(SideEffectJobNode):
    """A side-effect node of a loaded job: no pod, never delivers.

    Built by ``PipelineJob.load``. Its identity and node uri are the saved ones; the
    pipeline hash names the saved run's invocation log, which the node never reads or
    writes. Its schema under any column configuration is the restored upstream's, as
    for every side-effect node. A load-only run (``loaded.run(load_only=True)``,
    ``node.run(load_only=True)``) passes its input rows on, and the node holds them as
    loaded only, as every ``SideEffectJobNode`` does in such a run; its reads return
    those rows, and none before such a run. Any other run raises before anything is
    read, delivered or logged.

    Args:
        descriptor: The node's entry in the saved file.
        input_stream: The restored job node upstream of it.
        job_content_hash: The content hash the saved job's node had (the file's
            ``run.node_job_content_hashes``), as a loaded function node takes it. A
            file that does not record it leaves the descriptor's (blueprint) hash.

    Raises:
        ValueError: As for ``SavedSideEffectNode``.
    """

    def __init__(
        self,
        descriptor: dict[str, Any],
        input_stream: StreamProtocol,
        job_content_hash: str | None = None,
    ) -> None:
        from orcapod.types import ContentHash

        saved = SavedSideEffectNode(descriptor)
        super().__init__(
            # A loaded node has no pod: every method that would use it is overridden.
            side_effect_pod=None,  # pyright: ignore[reportArgumentType]
            input_stream=input_stream,
            label=saved.label,
        )
        self._saved: SavedSideEffectNode = saved
        self._content_hash: ContentHash = (
            ContentHash.from_string(job_content_hash)
            if job_content_hash is not None
            else saved.content_hash
        )
        self._descriptor: dict[str, Any] = descriptor
        self._data_context = saved.data_context
        self._orcapod_config = saved.orcapod_config

    @property
    @override
    def producer(self) -> SideEffectPod | None:
        """``None``: a loaded node has no pod."""
        return None

    @override
    def identity_structure(self) -> Any:
        """The saved job's content hash."""
        return self._content_hash

    @override
    def pipeline_identity_structure(self) -> Any:
        """The saved pipeline hash."""
        return self._saved.pipeline_hash

    @override
    def content_hash(self, hasher: Any = None) -> ContentHash:
        """The saved job's content hash (``job_content_hash``, else the descriptor's)."""
        return self._content_hash

    @override
    def pipeline_hash(self, hasher: Any = None) -> ContentHash:
        """The saved pipeline hash: it names the saved run's invocation log."""
        return self._saved.pipeline_hash

    @override
    def computed_label(self) -> str | None:
        return None

    @override
    def _rows_data_context(self) -> DataContext:
        return self._saved.data_context

    @property
    @override
    def node_uri(self) -> tuple[str, ...]:
        """The saved node uri."""
        return self._saved.node_uri

    @override
    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
        load_only: bool = False,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Pass every row on in a load-only run, as every side-effect job node does.

        Args:
            input_stream: Stream of ``(tag, data)`` pairs.
            observer: As for ``SideEffectJobNode.execute``.
            run_id: As for ``SideEffectJobNode.execute``.
            load_only: Must be true: the node can only pass rows on.

        Returns:
            The rows of ``input_stream``, which the node then holds as loaded only.

        Raises:
            RuntimeError: If ``load_only`` is false; nothing is read then.
        """
        if not load_only:
            raise cannot_deliver_error(self.label)
        # The live load-only path: it uses no pod, delivers nothing and logs nothing.
        return super().execute(
            input_stream, observer=observer, run_id=run_id, load_only=True
        )

    @override
    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
        load_only: bool = False,
    ) -> None:
        """Pass every row on in a load-only run, as every side-effect job node does.

        Closes ``output`` in every case.

        Args:
            inputs: Single-element sequence containing the input channel.
            output: Writable channel for the rows passed on.
            observer: As for ``SideEffectJobNode.async_execute``.
            run_id: As for ``SideEffectJobNode.async_execute``.
            load_only: Must be true: the node can only pass rows on.

        Raises:
            RuntimeError: If ``load_only`` is false; nothing is read then.
            ValueError: If ``inputs`` does not contain exactly one channel.
        """
        if not load_only:
            try:
                raise cannot_deliver_error(self.label)
            finally:
                await output.close()
        # The live load-only path: it uses no pod, delivers nothing and logs nothing.
        await super().async_execute(
            inputs, output, observer=observer, run_id=run_id, load_only=True
        )

    @override
    def run(self, *, load_only: bool = False) -> StreamView:
        """Run the node load-only, as ``loaded.run(load_only=True)`` runs it.

        The job nodes it depends on that hold no current run run first, load-only; the
        node then passes its input rows on and holds them. Nothing is delivered.

        Args:
            load_only: Must be true: the node can only pass rows on.

        Returns:
            A ``StreamView`` of the rows the node holds.

        Raises:
            RuntimeError: If ``load_only`` is false; nothing runs then.
        """
        import sys

        if not load_only:
            raise cannot_deliver_error(self.label)
        run_job_node(
            self,
            load_only=True,
            run_alone=lambda: self._run_alone(load_only=True),
            caller=sys._getframe(1),  # pyright: ignore[reportPrivateUsage]  # the caller's line is where a failure warning is shown
        )
        return self._with_run_status(self._view_of_rows(self.as_table(), "held"))
