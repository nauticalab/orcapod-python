from abc import abstractmethod
from orcapod.core.kernels import KernelStream, WrappedKernel
from orcapod.core.sources.base import InvocationBase
from orcapod.core.pods import CachedPod
from orcapod.protocols import core_protocols as cp, database_protocols as dbp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule
from typing import TYPE_CHECKING, Any
from orcapod.core.system_constants import constants
from orcapod.utils import arrow_utils
from collections.abc import Collection
from orcapod.core.streams import PodNodeStream

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
    import pandas as pd
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pd = LazyModule("pandas")


class NodeBase(
    InvocationBase,
):
    """
    Mixin class for pipeline nodes
    """

    def __init__(
        self,
        input_streams: Collection[cp.Stream],
        pipeline_database: dbp.ArrowDatabase,
        pipeline_path_prefix: tuple[str, ...] = (),
        kernel_type: str = "operator",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.kernel_type = kernel_type
        self._cached_stream: KernelStream | None = None
        self._input_streams = tuple(input_streams)
        self._pipeline_path_prefix = pipeline_path_prefix
        # compute invocation hash - note that empty () is passed into identity_structure to signify
        # identity structure of invocation with no input streams
        self.pipeline_node_hash = self.data_context.object_hasher.hash_object(
            self.identity_structure(())
        ).to_string()
        tag_types, packet_types = self.types(include_system_tags=True)

        self.tag_schema_hash = self.data_context.object_hasher.hash_object(
            tag_types
        ).to_string()

        self.packet_schema_hash = self.data_context.object_hasher.hash_object(
            packet_types
        ).to_string()

        self.pipeline_database = pipeline_database

    @property
    def id(self) -> str:
        return self.content_hash().to_string()

    @property
    def upstreams(self) -> tuple[cp.Stream, ...]:
        return self._input_streams

    def track_invocation(self, *streams: cp.Stream, label: str | None = None) -> None:
        # Node invocation should not be tracked
        return None

    @property
    def contained_kernel(self) -> cp.Kernel:
        raise NotImplementedError(
            "This property should be implemented by subclasses to return the contained kernel."
        )

    @property
    def reference(self) -> tuple[str, ...]:
        return self.contained_kernel.reference

    @property
    @abstractmethod
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        ...

    def validate_inputs(self, *streams: cp.Stream) -> None:
        return

    # def forward(self, *streams: cp.Stream) -> cp.Stream:
    #     # TODO: re-evaluate the use here -- consider semi joining with input streams
    #     # super().validate_inputs(*self.input_streams)
    #     return super().forward(*self.upstreams)  # type: ignore[return-value]

    def pre_kernel_processing(self, *streams: cp.Stream) -> tuple[cp.Stream, ...]:
        return self.upstreams

    def kernel_output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Return the output types of the node.
        This is used to determine the types of the output streams.
        """
        return self.contained_kernel.output_types(
            *self.upstreams, include_system_tags=include_system_tags
        )

    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        # construct identity structure from the node's information and the
        return self.contained_kernel.identity_structure(self.upstreams)

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all records associated with the node.
        If include_system_columns is True, system columns will be included in the result.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def flush(self):
        self.pipeline_database.flush()


class KernelNode(NodeBase, WrappedKernel):
    """
    A node in the pipeline that represents a kernel.
    This node can be used to execute the kernel and process data streams.
    """

    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        kernel: cp.Kernel,
        input_streams: Collection[cp.Stream],
        pipeline_database: dbp.ArrowDatabase,
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(
            kernel=kernel,
            input_streams=input_streams,
            pipeline_database=pipeline_database,
            pipeline_path_prefix=pipeline_path_prefix,
            **kwargs,
        )
        self.skip_recording = True

    @property
    def contained_kernel(self) -> cp.Kernel:
        return self.kernel

    def __repr__(self):
        return f"KernelNode(kernel={self.kernel!r})"

    def __str__(self):
        return f"KernelNode:{self.kernel!s}"

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        output_stream = super().forward(*streams)

        if not self.skip_recording:
            self.record_pipeline_output(output_stream)
        return output_stream

    def record_pipeline_output(self, output_stream: cp.Stream) -> None:
        key_column_name = self.HASH_COLUMN_NAME
        # FIXME: compute record id based on each record in its entirety
        output_table = output_stream.as_table(
            include_data_context=True,
            include_system_tags=True,
            include_source=True,
        )
        # compute hash for output_table
        # include system tags
        columns_to_hash = (
            output_stream.tag_keys(include_system_tags=True)
            + output_stream.packet_keys()
        )

        arrow_hasher = self.data_context.arrow_hasher
        record_hashes = []
        table_to_hash = output_table.select(columns_to_hash)

        for record_batch in table_to_hash.to_batches():
            for i in range(len(record_batch)):
                record_hashes.append(
                    arrow_hasher.hash_table(record_batch.slice(i, 1)).to_hex()
                )
        # add the hash column
        output_table = output_table.add_column(
            0, key_column_name, pa.array(record_hashes, type=pa.large_string())
        )

        self.pipeline_database.add_records(
            self.pipeline_path,
            output_table,
            record_id_column=key_column_name,
            skip_duplicates=True,
        )

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        return (
            self._pipeline_path_prefix  # pipeline ID
            + self.reference  # node ID
            + (
                f"node:{self.pipeline_node_hash}",  # pipeline node ID
                f"packet:{self.packet_schema_hash}",  # packet schema ID
                f"tag:{self.tag_schema_hash}",  # tag schema ID
            )
        )

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        results = self.pipeline_database.get_all_records(self.pipeline_path)

        if results is None:
            return None

        if not include_system_columns:
            system_columns = [
                c
                for c in results.column_names
                if c.startswith(constants.META_PREFIX)
                or c.startswith(constants.DATAGRAM_PREFIX)
            ]
            results = results.drop(system_columns)

        return results


class PodNode(NodeBase, CachedPod):
    def __init__(
        self,
        pod: cp.Pod,
        input_streams: Collection[cp.Stream],
        pipeline_database: dbp.ArrowDatabase,
        result_database: dbp.ArrowDatabase | None = None,
        record_path_prefix: tuple[str, ...] = (),
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(
            pod=pod,
            result_database=result_database,
            record_path_prefix=record_path_prefix,
            input_streams=input_streams,
            pipeline_database=pipeline_database,
            pipeline_path_prefix=pipeline_path_prefix,
            **kwargs,
        )

    def flush(self):
        self.pipeline_database.flush()
        if self.result_database is not None:
            self.result_database.flush()

    @property
    def contained_kernel(self) -> cp.Kernel:
        return self.pod

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        return (
            self._pipeline_path_prefix  # pipeline ID
            + self.reference  # node ID
            + (
                f"node:{self.pipeline_node_hash}",  # pipeline node ID
                f"tag:{self.tag_schema_hash}",  # tag schema ID
            )
        )

    def __repr__(self):
        return f"PodNode(pod={self.pod!r})"

    def __str__(self):
        return f"PodNode:{self.pod!s}"

    def call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        execution_engine_hash = execution_engine.name if execution_engine else "default"
        if record_id is None:
            record_id = self.get_record_id(packet, execution_engine_hash)

        tag, output_packet = super().call(
            tag,
            packet,
            record_id=record_id,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

        # if output_packet is not None:
        #     retrieved = (
        #         output_packet.get_meta_value(self.DATA_RETRIEVED_FLAG) is not None
        #     )
        #     # add pipeline record if the output packet is not None
        #     # TODO: verify cache lookup logic
        #     self.add_pipeline_record(
        #         tag,
        #         packet,
        #         record_id,
        #         retrieved=retrieved,
        #         skip_cache_lookup=skip_cache_lookup,
        #     )
        return tag, output_packet

    async def async_call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        execution_engine_hash = execution_engine.name if execution_engine else "default"
        if record_id is None:
            record_id = self.get_record_id(packet, execution_engine_hash)

        tag, output_packet = await super().async_call(
            tag,
            packet,
            record_id=record_id,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

        if output_packet is not None:
            retrieved = (
                output_packet.get_meta_value(self.DATA_RETRIEVED_FLAG) is not None
            )
            # add pipeline record if the output packet is not None
            # TODO: verify cache lookup logic
            self.add_pipeline_record(
                tag,
                packet,
                record_id,
                retrieved=retrieved,
                skip_cache_lookup=skip_cache_lookup,
            )
        return tag, output_packet

    def add_pipeline_record(
        self,
        tag: cp.Tag,
        input_packet: cp.Packet,
        packet_record_id: str,
        retrieved: bool | None = None,
        skip_cache_lookup: bool = False,
    ) -> None:
        # combine dp.Tag with packet content hash to compute entry hash
        # TODO: add system tag columns
        # TODO: consider using bytes instead of string representation
        tag_with_hash = tag.as_table(include_system_tags=True).append_column(
            constants.INPUT_PACKET_HASH,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
        )

        # unique entry ID is determined by the combination of tags, system_tags, and input_packet hash
        entry_id = self.data_context.arrow_hasher.hash_table(tag_with_hash).to_string()

        # check presence of an existing entry with the same entry_id
        existing_record = None
        if not skip_cache_lookup:
            existing_record = self.pipeline_database.get_record_by_id(
                self.pipeline_path,
                entry_id,
            )

        if existing_record is not None:
            # if the record already exists, then skip
            return

        # rename all keys to avoid potential collision with result columns
        renamed_input_packet = input_packet.rename(
            {k: f"_input_{k}" for k in input_packet.keys()}
        )
        input_packet_info = (
            renamed_input_packet.as_table(include_source=True)
            .append_column(
                constants.PACKET_RECORD_ID,
                pa.array([packet_record_id], type=pa.large_string()),
            )
            .append_column(
                f"{constants.META_PREFIX}input_packet{constants.CONTEXT_KEY}",
                pa.array([input_packet.data_context_key], type=pa.large_string()),
            )
            .append_column(
                self.DATA_RETRIEVED_FLAG,
                pa.array([retrieved], type=pa.bool_()),
            )
            .drop_columns(list(renamed_input_packet.keys()))
        )

        combined_record = arrow_utils.hstack_tables(
            tag.as_table(include_system_tags=True), input_packet_info
        )

        self.pipeline_database.add_record(
            self.pipeline_path,
            entry_id,
            combined_record,
            skip_duplicates=False,
        )

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        # TODO: re-evaluate the use here -- consider semi joining with input streams
        # super().validate_inputs(*self.input_streams)
        return PodNodeStream(self, *self.upstreams)  # type: ignore[return-value]

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        results = self.result_database.get_all_records(
            self.record_path, record_id_column=constants.PACKET_RECORD_ID
        )

        if self.pipeline_database is None:
            raise ValueError(
                "Pipeline database is not configured, cannot retrieve tag info"
            )
        taginfo = self.pipeline_database.get_all_records(
            self.pipeline_path,
        )

        if results is None or taginfo is None:
            return None

        # hack - use polars for join as it can deal with complex data type
        # TODO: convert the entire load logic to use polars with lazy evaluation

        joined_info = (
            pl.DataFrame(taginfo)
            .join(pl.DataFrame(results), on=constants.PACKET_RECORD_ID, how="inner")
            .to_arrow()
        )

        # joined_info = taginfo.join(
        #     results,
        #     constants.PACKET_RECORD_ID,
        #     join_type="inner",
        # )

        if not include_system_columns:
            system_columns = [
                c
                for c in joined_info.column_names
                if c.startswith(constants.META_PREFIX)
                or c.startswith(constants.DATAGRAM_PREFIX)
            ]
            joined_info = joined_info.drop(system_columns)
        return joined_info
