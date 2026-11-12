from collections.abc import Mapping
from typing import Any, Self


class InputValidationError(Exception):
    """
    Exception raised when the inputs are not valid.
    This is used to indicate that the inputs do not meet the requirements of the operator.
    """


class SchemaInconsistencyError(InputValidationError):
    """Raised when a data batch has a schema that is incompatible with the expected schema.

    This can happen in two situations:

    - A fetched batch is missing a field declared in ``tag_schema`` or ``data_schema``,
      or one of those fields has a different type than declared.
    - Consecutive batches from a ``PollingSource`` have different column sets or
      column types (schema drift between polls).

    ``SchemaInconsistencyError`` is a subclass of ``InputValidationError`` so existing
    ``except InputValidationError`` handlers continue to work.
    """


class DuplicateTagError(ValueError):
    """Raised when duplicate tag values are found and skip_duplicates=False"""

    pass


class UnsupportedValueError(ValueError):
    """A value cannot be stored by the logical type of its column.

    Raised at write time, for example for a masked or object-dtype NumPy array.
    The type converter prefixes the message with the column name.
    """


class DataFunctionUnavailableError(RuntimeError):
    """Raised when a data function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original data function is not available. Only cached results can
    be accessed.
    """


class FieldNotResolvableError(LookupError):
    """
    Raised when a source cannot resolve a field value for a given record ID.

    This may happen because:
    - The source is transient or randomly generated (no stable backing data)
    - The record ID is not found in the source
    - The field name does not exist in the source schema
    - The source type does not support field resolution

    The exception message should describe which condition applies.
    """

    pass


class UnboundSourceError(RuntimeError):
    """Raised when a data-producing method is called on an unbound SourceNode.

    Occurs when ``iter_data()`` or ``as_table()`` is called on a ``SourceNode``
    that has not been bound to a concrete source in a ``PipelineJob``.
    """


class SourceSpecMismatchError(ValueError):
    """Raised when a concrete source's schema is incompatible with a SourceNode slot.

    The class name ``SourceSpecMismatchError`` is preserved for compatibility
    with any code that catches it by name.

    Contains the slot name and a description of the incompatible field(s).
    Raised at ``bind()`` time — schema mismatches are rejected before execution.
    """


class InconsistentSourceError(ValueError):
    """Raised when two source nodes in the same pipeline share a name but differ in schema.

    Source node names are identity-forming: the same name in a given pipeline
    must always refer to the same input slot (same schema).  If ``compile()``
    finds two source nodes with identical names but different schemas — which
    can happen when two ``RootSource`` objects share the same ``source_id`` but
    produce different column sets — it raises this error rather than silently
    renaming one of them.

    Resolution: assign distinct ``source_id`` values to the conflicting sources
    so each slot has a unique, stable identity.
    """


def _in_table(message: str, table: str | None) -> str:
    """Prefix ``message`` with the table it concerns, when known."""
    return message if table is None else f"table {table}: {message}"


class NullabilityRecordError(ValueError):
    """A table's nullability record cannot be read, or a schema cannot be recorded.

    The record is the list of non-nullable column paths Orcapod keeps in a SpiralDB
    table's key-value metadata (ITL-936). An unknown format or version, a missing chunk, a
    digest mismatch or a path the table does not have is an error, never ignored.

    Attributes:
        reason: What is wrong with the record.
        table: The table, as ``"<project>/<dataset>/<table>"``, added by the caller that
            knows it; ``None`` otherwise.
    """

    def __init__(self, reason: str, *, table: str | None = None) -> None:
        self.reason = reason
        self.table = table
        super().__init__(_in_table(f"nullability record: {reason}", table))


class NullabilityRecordMismatchError(ValueError):
    """A write declares different non-nullable columns than the table's record.

    The record is never widened, narrowed or rewritten by a write.

    Attributes:
        only_in_record: Paths the record lists that the write declares nullable.
        only_in_write: Paths the write declares non-nullable that the record does not list.
        table: The table, added by the caller; ``None`` otherwise.
    """

    def __init__(
        self,
        only_in_record: list[str],
        only_in_write: list[str],
        *,
        table: str | None = None,
    ) -> None:
        self.only_in_record = list(only_in_record)
        self.only_in_write = list(only_in_write)
        self.table = table
        parts = []
        if only_in_record:
            parts.append(
                f"recorded non-nullable but nullable in the write: {only_in_record}"
            )
        if only_in_write:
            parts.append(f"non-nullable in the write but not recorded: {only_in_write}")
        super().__init__(
            _in_table(
                "the write's nullability differs from the table's nullability "
                "record (" + "; ".join(parts) + ")",
                table,
            )
        )


class NullInNonNullableColumnError(ValueError):
    """A column declared or recorded non-nullable holds a null.

    Attributes:
        column: The column path, e.g. ``"quality.snr"`` or ``"spikes[]"``.
        table: The table, added by the caller; ``None`` otherwise.
    """

    def __init__(self, column: str, *, table: str | None = None) -> None:
        self.column = column
        self.table = table
        super().__init__(
            _in_table(f"column {column!r} is non-nullable but holds nulls", table)
        )


class CursorInvalidatedError(Exception):
    """Raised by a ``DynamicSourceProtocol`` implementation when the previous
    cursor is no longer valid and the source state must be rebuilt from scratch.

    This is a terminal condition for ``PollingSource``. Rows already emitted
    downstream cannot be retracted, so continuing would leave downstream
    operators with a corrupted view. ``PollingSource`` logs the error, calls
    ``impl.close()``, and re-raises so the caller receives the exception rather
    than a silent end-of-stream.

    If full-reset semantics are required, use a static source re-run instead
    of ``PollingSource``.
    """


class PipelineJobRequiredError(RuntimeError):
    """Raised when a lightweight blueprint node is asked to produce data.

    Blueprint nodes (``FunctionNode``, ``OperatorNode``) carry no database
    references.  Wrap the containing ``Pipeline`` in a ``PipelineJob`` to
    obtain executable ``FunctionJobNode`` / ``OperatorJobNode`` variants.
    """


class EmptyDataAccessError(Exception):
    """Raised when a payload-access method is called on an ``EmptyData`` instance.

    ``EmptyData`` carries no data payload. Any method that would access the
    underlying columns (``as_dict``, ``as_table``, ``keys``, ``schema``,
    ``arrow_schema``, ``identity_structure``) raises this exception instead of
    returning empty or ``None`` results.

    Attributes:
        empty_data: The ``EmptyData`` instance on which the method was called.
        method_name: The name of the method that was called.
    """

    def __init__(self, empty_data: object, method_name: str) -> None:
        self.empty_data = empty_data
        self.method_name = method_name
        super().__init__(
            f"Cannot call {method_name!r} on EmptyData — "
            "this instance carries no data payload. "
            "Check isinstance(data, EmptyData) before accessing the payload."
        )


class EmptyDataHashMissingError(Exception):
    """Raised when ``content_hash()`` is called on an ``EmptyData`` that has no cached hash.

    An ``EmptyData`` constructed without a ``cached_content_hash`` is in degraded
    mode (old pipeline DB row lacking the hash column). Any code path that tries
    to use the hash fails loudly here.

    Attributes:
        empty_data: The ``EmptyData`` instance on which ``content_hash()`` was called.
    """

    def __init__(self, empty_data: object) -> None:
        self.empty_data = empty_data
        super().__init__(
            "content_hash() called on EmptyData with no cached_content_hash. "
            "The pipeline DB row that produced this token is missing the stored "
            "hash column (OUTPUT_DATA_HASH_COL or INPUT_DATA_HASH_COL). "
            "Flow-through is unavailable for this row."
        )


class SchemaVersionError(Exception):
    """Raised when an old schema version is detected at a database path.

    Occurs when a table exists at the unversioned (v0) path but not at the
    expected versioned path, and the node has not opted in to tolerating
    the old schema via ``NodeConfig.ignore_schema``.

    To suppress this error, set ``node.node_config = NodeConfig(ignore_schema=("v0",))``
    and re-run. The node will recompute all results from scratch rather than
    reading from the v0 cache.
    """


class EphemeralResultMissingError(Exception):
    """Raised when a downstream pod has a cache miss for an ``EmptyData`` input.

    Indicates that the upstream ephemeral result is gone AND the downstream pod
    has not yet computed a result for the same input content hash. No recovery
    is possible — the information has been lost.

    Attributes:
        tag: The tag associated with the missing input.
        cached_content_hash: The cached content hash from the ``EmptyData``, or
            ``None`` if the ``EmptyData`` itself lacked a hash.
        node_identity_path: Identity path of the downstream node raising this error.
    """

    def __init__(
        self,
        tag: object,
        cached_content_hash: object,
        node_identity_path: tuple[str, ...],
        message: str,
    ) -> None:
        self.tag = tag
        self.cached_content_hash = cached_content_hash
        self.node_identity_path = node_identity_path
        super().__init__(
            f"{message} "
            f"node={node_identity_path!r} "
            f"cached_content_hash={cached_content_hash!r}"
        )


class StoreError(RuntimeError):
    """Base of the errors raised when the store could not be read or written.

    A store error always fails the run, whatever the orchestrator's
    ``error_policy``: ``error_policy="continue"`` skips rows whose *function*
    raised, never rows whose stored data could not be read or written. Catch
    ``StoreError`` to handle both ``StoreReadError`` and ``StoreWriteError``.

    The backend's own exception is chained as ``__cause__``.

    Attributes:
        node_label: Label of the node the failure belongs to, or ``None`` when it
            is not known or not tied to one node.
        tag: Tag values of the row being processed, or ``None`` when the failure
            is not tied to one row.
    """

    def __init__(
        self,
        *args: object,
        node_label: str | None = None,
        tag: Mapping[str, Any] | None = None,
    ) -> None:
        """Create the error.

        Args:
            *args: The exception arguments, as for any exception.
            node_label: Label of the node the failure belongs to, if known.
            tag: Tag values of the row being processed, if known.
        """
        self.node_label: str | None = node_label
        self.tag: dict[str, Any] | None = dict(tag) if tag is not None else None
        super().__init__(*args)

    @classmethod
    def in_group(cls, group: BaseExceptionGroup) -> list[Self]:
        """Return every error of this class in an exception group, nested ones included.

        Args:
            group: An exception group, such as the one an ``asyncio.TaskGroup``
                raises.

        Returns:
            The matching leaves in the order the group holds them.
        """
        found: list[Self] = []
        for exc in group.exceptions:
            if isinstance(exc, BaseExceptionGroup):
                found.extend(cls.in_group(exc))
            elif isinstance(exc, cls):
                found.append(exc)
        return found

    def add_context(
        self, node_label: str | None = None, tag: Mapping[str, Any] | None = None
    ) -> None:
        """Fill in the node label and row tag where the raising site did not know them.

        Called by the code that runs the node or the row as the error passes
        through it. Values that are already set are kept.

        Args:
            node_label: Label of the node the failure belongs to.
            tag: Tag values of the row being processed.
        """
        if self.node_label is None:
            self.node_label = node_label
        if self.tag is None and tag is not None:
            self.tag = dict(tag)


class StoreWriteError(StoreError):
    """Raised when a node's results or records could not be written to the store.

    The run stops at the first failed write. Rows stored before the failure are
    kept, and re-running the job does not recompute stored function results.
    A write that fails because the stored table cannot be read raises
    ``StoreReadError`` instead.

    Attributes:
        record_path: Path of the table the write was for, relative to the
            database view the writer holds. For a failed flush, the base path of
            the database view that was flushed.
        node_label: Label of the node whose write failed, or ``None`` when the
            failure is not tied to one node (a flush at the end of a run).
        tag: Tag values of the row being stored, or ``None`` when the write
            covered many rows (an operator's output), no row, or was a flush.
        flush: ``True`` when committing pending writes failed. Every view of a
            store shares one set of pending writes, so the write that failed may
            belong to an earlier row or another table than the one being stored.
    """

    def __init__(
        self,
        record_path: tuple[str, ...],
        node_label: str | None = None,
        tag: Mapping[str, Any] | None = None,
        *,
        flush: bool = False,
    ) -> None:
        """Create the error for one failed write.

        Args:
            record_path: Path of the table the write was for, or of the database
                view that was flushed.
            node_label: Label of the node whose write failed, if the write site knows it.
            tag: Tag values of the row being stored, if the write covered one row.
            flush: Whether committing pending writes failed.
        """
        self.record_path: tuple[str, ...] = tuple(record_path)
        self.flush: bool = flush
        super().__init__(
            self.record_path, node_label, tag, node_label=node_label, tag=tag
        )

    def add_context(
        self, node_label: str | None = None, tag: Mapping[str, Any] | None = None
    ) -> None:
        """Fill in the node label and row tag if the write site did not know them.

        A failed flush never takes a row tag: the pending write that failed may
        belong to another row.

        Args:
            node_label: Label of the node that owns the write.
            tag: Tag values of the row being stored.
        """
        super().add_context(node_label, None if self.flush else tag)

    def __str__(self) -> str:
        """Describe the failed write: node, row, table and the backend's reason."""
        where = f"node {self.node_label!r}" if self.node_label else "the pipeline"
        row = f" for row {self.tag!r}" if self.tag else ""
        cause = self.__cause__
        reason = (
            f": {type(cause).__name__}: {str(cause).rstrip('.')}"
            if cause is not None
            else ""
        )
        retry = (
            "The run was stopped. Rows stored before this failure are kept; "
            "re-running the job does not recompute stored function results."
        )
        if self.flush:
            node = f" for node {self.node_label!r}" if self.node_label else ""
            return (
                f"Could not commit pending writes to the store{node} "
                f"(store {'/'.join(self.record_path)!r}){reason}. The failing write "
                "may belong to an earlier row or another table of this store. " + retry
            )
        return (
            f"Could not write to the store for {where}{row} "
            f"(table {'/'.join(self.record_path)!r}){reason}. " + retry
        )


class StoreReadError(StoreError):
    """Raised when a stored table exists but could not be read.

    A table that does not exist reads as empty. A table that exists but cannot be
    read (a missing or damaged data file, a log the backend cannot load, columns
    whose stored data no longer matches the table schema) raises this error
    instead, so stored results never look dropped. Its rows are not recomputed
    and the table is not overwritten.

    Attributes:
        table: Location of the table that could not be read (a directory or a
            cloud URI).
        record_path: Path of the table, including the database view's prefix.
        node_label: Label of the node whose read failed, or ``None`` when the
            read did not happen while a node ran.
        tag: Tag values of the row being processed when the read failed, or
            ``None`` when the read was not for one row.
    """

    def __init__(self, table: str, record_path: tuple[str, ...]) -> None:
        """Create the error for one table that could not be read.

        Args:
            table: Location of the table (a directory or a cloud URI).
            record_path: Path of the table, including the database view's prefix.
        """
        self.table: str = table
        self.record_path: tuple[str, ...] = tuple(record_path)
        super().__init__(table, self.record_path)

    def __str__(self) -> str:
        """Describe the failed read: table, node, row and the backend's reason."""
        node = f" for node {self.node_label!r}" if self.node_label else ""
        row = f" for row {self.tag!r}" if self.tag else ""
        cause = self.__cause__
        reason = f": {type(cause).__name__}: {cause}" if cause is not None else ""
        return (
            f"Could not read the stored table at {self.table!r}{node}{row}{reason}. "
            "The table exists, so its rows are not treated as missing and the table "
            "was not overwritten."
        )
