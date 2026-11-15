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


class TypeIdentityError(ValueError):
    """Raised when stored values and a registered user class disagree on identity.

    A stored dataclass or pydantic column records its class's identity (name, kind and
    field types). This error is raised when the class found for that name has a
    different identity or a different Arrow storage type, or when the column was
    written by an Orcapod version that did not record the identity. The message names
    the column where it is known, the class, and both shapes.
    """


class RecursiveTypeError(TypeError):
    """Raised when a class refers back to itself through its fields.

    Arrow cannot store a recursive struct, so a dataclass or pydantic model whose
    fields reach the class again (``next: Node | None``) cannot be stored. The message
    names the field path, e.g. ``Node.next -> Node``.
    """


class SourceTooLargeError(ValueError):
    """Raised when a read of a source would return more rows than its ``max_rows``.

    The message gives the number of rows found or expected, the cap, and the ways to
    read fewer rows or lift the cap.
    """


class DuplicateTagError(ValueError):
    """Raised when duplicate tag values are found and skip_duplicates=False"""

    pass


class CanonicalOrderError(ValueError):
    """Raised when a dict or set value has no canonical order.

    Dict entries and set elements are stored in canonical order (ITL-905), so dict keys
    and set elements must be totally ordered and unique. Raised for a NaN key or element,
    values that cannot be compared, two keys or elements with the same stored value, or
    a null entry in a stored key/value list.
    """


class UnsupportedValueError(ValueError):
    """A value cannot be stored by the logical type of its column.

    Raised at write time, for example for a masked or object-dtype NumPy array.
    The type converter prefixes the message with the column name.
    """


class MissingOptionalDependencyError(ImportError):
    """A stored value needs an optional dependency that is not installed.

    Raised when a column's Arrow extension type belongs to a value type that is
    registered lazily (for example ``orcapod.torch.tensor``, which needs PyTorch)
    and its library cannot be imported. The message names the extension type and
    the missing module.
    """


class DataFunctionUnavailableError(RuntimeError):
    """Raised when a data function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original data function is not available. Only cached results can
    be accessed.
    """


class ValueConversionWarning(UserWarning):
    """Orcapod accepted a value and stored a converted form of it.

    Example: a torch tensor on a GPU or with autograd history (a ``grad_fn``), which
    is stored as a CPU tensor without that history. The message names who gave the value (a function pod's output, a source's
    column), what was converted and how to avoid the conversion. A value passed in by
    the caller is never modified. The code that issues the warning issues it once per
    (function or source, output or column, conversion), however many rows a run has,
    and does not rely on Python's warning registry to do so. To make it an error, call
    ``warnings.simplefilter("error", ValueConversionWarning)``: every conversion then
    raises, on every row, and in a pipeline that row fails. Python's ``-W`` option
    and ``PYTHONWARNINGS`` cannot name this class: they are read before installed
    packages can be imported.
    """


def type_text(annotation: object) -> str:
    """Return a short, readable name for a type or annotation, for error messages.

    Builtin classes are shown bare (``int``); other classes with their module
    (``numpy.int8``, ``decimal.Decimal``); generic aliases and unions as Python prints
    them (``list[int]``, ``int | None``). Never use this for hashing or identity.

    Args:
        annotation: A class or a typing annotation.

    Returns:
        The display name.
    """
    if isinstance(annotation, type) and not hasattr(annotation, "__origin__"):
        module = annotation.__module__
        if module in ("builtins", "__main__"):
            return annotation.__qualname__
        return f"{module}.{annotation.__qualname__}"
    return str(annotation).replace("typing.", "")


class OutputTypeError(TypeError, ValueError):
    """A function returned a value that does not match its declared output type.

    Orcapod rejects incompatible returned values (ITL-895): pyarrow would
    otherwise truncate a ``float`` returned for an output declared ``int``. The data
    function raises this before anything is stored or hashed, so a pipeline treats it
    like any other failure of the function: a row failure under ``error_policy``,
    reported to the observer's ``on_data_crash``.

    It subclasses both ``TypeError`` (a value of the wrong type) and ``ValueError``
    (it also wraps conversion failures of a value of the right type, such as an
    integer out of range for the declared width or a naive ``datetime`` under the
    strict timezone policy, which were ``ValueError`` before).

    Attributes:
        function_name: Name of the function pod's function.
        output_key: Name of the output column.
        declared_type: The declared type of that output.
        value: The value the function returned for that output.
    """

    def __init__(
        self,
        function_name: str,
        output_key: str,
        declared_type: object,
        value: object,
        detail: str | None = None,
        hint: str | None = None,
    ) -> None:
        """Create the error for one output of one call.

        Args:
            function_name: Name of the function pod's function.
            output_key: Name of the output column.
            declared_type: The declared type of that output.
            value: The value the function returned for that output.
            detail: Where inside the value the mismatch is, or why the conversion
                failed, when that is not the value as a whole.
            hint: What the function should return instead, when there is a specific
                fix (``Return a float instead: 0.0 or float(value)``).
        """
        self.function_name: str = function_name
        self.output_key: str = output_key
        self.declared_type: object = declared_type
        self.value: object = value
        self.detail: str | None = detail
        self.hint: str | None = hint
        where = f": {detail}" if detail else ""
        fix = f". {hint}." if hint else "; convert the value in the function."
        # Types only, never the value: pipelines log this message, and a returned value
        # can be large or sensitive. The value itself stays on ``self.value``.
        super().__init__(
            f"Function {function_name!r} returned {type_text(type(value))} "
            + f"for output {output_key!r}, which is declared "
            + f"{type_text(declared_type)}{where}. The output does not conform{fix}"
        )

    def __reduce__(self) -> tuple[object, ...]:
        """Pickle with the constructor arguments (executors send errors across processes)."""
        return (
            type(self),
            (
                self.function_name,
                self.output_key,
                self.declared_type,
                self.value,
                self.detail,
                self.hint,
            ),
        )


class UnboundSourceError(RuntimeError):
    """Raised when a data-producing method is called on an unbound SourceNode.

    Occurs when ``iter_data()`` or ``as_table()`` is called on a ``SourceNode``
    that has not been bound to a concrete source in a ``PipelineJob``.
    """


class SourceSpecMismatchError(InputValidationError, ValueError):
    """Raised when a concrete source's schema differs from a source slot's declared schema.

    A source fits a slot when its tag and data columns have the slot's names, roles and
    Python types (ITL-1112): those are what every downstream pipeline hash, and so every
    stored path, is built from. The message names the slot and the column, and for a type
    difference both types. Raised by ``PipelineJob.bind(sources=...)`` and by assigning a
    source node's ``bound_source``, before anything is bound.
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


class RowFailuresWarning(UserWarning):
    """Issued once at the end of a job run in which rows failed.

    With the default ``error_policy="continue"``, a row whose function raises is skipped
    and the run goes on. The warning names each node with failed rows, how many failed out
    of how many, and the first error, and says how to print its full traceback. The run's
    ``RunSummary`` (returned by ``PipelineJob.run``, kept as ``job.last_run``) holds the
    same counts.
    """


class SourceCheckFailedWarning(UserWarning):
    """A source could not be checked for new data at the start of a run (ITL-1102).

    Every run (``job.run()``, ``run_async()``, a job node's ``run()``) first asks each
    source feeding it whether its data changed (``check_for_new_data``). A check that
    raises does not fail the run: this warning is issued once per source and run, naming
    the source and the error, the source keeps the data and modified time it had, and
    the run proceeds with that data. The run's ``RunSummary`` lists the source in
    ``unchecked_sources``. A later failure to read the source raises as it always has.
    """


class StoreContentWarning(UserWarning):
    """A loaded node's store holds another number of rows than the saved run recorded.

    A job file may record, in its optional ``run.expected_rows`` section, how many rows
    the store held under each node's run key when the job was saved. Reading a loaded
    node always returns what the attached store holds, which is the source of truth;
    when that differs from the recorded number, this warning is issued once per node.
    The store may not be the one the run wrote, or it may have changed since. A file
    without the section, or without a node's entry, is not checked.
    """


class ProvisionalHashWarning(UserWarning):
    """A value type was used whose value hash and stored identity are provisional.

    The library value types (``numpy.ndarray``, ``torch.Tensor``, pandas and polars
    ``DataFrame`` and ``Series``) are hashed and stored with formats that may still
    change in v0.4. A result computed now from such a value may then get another hash,
    so it is computed again after the upgrade: nothing stored is lost or wrong. The
    warning is issued once per type per process, on whichever use comes first: a value
    hashed, a column converted to or from Arrow, a function pod annotated with the type,
    a stored column read. Which types are provisional is declared in the ``provisional``
    list of ``orcapod/contexts/logical_types.json``.

    To silence it, call ``warnings.filterwarnings("ignore",
    category=ProvisionalHashWarning)`` after importing Orcapod. Python's ``-W`` option
    and ``PYTHONWARNINGS`` cannot name this class: they are read before installed
    packages can be imported.
    """


class SemiJoinIgnoredColumnWarning(UserWarning):
    """``SemiJoin`` inputs share a column name that is not a match key.

    ``SemiJoin`` matches on the columns that are tag columns of both inputs and on
    nothing else (ITL-1088). A name the inputs share as data columns, or as a tag on
    one side and a data column on the other, is ignored: it does not take part in
    matching, and the right input's data is never read. This warning names such
    columns, once per operator and set of columns, because before v0.3 they were
    match keys. The behaviour for them is provisional in v0.3 and may become an
    error.

    To silence it, rename or drop the column on the right input before the semi join
    (``map_data``, ``drop_data_columns``, ``select_data_columns``, or ``map_tags``
    for a tag), or call ``warnings.filterwarnings("ignore",
    category=SemiJoinIgnoredColumnWarning)``.
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


class TableListingNotSupportedError(NotImplementedError):
    """Raised by ``list_table_paths`` when a store cannot enumerate its tables.

    Some stores can read and write a table at a known path but have no way to list
    the tables they hold (for example a Delta store on cloud storage for which no
    file system package is installed). Orcapod then cannot look for tables written
    by an earlier version; it says so in a warning and carries on.
    """


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


class SideEffectLogDataContextError(StoreError):
    """Raised when a side-effecting node finds completions logged under another data context.

    A node whose run is an external action finds what it already did by keys hashed
    under its data context: a side-effect pod in its invocation log, a function pod
    that takes the invocation context (``ctx_arg``) in its function tables.
    Completions stored under another data context cannot be matched with current
    inputs: tables written by Orcapod up to v0.2 (an older table layout, data context
    ``std:v0.1:default``), or rows of another data context in the current tables
    (ITL-944). Running the node would perform every side effect again. Unlike a
    computed result, a side effect cannot be taken back, so the node raises this
    error before it does anything and leaves the tables as they are. Other function,
    operator and source tables do not fail this way: there, such rows are reported,
    skipped and computed again.

    Tables written by Orcapod up to v0.2 are found by name: every table at an older
    layout stored under the node's function or pod name in the job counts,
    whatever hashes its path holds (ITL-954). A table of another function or pod
    with the same name in the same job, or of another node of the same function,
    therefore raises this error too; ``record_paths`` names every table found.

    A fresh store is not a safe way around this error: it holds no completions, so on
    it the node performs every side effect again. Run the node on a fresh store only
    when a repeated delivery is acceptable; otherwise keep using the Orcapod version
    that wrote the tables. Migrating stored rows to the current data context is
    planned.

    Attributes:
        record_paths: Paths, from the root of the store, of the tables holding such
            completions.
        record_path: The first of ``record_paths``.
        found_context: Data context key of the completions that do not match.
        current_context: Data context key of the node.
        node_label: Label of the node.
    """

    def __init__(
        self,
        record_paths: tuple[tuple[str, ...], ...],
        found_context: str,
        current_context: str,
        node_label: str | None = None,
    ) -> None:
        """Create the error for the tables of one node.

        Args:
            record_paths: Paths of the tables holding completions that do not match.
            found_context: Data context key of those completions.
            current_context: Data context key of the node.
            node_label: Label of the node, if known.
        """
        self.record_paths: tuple[tuple[str, ...], ...] = tuple(
            tuple(path) for path in record_paths
        )
        self.record_path: tuple[str, ...] = self.record_paths[0]
        self.found_context: str = found_context
        self.current_context: str = current_context
        super().__init__(
            self.record_paths, found_context, current_context, node_label=node_label
        )

    def __str__(self) -> str:
        """Describe the tables, both data contexts and the remedy."""
        node = f"Node {self.node_label!r}" if self.node_label else "This node"
        tables = ", ".join(repr("/".join(path)) for path in self.record_paths)
        return (
            f"{node} performs side effects, and its stored completions were logged "
            f"under the data context {self.found_context!r} (in {tables}), but the "
            f"node runs under {self.current_context!r}. They cannot be matched with "
            "current inputs, so running the node would perform every side effect "
            "again; nothing was delivered or written. Keep using the Orcapod "
            "version that wrote these tables; they are left in place and can be "
            "migrated to the current data context later. A fresh store holds no "
            "completions, so on it the node performs every side effect again: use "
            "one only when a repeated delivery is acceptable."
        )
