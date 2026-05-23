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
