class InputValidationError(Exception):
    """
    Exception raised when the inputs are not valid.
    This is used to indicate that the inputs do not meet the requirements of the operator.
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
    """Raised when a data-producing method is called on an unbound SourceSpec.

    Occurs when ``iter_data()`` or ``as_table()`` is called on a ``SourceSpec``
    that has not been bound to a concrete source in a ``PipelineJob``.
    """


class SourceSpecMismatchError(ValueError):
    """Raised when a concrete source's schema is incompatible with a SourceSpec.

    Contains the spec name and a description of the incompatible field(s).
    Raised at ``bind()`` time — schema mismatches are rejected before execution.
    """


class CursorInvalidatedError(Exception):
    """Raised by a ``DynamicSourceProtocol`` implementation when the previous
    cursor is no longer valid and the source state must be rebuilt from scratch.

    This is a terminal condition for ``PollingSource``. Rows already emitted
    downstream cannot be retracted, so continuing would leave downstream
    operators with a corrupted view. ``PollingSource`` catches this, logs a
    clear error, closes its output channel cleanly, and calls ``close()``.

    If full-reset semantics are required, use a static source re-run instead
    of ``PollingSource``.
    """
