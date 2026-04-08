class InputValidationError(Exception):
    """
    Exception raised when the inputs are not valid.
    This is used to indicate that the inputs do not meet the requirements of the operator.
    """


class DuplicateTagError(ValueError):
    """Raised when duplicate tag values are found and skip_duplicates=False"""

    pass


class PacketFunctionUnavailableError(RuntimeError):
    """Raised when a packet function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original packet function is not available. Only cached results can
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
