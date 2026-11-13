"""What storing a torch tensor converts, and the warnings that report it (ITL-871).

Storage does not keep a tensor's device or its autograd history: a tensor on a
GPU (or another non-CPU device) is stored as a CPU tensor, and one computed from
tensors that track gradients (it has a ``grad_fn``) is stored as a leaf.
``requires_grad`` itself is stored, so a leaf tensor that tracks gradients is not
converted. The caller's tensor is never modified. The code
that knows where a value came from (a function pod's output, a source's column)
reports the conversion with ``orcapod.errors.ValueConversionWarning``, once per
(function or source, output or column, conversion).

This module never imports torch: a value is a tensor when a class in its MRO is
``torch.Tensor`` by qualified name, and such a value can only exist once torch has
been imported.
"""

from __future__ import annotations

import sys
import warnings
from collections.abc import Iterable, Mapping
from typing import Any

from orcapod.errors import ValueConversionWarning
from orcapod.logical_types.user_class_identity import class_kind, stored_field_names
from orcapod.utils.deferred_types import qualified_name

_TENSOR_CLASS = "torch.Tensor"

_HAS_HISTORY = "with autograd history (a grad_fn)"


def _is_tensor_type(cls: type) -> bool:
    """Whether ``cls`` is ``torch.Tensor`` or a subclass, by qualified name.

    Not cached: a cache would keep every class it sees alive, and the MRO walk
    is short.
    """
    return any(qualified_name(base) == _TENSOR_CLASS for base in cls.__mro__)


def tensor_conversions(value: Any) -> tuple[str, ...]:
    """Describe what storing ``value`` converts in the torch tensors it holds.

    Looks at ``value`` itself and, recursively, at the items of lists and tuples,
    the values of dicts and the stored fields of dataclass and pydantic instances.

    Args:
        value: Any value about to be stored.

    Returns:
        The conversions, in first-seen order without repeats:
        ``"on device 'cuda:0'"`` for a tensor that is not on the CPU, and
        ``"with autograd history (a grad_fn)"`` for one that is not a leaf.
        Empty when storing converts nothing. A ``meta`` tensor is not
        listed: it holds no data and storing it is refused.
    """
    found: dict[str, None] = {}
    _collect(value, found)
    return tuple(found)


def _collect(value: Any, found: dict[str, None]) -> None:
    """Add the conversions of ``value`` and the values it contains to ``found``."""
    if isinstance(value, (list, tuple)):
        for item in value:
            _collect(item, found)
    elif isinstance(value, dict):
        for item in value.values():
            _collect(item, found)
    elif _is_tensor_type(type(value)):
        device = value.device
        if device.type not in ("cpu", "meta"):
            found[f"on device '{device}'"] = None
        if value.grad_fn is not None:
            found[_HAS_HISTORY] = None
    elif type(value).__module__ != "builtins" and class_kind(type(value)) is not None:
        for name in stored_field_names(type(value)):
            _collect(getattr(value, name), found)


def describe(conversions: Iterable[str]) -> str:
    """Join conversions into one phrase (``"on device 'cuda:0' with autograd ..."``)."""
    return " ".join(conversions)


def _error_filter_active(message: str, module: str, lineno: int) -> bool:
    """Whether the active warning filters turn this warning into an error.

    Reads ``warnings.filters`` as Python does: the first filter whose category,
    message pattern, module pattern and line number match decides.

    Args:
        message: The warning text.
        module: Name of the module the warning is attributed to.
        lineno: Line number the warning is attributed to.

    Returns:
        True when that filter's action is ``"error"``.
    """
    for action, text, category, module_pattern, line in warnings.filters:
        if (
            issubclass(ValueConversionWarning, category)
            and (text is None or text.match(message))
            and (module_pattern is None or module_pattern.match(module))
            and line in (0, lineno)
        ):
            return action == "error"
    return False


def warn_once(
    issued: set[tuple[str, str]],
    scope: str,
    conversions: tuple[str, ...],
    message: str,
) -> None:
    """Issue ``ValueConversionWarning`` unless every conversion was already reported.

    The record is updated only after ``warnings.warn`` returns, so when the warning
    is escalated to an error every conversion raises, on every row. A conversion
    that was already reported still raises once an error filter is in place. The
    record is needed besides Python's own filters: the warning is reported once
    per owner also under an ``always`` filter.

    Args:
        issued: The ``(scope, conversion)`` pairs already reported; updated in place.
            Its owner (a function, a source) decides how long it lives.
        scope: What the value belongs to within the owner, such as an output key.
        conversions: The conversions of the value (``tensor_conversions``).
        message: The warning text.
    """
    if all((scope, c) in issued for c in conversions):
        # The frame the warning is attributed to (``stacklevel=3`` below).
        frame = sys._getframe(2)  # pyright: ignore[reportPrivateUsage]  # as the warnings module does
        module = frame.f_globals.get("__name__", "<string>")
        if not _error_filter_active(message, module, frame.f_lineno):
            return
    warnings.warn(message, ValueConversionWarning, stacklevel=3)
    issued.update((scope, c) for c in conversions)


def warn_source_tensor_conversions(
    source_id: str, rows: Iterable[Mapping[str, Any]]
) -> None:
    """Warn once per column of a Python-value source whose tensors are converted.

    Args:
        source_id: The source's id, named in the warning.
        rows: The rows the source was built from.
    """
    total = 0
    counts: dict[str, int] = {}
    kinds: dict[str, dict[str, None]] = {}
    for row in rows:
        total += 1
        for column, value in row.items():
            conversions = tensor_conversions(value)
            if conversions:
                counts[column] = counts.get(column, 0) + 1
                kinds.setdefault(column, {}).update(dict.fromkeys(conversions))
    for column, count in counts.items():
        warnings.warn(
            f"Source {source_id!r} column {column!r}: {count} of {total} values hold "
            f"a torch tensor {describe(kinds[column])}; Orcapod stored CPU tensors "
            "without autograd history, and the given tensors are unchanged. Pass "
            "value.detach().cpu() to avoid this warning.",
            ValueConversionWarning,
            stacklevel=3,
        )
