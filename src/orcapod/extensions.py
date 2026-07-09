"""Normalized extension registration API for orcapod (ITL-473).

Third-party extension packages expose a module-level singleton that implements
``OrcapodExtension`` and register it via ``op.register_extension()``.

Example:
    >>> import orcapod as op
    >>> from orcapod_extension_spikeinterface import spikeinterface_extension
    >>> op.register_extension(spikeinterface_extension)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from orcapod.contexts import DataContext

__all__ = ["OrcapodExtension", "register_extension"]


@runtime_checkable
class OrcapodExtension(Protocol):
    """Protocol that all orcapod extension objects must implement.

    Extension packages expose a module-level singleton instance of a class
    that implements this protocol and register it via ``op.register_extension()``.

    Example:
        >>> import orcapod as op
        >>> from orcapod_extension_spikeinterface import spikeinterface_extension
        >>> op.register_extension(spikeinterface_extension)

    Attributes:
        name: Short identifier used in log messages (e.g. ``"spikeinterface"``).
    """

    name: str

    def register(self, context: DataContext) -> None:
        """Register this extension's types into ``context``.

        ``context`` is always a concrete ``DataContext`` — never ``None``.
        Context resolution (default vs. explicit) is handled by
        ``register_extension`` before this method is called.

        Args:
            context: Target ``DataContext`` to register types into.
        """
        ...


def register_extension(
    extension: OrcapodExtension,
    context: DataContext | None = None,
) -> None:
    """Register an extension into a data context.

    Resolves ``context`` to the default context when ``None``, then delegates
    to ``extension.register(context)`` with the concrete context. Extensions
    never receive ``None`` for ``context``.

    Args:
        extension: An object implementing ``OrcapodExtension``.
        context: Target ``DataContext``. Resolves to the default context
            if ``None``.

    Raises:
        TypeError: If ``extension`` does not implement ``OrcapodExtension``
            (i.e. is missing ``name`` or ``register``).

    Example:
        >>> import orcapod as op
        >>> from orcapod_extension_spikeinterface import spikeinterface_extension
        >>> op.register_extension(spikeinterface_extension)
        >>> # or against a specific context:
        >>> op.register_extension(spikeinterface_extension, context=my_context)
    """
    if not isinstance(extension, OrcapodExtension):
        raise TypeError(
            f"extension must implement OrcapodExtension "
            f"(requires 'name: str' and 'register(context) -> None'); "
            f"got {type(extension)!r}"
        )
    from orcapod.contexts import get_default_context

    if context is None:
        context = get_default_context()
    extension.register(context)
