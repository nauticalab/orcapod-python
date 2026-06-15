"""Schema-walking hook for extension type auto-registration.

Call ``register_discovered_extensions(registry, schema)`` on any Arrow schema
that may contain extension types. It is a no-op when the schema contains no
extension types or when *registry* is ``None``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from orcapod.extension_types.registry import LogicalTypeRegistry
from orcapod.extension_types.schema_walker import walk_schema

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


def register_discovered_extensions(
    registry: LogicalTypeRegistry | None,
    schema: pa.Schema,
) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively to discover all Arrow extension types at any
    nesting depth. For each discovered type, delegates to
    ``registry.ensure_extension_type``.

    Already-registered types are detected and skipped inside the registry —
    this function itself is stateless beyond the registry it operates on.

    Args:
        registry: The ``LogicalTypeRegistry`` to use for lookup and registration.
            If ``None``, this call is a no-op — no extension types will be
            registered. Callers that want auto-registration must supply a registry
            explicitly; the typical source is
            ``data_context.logical_type_registry``.
        schema: The Arrow schema to inspect. May contain no extension types,
            in which case this call is a no-op.

    Raises:
        ValueError: Propagated from the registry if an extension type's metadata
            has no registered factory or is malformed.
    """
    if registry is None:
        logger.debug("register_discovered_extensions: no registry provided, skipping")
        return

    found = walk_schema(schema)
    if not found:
        logger.debug("register_discovered_extensions: no extension types in schema")
        return
    logger.debug(
        "register_discovered_extensions: found %d extension type(s) in schema: %s",
        len(found),
        [info.extension_name for info in found],
    )
    for info in found:
        registry.ensure_extension_type(
            info.extension_name,
            info.extension_metadata,
            info.storage_type,
        )
