"""SpikeInterface LogicalType and handler for orcapod (ITL-459).

`LogicalSIRecording` maps `spikeinterface.core.BaseRecording` ↔ Arrow
`large_string` using SpikeInterface's own `to_dict(recursive=True,
include_annotations=True, include_properties=False)` JSON dump (encoded
via `SIJsonEncoder`) as the storage envelope. `SIRecordingHandler` hashes
the same JSON bytes via SHA-256 for content identity.

This module requires the optional `spikeinterface` extras group:
`pip install orcapod[spikeinterface]`

Register SI types into the default orcapod context before using them in
pods: call `register_spikeinterface_types()` once at startup.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol

try:
    from spikeinterface.core import BaseRecording
except ImportError as _exc:
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod[spikeinterface]"
    ) from _exc

logger = logging.getLogger(__name__)


class LogicalSIRecording(BaseLogicalType):
    """Logical type for `spikeinterface.core.BaseRecording`.

    Stores `BaseRecording` instances as Arrow `large_string` columns
    tagged with extension name `"spikeinterface.recording"`. The stored
    value is SpikeInterface's own `to_dict(recursive=True,
    include_annotations=True, include_properties=False)` output, encoded
    via `SIJsonEncoder`. Loading reconstructs the recording via
    `spikeinterface.core.load(dict)`.

    Only recordings whose `check_serializability("json")` returns `True`
    are accepted. Lazy recordings built on top of file-backed data (zarr,
    binary folder, etc.) qualify. In-memory `NumpyRecording` objects do
    not and raise `ValueError` with clear save instructions.

    Example:
        >>> import tempfile, numpy as np
        >>> import spikeinterface.core as si
        >>> from orcapod.extension_types.spikeinterface_types import LogicalSIRecording
        >>> lt = LogicalSIRecording()
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     rec = si.NumpyRecording([np.zeros((100, 4), dtype="float32")], 30000)
        ...     saved = rec.save_to_folder(tmp + "/rec")
        ...     storage = lt.python_to_storage(saved)
        ...     recovered = lt.storage_to_python(storage)
        ...     saved.get_traces(segment_index=0).shape == recovered.get_traces(segment_index=0).shape
        True
    """

    _arrow_ext_class = make_arrow_extension_type("spikeinterface.recording", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("spikeinterface.recording", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "spikeinterface.recording"
    python_type: type = BaseRecording

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for `BaseRecording`.

        Returns:
            A `pa.ExtensionType` with extension name
            `"spikeinterface.recording"` and storage type `pa.large_string()`.
        """
        if LogicalSIRecording._arrow_ext is None:
            LogicalSIRecording._arrow_ext = LogicalSIRecording._arrow_ext_class()
        return LogicalSIRecording._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for `BaseRecording`.

        Returns:
            A `pl.BaseExtension` registered under `"spikeinterface.recording"`.
        """
        if LogicalSIRecording._polars_ext is None:
            LogicalSIRecording._polars_ext = LogicalSIRecording._polars_ext_class()
        return LogicalSIRecording._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Serialise a `BaseRecording` to its JSON storage representation.

        Args:
            value: A `BaseRecording` instance whose
                `check_serializability("json")` returns `True`.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string produced by `recording.to_dict(recursive=True,
            include_annotations=True, include_properties=False)` encoded
            via `SIJsonEncoder`.

        Raises:
            ValueError: If the recording is not JSON-serialisable (e.g. an
                in-memory `NumpyRecording`).
        """
        if not value.check_serializability("json"):
            raise ValueError(
                "This BaseRecording is not JSON-serializable and cannot be stored "
                "by orcapod. This typically means it holds data in memory (e.g. "
                "NumpyRecording). Lazy recordings built on top of file-backed data "
                "(zarr, binary folder, etc.) are fine and do not need to be "
                "materialized first. If your recording is in-memory, call "
                "recording.save_to_zarr(path) or recording.save_to_folder(path) "
                "first, then pass the returned extractor to the pod."
            )
        from spikeinterface.core.core_tools import SIJsonEncoder
        return json.dumps(
            value.to_dict(
                include_annotations=True,
                include_properties=False,
                recursive=True,
            ),
            cls=SIJsonEncoder,
        )

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> BaseRecording:
        """Reconstruct a `BaseRecording` from its JSON storage string.

        Args:
            storage_value: A JSON string as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A `BaseRecording` instance reconstructed via
            `spikeinterface.core.load_extractor`.

        Raises:
            ValueError: If `storage_value` is not valid JSON.
            FileNotFoundError: If the backing zarr/folder no longer exists
                (raised by SpikeInterface, propagated as-is).
        """
        from spikeinterface.core import load as si_load
        try:
            si_dict = json.loads(storage_value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(
                f"LogicalSIRecording: cannot deserialise storage value "
                f"{storage_value!r}; expected a JSON string."
            ) from exc
        return si_load(si_dict)


class SIRecordingHandler:
    """Semantic hash handler for `spikeinterface.core.BaseRecording`.

    Computes a SHA-256 `ContentHash` of the JSON bytes produced by
    `recording.to_dict(recursive=True, include_annotations=True,
    include_properties=False)` encoded via `SIJsonEncoder`. This is
    identical to the bytes that `LogicalSIRecording` stores in Arrow, so
    hash input and storage representation are always consistent.

    The `hasher` argument is accepted for protocol conformance but not used —
    hashing is done directly via `hashlib.sha256` to avoid overhead.
    """

    def handle(self, obj: Any, hasher: SemanticHasherProtocol | None) -> ContentHash:
        """Return a SHA-256 `ContentHash` of the recording's JSON dump.

        Args:
            obj: A `BaseRecording` instance.
            hasher: Accepted for protocol conformance; not used.

        Returns:
            A `ContentHash` with `method="sha256"` and digest equal to the
            SHA-256 of the JSON bytes from `to_dict(recursive=True,
            include_annotations=True, include_properties=False)` encoded
            via `SIJsonEncoder`.

        Raises:
            TypeError: If `obj` is not a `BaseRecording`.
            ValueError: If the recording is not JSON-serialisable (in-memory).
        """
        if not isinstance(obj, BaseRecording):
            raise TypeError(
                f"SIRecordingHandler: expected BaseRecording, got {type(obj)!r}"
            )
        if not obj.check_serializability("json"):
            raise ValueError(
                "Cannot hash an in-memory BaseRecording "
                "(check_serializability('json') is False). "
                "Save it to disk first with save_to_zarr() or save_to_folder()."
            )
        # TODO(ITL-467): phase 2 — also hash backing source directory contents
        from spikeinterface.core.core_tools import SIJsonEncoder
        json_bytes = json.dumps(
            obj.to_dict(include_annotations=True, include_properties=False, recursive=True),
            cls=SIJsonEncoder,
        ).encode()
        logger.debug("SIRecordingHandler: hashing %d JSON bytes", len(json_bytes))
        return ContentHash(
            method="sha256",
            digest=hashlib.sha256(json_bytes).digest(),
        )


def register_spikeinterface_types(context: Any = None) -> None:
    """Register SpikeInterface LogicalTypes into an orcapod `DataContext`.

    Call this once at startup, before any pods that use SpikeInterface types
    are declared or executed. If `context` is `None`, the default context
    (from `orcapod.contexts.get_default_context()`) is used.

    Args:
        context: A `DataContext` instance, or `None` to use the default.

    Example:
        >>> from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
        >>> register_spikeinterface_types()  # registers into the default context
    """
    if context is None:
        from orcapod.contexts import get_default_context
        context = get_default_context()

    lt = LogicalSIRecording()
    context.type_converter._logical_type_registry.register_logical_type(lt)
    context.semantic_hasher.type_handler_registry.register(BaseRecording, SIRecordingHandler())
    logger.debug(
        "register_spikeinterface_types: registered LogicalSIRecording and SIRecordingHandler"
    )
