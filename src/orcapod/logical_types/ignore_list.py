"""Value types whose hash ignores a configurable list of properties (ITL-981).

A value from a library (a NumPy array, a tensor, a data frame) has properties
beyond its content: memory order, byte order, column order. By default every
property the storage keeps is part of the value's hash. A type declares the
closed set of property names its hash can ignore (its *options*), and a data
context configures, per type, which of them are ignored (the *ignore list*).

The ignore list is saved in the Arrow extension metadata, so it is part of the
logical type identifier: two lists are two logical types. The metadata has one
shape for every such type, canonical JSON with sorted keys:

    {"ignore":[],"version":1}
    {"ignore":["byte_order","memory_order"],"version":1}

``ignore`` holds the names sorted by code point, each once; it is present also
when empty. ``version`` is the version of the type's stored layout and hashing.

The list is not part of the value hash preimage. A handler writes every optional
property into its preimage as a field that is always present, with the real
value, or with a fixed canonical value when the property is ignored. So values
that differ only in ignored properties hash equal.

A type is built from two base classes here: ``IgnoreListLogicalType`` (the
Arrow and Polars extension types) and ``IgnoreListHandler`` (the value hash).
A data context has exactly one list per type; a column stored under another
list is refused on read with ``LogicalTypeIdentifierError``.
"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Callable, Collection, Iterable
from typing import TYPE_CHECKING, Any, ClassVar, cast

from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.identifier import (
    LogicalTypeIdentifier,
    LogicalTypeIdentifierError,
)
from orcapod.types import ContentHash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

_OPTION_NAME = re.compile(r"[a-z][a-z0-9_]*")


def _sanitize(name: str) -> str:
    """Return ``name`` with every non-alphanumeric character replaced by ``_``."""
    return re.sub(r"[^A-Za-z0-9]", "_", name)


def checked_ignore(ignore: Iterable[str], options: Collection[str]) -> tuple[str, ...]:
    """Return an ignore list in canonical form, after checking every name.

    Args:
        ignore: The property names to ignore, in any order.
        options: The closed set of names the type's hash can ignore.

    Returns:
        The names sorted by code point, each once.

    Raises:
        ValueError: If ``ignore`` is a string, or holds a name that is not a
            string or not one of ``options``.
    """
    if isinstance(ignore, str):
        raise ValueError(
            f"an ignore list is a list of property names, got the string {ignore!r}"
        )
    names = list(ignore)
    for name in names:
        if not isinstance(name, str) or name not in options:
            allowed = ", ".join(sorted(options)) or "none"
            raise ValueError(
                f"unknown property name {name!r} in ignore list; allowed names: "
                f"{allowed}"
            )
    return tuple(sorted(set(names)))


def ignore_metadata(version: int, ignore: Iterable[str]) -> bytes:
    """Return the extension metadata for a version and an ignore list.

    Args:
        version: The version of the type's stored layout and hashing.
        ignore: The ignored property names, in any order.

    Returns:
        Canonical JSON with sorted keys and no whitespace, for example
        ``b'{"ignore":["byte_order"],"version":1}'``.

    Raises:
        ValueError: If a name is not lower-case snake case, or ``version`` is not
            a positive integer.
    """
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError(f"version must be a positive integer, got {version!r}")
    names = sorted(set(ignore))
    for name in names:
        if not isinstance(name, str) or _OPTION_NAME.fullmatch(name) is None:
            raise ValueError(
                f"a property name is lower-case snake case ([a-z][a-z0-9_]*), "
                f"got {name!r}"
            )
    return json.dumps(
        {"ignore": names, "version": version},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def parse_ignore_metadata(metadata: bytes) -> tuple[int, tuple[str, ...]]:
    """Return the version and the ignore list of stored extension metadata.

    Args:
        metadata: The ``ARROW:extension:metadata`` bytes of a column.

    Returns:
        ``(version, names)`` with the names sorted.

    Raises:
        ValueError: If ``metadata`` is not exactly what ``ignore_metadata``
            writes: other keys, other value types, unsorted or repeated names,
            or any other byte sequence.
    """
    try:
        parsed = json.loads(metadata.decode("ascii"))
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError(
            f"extension metadata {metadata!r} is not ignore-list metadata: not JSON"
        ) from exc
    if (
        not isinstance(parsed, dict)
        or set(parsed) != {"ignore", "version"}
        or not isinstance(parsed["ignore"], list)
    ):
        raise ValueError(
            f"extension metadata {metadata!r} is not ignore-list metadata: expected "
            'an object with exactly the keys "ignore" (a list) and "version"'
        )
    try:
        canonical = ignore_metadata(parsed["version"], parsed["ignore"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"extension metadata {metadata!r} is not ignore-list metadata: {exc}"
        ) from exc
    if canonical != metadata:
        raise ValueError(
            f"extension metadata {metadata!r} is not in canonical form; expected "
            f"{canonical!r}"
        )
    return parsed["version"], tuple(parsed["ignore"])


class _ExtensionFamily:
    """The Arrow and Polars extension types of one extension name, one per list.

    Each ignore list has its own ``pa.ExtensionType`` class, so two lists are
    unequal Arrow types everywhere (type equality, schema equality, concatenation).
    Whichever class is registered with Arrow under the name deserialises a stored
    column to the class of the column's own list.
    """

    def __init__(
        self,
        extension_name: str,
        storage_type: "pa.DataType",
        version: int,
        options: frozenset[str],
    ) -> None:
        self.extension_name = extension_name
        self.storage_type = storage_type
        self.version = version
        self.options = options
        self._arrow_types: dict[tuple[str, ...], pa.ExtensionType] = {}
        self._polars_class: type[pl.BaseExtension] | None = None
        self._polars_types: dict[tuple[str, ...], pl.BaseExtension] = {}

    def arrow_type(self, ignore: tuple[str, ...]) -> "pa.ExtensionType":
        """Return the Arrow extension type of a checked ignore list."""
        with _LOCK:
            cached = self._arrow_types.get(ignore)
            if cached is None:
                cached = self._make_arrow_class(ignore)()
                self._arrow_types[ignore] = cached
            return cached

    def _deserialize(
        self, storage_type: "pa.DataType", serialized: bytes
    ) -> "pa.ExtensionType":
        """Return the type of a stored column.

        Raises:
            TypeIdentityError: If ``storage_type`` is not the family's storage.
            ValueError: If ``serialized`` is not the metadata of a valid ignore
                list of this family's version.
        """
        from orcapod.errors import TypeIdentityError
        from orcapod.logical_types.registry import storage_type_matches

        if not storage_type_matches(self.storage_type, storage_type):
            raise TypeIdentityError(
                f"Arrow extension type '{self.extension_name}': expected storage_type "
                f"{self.storage_type!r} but got {storage_type!r}."
            )
        version, names = parse_ignore_metadata(serialized)
        if version != self.version:
            raise ValueError(f"version {version} is not version {self.version}")
        return self.arrow_type(checked_ignore(names, self.options))

    def _make_arrow_class(
        self, ignore: tuple[str, ...]
    ) -> Callable[[], "pa.ExtensionType"]:
        family = self
        metadata = ignore_metadata(self.version, ignore)
        suffix = "__ignore_" + "_and_".join(ignore) if ignore else ""

        def _init(self: "pa.ExtensionType") -> None:
            pa.ExtensionType.__init__(self, family.storage_type, family.extension_name)

        def _serialize(self: "pa.ExtensionType") -> bytes:
            return metadata

        def _deserialize(
            cls: type["pa.ExtensionType"],
            storage_type: "pa.DataType",
            serialized: bytes,
        ) -> "pa.ExtensionType":
            from orcapod.errors import TypeIdentityError

            try:
                return family._deserialize(storage_type, serialized)
            except TypeIdentityError:
                raise
            except ValueError as exc:
                # The class knows what it reads; the family only what is valid.
                raise LogicalTypeIdentifierError(
                    LogicalTypeIdentifier(family.extension_name, serialized),
                    (metadata,),
                ) from exc

        extension_class = type(
            f"_ArrowExt_{_sanitize(self.extension_name)}{suffix}",
            (pa.ExtensionType,),
            {
                "__init__": _init,
                "__arrow_ext_serialize__": _serialize,
                "__arrow_ext_deserialize__": classmethod(_deserialize),
                "readable_metadata": (metadata,),
            },
        )
        # The synthesized ``__init__`` takes no argument; the stubs know only the
        # base class's.
        return cast("Callable[[], pa.ExtensionType]", extension_class)

    def polars_type(self, ignore: tuple[str, ...]) -> "pl.BaseExtension":
        """Return the Polars extension type of a checked ignore list."""
        with _LOCK:
            cached = self._polars_types.get(ignore)
            if cached is None:
                if self._polars_class is None:
                    self._polars_class = self._make_polars_class()
                metadata = ignore_metadata(self.version, ignore).decode("ascii")
                cached = self._polars_class(metadata)  # pyright: ignore[reportCallIssue]  # the synthesized class takes the metadata text
                self._polars_types[ignore] = cached
            return cached

    def _make_polars_class(self) -> type["pl.BaseExtension"]:
        name = self.extension_name
        polars_dtype = cast(
            "pl.Series", pl.from_arrow(pa.array([], type=self.storage_type))
        ).dtype

        def _init(self: "pl.BaseExtension", metadata: str | None = None) -> None:
            pl.BaseExtension.__init__(self, name, polars_dtype, metadata)

        def _ext_from_params(
            cls: Any,
            ext_name: str,
            storage_dtype: Any,
            metadata_str: str | None,
        ) -> "pl.BaseExtension":
            # Polars has one class per extension name; the instance keeps the
            # metadata text of the column it was built for.
            return cls(metadata_str)

        return type(
            f"_PolarsExt_{_sanitize(name)}",
            (pl.BaseExtension,),
            {"__init__": _init, "ext_from_params": classmethod(_ext_from_params)},
        )


_FAMILIES: dict[str, _ExtensionFamily] = {}

_LOCK = threading.RLock()
"""Guards ``_FAMILIES`` and each family's caches.

Arrow compares extension types by class, so one ignore list must never get two
classes: every cache lookup that may create a class runs under this lock.
"""


def _family(
    extension_name: str,
    storage_type: "pa.DataType",
    version: int,
    options: frozenset[str],
) -> _ExtensionFamily:
    """Return the one extension family of ``extension_name`` in this process.

    Raises:
        ValueError: If the name was already declared with another storage type,
            version or set of options.
    """
    with _LOCK:
        family = _FAMILIES.get(extension_name)
        if family is None:
            family = _ExtensionFamily(extension_name, storage_type, version, options)
            _FAMILIES[extension_name] = family
        elif (
            family.storage_type != storage_type
            or family.version != version
            or family.options != options
        ):
            raise ValueError(
                f"extension name {extension_name!r} is already declared with storage "
                f"type {family.storage_type}, version {family.version} and options "
                f"{sorted(family.options)}"
            )
        return family


class IgnoreListLogicalType(BaseLogicalType):
    """Base of a logical type whose identifier carries an ignore list.

    A subclass sets the class attributes and implements ``python_to_storage``
    and ``storage_to_python``. Storage is the same under every ignore list; the
    list changes only the identifier, and with it what the value hash ignores.

    Attributes:
        EXTENSION_NAME: The Arrow extension name, also the logical type name.
        STORAGE_TYPE: The Arrow storage type of one value.
        VERSION: The version of the stored layout and hashing.
        OPTIONS: The closed set of property names the hash can ignore.
        python_type: The Python class of the values (a subclass sets it).

    Args:
        ignore: The property names this instance's identifier lists as ignored.

    Raises:
        ValueError: If ``ignore`` holds a name that is not in ``OPTIONS``.
    """

    EXTENSION_NAME: ClassVar[str]
    STORAGE_TYPE: ClassVar["pa.DataType"]
    VERSION: ClassVar[int]
    OPTIONS: ClassVar[frozenset[str]]
    python_type: type = object

    def __init__(self, ignore: Iterable[str] = ()) -> None:
        self._ignore: tuple[str, ...] = checked_ignore(ignore, self.OPTIONS)

    @property
    def logical_type_name(self) -> str:
        """The logical type name: the Arrow extension name."""
        return self.EXTENSION_NAME

    @property
    def ignore(self) -> tuple[str, ...]:
        """The ignored property names, sorted."""
        return self._ignore

    def _extension_family(self) -> _ExtensionFamily:
        return _family(
            self.EXTENSION_NAME, self.STORAGE_TYPE, self.VERSION, self.OPTIONS
        )

    def get_arrow_extension_type(self) -> "pa.ExtensionType":
        """Return the Arrow extension type whose metadata lists ``ignore``.

        Returns:
            The extension type; equal only to the type of the same ignore list.
        """
        return self._extension_family().arrow_type(self._ignore)

    def get_polars_extension_type(self) -> "pl.BaseExtension":
        """Return the Polars extension type whose metadata lists ``ignore``.

        Returns:
            The Polars extension registered under the extension name.
        """
        return self._extension_family().polars_type(self._ignore)


class IgnoreListHandler:
    """Base of a value hasher that honours an ignore list.

    A subclass sets ``OPTIONS`` and ``HASH_LABEL`` and implements ``digest``.
    A bare value is hashed with the list the handler was configured with (the
    data context's). A cell of a typed column is hashed with the list of the
    column's own extension metadata, through ``hash_ignoring``.

    Attributes:
        OPTIONS: The closed set of property names the hash can ignore; the same
            set as the logical type's.
        HASH_LABEL: The label of the hash algorithm (``ContentHash.method``).

    Args:
        ignore: The property names ignored when a bare value is hashed.

    Raises:
        ValueError: If ``ignore`` holds a name that is not in ``OPTIONS``.
    """

    OPTIONS: ClassVar[frozenset[str]]
    HASH_LABEL: ClassVar[str]

    def __init__(self, ignore: Iterable[str] = ()) -> None:
        self._ignore: frozenset[str] = frozenset(checked_ignore(ignore, self.OPTIONS))

    @property
    def ignore(self) -> frozenset[str]:
        """The property names ignored when a bare value is hashed."""
        return self._ignore

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return the hash of a bare value under the configured ignore list.

        Args:
            obj: The value.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            ``ContentHash(HASH_LABEL, digest(obj, ignore))``.
        """
        return ContentHash(
            method=self.HASH_LABEL, digest=self.digest(obj, self._ignore)
        )

    def hash_ignoring(self, obj: Any, ignore: Iterable[str]) -> ContentHash:
        """Return the hash of a value under a given ignore list.

        Args:
            obj: The value.
            ignore: The property names to ignore, for example those of a
                column's extension metadata.

        Returns:
            ``ContentHash(HASH_LABEL, digest(obj, ignore))``.

        Raises:
            ValueError: If ``ignore`` holds a name that is not in ``OPTIONS``.
        """
        names = frozenset(checked_ignore(ignore, self.OPTIONS))
        return ContentHash(method=self.HASH_LABEL, digest=self.digest(obj, names))

    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the digest of a value, ignoring the named properties.

        Every optional property is a field of the preimage that is always
        present: its real value, or its canonical value when its name is in
        ``ignore``. The list itself is never part of the preimage.

        Args:
            obj: The value.
            ignore: The property names to ignore; a subset of ``OPTIONS``.

        Returns:
            The digest bytes.

        Raises:
            NotImplementedError: Always; every handler implements this.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement digest.")
