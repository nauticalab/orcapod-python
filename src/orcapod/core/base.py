from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import orcapod.contexts as contexts
from orcapod.config import DEFAULT_CONFIG, Config
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)


# Base classes for Orcapod core components, providing common functionality.


class LabelableMixin:
    """
    Mixin class for objects that can have a label. Provides a mechanism to compute a label based on the object's content.
    By default, explicitly set label will always take precedence over computed label and inferred label.
    """

    def __init__(self, label: str | None = None, **kwargs):
        self._label = label
        super().__init__(**kwargs)

    @property
    def label(self) -> str:
        """
        Get the label of this object.

        Returns:
            str: The label of the object. In order of precedence: the
                explicitly assigned label, the computed label (if any),
                or the class name of the object as a fallback. Never
                returns ``None``.
        """
        return self._label or self.computed_label() or self.__class__.__name__

    @property
    def has_assigned_label(self) -> bool:
        """
        Check if the label has been explicitly set for this object.

        Returns:
            bool: True if the label is explicitly set, False otherwise.
        """
        return self._label is not None

    @label.setter
    def label(self, label: str | None) -> None:
        """
        Set the label of this object.

        Args:
            label (str | None): The label to set for this object.
        """
        self._label = label

    def computed_label(self) -> str | None:
        """
        Compute a label for this object based on its content. If label is not explicitly set for this object
        and computed_label returns a valid value, it will be used as label of this object.
        """
        return None


class DataContextMixin:
    """
    Mixin to associate data context and an Orcapod config with an object. Deriving class allows data context and Orcapod config to be
    explicitly specified and if not provided, use the default data context and Orcapod config.
    """

    def __init__(
        self,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data_context = contexts.resolve_context(data_context)
        if config is None:
            config = DEFAULT_CONFIG  # DEFAULT_CONFIG as defined in orcapod/config.py
        self._orcapod_config = config

    @property
    def orcapod_config(self) -> Config:
        return self._orcapod_config

    @property
    def data_context(self) -> contexts.DataContext:
        return self._data_context

    # TODO: re-evaluate whether changing data context should be allowed
    @data_context.setter
    def data_context(self, context: str | contexts.DataContext | None) -> None:
        self._data_context = contexts.resolve_context(context)

    @property
    def data_context_key(self) -> str:
        """Return the data context key."""
        return self._data_context.context_key


class ContentIdentifiableBase(DataContextMixin, ABC):
    """
    Base class for content-identifiable objects.
    This class provides a way to define objects that can be uniquely identified
    based on their content rather than their identity in memory. Specifically, the identity of the
    object is determined by the structure returned by the `identity_structure` method.
    The hash of the object is computed based on the `identity_structure` using the provided `ObjectHasher`,
    which defaults to the one returned by `get_default_semantic_hasher`.
    Two content-identifiable objects are considered equal if their `identity_structure` returns the same value.
    """

    def __init__(
        self,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the ContentHashable with an optional ObjectHasher.

        Args:
            identity_structure_hasher (ObjectHasher | None): An instance of ObjectHasher to use for hashing.
        """
        super().__init__(data_context=data_context, config=config, **kwargs)
        self._content_hash_cache: dict[str, ContentHash] = {}
        self._cached_int_hash: int | None = None

    @abstractmethod
    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this object.

        Override this method in your subclass to provide a stable representation
        of your object's content. The structure should contain all fields that
        determine the object's identity.

        Returns:
            Any: A structure representing this object's content, or None to use default hash
        """
        ...

    def content_hash(self, hasher=None) -> ContentHash:
        """
        Compute a hash based on the content of this object.

        The hasher is used for the entire recursive computation — all nested
        ContentIdentifiable objects are resolved using the same hasher, ensuring
        one consistent context per hash computation.

        Args:
            hasher: Optional semantic hasher to use.  When omitted, the hasher
                is resolved from this object's data_context and the result is
                cached by hasher_id for reuse.  When provided explicitly, the
                result is also cached by hasher_id, so repeated calls with the
                same hasher are free.

        Returns:
            ContentHash: Stable, content-based hash of the object.
        """
        if hasher is None:
            hasher = self.data_context.semantic_hasher
        cache_key = hasher.hasher_id

        def content_resolver(obj):
            return obj.content_hash(hasher)

        if cache_key not in self._content_hash_cache:
            self._content_hash_cache[cache_key] = hasher.hash_object(
                self.identity_structure(), resolver=content_resolver
            )
        return self._content_hash_cache[cache_key]

    def __hash__(self) -> int:
        """
        Hash implementation that uses the identity structure if provided,
        otherwise falls back to the superclass's hash method.

        Returns:
            int: A hash value based on either content or identity
        """
        # Get the identity structure
        return self.content_hash().to_int()

    def __eq__(self, other: object) -> bool:
        """
        Equality check that compares the identity structures of two objects.

        Args:
            other (object): The object to compare against.

        Returns:
            bool: True if both objects have the same identity structure, False otherwise.
        """
        if not isinstance(other, ContentIdentifiableBase):
            return NotImplemented

        return self.identity_structure() == other.identity_structure()


class PipelineElementBase(DataContextMixin, ABC):
    """
    Mixin providing pipeline-level identity for objects that participate in a
    pipeline graph.

    This is a parallel identity chain to ContentIdentifiableBase. Content
    identity (content_hash) captures the precise, data-inclusive identity of
    an object. Pipeline identity (pipeline_hash) captures only what is
    structurally meaningful for pipeline database path scoping: schemas and
    the recursive topology of upstream computation, with no data content.

    Must be used alongside DataContextMixin (directly or via TraceableBase),
    which provides self.data_context used by pipeline_hash().

    The only class that needs to override pipeline_identity_structure() in a
    non-trivial way is RootSource, which returns (tag_schema, packet_schema)
    as the base case of the recursion. All other pipeline elements return
    structures built from the pipeline_hash() values of their upstream
    components — ContentHash objects are terminal in the semantic hasher, so
    no special hashing mode is required.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._pipeline_hash_cache: dict[str, ContentHash] = {}

    @abstractmethod
    def pipeline_identity_structure(self) -> Any:
        """
        Return a structure representing this element's pipeline identity.

        Implementations may return raw ContentIdentifiable objects (such as
        upstream stream or pod references) as leaves — the pipeline resolver
        threaded through pipeline_hash() ensures that PipelineElementProtocol
        objects are resolved via pipeline_hash() and other ContentIdentifiable
        objects via content_hash(), both using the same hasher throughout.
        """
        ...

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """
        Return the pipeline-level hash of this element, computed from
        pipeline_identity_structure() and cached by hasher_id.

        The hasher is used for the entire recursive computation — all nested
        objects are resolved using the same hasher, ensuring one consistent
        context per hash computation.

        Args:
            hasher: Optional semantic hasher to use.  When omitted, resolved
                from this object's data_context.
        """
        if hasher is None:
            hasher = self.data_context.semantic_hasher
        cache_key = hasher.hasher_id
        if cache_key not in self._pipeline_hash_cache:
            from orcapod.protocols.hashing_protocols import PipelineElementProtocol

            def pipeline_resolver(obj: Any) -> ContentHash:
                if isinstance(obj, PipelineElementProtocol):
                    return obj.pipeline_hash(hasher)
                return obj.content_hash(hasher)

            self._pipeline_hash_cache[cache_key] = hasher.hash_object(
                self.pipeline_identity_structure(), resolver=pipeline_resolver
            )
        return self._pipeline_hash_cache[cache_key]


class TemporalMixin:
    """
    Mixin class that adds temporal functionality to an Orcapod entity.
    It provides methods to track and manage the last modified timestamp of the entity.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._update_modified_time()

    @property
    def last_modified(self) -> datetime | None:
        """
        When this object's content was last modified.

        Returns:
            datetime: Content last modified timestamp (timezone-aware)
            None: Modification time unknown (assume always changed)
        """
        return self._modified_time

    def _set_modified_time(self, modified_time: datetime | None) -> None:
        """
        Set the modified time for this object.

        Args:
            modified_time (datetime | None): The modified time to set. If None, clears the modified time.
        """
        self._modified_time = modified_time

    def _update_modified_time(self) -> None:
        """
        Update the modified time to the current time.
        """
        self._modified_time = datetime.now(timezone.utc)

    def updated_since(self, timestamp: datetime) -> bool:
        """
        Check if the object has been updated since the given timestamp.

        Args:
            timestamp (datetime): The timestamp to compare against.

        Returns:
            bool: True if the object has been updated since the given timestamp, False otherwise.
        """
        # if _modified_time is None, consider it always updated
        if self._modified_time is None:
            return True
        return self._modified_time > timestamp


class TraceableBase(
    TemporalMixin, LabelableMixin, ContentIdentifiableBase, PipelineElementBase
):
    """
    Base class for all default traceable entities, providing common functionality
    including data context awareness, content-based identity, (semantic) labeling,
    modification timestamp, and pipeline identity.

    Every computation-node class (streams, packet functions, pods) inherits from
    TraceableBase, getting both content identity (content_hash) and pipeline
    identity (pipeline_hash) automatically.
    """

    def __init__(
        self,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ):
        # Init provided here for explicit listing of parmeters
        super().__init__(label=label, data_context=data_context, config=config)

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return self.label
