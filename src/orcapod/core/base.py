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
            str | None: The label of the object, or None if not set.
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
    which defaults to the one returned by `get_default_object_hasher`.
    Two content-identifiable objects are considered equal if their `identity_structure` returns the same value.
    """

    def __init__(
        self,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        """
        Initialize the ContentHashable with an optional ObjectHasher.

        Args:
            identity_structure_hasher (ObjectHasher | None): An instance of ObjectHasher to use for hashing.
        """
        super().__init__(data_context=data_context, config=config)
        self._cached_content_hash: ContentHash | None = None
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

    def content_hash(self) -> ContentHash:
        """
        Compute a hash based on the content of this object.

        Returns:
            bytes: A byte representation of the hash based on the content.
                   If no identity structure is provided, return None.
        """
        if self._cached_content_hash is None:
            # hash of content identifiable should be identical to
            # the hash of its identity_structure
            structure = self.identity_structure()
            # processed_structure = process_structure(structure)
            self._cached_content_hash = self.data_context.semantic_hasher.hash_object(
                structure
            )
        return self._cached_content_hash

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


class TemporalMixin:
    """
    Mixin class that adds temporal functionality to an Orcapod entity.
    It provides methods to track and manage the last modified timestamp of the entity.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._modified_time = self._update_modified_time()

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


class TraceableBase(TemporalMixin, LabelableMixin, ContentIdentifiableBase):
    """
    Base class for all default traceable entities, providing common functionality
    including data context awareness, content-based identity, (semantic) labeling,
    and modification timestamp.
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
