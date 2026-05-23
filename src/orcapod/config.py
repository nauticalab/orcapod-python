# config.py
from dataclasses import dataclass, replace
from typing import Self


@dataclass(frozen=True)
class OrcapodConfig:
    """Immutable OrcaPod configuration object."""

    system_tag_hash_n_char: int = 12
    schema_hash_n_char: int = 12
    path_hash_n_char: int = 20

    def with_updates(self, **kwargs) -> Self:
        """Create a new ``OrcapodConfig`` instance with updated values."""
        return replace(self, **kwargs)

    def merge(self, other: "OrcapodConfig") -> "OrcapodConfig":
        """Merge with another config, other takes precedence."""
        if not isinstance(other, OrcapodConfig):
            raise TypeError("Can only merge with another OrcapodConfig instance")

        # Get all non-default values from other
        defaults = OrcapodConfig()
        updates = {}
        for field_name in self.__dataclass_fields__:
            other_value = getattr(other, field_name)
            default_value = getattr(defaults, field_name)
            if other_value != default_value:
                updates[field_name] = other_value

        return self.with_updates(**updates)


# Module-level default config - created at import time
DEFAULT_CONFIG = OrcapodConfig()
