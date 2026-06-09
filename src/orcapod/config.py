# config.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Self

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HashingConfig:
    """Hash truncation length settings.

    Controls the number of hex characters used when truncating hashes for
    system-tag column names, schema hashes, and database path scoping.
    """

    system_tag_n_char: int = 12
    schema_n_char: int = 12
    path_n_char: int = 20

    def merge(self, other: "HashingConfig") -> "HashingConfig":
        """Merge with another ``HashingConfig``; other takes precedence for non-default values.

        Args:
            other: Config to merge in. Must be a ``HashingConfig`` instance.

        Returns:
            New ``HashingConfig`` with non-default values from ``other`` applied.

        Raises:
            TypeError: If ``other`` is not a ``HashingConfig`` instance.
        """
        if not isinstance(other, HashingConfig):
            raise TypeError("Can only merge with another HashingConfig instance")
        defaults = HashingConfig()
        updates = {
            f: getattr(other, f)
            for f in self.__dataclass_fields__
            if getattr(other, f) != getattr(defaults, f)
        }
        return replace(self, **updates)


@dataclass(frozen=True)
class DisplayConfig:
    """Display and preview preference settings.

    Controls default row limits and column visibility when rendering streams
    and tables. These values are consumed by output methods when no explicit
    override is supplied by the caller.
    """

    max_rows: int | None = None
    show_meta_columns: bool = False
    show_source_columns: bool = False
    show_system_tag_columns: bool = False
    show_context_columns: bool = False

    def merge(self, other: "DisplayConfig") -> "DisplayConfig":
        """Merge with another ``DisplayConfig``; other takes precedence for non-default values.

        Args:
            other: Config to merge in. Must be a ``DisplayConfig`` instance.

        Returns:
            New ``DisplayConfig`` with non-default values from ``other`` applied.

        Raises:
            TypeError: If ``other`` is not a ``DisplayConfig`` instance.
        """
        if not isinstance(other, DisplayConfig):
            raise TypeError("Can only merge with another DisplayConfig instance")
        defaults = DisplayConfig()
        updates = {
            f: getattr(other, f)
            for f in self.__dataclass_fields__
            if getattr(other, f) != getattr(defaults, f)
        }
        return replace(self, **updates)


@dataclass(frozen=True)
class OrcapodConfig:
    """Top-level immutable OrcaPod configuration.

    Groups all configuration into typed sections. Construct directly for
    programmatic configuration, or use ``load_config()`` to load from TOML files.
    """

    hashing: HashingConfig = field(default_factory=HashingConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new ``OrcapodConfig`` with updated section values.

        Args:
            **kwargs: Section keyword arguments (e.g. ``hashing=HashingConfig(...)``).

        Returns:
            New ``OrcapodConfig`` with the given sections replaced.
        """
        return replace(self, **kwargs)

    def merge(self, other: "OrcapodConfig") -> "OrcapodConfig":
        """Merge with another ``OrcapodConfig``; other takes precedence for non-default values.

        Merging is performed section by section via each section's own ``merge()``.
        Within each section, fields that are non-default in ``other`` override the
        corresponding fields in ``self``; fields at their default in ``other`` are
        left unchanged.

        Args:
            other: Config to merge in. Must be an ``OrcapodConfig`` instance.

        Returns:
            New ``OrcapodConfig`` with merged sections.

        Raises:
            TypeError: If ``other`` is not an ``OrcapodConfig`` instance.
        """
        if not isinstance(other, OrcapodConfig):
            raise TypeError("Can only merge with another OrcapodConfig instance")
        return OrcapodConfig(
            hashing=self.hashing.merge(other.hashing),
            display=self.display.merge(other.display),
        )

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        source_path: Path | str | None = None,
    ) -> "OrcapodConfig":
        """Construct an ``OrcapodConfig`` from a plain dict.

        Unknown top-level section names and unknown field names within a known
        section are logged at WARNING level and otherwise ignored (forward-compat:
        a config written by a newer orcapod will not break an older version).

        Args:
            data: Mapping of section name to field dict (e.g. as produced by
                ``dataclasses.asdict()`` or parsed from a TOML file).
            source_path: Optional file path included in warning messages to help
                users locate typos.

        Returns:
            ``OrcapodConfig`` populated from ``data``; missing sections and fields
            fall back to built-in defaults.
        """
        known_sections = {"hashing", "display"}
        path_str = f" in {source_path}" if source_path is not None else ""

        for key in data:
            if key not in known_sections:
                logger.warning(
                    "Unknown config section %r%s — ignored", key, path_str
                )

        hashing_dict = data.get("hashing", {})
        known_hashing = set(HashingConfig.__dataclass_fields__)
        if not isinstance(hashing_dict, dict):
            logger.warning(
                "Config section [hashing]%s is not a table — ignored", path_str
            )
            hashing_dict = {}
        for key in hashing_dict:
            if key not in known_hashing:
                logger.warning(
                    "Unknown field %r in [hashing]%s — ignored", key, path_str
                )
        hashing = HashingConfig(
            **{k: v for k, v in hashing_dict.items() if k in known_hashing}
        )

        display_dict = data.get("display", {})
        known_display = set(DisplayConfig.__dataclass_fields__)
        if not isinstance(display_dict, dict):
            logger.warning(
                "Config section [display]%s is not a table — ignored", path_str
            )
            display_dict = {}
        for key in display_dict:
            if key not in known_display:
                logger.warning(
                    "Unknown field %r in [display]%s — ignored", key, path_str
                )
        display = DisplayConfig(
            **{k: v for k, v in display_dict.items() if k in known_display}
        )

        return cls(hashing=hashing, display=display)


# Module-level default config — created at import time.
DEFAULT_CONFIG = OrcapodConfig()


def load_config(
    project_config_path: Path | str | None = None,
    user_config_path: Path | str | None = None,
) -> OrcapodConfig:
    """Load and merge config from TOML files with precedence.

    Precedence (lowest to highest):
      built-in defaults
      → user-global config (``~/.orcapod/config.toml``)
      → project-local config (``./orcapod_config.toml`` in cwd)

    Missing files are silently skipped. Malformed TOML raises ``ValueError``
    with the offending file path included in the message.

    Args:
        project_config_path: Override the project-local config file path.
            Defaults to ``orcapod_config.toml`` in the current working directory.
        user_config_path: Override the user-global config file path.
            Defaults to ``~/.orcapod/config.toml``.

    Returns:
        Merged ``OrcapodConfig`` with all applicable overrides applied.

    Raises:
        ValueError: If a config file exists but contains invalid TOML.
    """
    import tomllib

    _user_path = (
        Path(user_config_path)
        if user_config_path is not None
        else Path.home() / ".orcapod" / "config.toml"
    )
    _project_path = (
        Path(project_config_path)
        if project_config_path is not None
        else Path.cwd() / "orcapod_config.toml"
    )

    known_hashing = set(HashingConfig.__dataclass_fields__)
    known_display = set(DisplayConfig.__dataclass_fields__)

    config = DEFAULT_CONFIG

    for path in (_user_path, _project_path):
        if not path.exists():
            continue
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Malformed TOML in {path}: {e}") from e

        # Warn on unknown top-level sections.
        for key in data:
            if key not in {"hashing", "display"}:
                logger.warning(
                    "Unknown config section %r in %s — ignored", key, path
                )

        # Apply only the keys explicitly present in this file so that a
        # higher-precedence file can reset a field back to its default value
        # and still win over a lower-precedence non-default.
        hashing_data = data.get("hashing", {})
        if not isinstance(hashing_data, dict):
            logger.warning(
                "Config section [hashing] in %s is not a table — ignored", path
            )
            hashing_data = {}
        else:
            for key in hashing_data:
                if key not in known_hashing:
                    logger.warning(
                        "Unknown field %r in [hashing] in %s — ignored", key, path
                    )
        hashing_updates = {k: v for k, v in hashing_data.items() if k in known_hashing}

        display_data = data.get("display", {})
        if not isinstance(display_data, dict):
            logger.warning(
                "Config section [display] in %s is not a table — ignored", path
            )
            display_data = {}
        else:
            for key in display_data:
                if key not in known_display:
                    logger.warning(
                        "Unknown field %r in [display] in %s — ignored", key, path
                    )
        display_updates = {
            k: v for k, v in display_data.items() if k in known_display
        }

        if hashing_updates:
            config = config.with_updates(
                hashing=replace(config.hashing, **hashing_updates)
            )
        if display_updates:
            config = config.with_updates(
                display=replace(config.display, **display_updates)
            )

    return config
