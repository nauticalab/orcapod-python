# config.py
#!? SECOND config system: OrcapodConfig (hashing/display/datetime, TOML-loaded, global) is entirely
#!? separate from the execution configs in types.py (PipelineConfig/PodConfig/NodeConfig). The two
#!? even use DIFFERENT merge semantics — here "non-DEFAULT value in other overrides"; NodeConfig uses
#!? "non-NONE in other overrides". A reader must know which convention applies where. Consider
#!? documenting the split (library/global vs per-execution) and unifying the merge philosophy.
#! There are legitimately two types of "config" although their interfaces should be as intuitively similar as possible
#! Namely one set of configs are what would be passed into configure target objects such as Pod, Node, etc. You'd expect
#! to create these configs and then pass them in. On the other than, there is the second category of config(s) which is
#! the configuration of Orcapod itself, largely globally. A lot of top level system in Orcapod can optionally take in an
#! instance of OrcapodConfig to override the default config set globally but canonical usage would be to configure
#! the entire library globally. We should remove unnecessary duplication/divergence such as disagreeing implementation of
#! merge but otherwise, we should make it very clear when something is about configuring Orcapod itself, whereas other is
#! about configuring specific objects/system within Orcapod library. We should consider actually have object-specific config
#! to reside *together* with the object it is meant to configure. This needs design spike for evaluation.
from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal, Self

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _SectionConfig:
    """Base class for frozen leaf config section dataclasses.

    Provides a shared ``merge()`` implementation: fields with non-default values
    in ``other`` override the corresponding fields in ``self``; fields that are
    at their default in ``other`` are left unchanged.

    Subclasses must be frozen dataclasses whose fields all carry default values
    so that ``type(self)()`` produces a valid defaults instance.
    """

    #!? DEAD + subtly broken. `_SectionConfig.merge` is only reached via `OrcapodConfig.merge`,
    #!? which itself has NO external callers (verified) — `load_config` deliberately avoids it.
    #!? WHY it's avoided: "non-default overrides" cannot express "reset a field back to its default"
    #!? (a default in `other` reads as "unset"), which is exactly what `load_config` needs for a
    #!? higher-precedence file to win. So merge() cannot do the one job the module actually requires.
    #!? Decide: remove the merge() chain, or redesign it with an explicit "unset" sentinel and route
    #!? load_config through it (killing the duplication below).
    #! This provides confusing diverging implementation to merge logic found in some config object found in types.py
    #! The implementations and inheritance hierarchy (if there is to be any) should be clearned up and
    #! fully documented to avoid confusion
    def merge(self, other: Self) -> Self:
        """Return a new instance with non-default fields from ``other`` applied.

        Args:
            other: Config to merge in. Must be an instance of the same concrete type.

        Returns:
            New instance with non-default values from ``other`` applied.

        Raises:
            TypeError: If ``other`` is not an instance of the same type.
        """
        if not isinstance(other, type(self)):
            raise TypeError(
                f"Can only merge with another {type(self).__name__} instance"
            )
        defaults = type(self)()
        updates = {
            f: getattr(other, f)
            for f in self.__dataclass_fields__
            if getattr(other, f) != getattr(defaults, f)
        }
        return replace(self, **updates)


@dataclass(frozen=True, slots=True)
class DatetimeConfig(_SectionConfig):
    """Datetime handling policy settings.

    Controls how ``UniversalTypeConverter`` treats Python ``datetime`` values
    when converting to Arrow.

    Attributes:
        timezone_policy: How to handle naive (timezone-less) ``datetime`` values.

            ``"strict"`` (default) — raise ``ValueError`` immediately when a naive
            datetime is passed to the converter.  Forces callers to be explicit
            about timezone semantics before data reaches Arrow.

            ``"coerce_utc"`` — silently attach ``timezone.utc`` to naive datetimes
            before writing to Arrow.  Convenient when the caller knows that all
            naive datetimes in their data represent UTC.
    """

    timezone_policy: Literal["strict", "coerce_utc"] = "strict"


@dataclass(frozen=True, slots=True)
class HashingConfig(_SectionConfig):
    """Hash length settings for system-tag column names, schema hashes, and path scoping.

    All fields default to ``None``, which means full-length hashes are used everywhere.
    Set a field to a positive integer to truncate hashes to that many hex characters
    (e.g. for backwards compatibility with existing stored data).
    """

    system_tag_n_char: int | None = None
    schema_n_char: int | None = None
    path_n_char: int | None = None


@dataclass(frozen=True)
class DisplayConfig(_SectionConfig):
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


@dataclass(frozen=True)
class OrcapodConfig:
    """Top-level immutable OrcaPod configuration.

    Groups all configuration into typed sections. Construct directly for
    programmatic configuration, or use ``load_config()`` to load from TOML files.
    """

    hashing: HashingConfig = field(default_factory=HashingConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)
    datetime: DatetimeConfig = field(default_factory=DatetimeConfig)

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new ``OrcapodConfig`` with updated section values.

        Args:
            **kwargs: Section keyword arguments (e.g. ``hashing=HashingConfig(...)``).

        Returns:
            New ``OrcapodConfig`` with the given sections replaced.
        """
        return replace(self, **kwargs)

    #! yet another implementation of merge that should be avoided
    #! as mentioned above, we should consolidate config merging logic
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
            datetime=self.datetime.merge(other.datetime),
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        source_path: Path | str | None = None,
    ) -> "OrcapodConfig":
        """Construct an ``OrcapodConfig`` from a plain dict.

        Unknown top-level section names and unknown field names within a known
        section are logged at WARNING level and otherwise ignored (forward-compat:
        a config written by a newer orcapod will not break an older version).

        Args:
            data: Mapping of section name to field mapping (e.g. as produced by
                ``dataclasses.asdict()`` or parsed from a TOML file).
            source_path: Optional file path included in warning messages to help
                users locate typos.

        Returns:
            ``OrcapodConfig`` populated from ``data``; missing sections and fields
            fall back to built-in defaults.
        """
        #! Indeed this is non-sense duplication -- there ought to be targetted issue(s) to address/refactor
        #! duplication
        #!? MASSIVE duplication: the unknown-section-warn + unknown-field-warn + filter block is
        #!? copy-pasted 3× here (hashing/display/datetime) AND re-implemented again in load_config()
        #!? below (~150 lines total). Extract one helper `_parse_section(name, raw, cls, path_str)`
        #!? and have BOTH from_dict and load_config call it. Also: from_dict checks isinstance(...,
        #!? Mapping) while load_config checks isinstance(..., dict) — pick one. The known-sections set
        #!? {"hashing","display","datetime"} is hardcoded in 3 places; derive from the dataclass fields.
        known_sections = {"hashing", "display", "datetime"}
        path_str = f" in {source_path}" if source_path is not None else ""

        for key in data:
            if key not in known_sections:
                logger.warning(
                    "Unknown config section %r%s — ignored", key, path_str
                )

        hashing_dict = data.get("hashing", {})
        known_hashing = set(HashingConfig.__dataclass_fields__)
        if not isinstance(hashing_dict, Mapping):
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
        if not isinstance(display_dict, Mapping):
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

        datetime_dict = data.get("datetime", {})
        known_datetime = set(DatetimeConfig.__dataclass_fields__)
        if not isinstance(datetime_dict, Mapping):
            logger.warning(
                "Config section [datetime]%s is not a table — ignored", path_str
            )
            datetime_dict = {}
        for key in datetime_dict:
            if key not in known_datetime:
                logger.warning(
                    "Unknown field %r in [datetime]%s — ignored", key, path_str
                )
        datetime_config = DatetimeConfig(
            **{k: v for k, v in datetime_dict.items() if k in known_datetime}
        )

        return cls(hashing=hashing, display=display, datetime=datetime_config)


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

    Missing or non-file paths are silently skipped. Malformed TOML raises
    ``ValueError`` with the offending file path included in the message.

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

    #! indeed this should make use of existing methods rather than providing bare implementation again
    #! furthermore, config should optionally support getting values set through env vars (low priority)
    #!? Reimplements from_dict()'s per-section parsing instead of reusing it. The "apply only keys
    #!? explicitly present so a higher-precedence file can reset to default" trick (below) is the real
    #!? reason merge() is bypassed — see the note on _SectionConfig.merge. If a shared _parse_section
    #!? helper returned the explicit-keys dict, load_config could layer files via replace() cleanly.
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
    known_datetime = set(DatetimeConfig.__dataclass_fields__)

    config = DEFAULT_CONFIG

    for path in (_user_path, _project_path):
        if not path.is_file():
            continue
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Malformed TOML in {path}: {e}") from e

        # Warn on unknown top-level sections.
        for key in data:
            if key not in {"hashing", "display", "datetime"}:
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

        datetime_data = data.get("datetime", {})
        if not isinstance(datetime_data, dict):
            logger.warning(
                "Config section [datetime] in %s is not a table — ignored", path
            )
            datetime_data = {}
        else:
            for key in datetime_data:
                if key not in known_datetime:
                    logger.warning(
                        "Unknown field %r in [datetime] in %s — ignored", key, path
                    )
        datetime_updates = {
            k: v for k, v in datetime_data.items() if k in known_datetime
        }

        if hashing_updates:
            config = config.with_updates(
                hashing=replace(config.hashing, **hashing_updates)
            )
        if display_updates:
            config = config.with_updates(
                display=replace(config.display, **display_updates)
            )
        if datetime_updates:
            config = config.with_updates(
                datetime=replace(config.datetime, **datetime_updates)
            )

    return config
