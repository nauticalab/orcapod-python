"""Tests for HashingConfig, DisplayConfig, OrcapodConfig, and load_config (PLT-964)."""
from __future__ import annotations


class TestHashingConfig:
    def test_defaults(self):
        from orcapod.config import HashingConfig

        cfg = HashingConfig()
        assert cfg.system_tag_n_char == 12
        assert cfg.schema_n_char == 12
        assert cfg.path_n_char == 20

    def test_is_frozen(self):
        import dataclasses

        import pytest

        from orcapod.config import HashingConfig

        cfg = HashingConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.system_tag_n_char = 99  # type: ignore[misc]

    def test_merge_non_default_wins(self):
        from orcapod.config import HashingConfig

        base = HashingConfig()
        other = HashingConfig(system_tag_n_char=8)
        merged = base.merge(other)
        assert merged.system_tag_n_char == 8
        assert merged.schema_n_char == 12  # unchanged

    def test_merge_default_does_not_override(self):
        from orcapod.config import HashingConfig

        base = HashingConfig(system_tag_n_char=6)
        other = HashingConfig()  # all defaults
        merged = base.merge(other)
        assert merged.system_tag_n_char == 6  # base value preserved


class TestDisplayConfig:
    def test_defaults(self):
        from orcapod.config import DisplayConfig

        cfg = DisplayConfig()
        assert cfg.max_rows is None
        assert cfg.show_meta_columns is False
        assert cfg.show_source_columns is False
        assert cfg.show_system_tag_columns is False
        assert cfg.show_context_columns is False

    def test_is_frozen(self):
        import dataclasses

        import pytest

        from orcapod.config import DisplayConfig

        cfg = DisplayConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.max_rows = 10  # type: ignore[misc]

    def test_merge_non_default_wins(self):
        from orcapod.config import DisplayConfig

        base = DisplayConfig()
        other = DisplayConfig(max_rows=100, show_source_columns=True)
        merged = base.merge(other)
        assert merged.max_rows == 100
        assert merged.show_source_columns is True
        assert merged.show_meta_columns is False  # unchanged

    def test_merge_default_does_not_override(self):
        from orcapod.config import DisplayConfig

        base = DisplayConfig(max_rows=50)
        other = DisplayConfig()  # all defaults
        merged = base.merge(other)
        assert merged.max_rows == 50  # base value preserved


class TestOrcapodConfig:
    def test_defaults(self):
        from orcapod.config import DisplayConfig, HashingConfig, OrcapodConfig

        cfg = OrcapodConfig()
        assert cfg.hashing == HashingConfig()
        assert cfg.display == DisplayConfig()

    def test_default_config_is_orcapod_config_instance(self):
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        assert isinstance(DEFAULT_CONFIG, OrcapodConfig)

    def test_with_updates_hashing_section(self):
        from orcapod.config import HashingConfig, OrcapodConfig

        cfg = OrcapodConfig()
        updated = cfg.with_updates(hashing=HashingConfig(system_tag_n_char=8))
        assert updated.hashing.system_tag_n_char == 8
        assert cfg.hashing.system_tag_n_char == 12  # original unchanged

    def test_with_updates_display_section(self):
        from orcapod.config import DisplayConfig, OrcapodConfig

        cfg = OrcapodConfig()
        updated = cfg.with_updates(display=DisplayConfig(max_rows=50))
        assert updated.display.max_rows == 50
        assert cfg.display.max_rows is None  # original unchanged

    def test_merge_hashing_non_default_wins(self):
        from orcapod.config import HashingConfig, OrcapodConfig

        base = OrcapodConfig()
        other = OrcapodConfig(hashing=HashingConfig(system_tag_n_char=8))
        merged = base.merge(other)
        assert merged.hashing.system_tag_n_char == 8

    def test_merge_hashing_default_does_not_override(self):
        from orcapod.config import HashingConfig, OrcapodConfig

        base = OrcapodConfig(hashing=HashingConfig(system_tag_n_char=6))
        other = OrcapodConfig()
        merged = base.merge(other)
        assert merged.hashing.system_tag_n_char == 6

    def test_merge_display_non_default_wins(self):
        from orcapod.config import DisplayConfig, OrcapodConfig

        base = OrcapodConfig()
        other = OrcapodConfig(display=DisplayConfig(max_rows=100))
        merged = base.merge(other)
        assert merged.display.max_rows == 100

    def test_merge_type_error(self):
        import pytest

        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        with pytest.raises(TypeError):
            cfg.merge("not a config")  # type: ignore[arg-type]


class TestOrcapodConfigFromDict:
    def test_empty_dict_returns_default(self):
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        assert OrcapodConfig.from_dict({}) == DEFAULT_CONFIG

    def test_partial_hashing_section(self):
        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig.from_dict({"hashing": {"system_tag_n_char": 8}})
        assert cfg.hashing.system_tag_n_char == 8
        assert cfg.hashing.schema_n_char == 12  # default preserved

    def test_full_sections_round_trip(self):
        from orcapod.config import OrcapodConfig

        data = {
            "hashing": {"system_tag_n_char": 8, "schema_n_char": 6, "path_n_char": 16},
            "display": {"max_rows": 100, "show_source_columns": True},
        }
        cfg = OrcapodConfig.from_dict(data)
        assert cfg.hashing.system_tag_n_char == 8
        assert cfg.hashing.schema_n_char == 6
        assert cfg.hashing.path_n_char == 16
        assert cfg.display.max_rows == 100
        assert cfg.display.show_source_columns is True

    def test_unknown_section_warns(self, caplog):
        import logging

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            OrcapodConfig.from_dict({"typo_section": {"foo": 1}})
        assert "typo_section" in caplog.text

    def test_unknown_field_within_section_warns(self, caplog):
        import logging

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            OrcapodConfig.from_dict({"display": {"max_rwos": 50}})
        assert "max_rwos" in caplog.text

    def test_source_path_appears_in_warning(self, caplog):
        import logging
        from pathlib import Path

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            OrcapodConfig.from_dict(
                {"bad_section": {}}, source_path=Path("/some/config.toml")
            )
        assert "/some/config.toml" in caplog.text

    def test_non_dict_hashing_section_warns_and_uses_defaults(self, caplog):
        import logging

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            cfg = OrcapodConfig.from_dict({"hashing": "not-a-table"})
        assert "hashing" in caplog.text
        assert cfg.hashing.system_tag_n_char == 12  # falls back to default

    def test_non_dict_display_section_warns_and_uses_defaults(self, caplog):
        import logging

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            cfg = OrcapodConfig.from_dict({"display": 42})
        assert "display" in caplog.text
        assert cfg.display.max_rows is None  # falls back to default


class TestLoadConfig:
    def test_no_files_returns_default(self, tmp_path):
        from orcapod.config import DEFAULT_CONFIG, load_config

        result = load_config(
            project_config_path=tmp_path / "nonexistent.toml",
            user_config_path=tmp_path / "nonexistent.toml",
        )
        assert result == DEFAULT_CONFIG

    def test_user_global_config_applied(self, tmp_path):
        from orcapod.config import load_config

        user_cfg = tmp_path / "config.toml"
        user_cfg.write_text("[hashing]\nsystem_tag_n_char = 8\n")
        result = load_config(
            user_config_path=user_cfg,
            project_config_path=tmp_path / "nonexistent.toml",
        )
        assert result.hashing.system_tag_n_char == 8

    def test_project_local_config_applied(self, tmp_path):
        from orcapod.config import load_config

        project_cfg = tmp_path / "orcapod_config.toml"
        project_cfg.write_text("[display]\nmax_rows = 50\n")
        result = load_config(
            project_config_path=project_cfg,
            user_config_path=tmp_path / "nonexistent.toml",
        )
        assert result.display.max_rows == 50

    def test_project_local_wins_over_user_global(self, tmp_path):
        from orcapod.config import load_config

        user_cfg = tmp_path / "user.toml"
        user_cfg.write_text("[hashing]\nsystem_tag_n_char = 6\n")
        project_cfg = tmp_path / "project.toml"
        project_cfg.write_text("[hashing]\nsystem_tag_n_char = 10\n")
        result = load_config(user_config_path=user_cfg, project_config_path=project_cfg)
        assert result.hashing.system_tag_n_char == 10

    def test_project_local_can_reset_field_to_default(self, tmp_path):
        """Project-local file can explicitly reset a field to its default value.

        Verifies that a higher-precedence file setting a value equal to the
        built-in default still wins over a lower-precedence non-default.
        """
        from orcapod.config import load_config

        user_cfg = tmp_path / "user.toml"
        user_cfg.write_text("[hashing]\nsystem_tag_n_char = 6\n")
        project_cfg = tmp_path / "project.toml"
        # Explicitly set back to the built-in default (12) — should win.
        project_cfg.write_text("[hashing]\nsystem_tag_n_char = 12\n")
        result = load_config(user_config_path=user_cfg, project_config_path=project_cfg)
        assert result.hashing.system_tag_n_char == 12

    def test_malformed_toml_raises_value_error(self, tmp_path):
        import pytest

        from orcapod.config import load_config

        bad_cfg = tmp_path / "bad.toml"
        bad_cfg.write_text("this is not valid toml ][[[")
        with pytest.raises(ValueError, match=str(bad_cfg)):
            load_config(
                project_config_path=bad_cfg,
                user_config_path=tmp_path / "nonexistent.toml",
            )

    def test_explicit_paths_used(self, tmp_path):
        from orcapod.config import load_config

        cfg_file = tmp_path / "custom.toml"
        cfg_file.write_text("[display]\nshow_source_columns = true\n")
        result = load_config(
            project_config_path=cfg_file,
            user_config_path=tmp_path / "nonexistent.toml",
        )
        assert result.display.show_source_columns is True


class TestOrcapodConfigTopLevelExport:
    def test_can_import_orcapod_config(self):
        from orcapod import OrcapodConfig  # noqa: F401

    def test_can_import_hashing_config(self):
        from orcapod import HashingConfig  # noqa: F401

    def test_can_import_display_config(self):
        from orcapod import DisplayConfig  # noqa: F401

    def test_can_import_load_config(self):
        from orcapod import load_config  # noqa: F401

    def test_orcapod_config_in_all(self):
        import orcapod

        assert "OrcapodConfig" in orcapod.__all__

    def test_hashing_config_in_all(self):
        import orcapod

        assert "HashingConfig" in orcapod.__all__

    def test_display_config_in_all(self):
        import orcapod

        assert "DisplayConfig" in orcapod.__all__

    def test_load_config_in_all(self):
        import orcapod

        assert "load_config" in orcapod.__all__

    def test_default_config_in_all(self):
        import orcapod

        assert "DEFAULT_CONFIG" in orcapod.__all__
