"""Tests for OrcapodConfig class naming (ENG-514)."""


class TestOrcapodConfigModule:
    def test_can_import_orcapod_config_from_config_module(self):
        from orcapod.config import OrcapodConfig  # noqa: F401

    def test_orcapod_config_is_instantiable_with_defaults(self):
        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        assert cfg.system_tag_hash_n_char == 12
        assert cfg.schema_hash_n_char == 12
        assert cfg.path_hash_n_char == 20

    def test_default_config_is_orcapod_config_instance(self):
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        assert isinstance(DEFAULT_CONFIG, OrcapodConfig)

    def test_orcapod_config_with_updates(self):
        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        updated = cfg.with_updates(system_tag_hash_n_char=8)
        assert updated.system_tag_hash_n_char == 8
        assert cfg.system_tag_hash_n_char == 12  # original unchanged

    def test_orcapod_config_merge(self):
        from orcapod.config import OrcapodConfig

        base = OrcapodConfig()
        other = OrcapodConfig(system_tag_hash_n_char=8)
        merged = base.merge(other)
        assert merged.system_tag_hash_n_char == 8
        assert merged.schema_hash_n_char == 12  # unchanged default

    def test_orcapod_config_merge_type_error(self):
        import pytest

        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        with pytest.raises(TypeError):
            cfg.merge("not a config")  # type: ignore[arg-type]


class TestOrcapodConfigTopLevelExport:
    def test_can_import_orcapod_config_from_orcapod(self):
        from orcapod import OrcapodConfig  # noqa: F401

    def test_orcapod_config_in_all(self):
        import orcapod

        assert "OrcapodConfig" in orcapod.__all__
