import pytest
import dataclasses


def test_node_config_is_result_ephemeral_default_false():
    """Test that is_result_ephemeral defaults to False."""
    from orcapod.types import NodeConfig

    cfg = NodeConfig()
    assert cfg.is_result_ephemeral is False


def test_node_config_is_result_ephemeral_true():
    """Test that is_result_ephemeral can be set to True."""
    from orcapod.types import NodeConfig

    cfg = NodeConfig(is_result_ephemeral=True)
    assert cfg.is_result_ephemeral is True


def test_node_config_is_result_ephemeral_is_frozen():
    """Test that NodeConfig is frozen and prevents field mutation."""
    from orcapod.types import NodeConfig

    cfg = NodeConfig(is_result_ephemeral=True)
    with pytest.raises((dataclasses.FrozenInstanceError, TypeError)):
        cfg.is_result_ephemeral = False  # type: ignore[misc]
