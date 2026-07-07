import pytest
import dataclasses


def test_node_config_ephemeral_result_default_false():
    """Test that ephemeral_result defaults to False."""
    from orcapod.types import NodeConfig

    cfg = NodeConfig()
    assert cfg.ephemeral_result is False


def test_node_config_ephemeral_result_true():
    """Test that ephemeral_result can be set to True."""
    from orcapod.types import NodeConfig

    cfg = NodeConfig(ephemeral_result=True)
    assert cfg.ephemeral_result is True


def test_node_config_ephemeral_result_is_frozen():
    """Test that NodeConfig is frozen and prevents field mutation."""
    from orcapod.types import NodeConfig

    cfg = NodeConfig(ephemeral_result=True)
    with pytest.raises((dataclasses.FrozenInstanceError, TypeError)):
        cfg.ephemeral_result = False  # type: ignore[misc]
