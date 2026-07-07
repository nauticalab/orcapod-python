def test_is_ephemeral_col():
    """Test that IS_EPHEMERAL_COL follows system constant naming conventions."""
    from orcapod.system_constants import constants

    col = constants.IS_EPHEMERAL_COL
    assert col == "__is_ephemeral", f"expected '__is_ephemeral', got {col!r}"
