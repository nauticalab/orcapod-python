"""Unit tests of the match-key freezing helper (ITL-1044)."""

from __future__ import annotations

import pyarrow as pa

from orcapod.core.operators.match_keys import frozen_scalar


def test_map_entries_freeze_by_position_whatever_their_field_names() -> None:
    """A map whose entry fields are not named ``key``/``value`` still freezes."""
    key_field = pa.field("k", pa.large_string(), nullable=False)
    item_field = pa.field("v", pa.int64())
    map_type = pa.map_(key_field, item_field)  # pyright: ignore[reportArgumentType, reportUnknownVariableType]  # stubs reject Field arguments that pyarrow accepts
    values = pa.array([[("a", 1), ("c", None)], [("a", 1), ("c", None)]], map_type)
    assert frozen_scalar(values[0]) == frozen_scalar(values[1])
    assert frozen_scalar(values[0]) == (("a", 1), ("c", None))
