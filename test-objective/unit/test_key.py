"""Specification-derived tests for Key."""

import pyarrow as pa
import pytest

from orcapod.core.datagrams.datagram import Datagram
from orcapod.core.datagrams.key_data import Key
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig

# Use the actual system key prefix from constants
_SYS_TAG_KEY = f"{constants.SYSTEM_KEY_PREFIX}src:abc"


def _make_context():
    """Create a DataContext for tests."""
    from orcapod.contexts import resolve_context
    return resolve_context(None)


# ---------------------------------------------------------------------------
# System keys stored separately from data columns
# ---------------------------------------------------------------------------

class TestKeySystemKeysSeparation:
    """System keys are stored separately from data columns."""

    def test_system_keys_not_in_keys_by_default(self):
        ctx = _make_context()
        key = Key({"x": 1, "y": "hello"}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        keys = list(key.keys())
        assert "x" in keys
        assert "y" in keys
        assert not any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in keys)

    def test_system_keys_not_in_as_dict_by_default(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        d = key.as_dict()
        assert not any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in d)

    def test_system_keys_not_in_as_table_by_default(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        table = key.as_table()
        assert not any(name.startswith(constants.SYSTEM_KEY_PREFIX) for name in table.column_names)

    def test_system_keys_not_in_schema_by_default(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        s = key.schema()
        assert not any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in s)


# ---------------------------------------------------------------------------
# System keys included with ColumnConfig
# ---------------------------------------------------------------------------

class TestKeySystemKeysWithConfig:
    """With ColumnConfig system_keys=True or all_info=True, system keys are included."""

    def test_keys_with_system_keys_true(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        keys = list(key.keys(columns=ColumnConfig(system_keys=True)))
        assert any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in keys)

    def test_as_dict_with_system_keys_true(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        d = key.as_dict(columns=ColumnConfig(system_keys=True))
        assert any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in d)

    def test_as_table_with_system_keys_true(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        table = key.as_table(columns=ColumnConfig(system_keys=True))
        assert any(name.startswith(constants.SYSTEM_KEY_PREFIX) for name in table.column_names)

    def test_keys_with_all_info(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        keys = list(key.keys(columns=ColumnConfig.all()))
        assert any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in keys)

    def test_schema_with_system_keys_true(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        s = key.schema(columns=ColumnConfig(system_keys=True))
        assert any(k.startswith(constants.SYSTEM_KEY_PREFIX) for k in s)


# ---------------------------------------------------------------------------
# system_keys() returns a dict COPY
# ---------------------------------------------------------------------------

class TestKeySystemKeysCopy:
    """system_keys() returns a dict COPY (not a reference)."""

    def test_system_keys_returns_dict(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        st = key.system_keys()
        assert isinstance(st, dict)
        assert _SYS_TAG_KEY in st

    def test_system_keys_is_copy(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        st = key.system_keys()
        st[_SYS_TAG_KEY] = "modified"
        # Original should be unchanged
        assert key.system_keys()[_SYS_TAG_KEY] == "val"


# ---------------------------------------------------------------------------
# copy() preserves system keys
# ---------------------------------------------------------------------------

class TestKeyCopy:
    """copy() preserves system keys."""

    def test_copy_preserves_system_keys(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={_SYS_TAG_KEY: "val"})
        copied = key.copy()
        assert copied is not key
        assert copied.system_keys() == key.system_keys()

    def test_copy_preserves_data(self):
        ctx = _make_context()
        key = Key({"x": 1, "y": "hello"}, data_context=ctx, system_keys={})
        copied = key.copy()
        assert copied["x"] == 1
        assert copied["y"] == "hello"


# ---------------------------------------------------------------------------
# as_datagram() returns Datagram, not Key
# ---------------------------------------------------------------------------

class TestKeyAsDatagram:
    """as_datagram() returns a Datagram (not Key)."""

    def test_as_datagram_returns_datagram_type(self):
        ctx = _make_context()
        key = Key({"x": 1}, data_context=ctx, system_keys={})
        dg = key.as_datagram()
        assert isinstance(dg, Datagram)
        assert not isinstance(dg, Key)

    def test_as_datagram_preserves_data(self):
        ctx = _make_context()
        key = Key({"x": 1, "y": "hello"}, data_context=ctx, system_keys={})
        dg = key.as_datagram()
        assert dg["x"] == 1
        assert dg["y"] == "hello"
