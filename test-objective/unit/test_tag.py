"""Specification-derived tests for Tag."""

import pyarrow as pa
import pytest

from orcapod.core.datagrams.datagram import Datagram
from orcapod.core.datagrams.tag_packet import Tag
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig

# Use the actual system tag prefix from constants
_SYS_TAG_KEY = f"{constants.SYSTEM_TAG_PREFIX}src:abc"


def _make_context():
    """Create a DataContext for tests."""
    from orcapod.contexts import resolve_context
    return resolve_context(None)


# ---------------------------------------------------------------------------
# System tags stored separately from data columns
# ---------------------------------------------------------------------------

class TestTagSystemTagsSeparation:
    """System tags are stored separately from data columns."""

    def test_system_tags_not_in_keys_by_default(self):
        ctx = _make_context()
        tag = Tag({"x": 1, "y": "hello"}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        keys = list(tag.keys())
        assert "x" in keys
        assert "y" in keys
        assert not any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in keys)

    def test_system_tags_not_in_as_dict_by_default(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        d = tag.as_dict()
        assert not any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in d)

    def test_system_tags_not_in_as_table_by_default(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        table = tag.as_table()
        assert not any(name.startswith(constants.SYSTEM_TAG_PREFIX) for name in table.column_names)

    def test_system_tags_not_in_schema_by_default(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        s = tag.schema()
        assert not any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in s)


# ---------------------------------------------------------------------------
# System tags included with ColumnConfig
# ---------------------------------------------------------------------------

class TestTagSystemTagsWithConfig:
    """With ColumnConfig system_tags=True or all_info=True, system tags are included."""

    def test_keys_with_system_tags_true(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        keys = list(tag.keys(columns=ColumnConfig(system_tags=True)))
        assert any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in keys)

    def test_as_dict_with_system_tags_true(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        d = tag.as_dict(columns=ColumnConfig(system_tags=True))
        assert any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in d)

    def test_as_table_with_system_tags_true(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        table = tag.as_table(columns=ColumnConfig(system_tags=True))
        assert any(name.startswith(constants.SYSTEM_TAG_PREFIX) for name in table.column_names)

    def test_keys_with_all_info(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        keys = list(tag.keys(columns=ColumnConfig.all()))
        assert any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in keys)

    def test_schema_with_system_tags_true(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        s = tag.schema(columns=ColumnConfig(system_tags=True))
        assert any(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in s)


# ---------------------------------------------------------------------------
# system_tags() returns a dict COPY
# ---------------------------------------------------------------------------

class TestTagSystemTagsCopy:
    """system_tags() returns a dict COPY (not a reference)."""

    def test_system_tags_returns_dict(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        st = tag.system_tags()
        assert isinstance(st, dict)
        assert _SYS_TAG_KEY in st

    def test_system_tags_is_copy(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        st = tag.system_tags()
        st[_SYS_TAG_KEY] = "modified"
        # Original should be unchanged
        assert tag.system_tags()[_SYS_TAG_KEY] == "val"


# ---------------------------------------------------------------------------
# copy() preserves system tags
# ---------------------------------------------------------------------------

class TestTagCopy:
    """copy() preserves system tags."""

    def test_copy_preserves_system_tags(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={_SYS_TAG_KEY: "val"})
        copied = tag.copy()
        assert copied is not tag
        assert copied.system_tags() == tag.system_tags()

    def test_copy_preserves_data(self):
        ctx = _make_context()
        tag = Tag({"x": 1, "y": "hello"}, data_context=ctx, system_tags={})
        copied = tag.copy()
        assert copied["x"] == 1
        assert copied["y"] == "hello"


# ---------------------------------------------------------------------------
# as_datagram() returns Datagram, not Tag
# ---------------------------------------------------------------------------

class TestTagAsDatagram:
    """as_datagram() returns a Datagram (not Tag)."""

    def test_as_datagram_returns_datagram_type(self):
        ctx = _make_context()
        tag = Tag({"x": 1}, data_context=ctx, system_tags={})
        dg = tag.as_datagram()
        assert isinstance(dg, Datagram)
        assert not isinstance(dg, Tag)

    def test_as_datagram_preserves_data(self):
        ctx = _make_context()
        tag = Tag({"x": 1, "y": "hello"}, data_context=ctx, system_tags={})
        dg = tag.as_datagram()
        assert dg["x"] == 1
        assert dg["y"] == "hello"
