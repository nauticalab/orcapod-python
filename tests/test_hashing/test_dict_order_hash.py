"""A dict's identity does not depend on its key order (ITL-905).

A ``dict[K, V]`` column is stored as ``large_list<struct<key, value>>``. Equal Python
dicts must give equal row hashes whatever order their entries were inserted in, on every
path that turns a Python dict into Arrow: sources, nested dicts, dataclass fields,
function pod outputs (sync and async), and cached results read back from a store. An
Arrow table that already holds a key/value list is hashed exactly as stored.
"""

from __future__ import annotations

import tarfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.datagrams import Datagram
from orcapod.core.function_pod import function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource, DictSource

V020_STORE = Path(__file__).parent / "data" / "v0_2_0_dict_store.tar.gz"

KV_TYPE = pa.large_list(
    pa.struct([pa.field("key", pa.large_string()), pa.field("value", pa.int64())])
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source(value: Any, value_type: Any = None) -> DictSource:
    """Return a one-row ``DictSource`` whose ``m`` column holds ``value``."""
    schema = None if value_type is None else {"k": str, "m": value_type}
    return DictSource(
        data=[{"k": "r", "m": value}], tag_columns=["k"], data_schema=schema
    )


def _data(value: Any, value_type: Any = None) -> Any:
    """Return the ``Data`` of the one row of ``_source(value, value_type)``."""
    _, data = next(iter(_source(value, value_type).iter_data()))
    return data


def _row_hash(value: Any, value_type: Any = None) -> str:
    """Return the row data hash of ``value`` stored in a one-row source."""
    return _data(value, value_type).content_hash().to_string()


def _arrow_source(entries: list[dict[str, Any]]) -> ArrowTableSource:
    """Return a one-row ``ArrowTableSource`` holding ``entries`` as a key/value list."""
    # Non-nullable like a DictSource column declared ``dict[str, int]``.
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "m": pa.array([entries], KV_TYPE),
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("m", KV_TYPE, nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="itl905")


def _arrow_row_hash(entries: list[dict[str, Any]]) -> str:
    """Return the row data hash of a key/value list given in Arrow form."""
    _, data = next(iter(_arrow_source(entries).iter_data()))
    return data.content_hash().to_string()


def total(m: dict[str, int]) -> int:
    """Return the sum of the dict's values (the pod cached in the v0.2.0 store)."""
    return sum(m.values())


def tally(n: int) -> dict[str, int]:
    """Return a dict built out of key order (the pod cached in the v0.2.0 store)."""
    return {"z": n, "a": n}


def echo(m: dict[str, int]) -> dict[str, int]:
    """Return the input dict unchanged."""
    return m


def reorder(m: dict[str, int]) -> dict[str, int]:
    """Return an equal dict in reverse insertion order."""
    return dict(reversed(list(m.items())))


def _pod(fn: Any, output_key: str) -> Any:
    """Return the function pod of ``fn`` with one output key."""
    return function_pod(output_keys=output_key)(fn).pod


def _keys(value: Any) -> list[Any]:
    """Return a dict value's keys in iteration order."""
    assert isinstance(value, dict)
    return list(value)


def _stored(source: Any, column: str) -> Any:
    """Return the first row's stored (Arrow) value of ``column``."""
    return source.as_table().column(column).to_pylist()[0]


@dataclass
class Holder:
    """A dataclass with a dict field: dicts reach Arrow through the logical type."""

    name: str
    counts: dict[str, int]


# ---------------------------------------------------------------------------
# Python values through the converter
# ---------------------------------------------------------------------------


class TestPythonDictValues:
    """Python dicts reach Arrow in canonical key order."""

    def test_inferred_schema_reordered_dicts_hash_equal(self):
        """Reordered dicts hash equal with an inferred schema."""
        assert _row_hash({"a": 1, "b": 2}) == _row_hash({"b": 2, "a": 1})

    def test_declared_schema_reordered_dicts_hash_equal(self):
        """Reordered dicts hash equal with a declared ``dict[str, int]``."""
        t = dict[str, int]
        assert _row_hash({"a": 1, "b": 2}, t) == _row_hash({"b": 2, "a": 1}, t)

    def test_stored_entries_are_in_key_order(self):
        """The stored key/value list is sorted by key."""
        entries = _stored(_source({"b": 2, "a": 1}), "m")
        assert [e["key"] for e in entries] == ["a", "b"]

    def test_str_keys_order_by_code_point_not_locale(self):
        """``str`` keys sort by code point."""
        entries = _stored(_source({"é": 1, "z": 2, "A": 3}), "m")
        assert [e["key"] for e in entries] == ["A", "z", "é"]

    def test_different_values_still_hash_differently(self):
        """Sorting does not merge dicts with different values."""
        assert _row_hash({"a": 1, "b": 2}) != _row_hash({"a": 2, "b": 1})

    @pytest.mark.parametrize(
        ("value_type", "first", "second"),
        [
            (dict[int, str], {10: "a", 9: "b", -1: "c"}, {-1: "c", 9: "b", 10: "a"}),
            (dict[bool, int], {True: 1, False: 0}, {False: 0, True: 1}),
            (dict[bytes, int], {b"\xff": 1, b"\x00": 2}, {b"\x00": 2, b"\xff": 1}),
            (dict[float, int], {1.5: 1, -0.0: 2, -3.0: 3}, {-3.0: 3, -0.0: 2, 1.5: 1}),
            (
                dict[date, int],
                {date(2026, 1, 2): 1, date(2025, 5, 1): 2},
                {date(2025, 5, 1): 2, date(2026, 1, 2): 1},
            ),
            (
                dict[datetime, int],
                {
                    datetime(2026, 1, 2, tzinfo=UTC): 1,
                    datetime(2025, 5, 1, tzinfo=UTC): 2,
                },
                {
                    datetime(2025, 5, 1, tzinfo=UTC): 2,
                    datetime(2026, 1, 2, tzinfo=UTC): 1,
                },
            ),
        ],
    )
    def test_key_types_reordered_hash_equal(self, value_type, first, second):
        """Every supported key type orders deterministically."""
        assert _row_hash(first, value_type) == _row_hash(second, value_type)

    def test_nan_key_is_rejected_naming_the_column(self):
        """A NaN key has no order and raises, naming the column."""
        with pytest.raises(ValueError, match="'m'.*NaN|NaN.*'m'"):
            _row_hash({float("nan"): 1, 1.0: 2}, dict[float, int])

    def test_numpy_nan_key_is_rejected(self):
        """A NaN that is not a Python ``float`` (``numpy.float32``) is rejected too."""
        with pytest.raises(ValueError, match="NaN"):
            _row_hash({np.float32("nan"): 1, np.float32(1.0): 2}, dict[float, int])


class TestNestedDicts:
    """Dicts nested in dicts, lists and dataclass fields are ordered at every depth."""

    def test_dict_of_dicts_inner_and_outer_reordered(self):
        """Inner and outer dicts are both ordered."""
        t = dict[str, dict[str, int]]
        first = {"x": {"a": 1, "b": 2}, "y": {"c": 3}}
        second = {"y": {"c": 3}, "x": {"b": 2, "a": 1}}
        assert _row_hash(first, t) == _row_hash(second, t)

    def test_list_of_dicts_reordered_inside(self):
        """Dicts inside a list are ordered."""
        t = list[dict[str, int]]
        assert _row_hash([{"a": 1, "b": 2}, {"c": 3}], t) == _row_hash(
            [{"b": 2, "a": 1}, {"c": 3}], t
        )

    def test_list_of_dicts_keeps_list_order(self):
        """The list itself keeps its order."""
        t = list[dict[str, int]]
        assert _row_hash([{"a": 1}, {"c": 3}], t) != _row_hash([{"c": 3}, {"a": 1}], t)

    def test_dict_of_list_of_dicts_reordered(self):
        """Dicts inside a list inside a dict are ordered."""
        t = dict[str, list[dict[str, int]]]
        first = {"x": [{"a": 1, "b": 2}], "w": []}
        second = {"w": [], "x": [{"b": 2, "a": 1}]}
        assert _row_hash(first, t) == _row_hash(second, t)

    def test_dict_inside_dataclass_field_reordered(self):
        """A dict in a dataclass field is ordered when stored."""
        first = Holder("h", {"a": 1, "b": 2})
        second = Holder("h", {"b": 2, "a": 1})
        assert _row_hash(first, Holder) == _row_hash(second, Holder)

    def test_dataclass_dict_storage_is_canonical_but_fresh_object_is_unchanged(self):
        """User objects keep their dict order; stored and read-back values are canonical."""
        original = Holder("h", {"b": 2, "a": 1})
        # Registered dataclasses work at runtime but are absent from DataValue's union.
        fresh_values: dict[str, Any] = {"m": original}
        ordered_values: dict[str, Any] = {"m": Holder("h", {"a": 1, "b": 2})}
        fresh = Datagram(fresh_values, python_schema={"m": Holder})
        ordered = Datagram(ordered_values, python_schema={"m": Holder})
        assert fresh.as_dict()["m"] is original
        assert list(original.counts) == ["b", "a"]
        assert fresh.content_hash() == ordered.content_hash()
        assert fresh.as_table().equals(ordered.as_table())

        restored = Datagram(fresh.as_table()).as_dict()["m"]
        assert isinstance(restored, Holder)
        assert list(restored.counts) == ["a", "b"]
        assert restored == original
        assert list(original.counts) == ["b", "a"]


# ---------------------------------------------------------------------------
# Arrow tables that already hold key/value lists
# ---------------------------------------------------------------------------


class TestArrowNativeKeyValueLists:
    """An Arrow table is hashed exactly as stored (developer, November 11, 2026).

    A ``list<struct<key, value>>`` that arrives already in Arrow form is an ordered list
    at the Arrow level; Python reads it as a dict, but its stored order is part of its
    hash. Canonical order is established only where a Python dict becomes Arrow.
    """

    def test_unsorted_entries_hash_as_stored(self):
        """An unsorted Arrow key/value list keeps its order in the hash."""
        sorted_entries = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        unsorted_entries = [{"key": "b", "value": 2}, {"key": "a", "value": 1}]
        assert _arrow_row_hash(sorted_entries) != _arrow_row_hash(unsorted_entries)

    def test_key_sorted_entries_match_python_dicts_in_any_order(self):
        """A key-sorted Arrow list hashes like the Python dict."""
        sorted_entries = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        t = dict[str, int]
        assert _arrow_row_hash(sorted_entries) == _row_hash({"b": 2, "a": 1}, t)
        assert _arrow_row_hash(sorted_entries) == _row_hash({"a": 1, "b": 2}, t)

    def test_user_table_is_not_rewritten(self):
        """Hashing leaves the user's Arrow table as given."""
        unsorted_entries = [{"key": "b", "value": 2}, {"key": "a", "value": 1}]
        source = _arrow_source(unsorted_entries)
        _, data = next(iter(source.iter_data()))
        data.content_hash()
        assert source.as_table().column("m").to_pylist()[0] == unsorted_entries

    def test_ordered_struct_pairs_are_not_sorted(self):
        """A list of ``f0``/``f1`` pairs is an ordered list, never sorted."""
        pair_type = pa.large_list(
            pa.struct([pa.field("f0", pa.large_string()), pa.field("f1", pa.int64())])
        )

        def pair_hash(entries: list[dict[str, Any]]) -> str:
            """Return the row hash of ``entries`` stored as a list of ordered pairs."""
            table = pa.table(
                {
                    "k": pa.array(["r"], pa.large_string()),
                    "m": pa.array([entries], pair_type),
                }
            )
            src = ArrowTableSource(table, tag_columns=["k"], source_id="pairs")
            _, data = next(iter(src.iter_data()))
            return data.content_hash().to_string()

        assert pair_hash([{"f0": "a", "f1": 1}, {"f0": "b", "f1": 2}]) != pair_hash(
            [{"f0": "b", "f1": 2}, {"f0": "a", "f1": 1}]
        )


# ---------------------------------------------------------------------------
# Function pods: outputs, async, caches
# ---------------------------------------------------------------------------


class TestFunctionPodDictOutputs:
    """Dicts returned by a function pod, sync and async, are ordered."""

    def test_pod_output_reordered_dicts_hash_equal(self):
        """Equal pod outputs built in different orders hash equal."""
        source = _source({"a": 1, "b": 2}, dict[str, int])
        [(_, plain)] = list(_pod(echo, "out").process(source).iter_data())
        [(_, flipped)] = list(_pod(reorder, "out").process(source).iter_data())
        assert plain.content_hash() == flipped.content_hash()

    def test_fresh_pod_output_iterates_in_key_order(self):
        """A fresh pod output dict iterates in key order."""
        source = _source({"b": 2, "a": 1}, dict[str, int])
        [(_, out)] = list(_pod(echo, "out").process(source).iter_data())
        assert _keys(out.as_dict()["out"]) == ["a", "b"]

    def test_pod_does_not_mutate_the_returned_dict(self):
        """The dict the pod returned keeps its own order."""
        returned: list[dict[str, int]] = []

        def keep(m: dict[str, int]) -> dict[str, int]:
            """Return a new dict out of key order and remember it."""
            result = {"b": m["b"], "a": m["a"]}
            returned.append(result)
            return result

        source = _source({"a": 1, "b": 2}, dict[str, int])
        list(_pod(keep, "out").process(source).iter_data())
        assert list(returned[0]) == ["b", "a"]

    @pytest.mark.asyncio
    async def test_async_output_matches_sync_and_is_order_independent(self):
        """The async path stores and hashes like the sync path."""
        source = _source({"a": 1, "b": 2}, dict[str, int])
        tag, data = next(iter(source.iter_data()))
        _, sync_out = _pod(echo, "out").process_data(tag, data)
        _, async_plain = await _pod(echo, "out").async_process_data(tag, data)
        _, async_flipped = await _pod(reorder, "out").async_process_data(tag, data)
        assert sync_out is not None
        assert async_plain is not None
        assert async_flipped is not None
        assert sync_out.content_hash() == async_plain.content_hash()
        assert async_plain.content_hash() == async_flipped.content_hash()
        assert _keys(async_flipped.as_dict()["out"]) == ["a", "b"]


# ---------------------------------------------------------------------------
# Datagram column updates
# ---------------------------------------------------------------------------


class TestDatagramColumnUpdates:
    """``update`` and ``with_columns`` hold dict values in canonical key order."""

    @pytest.mark.parametrize("operation", ["update", "with_columns"])
    def test_arrow_backed_dict_write_keeps_schema_and_canonical_order(
        self, operation: str
    ):
        """Dict writes through a known Arrow field retain its schema and canonical keys."""
        original = Datagram({"m": {"a": 1}}, python_schema={"m": dict[str, int]})
        field = original.as_table().schema.field("m")
        value = {"b": 2, "a": 1}
        if operation == "update":
            result = original.update(m=value)
            name = "m"
        else:
            result = original.with_columns(column_types={"added": field}, added=value)
            name = "added"
        assert (
            result.as_table()
            .schema.field(name)
            .equals(field.with_name(name), check_metadata=True)
        )
        assert _keys(result.as_dict()[name]) == ["a", "b"]
        assert list(value) == ["b", "a"]

    def test_update_holds_dict_in_key_order(self):
        """A replaced dict value iterates in key order, like a constructed one."""
        datagram = Datagram({"m": {"a": 1}}, python_schema={"m": dict[str, int]})
        updated = datagram.update(m={"b": 2, "a": 1})
        assert _keys(updated.as_dict()["m"]) == ["a", "b"]

    def test_with_columns_holds_dict_in_key_order(self):
        """A new dict column iterates in key order, like a constructed one."""
        datagram = Datagram({"x": 1}, python_schema={"x": int})
        extended = datagram.with_columns(
            column_types={"m": dict[str, int]}, m={"b": 2, "a": 1}
        )
        assert _keys(extended.as_dict()["m"]) == ["a", "b"]

    def test_update_and_with_columns_do_not_mutate_the_given_dict(self):
        """The caller's dict keeps its own order."""
        given = {"b": 2, "a": 1}
        Datagram({"m": {"a": 1}}, python_schema={"m": dict[str, int]}).update(m=given)
        Datagram({"x": 1}, python_schema={"x": int}).with_columns(
            column_types={"m": dict[str, int]}, m=given
        )
        assert list(given) == ["b", "a"]


def _extract_data_only(archive: tarfile.TarFile, target: Path) -> None:
    """Extract ``archive`` under ``target``, allowing only plain files and directories.

    ``extractall(filter="data")`` exists from Python 3.11.4; on older 3.11 releases the
    same guarantee is checked by hand: no links or devices, no absolute or ``..`` paths.
    """
    if hasattr(tarfile, "data_filter"):
        archive.extractall(target, filter="data")
        return
    root = target.resolve()
    for member in archive.getmembers():
        destination = (target / member.name).resolve()
        if not (member.isfile() or member.isdir()) or not destination.is_relative_to(
            root
        ):
            raise ValueError(f"unsafe archive member: {member.name!r}")
    archive.extractall(target)  # noqa: S202 -- every member was checked above


def _delta_db(tmp_path: Path) -> DeltaTableDatabase:
    """Return a Delta result store under ``tmp_path``."""
    return DeltaTableDatabase(tmp_path / "store")


@pytest.fixture(params=["in_memory", "delta"])
def result_db(request, tmp_path):
    """Yield an in-memory or a Delta result store."""
    if request.param == "in_memory":
        return InMemoryArrowDatabase()
    return _delta_db(tmp_path)


class TestCachedPodHit:
    """Equal dicts built in different orders share one cache entry."""

    def test_reordered_equal_dict_is_a_cache_hit(self, result_db):
        """An equal input dict in another order is a cache hit."""
        pod = CachedFunctionPod(_pod(total, "total"), result_database=result_db)
        first = _source({"b": 2, "a": 1}, dict[str, int])
        second = _source({"a": 1, "b": 2}, dict[str, int])

        [(_, out1)] = list(pod.process(first).iter_data())
        result_db.flush()
        [(_, out2)] = list(pod.process(second).iter_data())

        assert out1.get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert out2.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
        records = result_db.get_all_records(pod.record_path)
        assert records is not None
        assert records.num_rows == 1

    @pytest.mark.asyncio
    async def test_reordered_equal_dict_is_an_async_cache_hit(self, result_db):
        """The same holds on the async path."""
        pod = CachedFunctionPod(_pod(total, "total"), result_database=result_db)
        tag1, data1 = next(iter(_source({"b": 2, "a": 1}, dict[str, int]).iter_data()))
        tag2, data2 = next(iter(_source({"a": 1, "b": 2}, dict[str, int]).iter_data()))

        _, out1 = await pod.async_process_data(tag1, data1)
        result_db.flush()
        _, out2 = await pod.async_process_data(tag2, data2)

        assert out1 is not None
        assert out2 is not None
        assert out1.get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert out2.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False

    def test_fresh_and_cached_dict_outputs_iterate_identically(self, result_db):
        """Fresh and cached outputs iterate in the same order."""
        pod = CachedFunctionPod(_pod(reorder, "out"), result_database=result_db)
        source = _source({"a": 1, "b": 2, "c": 3}, dict[str, int])

        [(_, fresh)] = list(pod.process(source).iter_data())
        result_db.flush()
        [(_, cached)] = list(pod.process(source).iter_data())

        assert cached.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
        assert _keys(fresh.as_dict()["out"]) == _keys(cached.as_dict()["out"])
        assert _keys(cached.as_dict()["out"]) == ["a", "b", "c"]
        assert fresh.content_hash() == cached.content_hash()


class TestV020Store:
    """A Delta result store written by the released v0.2.0 code.

    ``data/v0_2_0_dict_store.tar.gz`` was produced by v0.2.0 running ``total`` over two
    inputs: ``k=unsorted`` holding ``{"b": 2, "a": 1}`` (stored in that order) and
    ``k=sorted`` holding ``{"a": 1, "c": 3}``; and ``tally`` over ``n=5``, whose output
    ``{"z": 5, "a": 5}`` is stored in insertion order.
    ``data/generate_v0_2_0_dict_store.py`` regenerates it from a checkout of the
    ``v0.2.0`` tag.

    v0.3 reads no table v0.2 wrote (new table layouts and paths, ITL-944/ITL-945), so
    no row stored in v0.2's insertion order is ever served: every input is computed
    again, whether its dict was in key order or not, and the v0.2 tables stay as they
    were.
    """

    @pytest.fixture
    def store(self, tmp_path):
        """Return the v0.2.0 Delta store, extracted under ``tmp_path``."""
        # The store is shipped as a tarball: its directory names contain ":".
        target = tmp_path / "v020_store"
        with tarfile.open(V020_STORE) as archive:
            _extract_data_only(archive, target)
        return DeltaTableDatabase(target)

    def _inputs(self) -> DictSource:
        """Return the two inputs the v0.2.0 store was built from."""
        return DictSource(
            data=[
                {"k": "unsorted", "m": {"b": 2, "a": 1}},
                {"k": "sorted", "m": {"a": 1, "c": 3}},
            ],
            tag_columns=["k"],
            data_schema={"k": str, "m": dict[str, int]},
            source_id="itl905_v020",
        )

    @staticmethod
    def _row_counts(
        store: DeltaTableDatabase, paths: list[tuple[str, ...]]
    ) -> dict[tuple[str, ...], int]:
        """Return the number of rows in each of ``paths``."""
        counts = {}
        for path in paths:
            records = store.get_all_records(path)
            counts[path] = 0 if records is None else records.num_rows
        return counts

    def test_v020_rows_are_not_read_and_stay(self, store):
        """Every input is computed once; the v0.2.0 tables are left as they were."""
        old_paths = store.list_table_paths(("total",))
        old_counts = self._row_counts(store, old_paths)
        assert sorted(old_counts.values()) == [2]

        pod = CachedFunctionPod(_pod(total, "total"), result_database=store)
        assert pod.record_path not in old_paths
        assert store.get_all_records(pod.record_path) is None

        results = {
            tag.as_dict()["k"]: out
            for tag, out in pod.process(self._inputs()).iter_data()
        }
        store.flush()

        # Neither the key-ordered nor the out-of-order input is served from v0.2.0.
        assert results["sorted"].get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert results["unsorted"].get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert results["unsorted"].as_dict()["total"] == 3
        assert self._row_counts(store, old_paths) == old_counts
        new_records = store.get_all_records(pod.record_path)
        assert new_records is not None
        assert new_records.num_rows == 2

        again = {
            tag.as_dict()["k"]: out
            for tag, out in pod.process(self._inputs()).iter_data()
        }
        assert all(
            out.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
            for out in again.values()
        )

    def test_v020_dict_output_in_insertion_order_is_never_served(self, store):
        """A dict output v0.2.0 stored out of key order is recomputed, not served."""
        inputs = DictSource(
            data=[{"k": "r", "n": 5}],
            tag_columns=["k"],
            data_schema={"k": str, "n": int},
            source_id="itl905_v020_int",
        )
        old_paths = store.list_table_paths(("tally",))
        [old_path] = old_paths
        old_row = store.get_all_records(old_path)
        assert old_row is not None
        # The released store holds the output in insertion order.
        assert [e["key"] for e in old_row.column("counts")[0].as_py()] == ["z", "a"]

        pod = CachedFunctionPod(_pod(tally, "counts"), result_database=store)
        [(_, computed)] = list(pod.process(inputs).iter_data())
        store.flush()
        [(_, cached)] = list(pod.process(inputs).iter_data())

        assert computed.get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert cached.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
        assert _keys(computed.as_dict()["counts"]) == ["a", "z"]
        assert _keys(cached.as_dict()["counts"]) == ["a", "z"]
        assert cached.content_hash() == computed.content_hash()
        assert self._row_counts(store, old_paths) == {old_path: 1}
