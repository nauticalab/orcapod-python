"""The hash label rule (ITL-945; ITL-938 design, section 5.6).

1. A label names the algorithm that produced a digest; its format is ``<algorithm>_v<N>``.
   The file content hash keeps ``sha256``.
2. A label never enters a hash computation: renaming a label changes no digest.
3. A hash inside a name (path component, function uri, source id, slot name) is the bare
   hex digest.
4. A hash stored as a value carries its label, including the row keys of operator-cache
   and cached-source tables.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import orcapod
import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.sources import CachedSource
from orcapod.core.sources.derived_source import DerivedSource
from orcapod.core.sources.list_source import ListSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing import directory_hashers, pandas_value_hash
from orcapod.hashing.file_hashers import FileHasher
from orcapod.hashing.semantic_hashing import polars_expr
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    NumpyArrayHandler,
    PandasDataFrameHandler,
    PandasSeriesHandler,
    PolarsDataFrameHandler,
    PolarsSeriesHandler,
)
from orcapod.operators import Batch, GroupBy, Join
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource
from orcapod.types import CacheMode, ContentHash
from tests.test_hashing.golden import label_feeds

LABEL_FORMAT = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
BARE_SHA256 = re.compile(r"[0-9a-f]{64}")
# A starfix digest: 3 bytes of hash format number, then SHA-256.
ARROW_DIGEST_BYTES = 35
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@function_pod(output_keys="doubled")
def label_rule_double(x: int, y: int) -> int:
    """Function pod used by the tests of names and stored values."""
    return (x + y) * 2


def _source() -> DictSource:
    """Return a two-row source with tag ``id`` and data column ``x``."""
    return DictSource(
        data=[{"id": "a", "x": 1}, {"id": "b", "x": 2}],
        tag_columns=["id"],
        tag_only_columns=True,
        source_id="label_rule_source",
    )


def _other_source() -> DictSource:
    """Return a two-row source with tag ``id`` and data column ``y``."""
    return DictSource(
        data=[{"id": "a", "y": 10}, {"id": "b", "y": 20}],
        tag_columns=["id"],
        tag_only_columns=True,
        source_id="label_rule_other",
    )


# ---------------------------------------------------------------------------
# Rule 2: the format
# ---------------------------------------------------------------------------


class TestLabelFormat:
    """Rule 2: every label is ``<algorithm>_v<N>``; the file hash keeps ``sha256``."""

    def test_structure_hash_label(self) -> None:
        """The structure hasher labels its hashes ``semantic_v1``."""
        hasher = get_default_context().semantic_hasher
        assert hasher.hasher_id == "semantic_v1"
        assert hasher.hash_object({"a": 1}).method == "semantic_v1"

    def test_arrow_hash_label(self) -> None:
        """The Arrow hasher labels its hashes ``arrow_v1``."""
        hasher = get_default_context().arrow_hasher
        assert hasher.hasher_id == "arrow_v1"
        assert hasher.hash_table(pa.table({"x": [1]})).method == "arrow_v1"

    def test_ordered_arrow_hash_has_its_own_label(self) -> None:
        """The order-including Arrow hash is a procedure of its own: ``arrow_ordered_v1``."""
        hasher = get_default_context().arrow_hasher
        table = pa.table({"x": [1]})
        assert hasher.hash_table_ordered(table).method == "arrow_ordered_v1"

    def test_value_hash_labels(self, tmp_path: Path) -> None:
        """Array, pandas, Polars expression and directory values carry their own label."""
        hasher = get_default_context().semantic_hasher
        (tmp_path / "f.txt").write_text("hello")
        assert hasher.hash_object(np.arange(3)).method == "ndarray_v1"
        frame = pd.DataFrame({"a": [1]})
        assert hasher.hash_object(frame).method == "pandas_dataframe_v1"
        assert hasher.hash_object(frame["a"]).method == "pandas_series_v1"
        assert hasher.hash_object(pl.col("x") > 1).method == "polars_expr_v1"
        frame = pl.DataFrame({"x": [1]})
        assert hasher.hash_object(frame).method == "polars_dataframe_v1"
        assert hasher.hash_object(frame["x"]).method == "polars_series_v1"
        assert hasher.hash_object(op.Directory(tmp_path)).method == "directory_v1"

    def test_file_hash_keeps_the_digest_function_name(self, tmp_path: Path) -> None:
        """A file hash is labelled ``sha256``."""
        # The one stated exception to the format.
        path = tmp_path / "f.txt"
        path.write_text("hello")
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(op.File(path)).method == "sha256"

    def test_every_label_but_the_file_hash_follows_the_format(
        self, tmp_path: Path
    ) -> None:
        """Every label except the file hash matches ``<algorithm>_v<N>``."""
        context = get_default_context()
        (tmp_path / "f.txt").write_text("hello")
        labels = {
            context.semantic_hasher.hasher_id,
            context.arrow_hasher.hasher_id,
            context.semantic_hasher.hash_object(np.arange(3)).method,
            context.semantic_hasher.hash_object(pd.DataFrame({"a": [1]})).method,
            context.semantic_hasher.hash_object(pd.Series([1])).method,
            context.semantic_hasher.hash_object(pl.col("x") > 1).method,
            context.semantic_hasher.hash_object(pl.DataFrame({"x": [1]})).method,
            context.semantic_hasher.hash_object(pl.Series("x", [1])).method,
            context.semantic_hasher.hash_object(op.Directory(tmp_path)).method,
        }
        assert all(LABEL_FORMAT.fullmatch(label) for label in labels), labels

    def test_directory_label_names_the_sha256_procedure_only(self) -> None:
        """The directory hasher refuses any algorithm but SHA-256."""
        with pytest.raises(ValueError, match="sha256"):
            directory_hashers.BasicDirectoryHasher(
                file_hasher=FileHasher(), algorithm="md5"
            )


# ---------------------------------------------------------------------------
# Rule 1 (ITL-1154): a hash fed into another hash carries its label
# ---------------------------------------------------------------------------


class TestNestedHashCarriesItsLabel:
    """In the structure hasher a nested hash enters as its label and digest."""

    def test_nested_hash_is_its_tagged_label_and_digest(self) -> None:
        """A nested hash is ``{"__type__": "hash", "label": ..., "digest": ...}``."""
        hasher = get_default_context().semantic_hasher
        digest = bytes(range(32))
        tree = ["x", {"__type__": "hash", "digest": digest.hex(), "label": "one_v1"}]
        text = json.dumps(tree, sort_keys=True, separators=(",", ":"))
        expected = hashlib.sha256(text.encode("utf-8")).digest()
        assert hasher.hash_object(["x", ContentHash("one_v1", digest)]).digest == (
            expected
        )

    def test_nested_hashes_that_differ_only_in_label_differ(self) -> None:
        """Two nested hashes with one digest and different labels give different hashes."""
        hasher = get_default_context().semantic_hasher
        digest = bytes(range(32))
        one = hasher.hash_object(["x", ContentHash("one_v1", digest)])
        other = hasher.hash_object(["x", ContentHash("other_v7", digest)])
        assert one != other

    def test_nested_hash_differs_from_its_hex_string(self) -> None:
        """A nested hash and a string equal to its hex digest hash differently."""
        hasher = get_default_context().semantic_hasher
        digest = bytes(range(32))
        as_hash = hasher.hash_object(["x", ContentHash("one_v1", digest)])
        as_text = hasher.hash_object(["x", digest.hex()])
        assert as_hash != as_text

    def test_nested_hashes_with_different_digests_differ(self) -> None:
        """Different nested digests give different enclosing hashes."""
        hasher = get_default_context().semantic_hasher
        one = hasher.hash_object([ContentHash("one_v1", bytes(32))])
        other = hasher.hash_object([ContentHash("one_v1", bytes([1] * 32))])
        assert one != other


@pytest.fixture
def renamed_labels() -> Iterator[dict[str, str]]:
    """Rename every label in place, for the duration of one test.

    ``label_feeds.rename_labels`` sets the non-public definitions (there is no public way
    to rename a label); the fixture restores them afterwards.
    """
    from orcapod.hashing import arrow_hashers

    context = get_default_context()
    saved: list[tuple[object, str, object]] = [
        (context.semantic_hasher, "_hasher_id", context.semantic_hasher.hasher_id),
        (context.arrow_hasher, "_hasher_id", context.arrow_hasher.hasher_id),
        (NumpyArrayHandler, "HASH_LABEL", NumpyArrayHandler.HASH_LABEL),
        (PandasDataFrameHandler, "HASH_LABEL", PandasDataFrameHandler.HASH_LABEL),
        (PandasSeriesHandler, "HASH_LABEL", PandasSeriesHandler.HASH_LABEL),
        (PolarsDataFrameHandler, "HASH_LABEL", PolarsDataFrameHandler.HASH_LABEL),
        (PolarsSeriesHandler, "HASH_LABEL", PolarsSeriesHandler.HASH_LABEL),
    ]
    try:  # torch is optional
        from orcapod.hashing.semantic_hashing.torch_handler import TorchTensorHandler
    except ImportError:
        pass
    else:
        saved.append((TorchTensorHandler, "HASH_LABEL", TorchTensorHandler.HASH_LABEL))
    for module, name in (
        (pandas_value_hash, "PANDAS_DATAFRAME_HASH_LABEL"),
        (pandas_value_hash, "PANDAS_SERIES_HASH_LABEL"),
        (polars_expr, "POLARS_EXPR_HASH_LABEL"),
        (directory_hashers, "DIRECTORY_HASH_LABEL"),
        (arrow_hashers, "ARROW_ORDERED_HASH_LABEL"),
    ):
        if hasattr(module, name):
            saved.append((module, name, getattr(module, name)))
    try:
        label_feeds.rename_labels()
        yield label_feeds.RENAMED
    finally:
        for owner, name, value in saved:
            setattr(owner, name, value)


_HEX_RUN = re.compile(r"[0-9a-f]{16,}")


def _subprocess_env() -> dict[str, str]:
    """The environment for a fresh interpreter: ``src`` on ``PYTHONPATH``.

    The child does not inherit pytest.ini's ``pythonpath = src``, so it is passed
    explicitly rather than relying on orcapod being installed.
    """
    env = dict(os.environ)
    src = str(PROJECT_ROOT / "src")
    env["PYTHONPATH"] = (
        os.pathsep.join([src, env["PYTHONPATH"]]) if env.get("PYTHONPATH") else src
    )
    return env


def _run_module(side: str) -> Any:
    """Run ``label_feeds`` in a fresh process and return its JSON output."""
    result = subprocess.run(
        [sys.executable, "-m", "tests.test_hashing.golden.label_feeds", side],
        capture_output=True,
        text=True,
        check=True,
        cwd=PROJECT_ROOT,
        env=_subprocess_env(),
    )
    return json.loads(result.stdout)


def _by_name_pattern(values: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    """Group entries by their name with every hex run replaced, so moved names align."""
    grouped: dict[str, list[tuple[str, str]]] = {}
    for name, value in values.items():
        key = _HEX_RUN.sub("<H>", name)
        grouped.setdefault(key, []).append((name, json.dumps(value, sort_keys=True)))
    return {key: sorted(entries) for key, entries in grouped.items()}


class TestRenamingALabelMovesEveryHashItFeeds:
    """Rule 1 end to end: renaming a label moves exactly the hashes it feeds."""

    def test_golden_values_move_exactly_where_a_label_feeds_them(self) -> None:
        """A golden value moves under renamed labels if and only if a label feeds it.

        One fresh process computes every golden value and records, for each digest it
        produces, the hashes fed into it (``label_feeds.record_feeds``); another computes
        every golden value under renamed labels. A value is fed when a digest in its name
        or value embeds a renamed label's hash, directly or through other digests.
        """
        recorded = _run_module("feeds")
        renamed: dict[str, dict[str, Any]] = _run_module("renamed")
        values: dict[str, dict[str, Any]] = recorded["values"]
        feeds: dict[str, list[list[str]]] = recorded["feeds"]

        memo: dict[str, bool] = {}

        def moves(digest: str) -> bool:
            if digest not in memo:
                memo[digest] = False
                memo[digest] = any(
                    label in label_feeds.RENAMED or moves(inner)
                    for label, inner in feeds.get(digest, [])
                )
            return memo[digest]

        def fed(run: str) -> bool:
            return (
                any(
                    moves(digest)
                    for digest in feeds
                    if digest.startswith(run) and len(run) >= 16
                )
                if len(run) < 64
                else moves(run)
            )

        moved: set[str] = set()
        reached: set[str] = set()
        for group, entries in values.items():
            before = _by_name_pattern(entries)
            after = _by_name_pattern(renamed[group])
            for key in before.keys() | after.keys():
                if before.get(key) != after.get(key):
                    moved.add(f"{group}:{key}")
                runs = [
                    run
                    for name, value in before.get(key, [])
                    for run in _HEX_RUN.findall(name + value)
                ]
                if any(fed(run) for run in runs):
                    reached.add(f"{group}:{key}")
        assert moved, "renaming the labels moved no golden value"
        assert sorted(moved - reached) == [], "moved, but no label feeds it"
        assert sorted(reached - moved) == [], "a label feeds it, but it did not move"

    def test_nested_directory_hash_moves(
        self, tmp_path: Path, request: pytest.FixtureRequest
    ) -> None:
        """A structure holding a ``Directory`` moves when ``directory_v1`` is renamed."""
        (tmp_path / "sub").mkdir()
        (tmp_path / "a.txt").write_text("alpha")
        (tmp_path / "sub" / "b.txt").write_text("beta")
        hasher = get_default_context().semantic_hasher

        before = hasher.hash_object(op.Directory(tmp_path))
        nested_before = hasher.hash_object(["tree", op.Directory(tmp_path)])
        renamed: dict[str, str] = request.getfixturevalue("renamed_labels")
        after = hasher.hash_object(op.Directory(tmp_path))
        nested_after = hasher.hash_object(["tree", op.Directory(tmp_path)])

        assert before.method == "directory_v1"
        assert after.method == renamed["directory_v1"]
        # The directory hash itself embeds no renamed label: its digest stays.
        assert after.digest == before.digest
        # The structure it is fed into moves.
        assert nested_after.digest != nested_before.digest

    def test_directory_entry_takes_the_file_hash_with_its_label(
        self, tmp_path: Path
    ) -> None:
        """A directory's file entry embeds ``b"sha256:" + file digest`` (D2)."""
        (tmp_path / "a.txt").write_bytes(b"alpha")
        file_digest = hashlib.sha256(b"alpha").digest()
        entry = hashlib.sha256(b"file\x00a.txt\x00" + b"sha256:" + file_digest).digest()
        expected = hashlib.sha256(b"directory\x00" + entry).digest()
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(op.Directory(tmp_path)).digest == expected

    def test_logical_type_cell_moves_with_the_value_label(
        self, request: pytest.FixtureRequest
    ) -> None:
        """A table with an array column moves when ``ndarray_v1`` is renamed."""
        context = get_default_context()
        stream = DictSource(
            data=[{"id": "a", "arr": np.arange(3)}],
            tag_columns=["id"],
            source_id="label_rule_arrays",
            tag_only_columns=True,
        )
        table = stream.as_table()
        before = context.arrow_hasher.hash_table(table)
        _ = request.getfixturevalue("renamed_labels")
        after = context.arrow_hasher.hash_table(table)
        assert after.digest != before.digest

    @pytest.mark.parametrize(
        "value",
        [pd.DataFrame({"a": [1, 2]}), pd.Series([1, 2], name="a")],
        ids=["dataframe", "series"],
    )
    def test_pandas_value_moves_with_the_ordered_arrow_label(
        self, value: Any, request: pytest.FixtureRequest
    ) -> None:
        """A pandas value hash takes the ordered Arrow digest with its label (D1)."""
        hasher = get_default_context().semantic_hasher
        before = hasher.hash_object(value)
        _ = request.getfixturevalue("renamed_labels")
        assert hasher.hash_object(value).digest != before.digest

    def test_plain_table_keeps_its_digest(self, request: pytest.FixtureRequest) -> None:
        """A table of plain columns embeds no hash: a renamed label leaves its digest."""
        context = get_default_context()
        table = pa.table({"id": ["a", "b"], "x": [1, 2]})
        before = context.arrow_hasher.hash_table(table)
        _ = request.getfixturevalue("renamed_labels")
        after = context.arrow_hasher.hash_table(table)
        assert after.digest == before.digest

    def test_entry_ids_move_with_the_data_hash_label(
        self, request: pytest.FixtureRequest
    ) -> None:
        """Pipeline entry ids move when the Arrow label of the input data hash is renamed."""

        def entry_ids() -> list[bytes]:
            _, store = _run_job()
            path = next(
                p
                for p in store.list_sources()
                if "label_rule_double" in p and "_result" not in p
            )
            records = store.get_all_records(path)
            assert records is not None
            return sorted(
                ContentHash.from_prefixed_digest(value).digest
                for value in cast(
                    "list[bytes]",
                    records.column("__pipeline_base_entry_id").to_pylist(),
                )
            )

        before = entry_ids()
        _ = request.getfixturevalue("renamed_labels")
        after = entry_ids()
        assert len(before) == 2
        assert len(after) == 2
        assert set(after).isdisjoint(before)

    def test_operator_cache_and_cached_source_keys_move(
        self, request: pytest.FixtureRequest
    ) -> None:
        """Both cache row keys move under renamed labels.

        The operator-cache row embeds the node content hash; both rows carry system-tag
        column names that hold the schema digest, which moves with the structure label.
        """
        before = _stored_row_keys()
        renamed: dict[str, str] = request.getfixturevalue("renamed_labels")
        after = _stored_row_keys()
        prefix_old, prefix_new = b"arrow_v1:", renamed["arrow_v1"].encode() + b":"
        before_digests = [key.removeprefix(prefix_old) for key in before]
        after_digests = [key.removeprefix(prefix_new) for key in after]
        assert all(key.startswith(prefix_new) for key in after)
        assert len(before_digests) == 4
        assert len(after_digests) == 4
        assert set(after_digests).isdisjoint(before_digests)

    def test_run_snapshot_moves_with_the_structure_label(
        self, request: pytest.FixtureRequest
    ) -> None:
        """The snapshot in the run URI is built from labelled sink hashes."""

        class _Uri(NoOpObserver):
            uri: str = ""

            def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
                del run_id
                type(self).uri = pipeline_uri

        def snapshot() -> str:
            job = PipelineJob(
                name="label_rule_snapshot", database=InMemoryArrowDatabase()
            )
            with job:
                joined = Join(label="joined")(_source(), _other_source())
                label_rule_double.pod(joined, label="double")
            job.run(observer=_Uri())
            return _Uri.uri.split("@")[1]

        before = snapshot()
        _ = request.getfixturevalue("renamed_labels")
        after = snapshot()
        assert BARE_SHA256.fullmatch(before.ljust(64, "0"))
        assert after != before

    def test_list_source_tag_function_hash_carries_its_label(
        self, request: pytest.FixtureRequest
    ) -> None:
        """A ``ListSource`` hashing its tag function's source text embeds that hash."""

        def content_hash() -> bytes:
            source = ListSource(
                name="item",
                data=[1, 2],
                tag_function=_label_rule_tag,
                expected_tag_keys=["index"],
                tag_function_hash_mode="content",
            )
            return source.content_hash().digest

        before = content_hash()
        _ = request.getfixturevalue("renamed_labels")
        assert content_hash() != before


def _label_rule_tag(element: int, index: int) -> dict[str, int]:
    """Tag function of the ``ListSource`` test: the element's position."""
    del element
    return {"index": index}


# ---------------------------------------------------------------------------
# Rule 5: bare digests inside names
# ---------------------------------------------------------------------------


def _run_cached_join() -> tuple[OperatorJobNode, InMemoryArrowDatabase]:
    """Run a join whose rows are written to an operator-cache table."""
    store = InMemoryArrowDatabase()
    node = OperatorJobNode(
        operator=Join(),
        input_streams=(_source(), _other_source()),
        pipeline_database=store,
        cache_mode=CacheMode.LOG,
    )
    node.run()
    return node, store


def _run_job() -> tuple[PipelineJob, InMemoryArrowDatabase]:
    """Run a job with a join and a function pod on an in-memory store."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(name="label_rule_job", database=store)
    with job:
        joined = Join(label="joined")(_source(), _other_source())
        label_rule_double.pod(joined, label="double")
    job.run()
    return job, store


class TestNamesHoldBareDigests:
    """Rule 5: a hash inside a name is the bare hex digest."""

    def test_function_uri_holds_the_bare_output_schema_digest(self) -> None:
        """The second component of a function ``uri`` is 64 hex characters."""
        uri = label_rule_double.pod.uri
        assert uri[0] == "label_rule_double"
        assert BARE_SHA256.fullmatch(uri[1]), uri

    def test_function_table_paths_hold_bare_digests(self) -> None:
        """Function table paths hold the bare output-schema and pipeline digests."""
        _, store = _run_job()
        paths = ["/".join(path) for path in store.list_sources()]
        function_paths = [p for p in paths if "label_rule_double" in p]
        assert function_paths
        for path in function_paths:
            assert ":" not in path.replace("schema:", ""), path
        pipeline_table = next(p for p in function_paths if "/_result/" not in p)
        components = pipeline_table.split("/")
        assert BARE_SHA256.fullmatch(components[2]), pipeline_table
        schema_component = next(c for c in components if c.startswith("schema:"))
        assert BARE_SHA256.fullmatch(schema_component.removeprefix("schema:"))

    def test_operator_table_path_holds_bare_digests(self) -> None:
        """An operator-cache table path holds bare digests, then the layout (``odb_v1``)."""
        node, store = _run_cached_join()
        (path,) = store.list_sources()
        assert path == node.node_identity_path + ("odb_v1",)
        assert path[0] == "Join"
        assert BARE_SHA256.fullmatch(path[1]), path
        assert path[2].startswith("schema:")
        assert BARE_SHA256.fullmatch(path[2].removeprefix("schema:")), path

    def test_no_stored_table_path_holds_a_label(self) -> None:
        """No component of a stored table path contains a hash label."""
        _, store = _run_job()
        for path in store.list_sources():
            for component in path:
                assert "semantic_v" not in component, path
                assert "arrow_v" not in component, path

    def test_cached_source_path_holds_the_bare_content_digest(self) -> None:
        """The cached-source path is ``node:<bare content digest>``, then the layout."""
        cached = CachedSource(_source(), cache_database=InMemoryArrowDatabase())
        assert cached.cache_path[-1] == "sdb_v1"
        component = cached.cache_path[-2]
        assert component.startswith("node:")
        assert BARE_SHA256.fullmatch(component.removeprefix("node:")), component
        assert component.removeprefix("node:") == cached.content_hash().to_hex()

    def test_default_derived_source_ids_keep_the_hash_label(self) -> None:
        """A source id is the source's name: a default built from a hash keeps its label.

        This is the stated exception to bare digests inside names (developer,
        November 11, 2026): a source-kind prefix, then the labelled hash.
        """
        job, _ = _run_job()
        node = job.nodes["double"]
        labelled = node.content_hash().to_string()
        assert labelled.startswith("semantic_v1:")

        # as_source() gives the DerivedSource default; the node path is not part
        # of the id (ITL-957).
        from_node = node.as_source()
        assert from_node.source_id == f"derived:{labelled}"

        plain = DerivedSource(origin=node)
        assert plain.source_id == f"derived:{labelled}"

    def test_source_info_token_of_a_function_output_holds_no_label(self) -> None:
        """A function output's source-info token starts with the bare function ``uri``."""
        stream = label_rule_double.pod(Join()(_source(), _other_source()))
        table = stream.as_table(columns={"source": True})
        tokens = cast("list[str]", table.column("_source_doubled").to_pylist())
        assert tokens
        for token in tokens:
            assert "semantic_v" not in token, token
            assert token.startswith(
                "label_rule_double:" + label_rule_double.pod.uri[1] + ":"
            )


# ---------------------------------------------------------------------------
# Rule 4: stored hash values carry their label
# ---------------------------------------------------------------------------


def _stored_row_keys() -> list[bytes]:
    """Row keys of an operator-cache table and of a cached-source table."""
    _, store = _run_cached_join()
    (operator_path,) = store.list_sources()
    records = store.get_all_records(operator_path, record_id_column="__row_key")
    assert records is not None
    keys = sorted(cast("list[bytes]", records.column("__row_key").to_pylist()))

    cache = InMemoryArrowDatabase()
    cached = CachedSource(_source(), cache_database=cache)
    assert cached.as_table().num_rows == 2
    cached_records = cache.get_all_records(
        cached.cache_path, record_id_column="__row_key"
    )
    assert cached_records is not None
    return keys + sorted(
        cast("list[bytes]", cached_records.column("__row_key").to_pylist())
    )


class TestStoredHashValuesCarryTheirLabel:
    """Rule 4: a hash stored as a value carries its label."""

    @pytest.mark.parametrize(
        ("operator", "label"),
        [(Batch(batch_size=2), "batch_v1"), (GroupBy(by=["cohort"]), "group_v1")],
        ids=["batch", "group_by"],
    )
    def test_folded_source_id_carries_the_fold_label(
        self, operator: Batch | GroupBy, label: str
    ) -> None:
        """A folded source id and record id are both ``"<fold label>:<hex>"``."""
        source = DictSource(
            data=[
                {"id": "a", "cohort": "x", "v": 1},
                {"id": "b", "cohort": "x", "v": 2},
            ],
            tag_columns=["id", "cohort"],
            source_id="label_rule_fold",
            tag_only_columns=True,
        )
        table = operator(source).as_table(columns={"system_tags": True})
        (source_column,) = [
            n for n in table.column_names if n.startswith("_tag_source_id")
        ]
        (record_column,) = [
            n for n in table.column_names if n.startswith("_tag_record_id")
        ]
        (folded,) = cast("list[str]", table.column(source_column).to_pylist())
        assert folded.startswith(f"{label}:")
        assert BARE_SHA256.fullmatch(folded.removeprefix(f"{label}:"))
        # The folded record id carries the operation's label too (ITL-1154).
        (record_id,) = cast("list[str]", table.column(record_column).to_pylist())
        assert record_id.startswith(f"{label}:")
        assert BARE_SHA256.fullmatch(record_id.removeprefix(f"{label}:"))

    def test_operator_cache_and_cached_source_row_keys_are_labelled(self) -> None:
        """The row keys of the two cache tables are ``b"arrow_v1:" + digest``."""
        keys = _stored_row_keys()
        assert len(keys) == 4
        for key in keys:
            assert key.startswith(b"arrow_v1:"), key
            parsed = ContentHash.from_prefixed_digest(key)
            assert len(parsed.digest) == ARROW_DIGEST_BYTES

    def test_function_tables_keep_labelled_hash_columns(self) -> None:
        """The hash columns of a function pipeline table stay labelled."""
        _, store = _run_job()
        path = next(
            p
            for p in store.list_sources()
            if "label_rule_double" in p and "_result" not in p
        )
        records = store.get_all_records(path)
        assert records is not None
        input_hashes = cast(
            "list[bytes]", records.column("__input_data_hash").to_pylist()
        )
        node_hashes = cast(
            "list[bytes]", records.column("__node_content_hash").to_pylist()
        )
        assert input_hashes and node_hashes
        assert all(value.startswith(b"arrow_v1:") for value in input_hashes)
        assert all(value.startswith(b"semantic_v1:") for value in node_hashes)


# ---------------------------------------------------------------------------
# Rule 6 at a stream with no producer: its identity covers every column group but the
# derived per-row content hash (whose values carry the Arrow label)
# ---------------------------------------------------------------------------


def _bare_stream(
    *,
    tag: str = "a",
    data: int = 1,
    system_tag: str = "s",
    source_info: str = "origin",
    meta: str = "m",
    context_key: str | None = None,
) -> ArrowTableStream:
    """Build a one-row stream with no producer; each argument sets one value."""
    names = ["id", "x", "__note", "_tag_origin"]
    arrays: list[pa.Array[Any]] = [
        pa.array([tag], pa.large_string()),
        pa.array([data], pa.int64()),
        pa.array([meta], pa.large_string()),
        pa.array([system_tag], pa.large_string()),
    ]
    if context_key is not None:
        names.append("_context_key")
        arrays.append(pa.array([context_key], pa.large_string()))
    return ArrowTableStream(
        pa.Table.from_arrays(arrays, names=names),
        tag_columns=["id"],
        source_info={"x": source_info},
    )


class TestProducerlessStreamIdentity:
    """The identity of a stream with no producer covers every value but the derived row hash."""

    def test_equal_streams_have_equal_content_hashes(self) -> None:
        """Two streams built from equal values have equal content hashes."""
        assert _bare_stream().content_hash() == _bare_stream().content_hash()

    @pytest.mark.parametrize(
        "changed",
        [
            {"tag": "b"},
            {"data": 2},
            {"system_tag": "t"},
            {"source_info": "elsewhere"},
            {"meta": "n"},
            {"context_key": "std:v9:other"},
        ],
        ids=["tag", "data", "system_tag", "source_info", "meta", "context"],
    )
    def test_any_changed_value_changes_the_content_hash(
        self, changed: dict[str, Any]
    ) -> None:
        """Changing any one value changes the content hash."""
        assert _bare_stream().content_hash() != _bare_stream(**changed).content_hash()

    def test_identity_does_not_cover_the_derived_content_hash_column(self) -> None:
        """The identity table leaves out the ``_content_hash`` column and nothing else."""
        stream = _bare_stream()
        with_hashes = stream.as_table(all_info=True)
        row_hashes = cast("list[str]", with_hashes.column("_content_hash").to_pylist())
        assert row_hashes[0].startswith("arrow_v1:")
        # The public identity_structure() is what content_hash() hashes.
        _, identity_table, _ = stream.identity_structure()
        assert "_content_hash" not in identity_table.column_names
        assert set(with_hashes.column_names) - set(identity_table.column_names) == {
            "_content_hash"
        }


# ---------------------------------------------------------------------------
# Rule 9: each label is defined in a single place, in code beside its algorithm
# ---------------------------------------------------------------------------

SOURCE_ROOT = Path(orcapod.__file__).parent
LABEL_DEFINITIONS = {
    "semantic_v1": "hashing/semantic_hashing/semantic_hasher.py",
    "arrow_v1": "hashing/arrow_hashers.py",
    "arrow_ordered_v1": "hashing/arrow_hashers.py",
    "directory_v1": "hashing/directory_hashers.py",
    "ndarray_v1": "hashing/semantic_hashing/builtin_handlers.py",
    "polars_expr_v1": "hashing/semantic_hashing/polars_expr.py",
    "polars_dataframe_v1": "hashing/semantic_hashing/builtin_handlers.py",
    "polars_series_v1": "hashing/semantic_hashing/builtin_handlers.py",
    "group_v1": "core/operators/group_by.py",
    "batch_v1": "core/operators/batch.py",
}


def _string_literals_equal_to(label: str) -> list[str]:
    """Return the source files, relative to the package, that spell ``label`` in code.

    Only string constants equal to the label count: docstrings and comments that
    mention a label are prose, not definitions.
    """
    found: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value == label:
                found.append(path.relative_to(SOURCE_ROOT).as_posix())
    return found


def _json_string_values(node: Any) -> set[str]:
    """Return every string value in a parsed JSON document, at any depth.

    Args:
        node: A parsed JSON value.

    Returns:
        The string values of all objects and arrays in ``node``. Object keys are
        not included.
    """
    if isinstance(node, str):
        return {node}
    if isinstance(node, dict):
        children = cast("dict[str, Any]", node).values()
    elif isinstance(node, list):
        children = cast("list[Any]", node)
    else:
        return set()
    found: set[str] = set()
    for child in children:
        found |= _json_string_values(child)
    return found


class TestEachLabelIsDefinedOnce:
    """Rule 9: one definition per label, in code beside its algorithm."""

    @pytest.mark.parametrize("label", sorted(LABEL_DEFINITIONS))
    def test_label_is_spelled_once_in_code(self, label: str) -> None:
        """Each label literal appears in exactly one source file."""
        assert _string_literals_equal_to(label) == [LABEL_DEFINITIONS[label]]

    def test_no_data_context_file_spells_a_hasher_label(self) -> None:
        """No data context file has a ``hasher_id`` key or a label as a value."""
        offenders: list[str] = []
        for path in sorted((SOURCE_ROOT / "contexts").rglob("*.json")):
            text = path.read_text(encoding="utf-8")
            values = _json_string_values(json.loads(text))
            if '"hasher_id"' in text or values & set(LABEL_DEFINITIONS):
                offenders.append(path.relative_to(SOURCE_ROOT).as_posix())
        assert not offenders, (
            "A data context file names the hasher class and must not repeat its "
            f"label: {offenders}"
        )

    def test_hashers_take_their_label_from_the_definition(self) -> None:
        """Hashers and factories default to the label constants."""
        from orcapod.hashing.arrow_hashers import ARROW_HASH_LABEL
        from orcapod.hashing.semantic_hashing.semantic_hasher import (
            SEMANTIC_HASH_LABEL,
            SemanticAwarePythonHasher,
        )
        from orcapod.hashing.versioned_hashers import (
            get_versioned_semantic_arrow_hasher,
            get_versioned_semantic_hasher,
        )

        context = get_default_context()
        assert context.semantic_hasher.hasher_id == SEMANTIC_HASH_LABEL
        assert context.arrow_hasher.hasher_id == ARROW_HASH_LABEL
        assert SemanticAwarePythonHasher().hasher_id == SEMANTIC_HASH_LABEL
        assert get_versioned_semantic_hasher().hasher_id == SEMANTIC_HASH_LABEL
        assert get_versioned_semantic_arrow_hasher().hasher_id == ARROW_HASH_LABEL


def _two_row_stream(order: tuple[str, str]) -> ArrowTableStream:
    """Build a producer-less stream of rows ``a`` and ``b`` in the given row order.

    Args:
        order: The tag values, in the order the rows are stored.

    Returns:
        A stream with tag column ``id`` and data column ``x``.
    """
    values = {"a": 1, "b": 2}
    table = pa.Table.from_arrays(
        [
            pa.array(list(order), pa.large_string()),
            pa.array([values[tag] for tag in order], pa.int64()),
        ],
        names=["id", "x"],
    )
    return ArrowTableStream(table, tag_columns=["id"])


def test_producerless_stream_identity_does_not_depend_on_row_order() -> None:
    """The identity table stays sorted by tags, as it was with ``all_info=True``."""
    forward = _two_row_stream(("a", "b"))
    backward = _two_row_stream(("b", "a"))
    assert forward.content_hash() == backward.content_hash()
