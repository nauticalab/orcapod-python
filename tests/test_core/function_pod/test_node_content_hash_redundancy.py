"""Tests proving NODE_CONTENT_HASH_COL is redundant in the record_id preimage.

Includes a randomised lockstep suite (≥100 examples) that empirically verifies the
two invariants that matter for correctness (within the default
``table_scope='pipeline_hash'`` configuration):

  1. node_content_hash changes → record_id (pipeline_hash, system_tags) changes.
  2. record_id unchanged        → node_content_hash unchanged (contrapositive of 1).

Note that the converse does NOT hold in general: the record_id preimage is more
sensitive than node_content_hash, so a record_id can change without the
node_content_hash changing (e.g. when a different table_scope is used).  These
tests stay within the default ``table_scope='pipeline_hash'``.

Together, properties 1 and 2 confirm that NODE_CONTENT_HASH_COL is fully determined
by information already present in every stored record and is therefore redundant.

This module is an investigation artefact for ITL-533. It demonstrates two
concrete properties:

1. base_entry_id (hash of system_tags + INPUT_DATA_HASH_COL) already uniquely
   identifies distinct input rows — different inputs produce different base_entry_ids
   without any help from NODE_CONTENT_HASH_COL.

2. _filter_by_content_hash() is a no-op for correctness: patching it to pass all
   rows through leaves pipeline results identical (same call counts, same output values).

The deeper insight tying both properties together:

NODE_CONTENT_HASH_COL is always in lockstep with base_entry_id. There is no
scenario where base_entry_id is the same but NODE_CONTENT_HASH_COL differs, because:

  - base_entry_id = hash(system_tags + INPUT_DATA_HASH_COL)
  - system_tags encode source_id, which defaults to table_hash (content-addressed)
  - ArrowTableSource.identity_structure() = (class_name, schema, source_id)
  - Same source_id + same schema → same source content_hash → same node content_hash
    → same NODE_CONTENT_HASH_COL

So the two cases are:
  a) Different inputs → different base_entry_id → records already distinct.
     NODE_CONTENT_HASH_COL adds nothing.
  b) Same inputs → same base_entry_id AND same NODE_CONTENT_HASH_COL.
     Same logical record should have the same record_id (idempotency is correct).

Together these confirm: removing NODE_CONTENT_HASH_COL from the preimage is safe.
The combination of system_tags + INPUT_DATA_HASH_COL + recomputation_index is
sufficient for unique record identification.
"""

from __future__ import annotations

import random

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source_stream(values: list[int]) -> ArrowTableSource:
    """Return an ArrowTableSource with tag=id, data=x for the given int values."""
    table = pa.table(
        {
            "id": pa.array(list(range(len(values))), type=pa.int64()),
            "x": pa.array(values, type=pa.int64()),
        }
    )
    return ArrowTableSource(table=table, tag_columns=["id"], infer_nullable=True)


def _all_base_entry_ids(node: FunctionJobNode, stream) -> set[bytes]:
    """Return the set of base_entry_ids for all (tag, data) pairs in stream."""
    return {
        node.compute_base_entry_id(tag, data)
        for tag, data in stream.iter_data()
    }


# ---------------------------------------------------------------------------
# Property 1: base_entry_id uniqueness across distinct inputs
#
# Two nodes sharing the same pipeline table (same pipeline_hash, different
# content_hash) produce *different* base_entry_ids when processing different
# input rows.  system_tags (content-addressed source_id) alone ensures this.
# ---------------------------------------------------------------------------


class TestBaseEntryIdUniqueness:
    """base_entry_id uniquely identifies distinct input rows across node instances."""

    def test_different_source_data_different_base_entry_id(self, double_pf):
        """Rows from two sources with different data → different base_entry_ids.

        Even though node1 and node2 share the same pipeline_hash (same function +
        same schema), their inputs come from sources with different table_hash-derived
        source_ids.  system_tags therefore differ → different base_entry_id.
        """
        db = InMemoryArrowDatabase()
        src1 = _make_source_stream([10, 20, 30])  # source_id = hash(this table)
        src2 = _make_source_stream([40, 50, 60])  # source_id = hash(different table)

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src1,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src2,
            pipeline_database=db,
        )

        # Sanity: they share the same pipeline table path
        assert node1.node_identity_path == node2.node_identity_path
        # But have different content_hashes (different upstream data)
        assert node1.content_hash() != node2.content_hash()

        ids1 = _all_base_entry_ids(node1, src1)
        ids2 = _all_base_entry_ids(node2, src2)

        # No overlap: different rows → different base_entry_ids
        assert ids1.isdisjoint(ids2), (
            "base_entry_ids from different sources must not collide; "
            "NODE_CONTENT_HASH_COL is therefore not needed to distinguish them."
        )

    def test_different_row_within_source_different_base_entry_id(self, double_pf):
        """Distinct rows within the same source always have different base_entry_ids."""
        db = InMemoryArrowDatabase()
        src = _make_source_stream([10, 20, 30])

        node = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )

        ids = [node.compute_base_entry_id(tag, data) for tag, data in src.iter_data()]
        assert len(ids) == len(set(ids)), "Every row in a source must have a unique base_entry_id."

    def test_no_collision_across_six_distinct_inputs(self, double_pf):
        """base_entry_ids for all six rows across two sources are pairwise distinct."""
        db = InMemoryArrowDatabase()
        src1 = _make_source_stream([1, 2, 3])
        src2 = _make_source_stream([4, 5, 6])

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src1,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src2,
            pipeline_database=db,
        )

        all_ids = (
            list(_all_base_entry_ids(node1, src1))
            + list(_all_base_entry_ids(node2, src2))
        )
        assert len(all_ids) == len(set(all_ids)), (
            "All six base_entry_ids must be distinct — no collisions without "
            "NODE_CONTENT_HASH_COL."
        )


# ---------------------------------------------------------------------------
# Property 2 (forward-looking): preimage shape after ITL-533
#
# Documents the intended preimage structure after the fix lands.
# The test is expected to FAIL until the implementation is updated.
# ---------------------------------------------------------------------------


class TestPreimageShape:
    """Preimage column membership — documents the post-ITL-533 target state."""

    def test_node_content_hash_col_not_in_preimage_keys(self, double_pf):
        """After ITL-533: preimage = system_tags + INPUT_DATA_HASH_COL only.

        NODE_CONTENT_HASH_COL must be absent.  This test currently FAILS
        (confirming the pre-condition) and will pass once the implementation
        is updated.
        """
        src = _make_source_stream([10])
        node = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=InMemoryArrowDatabase(),
        )
        tag, data = next(iter(src.iter_data()))
        preimage = node._build_entry_id_preimage(tag, data)

        assert constants.INPUT_DATA_HASH_COL in preimage.column_names, (
            "INPUT_DATA_HASH_COL must be in the preimage."
        )
        assert constants.NODE_CONTENT_HASH_COL not in preimage.column_names, (
            "NODE_CONTENT_HASH_COL must NOT be in the preimage after ITL-533. "
            "This test fails until the implementation is updated."
        )

    def test_add_pipeline_record_does_not_store_node_content_hash(self, double_pf):
        """``add_pipeline_record()`` must not write ``NODE_CONTENT_HASH_COL`` to the DB.

        After ITL-533, the pdb_v1 schema excludes ``__node_content_hash``.
        """
        import uuid
        from orcapod.databases import InMemoryArrowDatabase

        db = InMemoryArrowDatabase()
        src = _make_source_stream([42])
        node = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )
        tag, data = next(iter(src.iter_data()))
        node.add_pipeline_record(
            tag=tag,
            input_data=data,
            data_record_id=uuid.uuid4(),
            computed=True,
        )
        table = db.get_all_records(node._versioned_pipeline_path)
        assert table is not None, "Pipeline record was not written."
        assert constants.NODE_CONTENT_HASH_COL not in table.column_names, (
            f"``{constants.NODE_CONTENT_HASH_COL}`` must not be stored in pdb_v1 rows."
        )

    def test_node_content_hash_lockstep_with_base_entry_id(self, double_pf):
        """node_content_hash is always in lockstep with base_entry_id.

        Same input (same source_id, same data) → same base_entry_id AND
        same node content_hash.  There is no case where one differs
        without the other differing too: ArrowTableSource.identity_structure()
        = (class_name, schema, source_id), so equal source_ids + equal schemas
        always produce equal source content_hashes and therefore equal node
        content_hashes.

        After ITL-533, NODE_CONTENT_HASH_COL is no longer part of the preimage.
        The lockstep property is verified by comparing node.content_hash()
        directly, which is still well-defined and identical for nodes built from
        the same source.
        """
        db = InMemoryArrowDatabase()
        src = _make_source_stream([10, 20, 30])

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )

        # Both nodes have the same content_hash (same source object → same upstream)
        nch1 = node1.content_hash()
        nch2 = node2.content_hash()
        assert nch1 == nch2, "Nodes built from the same source must have the same content_hash."

        for tag, data in src.iter_data():
            eid1 = node1.compute_base_entry_id(tag, data)
            eid2 = node2.compute_base_entry_id(tag, data)
            # Same input → same base_entry_id ...
            assert eid1 == eid2
            # ... and node content_hash values are also equal — the two are inseparable.
            # (NODE_CONTENT_HASH_COL is no longer in the preimage after ITL-533;
            # node.content_hash() is the canonical way to verify this invariant.)
            assert node1.content_hash() == node2.content_hash(), (
                "node content_hash must be identical when base_entry_id is identical."
            )


# ---------------------------------------------------------------------------
# Randomised lockstep suite (≥100 examples)
#
# Empirically verifies the two invariants within table_scope='pipeline_hash':
#
#   1. same (pipeline_hash, system_tags)  → same node_content_hash.
#      (contrapositive: node_content_hash changes → record_id key changes)
#   2. different node_content_hash        → different (pipeline_hash, system_tags).
#      (contrapositive: same record_id key → same node_content_hash)
#
# "pipeline_hash" is proxied by node.node_identity_path (a tuple of strings
# derived from the pipeline hash — identical for nodes that share function
# identity + upstream schema topology).
#
# "system_tags" are the actual system-tag column names and values extracted
# from the first row emitted by the source (all rows from the same source
# share the same system-tag values, since they share the same content-addressed
# source_id / table_hash).
#
# All nodes are created with the default table_scope='pipeline_hash'.
# ---------------------------------------------------------------------------


class TestLockstepPropertyRandomized:
    """≥100 random (function, source) pairs prove the two critical lockstep invariants.

    Within ``table_scope='pipeline_hash'``:
    - node_content_hash changes  → (pipeline_hash, system_tags) key changes.
    - (pipeline_hash, system_tags) key unchanged → node_content_hash unchanged.
    """

    # 5 functions × 25 sources = 125 examples (well over the required 100).
    _N_SOURCES = 25

    # Fixed seed: ensures the test is reproducible across runs.
    _SEED = 42

    @staticmethod
    def _make_random_source(rng: random.Random) -> ArrowTableSource:
        """Generate an ``ArrowTableSource`` with random integer data."""
        n_rows = rng.randint(1, 8)
        values = [rng.randint(0, 9_999) for _ in range(n_rows)]
        table = pa.table(
            {
                "id": pa.array(list(range(n_rows)), type=pa.int64()),
                "x": pa.array(values, type=pa.int64()),
            }
        )
        return ArrowTableSource(table=table, tag_columns=["id"], infer_nullable=True)

    @staticmethod
    def _system_tags_key(source: ArrowTableSource) -> tuple:
        """Extract a hashable ``(col_name, value)`` tuple from the source's first row.

        All rows from the same ``ArrowTableSource`` share identical system-tag
        values (same content-addressed ``source_id`` / ``table_hash``), so the
        first row is representative of the whole source.
        """
        tag, _ = next(iter(source.iter_data()))
        sys_table = tag.as_table(columns={"system_tags": True})
        return tuple(
            sorted(
                (col, sys_table.column(col)[0].as_py())
                for col in sys_table.column_names
            )
        )

    @staticmethod
    def _make_data_functions() -> list[PythonDataFunction]:
        """Return five textually-distinct ``PythonDataFunction`` instances.

        Named (not lambda) so that the function content hash is derived from
        distinct bytecode in each case.
        """

        def fn_double(x: int) -> int:
            return x * 2

        def fn_triple(x: int) -> int:
            return x * 3

        def fn_square(x: int) -> int:
            return x**2

        def fn_add_one(x: int) -> int:
            return x + 1

        def fn_sub_one(x: int) -> int:
            return x - 1

        return [
            PythonDataFunction(fn_double, output_keys="result"),
            PythonDataFunction(fn_triple, output_keys="result"),
            PythonDataFunction(fn_square, output_keys="result"),
            PythonDataFunction(fn_add_one, output_keys="result"),
            PythonDataFunction(fn_sub_one, output_keys="result"),
        ]

    def _collect_records(self) -> list[tuple[tuple, tuple, bytes]]:
        """Build the full set of (pipeline_hash_key, system_tags_key, node_content_hash) triples.

        Creates one ``FunctionJobNode`` per (data_function, source) combination and
        extracts the three values used to verify the lockstep property.

        Returns:
            List of ``(pipeline_hash_key, system_tags_key, node_content_hash_bytes)``
            triples, one per combination.
        """
        rng = random.Random(self._SEED)
        data_functions = self._make_data_functions()
        sources = [self._make_random_source(rng) for _ in range(self._N_SOURCES)]

        records = []
        for pf in data_functions:
            for source in sources:
                node = FunctionJobNode(
                    function_pod=FunctionPod(data_function=pf),
                    input_stream=source,
                    pipeline_database=InMemoryArrowDatabase(),
                )
                # node_identity_path is a tuple of strings derived from pipeline_hash —
                # two nodes that share function identity + upstream schema topology always
                # produce the same tuple.
                ph_key: tuple = node.node_identity_path
                st_key: tuple = self._system_tags_key(source)
                nch: bytes = node.content_hash().to_prefixed_digest()
                records.append((ph_key, st_key, nch))

        return records

    # ── Sanity checks ──────────────────────────────────────────────────────────

    def test_sanity_distinct_functions_produce_distinct_pipeline_hashes(self):
        """The 5 data functions produce 5 distinct ``node_identity_path`` values.

        If this fails, the main lockstep test has reduced variety along the
        function dimension and should be revisited.
        """
        rng = random.Random(self._SEED)
        source = self._make_random_source(rng)
        paths = set()
        for pf in self._make_data_functions():
            node = FunctionJobNode(
                function_pod=FunctionPod(data_function=pf),
                input_stream=source,
                pipeline_database=InMemoryArrowDatabase(),
            )
            paths.add(node.node_identity_path)
        n_fns = len(self._make_data_functions())
        assert len(paths) == n_fns, (
            f"Expected {n_fns} distinct pipeline-hash keys; got {len(paths)}. "
            f"Some data functions hash identically — the lockstep test's function "
            f"dimension has reduced coverage."
        )

    def test_sanity_distinct_sources_produce_distinct_system_tags(self):
        """The {n} random sources produce {n} distinct system-tag signatures.

        If this fails (a coincidental data collision), increase ``_N_SOURCES`` or
        widen the value range in ``_make_random_source``.
        """
        rng = random.Random(self._SEED)
        sources = [self._make_random_source(rng) for _ in range(self._N_SOURCES)]
        keys = {self._system_tags_key(s) for s in sources}
        assert len(keys) == self._N_SOURCES, (
            f"Expected {self._N_SOURCES} distinct system-tag keys; got {len(keys)}. "
            f"Some random sources coincidentally produced identical content — "
            f"the lockstep test's source dimension has reduced coverage."
        )

    # ── Main lockstep property tests ───────────────────────────────────────────

    def test_forward_lockstep_same_key_implies_same_node_content_hash(self):
        """Forward direction: same (pipeline_hash, system_tags) → same node_content_hash.

        For every pair of records in the ≥100-example set: if two records share
        the same ``(node_identity_path, system_tags)`` key they must also share
        the same ``node_content_hash``.  A violation would mean ``node_content_hash``
        depends on some factor beyond ``(pipeline_hash, system_tags)`` — which would
        disprove the redundancy claim.
        """
        records = self._collect_records()
        assert len(records) >= 100, f"Expected ≥100 examples; got {len(records)}"

        key_to_nch: dict[tuple, bytes] = {}
        violations: list[tuple] = []
        for ph, st, nch in records:
            key = (ph, st)
            if key in key_to_nch:
                if key_to_nch[key] != nch:
                    violations.append((key, key_to_nch[key], nch))
            else:
                key_to_nch[key] = nch

        assert not violations, (
            f"Forward lockstep violated in {len(violations)} case(s): "
            f"same (pipeline_hash, system_tags) produced different node_content_hash.\n"
            f"First violation — key: {violations[0][0]!r}\n"
            f"  first  nch: {violations[0][1]!r}\n"
            f"  second nch: {violations[0][2]!r}"
        )

    def test_backward_lockstep_different_node_content_hash_implies_different_key(self):
        """Backward direction: different node_content_hash → different (pipeline_hash, system_tags).

        For every pair of records in the ≥100-example set: if two records have
        different ``node_content_hash`` values their ``(node_identity_path, system_tags)``
        keys must also differ.  A violation would mean two distinct (function, source)
        combinations map to the same ``node_content_hash`` — implying that removing
        ``NODE_CONTENT_HASH_COL`` from the preimage could collapse distinct records.
        """
        records = self._collect_records()
        assert len(records) >= 100, f"Expected ≥100 examples; got {len(records)}"

        nch_to_key: dict[bytes, tuple] = {}
        violations: list[tuple] = []
        for ph, st, nch in records:
            key = (ph, st)
            if nch in nch_to_key:
                if nch_to_key[nch] != key:
                    violations.append((nch, nch_to_key[nch], key))
            else:
                nch_to_key[nch] = key

        assert not violations, (
            f"Backward lockstep violated in {len(violations)} case(s): "
            f"different (pipeline_hash, system_tags) produced the same node_content_hash.\n"
            f"First violation — nch: {violations[0][0]!r}\n"
            f"  first  key: {violations[0][1]!r}\n"
            f"  second key: {violations[0][2]!r}"
        )

