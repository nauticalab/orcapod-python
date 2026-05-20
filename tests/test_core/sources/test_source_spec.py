from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.errors import UnboundSourceError, SourceSpecMismatchError
from orcapod.types import Schema


def _make_source(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(["a", "b"], type=pa.large_string()),
            data_col: pa.array([1, 2], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


class TestSourceSpecConstruction:
    def test_construct_with_name_and_schemas(self):
        spec = SourceSpec(
            name="my_source",
            tag_schema=Schema({"key": str}),
            data_schema=Schema({"value": int}),
        )
        assert spec.name == "my_source"
        assert "key" in spec.tag_schema
        assert "value" in spec.data_schema

    def test_output_schema_returns_tag_and_data(self):
        tag = Schema({"key": str})
        data = Schema({"value": int})
        spec = SourceSpec(name="s", tag_schema=tag, data_schema=data)
        out_tag, out_data = spec.output_schema()
        assert out_tag == tag
        assert out_data == data

    def test_keys_returns_tag_column_names(self):
        spec = SourceSpec(
            name="s",
            tag_schema=Schema({"key": str, "group": str}),
            data_schema=Schema({"value": int}),
        )
        tag_keys, data_keys = spec.keys()
        assert set(tag_keys) == {"key", "group"}
        assert set(data_keys) == {"value"}

    def test_label_returns_name(self):
        spec = SourceSpec(name="my_spec", tag_schema=Schema({"k": str}), data_schema=Schema({"v": int}))
        assert spec.label == "my_spec"


class TestSourceSpecHashing:
    def test_pipeline_hash_matches_compatible_source(self):
        """SourceSpec.pipeline_hash() must equal a schema-compatible source's pipeline_hash()."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec = SourceSpec(name="my_source", tag_schema=tag_schema, data_schema=data_schema)

        assert spec.pipeline_hash() == source.pipeline_hash()

    def test_content_hash_differs_by_name(self):
        """Two specs with the same schema but different names must have different content hashes."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec_a = SourceSpec(name="source_a", tag_schema=tag_schema, data_schema=data_schema)
        spec_b = SourceSpec(name="source_b", tag_schema=tag_schema, data_schema=data_schema)

        assert spec_a.content_hash() != spec_b.content_hash()

    def test_pipeline_hash_same_for_different_names(self):
        """SourceSpec.pipeline_hash() must be schema-only (ignoring name)."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec_a = SourceSpec(name="a", tag_schema=tag_schema, data_schema=data_schema)
        spec_b = SourceSpec(name="b", tag_schema=tag_schema, data_schema=data_schema)

        assert spec_a.pipeline_hash() == spec_b.pipeline_hash()

    def test_content_hash_stable(self):
        """Same name + schemas → same content hash across calls."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec = SourceSpec(name="s", tag_schema=tag_schema, data_schema=data_schema)
        assert spec.content_hash() == spec.content_hash()


class TestSourceSpecUnboundBehavior:
    def test_iter_data_raises_unbound_error(self):
        spec = SourceSpec(name="s", tag_schema=Schema({"k": str}), data_schema=Schema({"v": int}))
        with pytest.raises(UnboundSourceError, match="s"):
            list(spec.iter_data())

    def test_as_table_raises_unbound_error(self):
        spec = SourceSpec(name="s", tag_schema=Schema({"k": str}), data_schema=Schema({"v": int}))
        with pytest.raises(UnboundSourceError, match="s"):
            spec.as_table()


class TestSourceSpecValidate:
    def test_validate_passes_for_compatible_source(self):
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec = SourceSpec(name="s", tag_schema=tag_schema, data_schema=data_schema)
        # Should not raise
        spec.validate(source)

    def test_validate_raises_for_extra_tag_column(self):
        """Source has extra tag column not in spec → SourceSpecMismatchError."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        # Spec declares extra tag column not present in source
        extra_tag = Schema({"key": str, "unexpected": str})
        spec = SourceSpec(name="s", tag_schema=extra_tag, data_schema=data_schema)
        with pytest.raises(SourceSpecMismatchError):
            spec.validate(source)

    def test_validate_raises_for_missing_data_column(self):
        """Source missing a required data column → SourceSpecMismatchError."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        # Spec requires an extra data column the source doesn't have
        wider_data = Schema({"value": int, "extra": str})
        spec = SourceSpec(name="s", tag_schema=tag_schema, data_schema=wider_data)
        with pytest.raises(SourceSpecMismatchError):
            spec.validate(source)
