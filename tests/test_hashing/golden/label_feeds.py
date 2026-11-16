"""Which golden values a hash label feeds, and what they are under renamed labels (ITL-1154).

The one-line rule: a hash always carries its label, except where it is written into a
name, whose grammar fixes which algorithm produced it. So renaming a label must move
every hash it feeds. This module gives ``test_hash_label_rule.py`` the two sides of that
statement, each computed in a fresh process (``python -m``), so no hash cached by an
earlier computation hides a dependency:

* ``feeds``: every golden value, and for every digest produced while computing them, the
  hashes that digest embeds: the nested hashes of a structure hash (read from the tagged
  JSON tree the structure hasher digests), the value hashes in the logical-type cells of
  an Arrow hash and the labelled hash columns of an entry-id or row-key table, and the
  ordered Arrow digest inside a pandas value hash. A digest moves under renamed labels
  when it embeds a hash whose label is renamed, or a digest that moves.
* ``renamed``: every golden value computed with every label renamed (``RENAMED``), the old
  labels then put back into the text.

Usage::

    uv run python -m tests.test_hashing.golden.label_feeds feeds
    uv run python -m tests.test_hashing.golden.label_feeds renamed
"""

from __future__ import annotations

import json
import re
import sys
import warnings
from typing import Any

RENAMED: dict[str, str] = {
    "semantic_v1": "renamedsemantic_v9",
    "arrow_v1": "renamedarrow_v9",
    "arrow_ordered_v1": "renamedarrowordered_v9",
    "ndarray_v1": "renamedndarray_v9",
    "pandas_dataframe_v1": "renamedpandasdataframe_v9",
    "pandas_series_v1": "renamedpandasseries_v9",
    "polars_expr_v1": "renamedpolarsexpr_v9",
    "polars_dataframe_v1": "renamedpolarsdataframe_v9",
    "polars_series_v1": "renamedpolarsseries_v9",
    "directory_v1": "renameddirectory_v9",
    "torch_tensor_v1": "renamedtorchtensor_v9",
}
"""Every label a test renames, with its stand-in. The file hash's ``sha256`` names the
digest function and is not renamed."""

_PREFIXED = re.compile(rb"^([a-z][a-z0-9_]*):")
_HASH_COLUMNS = ("__input_data_hash", "__node_content_hash")


def rename_labels() -> None:
    """Rename every label in ``RENAMED`` in this process, by setting the definitions.

    There is no public way to rename a label (each is defined once, in code), so the
    non-public definitions are set directly.
    """
    from orcapod.contexts import get_default_context
    from orcapod.hashing import arrow_hashers, directory_hashers, pandas_value_hash
    from orcapod.hashing.semantic_hashing import builtin_handlers, polars_expr

    context = get_default_context()
    context.semantic_hasher._hasher_id = RENAMED["semantic_v1"]  # pyright: ignore[reportAttributeAccessIssue]
    context.arrow_hasher._hasher_id = RENAMED["arrow_v1"]  # pyright: ignore[reportAttributeAccessIssue]
    if hasattr(arrow_hashers, "ARROW_ORDERED_HASH_LABEL"):
        arrow_hashers.ARROW_ORDERED_HASH_LABEL = RENAMED["arrow_ordered_v1"]  # pyright: ignore[reportConstantRedefinition]
    builtin_handlers.NumpyArrayHandler.HASH_LABEL = RENAMED["ndarray_v1"]
    pandas_value_hash.PANDAS_DATAFRAME_HASH_LABEL = RENAMED["pandas_dataframe_v1"]  # pyright: ignore[reportConstantRedefinition]
    pandas_value_hash.PANDAS_SERIES_HASH_LABEL = RENAMED["pandas_series_v1"]  # pyright: ignore[reportConstantRedefinition]
    builtin_handlers.PandasDataFrameHandler.HASH_LABEL = RENAMED["pandas_dataframe_v1"]
    builtin_handlers.PandasSeriesHandler.HASH_LABEL = RENAMED["pandas_series_v1"]
    polars_expr.POLARS_EXPR_HASH_LABEL = RENAMED["polars_expr_v1"]  # pyright: ignore[reportConstantRedefinition]
    builtin_handlers.PolarsDataFrameHandler.HASH_LABEL = RENAMED["polars_dataframe_v1"]
    builtin_handlers.PolarsSeriesHandler.HASH_LABEL = RENAMED["polars_series_v1"]
    directory_hashers.DIRECTORY_HASH_LABEL = RENAMED["directory_v1"]  # pyright: ignore[reportConstantRedefinition]
    try:  # torch is optional: its label is renamed only where it is installed
        from orcapod.hashing.semantic_hashing.torch_handler import TorchTensorHandler
    except ImportError:
        return
    TorchTensorHandler.HASH_LABEL = RENAMED["torch_tensor_v1"]


def _tree_hashes(tree: Any, out: list[tuple[str, str]]) -> None:
    """Collect ``(label, hex)`` of every nested-hash node of a structure hasher tree."""
    if isinstance(tree, dict):
        if tree.get("__type__") == "hash":
            out.append((str(tree.get("label", "")), str(tree["digest"])))
            return
        for value in tree.values():
            _tree_hashes(value, out)
    elif isinstance(tree, list):
        for value in tree:
            _tree_hashes(value, out)


def _parse_prefixed(raw: bytes) -> tuple[str, str] | None:
    """Return ``(label, hex)`` of ``b"<label>:" + digest``, or ``None``."""
    match = _PREFIXED.match(raw)
    if match is None:
        return None
    return match.group(1).decode(), raw[match.end() :].hex()


def record_feeds() -> tuple[
    dict[str, dict[str, Any]], dict[str, list[tuple[str, str]]]
]:
    """Compute every golden value and record what each digest produced embeds.

    Returns:
        The golden values, and a map from each digest produced (hex) to the
        ``(label, hex)`` of every hash fed into it.
    """
    import pyarrow as pa

    from orcapod.hashing import arrow_hashers, labelled_hash
    from orcapod.hashing.semantic_hashing import semantic_hasher
    from orcapod.hashing.visitors import SemanticHashingVisitor

    feeds: dict[str, list[tuple[str, str]]] = {}
    arrow_stack: list[list[tuple[str, str]]] = []

    structure_hash = semantic_hasher.SemanticAwarePythonHasher._hash_to_content_hash  # pyright: ignore[reportPrivateUsage]

    def traced_structure_hash(self: Any, obj: Any) -> Any:
        result = structure_hash(self, obj)
        embedded: list[tuple[str, str]] = []
        _tree_hashes(obj, embedded)
        feeds.setdefault(result.to_hex(), []).extend(embedded)
        return result

    visit_extension = SemanticHashingVisitor.visit_extension

    def traced_visit_extension(self: Any, *args: Any, **kwargs: Any) -> Any:
        hashed_type, value = visit_extension(self, *args, **kwargs)
        if arrow_stack and isinstance(value, bytes) and b"::" in value:
            parsed = _parse_prefixed(value.split(b"::", 1)[1])
            if parsed is not None:
                arrow_stack[-1].append(parsed)
        return hashed_type, value

    digest = arrow_hashers.StarfixArrowHasher._digest  # pyright: ignore[reportPrivateUsage]

    def traced_digest(self: Any, table: Any, **kwargs: Any) -> bytes:
        arrow_stack.append([])
        try:
            result = digest(self, table, **kwargs)
        finally:
            embedded = arrow_stack.pop()
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])
        for name in _HASH_COLUMNS:
            if name in table.column_names:
                for raw in table.column(name).to_pylist():
                    parsed = _parse_prefixed(raw) if isinstance(raw, bytes) else None
                    if parsed is not None:
                        embedded.append(parsed)
        feeds.setdefault(result.hex(), []).extend(embedded)
        return result

    labelled = labelled_hash.labelled_content_hash

    def traced_labelled(method: str, descriptor: Any, values_digest: bytes) -> Any:
        result = labelled(method, descriptor, values_digest)
        parsed = _parse_prefixed(values_digest)
        feeds.setdefault(result.to_hex(), []).extend([parsed] if parsed else [])
        return result

    semantic_hasher.SemanticAwarePythonHasher._hash_to_content_hash = (
        traced_structure_hash  # pyright: ignore[reportAttributeAccessIssue]
    )
    SemanticHashingVisitor.visit_extension = traced_visit_extension  # pyright: ignore[reportAttributeAccessIssue]
    arrow_hashers.StarfixArrowHasher._digest = traced_digest  # pyright: ignore[reportAttributeAccessIssue]
    labelled_hash.labelled_content_hash = traced_labelled
    from orcapod.hashing import pandas_value_hash

    pandas_value_hash.labelled_content_hash = traced_labelled  # pyright: ignore[reportAttributeAccessIssue]

    from tests.test_hashing.golden import cases

    return cases.compute_all(), feeds


def compute_renamed() -> dict[str, dict[str, Any]]:
    """Compute every golden value under renamed labels, with the old labels put back."""
    rename_labels()
    from tests.test_hashing.golden import cases

    text = json.dumps(cases.compute_all(), sort_keys=True)
    for old, new in RENAMED.items():
        text = text.replace(new, old)
    return json.loads(text)


def main(argv: list[str]) -> None:
    """Print the requested side as JSON on stdout."""
    warnings.filterwarnings("ignore")
    if argv == ["feeds"]:
        values, feeds = record_feeds()
        print(json.dumps({"values": values, "feeds": feeds}, sort_keys=True))
    elif argv == ["renamed"]:
        print(json.dumps(compute_renamed(), sort_keys=True))
    else:
        raise SystemExit(
            "usage: python -m tests.test_hashing.golden.label_feeds feeds|renamed"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
