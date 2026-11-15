"""ITL-1066: ``SideEffectPodStream.as_table`` under each column configuration (no job).

With ``context`` (and so ``all_info``) it raises ``Duplicate column name '_context_key'``;
with ``content_hash`` it leaves the column out; with ``sort_by_tags`` it does not sort.
Measured on main 89160c47 (November 13, 2026).
"""

from typing import Any
from orcapod.side_effects import SideEffectPod
from orcapod.sources import DictSource


def deliver(x: int, ctx: Any) -> None:
    pass


source = DictSource(
    [{"k": f"r{i}", "x": i} for i in (2, 0, 1)], tag_columns=["k"], source_id="src"
)
stream: Any = SideEffectPod(deliver)(source)
reference = source.as_table(all_info=True)
for name, kwargs in [
    ("default", {}),
    ("content_hash", {"columns": {"content_hash": True}}),
    ("context", {"columns": {"context": True}}),
    ("sort_by_tags", {"columns": {"sort_by_tags": True}}),
    ("all_info", {"all_info": True}),
]:
    try:
        t = stream.as_table(**kwargs)
        print(f"{name:13s} ok: columns {t.column_names}; k {t.column('k').to_pylist()}")
    except Exception as exc:
        print(f"{name:13s} RAISES {type(exc).__name__}: {exc}")
print(
    "the source's own all_info table:",
    reference.column_names,
    "k",
    reference.column("k").to_pylist(),
)
