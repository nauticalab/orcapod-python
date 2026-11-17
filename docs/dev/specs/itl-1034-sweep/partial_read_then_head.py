"""ITL-1065: after a partial read, a pod stream fed by an operator stream shows no preview.

``f.pod(source)`` where the source has an extra data column, so the automatic select (an
operator stream) feeds the pod. Three rows are read, then ``head(2)`` returns
``NOT_COMPUTED`` with no rows and the stream is stale; without the extra column it shows
the two rows (``PARTIAL``). On main 89160c47 (#355); 1890555b, just before it, shows the
rows (November 13, 2026).
"""

from typing import Any

from orcapod import function_pod
from orcapod.sources import DictSource

for extra in (False, True):

    @function_pod(output_keys="y")
    def f(x: int) -> int:
        return 10 * x

    rows = [{"k": i, "x": i, **({"s": f"s{i}"} if extra else {})} for i in range(6)]
    s: Any = f.pod(DictSource(rows, tag_columns=["k"]))
    it = iter(s.iter_data())
    next(it)
    next(it)
    next(it)
    del it
    h = s.head(2)
    print(
        f"extra column={extra!s:5} stale={s.is_stale!s:5} head status={h.status} rows={h.as_table().num_rows} note={getattr(h, 'note', None)}"
    )
