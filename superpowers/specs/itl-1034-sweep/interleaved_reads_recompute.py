"""ITL-1065: interleaved reads of a pod stream behind an operator stream recompute a row.

``f.pod(source)`` where the source has an extra data column, so the automatic select (an
operator stream) feeds the pod. Two interleaved iterations: the function is called 7
times for 6 rows, and the first iteration's rows differ from a later read's. Without the
extra column it is 6 calls and the reads agree. On main 89160c47 (#355); 1890555b, just
before #355, gives 6 calls (November 13, 2026).
"""

import itertools
from typing import Any
from orcapod import function_pod
from orcapod.sources import DictSource

calls = []


@function_pod(output_keys="y")
def f(x: int) -> int:
    calls.append(x)
    return 10 * x


for extra in (False, True):
    calls.clear()
    rows = [{"k": i, "x": i, **({"s": f"s{i}"} if extra else {})} for i in range(6)]
    s: Any = f.pod(DictSource(rows, tag_columns=["k"]))
    first = iter(s.iter_data())
    second = iter(s.iter_data())
    a = [next(first)]
    b = list(itertools.islice(second, 3))
    a += list(first)
    b += list(second)
    stale = s.is_stale
    third = list(s.iter_data())
    same = [d.datagram_uuid for _, d in a] == [d.datagram_uuid for _, d in third]
    print(
        f"extra column={extra!s:5} calls={len(calls):2d} (6 expected) stale after the reads={stale!s:5} first read == third read: {same}"
    )
