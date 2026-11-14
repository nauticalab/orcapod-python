"""A failed prior-completion lookup names the side-effect node and the row (ITL-919).

A side-effect pod that tracks completion looks up each row's invocation record before
delivering it. When that lookup cannot read the store, the ``StoreReadError`` names the
pod and the row's tag, as a failed write of the same record does (CodeRabbit on #290).
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import StoreReadError
from orcapod.pipeline.job import PipelineJob
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource


class UnreadableCompletions:
    """A store whose record-by-id lookups (the completion check) cannot read."""

    def __init__(self, inner: Any) -> None:
        """Wrap ``inner``."""
        self._inner = inner

    def at(self, *path_components: str) -> UnreadableCompletions:
        """A view with the same failure."""
        return UnreadableCompletions(self._inner.at(*path_components))

    def get_record_by_id(self, record_path: tuple[str, ...], *args: Any, **kwargs: Any):
        """Fail as an unreadable stored table would."""
        raise StoreReadError("/store/" + "/".join(record_path), tuple(record_path))

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


def test_completion_lookup_failure_names_the_node_and_row() -> None:
    """The lookup's ``StoreReadError`` carries the pod's label and the row's tag."""
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    pod = SideEffectPod(deliver, config=SideEffectPodConfig(track_completion=True))
    store = UnreadableCompletions(InMemoryArrowDatabase())
    job = PipelineJob(name="effects", database=store)  # pyright: ignore[reportArgumentType]  # failing wrapper of a real store
    with job:
        pod.process(DictSource([{"id": 0, "value": 10}], tag_columns=["id"]))

    with pytest.raises(StoreReadError) as info:
        job.run()
    assert info.value.tag == {"id": 0}
    assert info.value.node_label is not None
    assert delivered == []
