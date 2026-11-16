"""A saved ``PipelineJob`` with tensor results is loaded back and read (ITL-871, ITL-904)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("torch")

import torch  # noqa: E402

import orcapod as op  # noqa: E402
from orcapod.contexts import get_default_context  # noqa: E402
from orcapod.core.operators import Join  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.logical_types.identifier import logical_type_identifier  # noqa: E402
from orcapod.pipeline.serialization import LoadStatus  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402


def weights(n: int) -> torch.Tensor:
    """Return a transposed float32 tensor that tracks gradients."""
    base = torch.arange(n * 2, dtype=torch.float32).reshape(2, n)
    return base.t().clone(memory_format=torch.preserve_format).requires_grad_(True)


def parameter(n: int) -> torch.nn.Parameter:
    """Return an ``nn.Parameter`` of ``n`` ones."""
    return torch.nn.Parameter(torch.ones(n))


def _save(tmp_path: Path, function: Any) -> Path:
    """Run and save a job whose pod returns a tensor, followed by a ``Join``."""
    job = op.PipelineJob(
        name="tensors", database=DeltaTableDatabase(tmp_path / "store")
    )
    with job:
        source = DictSource(
            [{"id": 1, "n": 3}, {"id": 2, "n": 4}],
            tag_columns=["id"],
            tag_only_columns=True,
            source_id="n",
        )
        result = op.function_pod(output_keys="value")(function).pod(
            source, label="value"
        )
        other = DictSource(
            [{"id": 1, "other": 2}, {"id": 2, "other": 3}],
            tag_columns=["id"],
            tag_only_columns=True,
            source_id="other",
        )
        Join()(result, other, label="joined")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    return path


def _values(node: Any) -> dict[int, Any]:
    """Map each row's ``id`` tag to its ``value``."""
    return {t.as_dict()["id"]: d.as_dict()["value"] for t, d in node.iter_data()}


@pytest.mark.parametrize(
    ("function", "declared"),
    [(weights, torch.Tensor), (parameter, torch.nn.Parameter)],
    ids=["tensor", "parameter"],
)
def test_a_loaded_job_reads_its_stored_tensors(
    tmp_path: Path, function: Any, declared: type
) -> None:
    """The loaded job has the tensor schema and reads the stored tensors exactly."""
    path = _save(tmp_path, function)
    descriptors = json.loads(path.read_text())["pipeline"]["nodes"].values()
    descriptor = next(d for d in descriptors if d["label"] == "value")
    logical = get_default_context().type_converter.get_logical_type(torch.Tensor)
    assert logical is not None
    identifier = logical_type_identifier(logical)
    assert identifier.text.startswith("orcapod.torch.tensor;")
    assert descriptor["output_schema"]["data"]["value"] == (
        f"extension<{identifier.text}>"
    )

    loaded = op.PipelineJob.load(path)
    assert loaded.nodes["value"].output_schema()[1]["value"] is torch.Tensor
    assert loaded.nodes["joined"].load_status == LoadStatus.FULL
    assert loaded.nodes["joined"].output_schema()[1]["value"] is torch.Tensor
    # A loaded node is empty until it runs (ITL-1095): read the stored view.
    got = _values(loaded.nodes["value"].as_source())
    assert sorted(got) == [1, 2]
    for row_id, n in ((1, 3), (2, 4)):
        expected = function(n)
        value = got[row_id]
        assert type(value) is type(expected) and isinstance(value, declared)
        assert value.dtype == expected.dtype
        assert value.stride() == expected.stride()
        assert value.requires_grad == expected.requires_grad
        assert torch.equal(value.detach(), expected.detach())
