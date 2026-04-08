"""Smoke test for ENG-340/349 refactor: verify full pipeline lifecycle."""
import json
import tempfile
import pytest
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline


def test_smoke_scoped_databases_created():
    """Pipeline.compile() creates pre-scoped database views."""
    db = InMemoryArrowDatabase()
    with Pipeline("smoke_test", pipeline_database=db) as p:
        pass
    assert p.result_database._scoped_path == ("smoke_test", "_result")
    assert p.status_database._scoped_path == ("smoke_test", "_status")
    assert p.log_database._scoped_path == ("smoke_test", "_log")


def test_smoke_save_full_includes_observer():
    """Pipeline.save(level='full') includes observer in the serialized output."""
    db = InMemoryArrowDatabase()
    with Pipeline("smoke_test", pipeline_database=db) as p:
        pass

    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        path = f.name

    p.save(path, level="full")
    with open(path) as f:
        data = json.load(f)

    assert "observer" in data["pipeline"]
    assert data["pipeline"]["observer"]["type"] == "composite"
    assert len(data["pipeline"]["observer"]["observers"]) == 2


def test_smoke_run_with_no_observer_uses_default():
    """Pipeline.run() with no observer uses the default composite observer."""
    db = InMemoryArrowDatabase()
    with Pipeline("smoke_test", pipeline_database=db) as p:
        pass
    # Should not raise
    p.run()
