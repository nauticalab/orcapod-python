"""``list_table_paths``: the tables a store holds under a path prefix (ITL-954).

Every database lists, without opening a table, the paths of the tables whose path starts
with a prefix, relative to its ``base_path`` like ``table_exists``. Tables nested below
another table are listed too: Orcapod v0.3 stores a table one level below where v0.2
stored it.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
)
from orcapod.databases.extension_aware_database import ExtensionAwareDatabase
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.errors import TableListingNotSupportedError
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

Store = InMemoryArrowDatabase | DeltaTableDatabase | ConnectorArrowDatabase

# Paths whose components need no sanitizing, so every backend returns them as given.
PATHS = [
    ("job", "double", "h1", "pdb_v1"),
    ("job", "double", "h1"),  # a table with another table below it
    ("job", "double", "h2", "pdb_v2"),
    ("job", "doubled", "h3", "pdb_v2"),  # shares a string prefix, not a component
    ("job", "_result", "double", "h1", "rdb_v2"),
    ("other", "double", "h1", "pdb_v2"),
]


def _fill(store: ArrowDatabaseProtocol, paths: list[tuple[str, ...]] = PATHS) -> None:
    """Store one row at each of ``paths``."""
    for i, path in enumerate(paths):
        store.add_record(path, f"r{i}".encode(), pa.table({"x": [i]}))
    store.flush()


@pytest.fixture(params=["in_memory", "delta", "sqlite"])
def make_store(request: pytest.FixtureRequest, tmp_path: Path) -> Callable[[], Store]:
    """A factory of database objects over one empty store of each backend."""
    connector = SQLiteConnector(str(tmp_path / "store.db"))
    shared = InMemoryArrowDatabase()

    def make() -> Store:
        """Return a new store object over the same underlying storage."""
        if request.param == "in_memory":
            return shared
        if request.param == "delta":
            return DeltaTableDatabase(tmp_path / "delta")
        return ConnectorArrowDatabase(connector)

    return make


class TestEveryBackend:
    """Listing behaviour that every store from the ``make_store`` fixture shares."""

    def test_lists_every_table_sorted(self, make_store: Callable[[], Store]):
        """Without a prefix, a second store object lists every written path in sorted
        order.
        """
        _fill(make_store())
        assert make_store().list_table_paths() == sorted(PATHS)

    def test_prefix_keeps_the_tables_below_it(self, make_store: Callable[[], Store]):
        """A prefix lists only the tables under it, at any depth."""
        _fill(make_store())
        assert make_store().list_table_paths(("job", "double")) == [
            ("job", "double", "h1"),
            ("job", "double", "h1", "pdb_v1"),
            ("job", "double", "h2", "pdb_v2"),
        ]

    def test_a_table_at_the_prefix_itself_is_listed(
        self, make_store: Callable[[], Store]
    ):
        """A table whose path equals the prefix is listed with the tables below it."""
        _fill(make_store())
        assert make_store().list_table_paths(("job", "double", "h1")) == [
            ("job", "double", "h1"),
            ("job", "double", "h1", "pdb_v1"),
        ]

    def test_prefix_without_tables_gives_nothing(self, make_store: Callable[[], Store]):
        """A prefix with no table under it gives an empty list."""
        _fill(make_store())
        assert make_store().list_table_paths(("job", "missing")) == []

    def test_empty_store_gives_nothing(self, make_store: Callable[[], Store]):
        """A store with no table gives an empty list, with or without a prefix."""
        assert make_store().list_table_paths() == []
        assert make_store().list_table_paths(("job",)) == []

    def test_scoped_view_lists_relative_to_its_base_path(
        self, make_store: Callable[[], Store]
    ):
        """A view made with ``at`` lists paths without its base path components."""
        _fill(make_store())
        scoped = make_store().at("job")
        assert scoped.list_table_paths(("double",)) == [
            ("double", "h1"),
            ("double", "h1", "pdb_v1"),
            ("double", "h2", "pdb_v2"),
        ]
        assert scoped.at("_result").list_table_paths() == [("double", "h1", "rdb_v2")]

    def test_every_listed_path_is_a_table(self, make_store: Callable[[], Store]):
        """``table_exists`` is true for every listed path."""
        _fill(make_store())
        store = make_store()
        assert all(store.table_exists(path) for path in store.list_table_paths())

    def test_listing_changes_nothing(self, make_store: Callable[[], Store]):
        """The records of every table are the same before and after a listing."""
        _fill(make_store())
        store = make_store()
        before = {
            path: store.get_all_records(path) for path in store.list_table_paths()
        }
        _ = store.list_table_paths(("job",))
        after = {path: store.get_all_records(path) for path in store.list_table_paths()}
        assert before == after


class TestInMemory:
    """Listing behaviour specific to ``InMemoryArrowDatabase``."""

    def test_tables_not_flushed_yet_are_listed(self):
        """A table with a record added but not flushed is listed."""
        store = InMemoryArrowDatabase()
        store.add_record(("job", "f", "pdb_v2"), b"r", pa.table({"x": [1]}))
        assert store.list_table_paths(("job",)) == [("job", "f", "pdb_v2")]


class TestDelta:
    """Listing behaviour specific to ``DeltaTableDatabase``, local and cloud."""

    def test_hash_components_come_back_unchanged(self, tmp_path: Path):
        """Path components containing ``:`` and ``.`` are listed exactly as written."""
        path = ("job", "f", "semantic_v0.1:ab12", "v0", "python.function.v0", "rdb_v1")
        _fill(DeltaTableDatabase(tmp_path), [path])
        assert DeltaTableDatabase(tmp_path).list_table_paths(("job", "f")) == [path]

    def test_directories_that_are_not_tables_are_not_listed(self, tmp_path: Path):
        """A plain directory and a stray file under the root are left out of the
        listing.
        """
        _fill(DeltaTableDatabase(tmp_path), [("job", "f", "pdb_v2")])
        (tmp_path / "job" / "f" / "notes").mkdir()
        (tmp_path / "job" / "stray.txt").write_text("x")
        assert DeltaTableDatabase(tmp_path).list_table_paths() == [
            ("job", "f", "pdb_v2")
        ]

    def test_a_directory_symlink_is_not_followed(self, tmp_path: Path):
        """A link to an ancestor does not send the walk in circles."""
        _fill(DeltaTableDatabase(tmp_path), [("job", "f", "pdb_v2")])
        (tmp_path / "job" / "loop").symlink_to(tmp_path, target_is_directory=True)
        assert DeltaTableDatabase(tmp_path).list_table_paths() == [
            ("job", "f", "pdb_v2")
        ]

    def test_missing_root_gives_nothing(self, tmp_path: Path):
        """A store whose root directory does not exist gives an empty list."""
        assert DeltaTableDatabase(tmp_path / "absent").list_table_paths() == []

    @pytest.mark.skipif(
        importlib.util.find_spec("gcsfs") is not None,
        reason="gcsfs is installed, so gs:// can be listed",
    )
    def test_cloud_store_without_a_file_system_package_cannot_list(self):
        """A ``gs://`` store raises ``TableListingNotSupportedError`` naming ``gs``."""
        store = DeltaTableDatabase("gs://bucket/root")
        with pytest.raises(TableListingNotSupportedError, match="gs"):
            store.list_table_paths(("job",))


class TestDeltaOnCloudStorage:
    """The cloud walk, on a local directory that stands in for the bucket."""

    @pytest.fixture
    def bucket(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> list[dict[str, Any]]:
        """Serve ``tmp_path`` as the file system of ``s3://``; return the calls made."""
        import fsspec
        from fsspec.implementations.dirfs import DirFileSystem
        from fsspec.implementations.local import LocalFileSystem

        calls: list[dict[str, Any]] = []

        def filesystem(protocol: str, **options: object) -> DirFileSystem:
            """Record the call and return a local file system rooted at ``tmp_path``."""
            calls.append({"protocol": protocol, **options})
            return DirFileSystem(path=str(tmp_path), fs=LocalFileSystem())

        monkeypatch.setattr(fsspec, "filesystem", filesystem)
        # The endpoint is also read from the environment: these tests must not depend
        # on what the machine running them has set (ITL-975).
        monkeypatch.delenv("FSSPEC_S3_ENDPOINT_URL", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        _fill(DeltaTableDatabase(tmp_path / "bucket" / "root"))
        return calls

    def test_tables_and_nested_tables_are_listed(self, bucket: list[dict[str, Any]]):
        """An ``s3://`` store lists tables under a prefix, through a scoped view, and
        none for an absent prefix.
        """
        store = DeltaTableDatabase("s3://bucket/root")
        assert store.list_table_paths(("job", "double")) == [
            ("job", "double", "h1"),
            ("job", "double", "h1", "pdb_v1"),
            ("job", "double", "h2", "pdb_v2"),
        ]
        assert store.at("job").list_table_paths(("_result",)) == [
            ("_result", "double", "h1", "rdb_v2")
        ]
        assert store.list_table_paths(("absent",)) == []

    def test_s3fs_gets_the_credentials_of_the_storage_options(
        self, bucket: list[dict[str, Any]]
    ):
        """The AWS storage options are passed to ``fsspec.filesystem`` as s3fs
        arguments.
        """
        store = DeltaTableDatabase(
            "s3a://bucket/root",
            storage_options={
                "AWS_ACCESS_KEY_ID": "key-id",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_ENDPOINT_URL": "http://localhost:9000",
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
            },
        )
        _ = store.list_table_paths()
        assert bucket == [
            {
                "protocol": "s3",
                "use_listings_cache": False,
                "skip_instance_cache": True,
                "key": "key-id",
                "secret": "secret",
                "client_kwargs": {
                    "endpoint_url": "http://localhost:9000",
                    "region_name": "us-east-1",
                },
            }
        ]

    def test_an_http_endpoint_is_listed_only_when_http_is_allowed(
        self, bucket: list[dict[str, Any]]
    ):
        """Without ``AWS_ALLOW_HTTP`` the credentials are not sent over plain HTTP."""
        options = {
            "AWS_ACCESS_KEY_ID": "key-id",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
        }
        store = DeltaTableDatabase("s3://bucket/root", storage_options=options)
        with pytest.raises(TableListingNotSupportedError, match="AWS_ALLOW_HTTP"):
            store.list_table_paths()
        assert bucket == []  # no file system was built, nothing was sent

        allowed = DeltaTableDatabase(
            "s3://bucket/root", storage_options={**options, "aws_allow_http": "TRUE"}
        )
        assert allowed.list_table_paths(("absent",)) == []
        https = DeltaTableDatabase(
            "s3://bucket/root",
            storage_options={**options, "AWS_ENDPOINT_URL": "https://example.test"},
        )
        assert https.list_table_paths(("absent",)) == []

    def test_an_http_endpoint_from_the_environment_follows_the_same_rule(
        self, bucket: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
    ):
        """An endpoint the file system would take from the environment is checked too."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
        store = DeltaTableDatabase("s3://bucket/root")
        with pytest.raises(TableListingNotSupportedError, match="AWS_ALLOW_HTTP"):
            store.list_table_paths()
        assert bucket == []

        allowed = DeltaTableDatabase(
            "s3://bucket/root", storage_options={"AWS_ALLOW_HTTP": "true"}
        )
        assert allowed.list_table_paths(("absent",)) == []
        # An endpoint given in the storage options wins over the environment.
        https = DeltaTableDatabase(
            "s3://bucket/root",
            storage_options={"AWS_ENDPOINT_URL": "https://example.test"},
        )
        assert https.list_table_paths(("absent",)) == []

    def test_a_listing_that_fails_means_the_store_cannot_list(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """An error from the file system's ``ls`` is raised as
        ``TableListingNotSupportedError`` with its message.
        """
        import fsspec

        class Unreachable:
            """A file system whose listing always fails."""

            def ls(self, path: str, detail: bool = True) -> list[dict[str, Any]]:
                """Raise ``PermissionError`` for every listing."""
                raise PermissionError("access denied")

        monkeypatch.setattr(fsspec, "filesystem", lambda *a, **k: Unreachable())
        store = DeltaTableDatabase("s3://bucket/root")
        with pytest.raises(TableListingNotSupportedError, match="access denied"):
            store.list_table_paths(("job",))


class TestConnector:
    """Listing behaviour specific to ``ConnectorArrowDatabase``."""

    def test_components_after_the_prefix_come_back_sanitized(self, tmp_path: Path):
        """Listed components are the sanitized names, and the sanitized path reads the
        same table.
        """
        store = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "s.db")))
        path = ("job", "f", "semantic_v0.1:ab12", "v0", "python.function.v0", "rdb_v1")
        _fill(store, [path])
        listed = store.list_table_paths(("job", "f"))
        assert listed == [
            ("job", "f", "semantic_v0_1_ab12", "v0", "python_function_v0", "rdb_v1")
        ]
        # The sanitized path names the same table.
        assert store.table_exists(listed[0])
        records = store.get_all_records(listed[0])
        assert records is not None and records.num_rows == 1

    def test_the_last_component_is_the_layout(self, tmp_path: Path):
        """The last listed component is the sanitized last component of the written
        path.
        """
        store = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "s.db")))
        _fill(
            store,
            [
                ("job", "_result", "f", "semantic_v0.1:ab", "rdb_v2"),
                ("job", "f", "schema:semantic_v0.1:cd"),
            ],
        )
        assert [path[-1] for path in store.list_table_paths(("job",))] == [
            "rdb_v2",
            "schema_semantic_v0_1_cd",
        ]


class TestWrappers:
    """Listing through ``ExtensionAwareDatabase`` and ``NoOpArrowDatabase``."""

    def test_extension_aware_database_delegates(self):
        """The wrapper lists the same paths as the store it wraps."""
        inner = InMemoryArrowDatabase()
        _fill(inner)
        wrapper = ExtensionAwareDatabase(inner, UniversalTypeConverter())
        assert wrapper.list_table_paths(("job", "double")) == inner.list_table_paths(
            ("job", "double")
        )

    def test_no_op_database_holds_no_table(self):
        """A ``NoOpArrowDatabase`` lists nothing even after a record is added."""
        store = NoOpArrowDatabase()
        store.add_record(("job", "f", "pdb_v2"), b"r", pa.table({"x": [1]}))
        assert store.list_table_paths() == []
        assert store.list_table_paths(("job",)) == []


@pytest.mark.s3
def test_s3_lists_tables_and_nested_tables(s3_base_uri, s3_storage_options):
    """A Delta store on an S3 endpoint lists the tables under a prefix, nested ones
    included.
    """
    store = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    _fill(store)
    listed = DeltaTableDatabase(
        s3_base_uri, storage_options=s3_storage_options
    ).list_table_paths(("job", "double"))
    assert listed == [
        ("job", "double", "h1"),
        ("job", "double", "h1", "pdb_v1"),
        ("job", "double", "h2", "pdb_v2"),
    ]
