"""Tests for orcapod.extension_types.file_type.File."""

from __future__ import annotations

import pytest

from orcapod.extension_types.file_type import File


class TestFileConstructor:
    def test_rejects_nonexistent_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            File(tmp_path / "does_not_exist.txt")

    def test_rejects_directory(self, tmp_path):
        with pytest.raises(IsADirectoryError):
            File(tmp_path)

    def test_rejects_symlink_to_directory_when_following(self, tmp_path):
        target_dir = tmp_path / "real_dir"
        target_dir.mkdir()
        link = tmp_path / "link_to_dir"
        link.symlink_to(target_dir)
        with pytest.raises(IsADirectoryError):
            File(link)  # follow_symlinks=True default, target is a dir

    def test_rejects_symlink_when_follow_symlinks_false(self, tmp_path):
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        link = tmp_path / "link_to_file"
        link.symlink_to(real_file)
        with pytest.raises(ValueError, match="symlink"):
            File(link, follow_symlinks=False)

    def test_accepts_symlink_to_file_when_following(self, tmp_path):
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        link = tmp_path / "link_to_file"
        link.symlink_to(real_file)
        f = File(link)  # follow_symlinks=True default
        assert str(f) == str(link)

    def test_accepts_zero_byte_file(self, tmp_path):
        empty = tmp_path / "empty.txt"
        empty.write_bytes(b"")
        f = File(empty)
        assert str(f) == str(empty)

    def test_accepts_regular_file(self, tmp_path):
        regular = tmp_path / "regular.txt"
        regular.write_text("hello")
        f = File(regular)
        assert str(f) == str(regular)

    def test_str_returns_path_string(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert str(f) == str(p)
