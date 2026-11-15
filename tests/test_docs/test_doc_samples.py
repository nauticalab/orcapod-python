"""Execute the code samples in the user-facing documentation.

Every fenced ``python`` block in the pages listed in ``DOC_PAGES`` is run, page
by page, and what each block prints is compared with the ``# ...`` comments the
block shows. See ``_doc_samples`` for the rules a documentation author has to
know (shared namespace per page, the ``<!-- docs-test: skip -->`` marker, the
``...`` wildcard).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_docs._doc_samples import (
    Block,
    expected_patterns,
    extract_blocks,
    run_blocks,
    unmatched_output,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

DOC_PAGES = sorted(
    str(p.relative_to(REPO_ROOT))
    for p in [
        REPO_ROOT / "README.md",
        REPO_ROOT / "docs" / "index.md",
        REPO_ROOT / "docs" / "getting-started.md",
        REPO_ROOT / "docs" / "troubleshooting.md",
        *(REPO_ROOT / "docs" / "concepts").glob("*.md"),
        *(REPO_ROOT / "docs" / "reference").glob("*.md"),
        *(REPO_ROOT / "docs" / "tutorial").glob("*.md"),
    ]
)

# The tutorial pages start with ``from workshop_data import make_dataset``; the
# generator lives next to them so that the built site serves it for download.
TUTORIAL_DIR = REPO_ROOT / "docs" / "tutorial"


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestExtractBlocks:
    def test_only_python_fences_are_extracted(self):
        text = "\n".join(
            [
                "intro",
                "```bash",
                "uv add orcapod",
                "```",
                "```python",
                "x = 1",
                "```",
                "```",
                "plain fence",
                "```",
            ]
        )
        blocks = extract_blocks(text)
        assert blocks == [Block(lineno=5, source="x = 1\n", skip=False)]

    def test_indented_fence_is_dedented(self):
        text = "\n".join(
            [
                "!!! tip",
                "    ```python",
                "    if True:",
                "        x = 1",
                "    ```",
            ]
        )
        (block,) = extract_blocks(text)
        assert block.source == "if True:\n    x = 1\n"

    def test_skip_marker_applies_to_next_fence_only(self):
        text = "\n".join(
            [
                "<!-- docs-test: skip -->",
                "",
                "```python",
                "needs_postgres()",
                "```",
                "```python",
                "y = 2",
                "```",
            ]
        )
        first, second = extract_blocks(text)
        assert first.skip is True
        assert second.skip is False

    def test_skip_marker_does_not_leak_past_prose(self):
        text = "\n".join(
            [
                "<!-- docs-test: skip -->",
                "A paragraph in between.",
                "```python",
                "y = 2",
                "```",
            ]
        )
        (block,) = extract_blocks(text)
        assert block.skip is False

    def test_unterminated_fence_raises(self):
        with pytest.raises(ValueError, match="line 1"):
            extract_blocks("```python\nx = 1\n")

    @pytest.mark.parametrize("fence", ["````", "~~~", "~~~~~"])
    def test_longer_and_tilde_fences_are_extracted(self, fence: str):
        text = "\n".join([f"{fence}python", "x = 1", "```", "y = 2", fence])
        assert extract_blocks(text) == [
            Block(lineno=1, source="x = 1\n```\ny = 2\n", skip=False)
        ]

    def test_fence_with_attributes_is_extracted(self):
        text = "\n".join(['```python title="demo.py"', "x = 1", "```"])
        assert extract_blocks(text) == [Block(lineno=1, source="x = 1\n", skip=False)]

    def test_python_fence_quoted_inside_another_fence_is_not_a_sample(self):
        text = "\n".join(
            [
                "````markdown",
                "```python",
                "raise SystemExit('quoted, not a sample')",
                "```",
                "````",
                "```python",
                "x = 1",
                "```",
            ]
        )
        assert extract_blocks(text) == [Block(lineno=6, source="x = 1\n", skip=False)]

    def test_python_line_inside_a_plain_fence_is_not_a_sample(self):
        # Inside a fence, "```python" is not a closing fence, so it is text.
        text = "\n".join(["```text", "```python", "x = 1", "```", "y = 2"])
        assert extract_blocks(text) == []

    def test_other_language_fence_cancels_the_skip_marker(self):
        text = "\n".join(
            [
                "<!-- docs-test: skip -->",
                "```bash",
                "uv add orcapod",
                "```",
                "```python",
                "y = 2",
                "```",
            ]
        )
        (block,) = extract_blocks(text)
        assert block.skip is False

    def test_ambiguous_longer_closing_fence_raises(self):
        with pytest.raises(ValueError, match="line 3"):
            extract_blocks("```python\nx = 1\n````\n")

    def test_inline_code_line_is_not_a_fence(self):
        text = "\n".join(["```not a fence```", "```python", "x = 1", "```"])
        assert extract_blocks(text) == [Block(lineno=2, source="x = 1\n", skip=False)]


class TestExpectedPatterns:
    def test_collects_full_line_and_trailing_comments(self):
        source = "print(a)  # ('x',)\n# {'k': 1}\n"
        assert expected_patterns(source) == ["('x',)", "{'k': 1}"]

    def test_hash_inside_string_is_not_a_comment(self):
        assert expected_patterns('print("# not a comment")\n') == []


class TestUnmatchedOutput:
    def test_documented_output_matches(self):
        source = "print({'bmi': 1.0})\n# {'bmi': 1.0}\n"
        assert unmatched_output("{'bmi': 1.0}\n", source) == []

    def test_wrong_shown_value_is_reported(self):
        source = "print({'bmi': 2.0})\n# {'bmi': 1.0}\n"
        assert unmatched_output("{'bmi': 2.0}\n", source) == ["{'bmi': 2.0}"]

    def test_undocumented_output_is_reported(self):
        assert unmatched_output("hello\n", "print('hello')\n") == ["hello"]

    def test_ellipsis_is_a_wildcard(self):
        source = "print(h)\n# ['a', '_tag::...', 'z']\n"
        assert unmatched_output("['a', '_tag::source:abc123', 'z']\n", source) == []

    def test_ellipsis_does_not_match_everything_else(self):
        source = "print(h)\n# ['a', '_tag::...', 'z']\n"
        assert unmatched_output("['b', '_tag::q', 'z']\n", source) != []

    def test_whitespace_runs_are_collapsed(self):
        source = "print(df)\n#   subject_id  age\n# 0   mouse_01   12\n"
        stdout = "  subject_id  age\n0   mouse_01   12\n"
        assert unmatched_output(stdout, source) == []

    def test_blank_output_lines_are_ignored(self):
        assert unmatched_output("\n\n", "print()\n") == []


class TestRunBlocks:
    def test_blocks_share_a_namespace(self):
        results = run_blocks(
            [
                Block(lineno=1, source="x = 41\n", skip=False),
                Block(lineno=5, source="print(x + 1)  # 42\n", skip=False),
            ]
        )
        assert [r["status"] for r in results] == ["passed", "passed"]

    def test_exception_is_reported_with_line_and_run_continues(self):
        results = run_blocks(
            [
                Block(lineno=3, source="raise KeyError('boom')\n", skip=False),
                Block(lineno=9, source="y = 1\n", skip=False),
            ]
        )
        assert results[0]["status"] == "failed"
        assert results[0]["lineno"] == 3
        assert "KeyError" in results[0]["error"]
        assert results[1]["status"] == "passed"

    def test_wrong_output_fails_the_block(self):
        (result,) = run_blocks([Block(lineno=1, source="print(2)  # 3\n", skip=False)])
        assert result["status"] == "failed"
        assert result["unmatched"] == ["2"]

    def test_skipped_block_is_not_executed(self):
        (result,) = run_blocks(
            [Block(lineno=1, source="raise SystemExit(1)\n", skip=True)]
        )
        assert result["status"] == "skipped"


# ---------------------------------------------------------------------------
# The documentation itself
# ---------------------------------------------------------------------------


def test_doc_pages_are_discovered():
    """A moved or renamed page must not silently drop out of the run."""
    for expected in (
        "README.md",
        "docs/index.md",
        "docs/getting-started.md",
        "docs/concepts/function-pods.md",
        "docs/concepts/operators.md",
        "docs/concepts/pipeline-job.md",
        "docs/concepts/file-types.md",
        "docs/troubleshooting.md",
        "docs/tutorial/index.md",
        "docs/tutorial/00-setup.md",
        "docs/tutorial/01-first-stream.md",
        "docs/tutorial/02-function-pods.md",
        "docs/tutorial/03-operators.md",
        "docs/tutorial/04-many-rows-into-one.md",
        "docs/tutorial/05-pipeline-job.md",
        "docs/tutorial/06-reruns-and-caching.md",
        "docs/tutorial/07-files.md",
        "docs/tutorial/08-inspecting-and-tracing.md",
        "docs/tutorial/09-when-things-go-wrong.md",
        "docs/tutorial/10-where-next.md",
    ):
        assert expected in DOC_PAGES


@pytest.mark.parametrize("page", DOC_PAGES)
def test_doc_page_samples_run(page: str, tmp_path: Path):
    """Every python sample on the page runs and prints what the page shows.

    Each page runs in its own interpreter, with the home directory (``HOME``,
    and ``USERPROFILE``, which is what ``Path.home()`` reads on Windows) and
    the working directory pointed at a temp directory: samples such as
    ``enable_file_hash_caching()`` change process-global state and write under
    ``~/.orcapod``.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT), str(REPO_ROOT / "src"), str(TUTORIAL_DIR)]
    )
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env.pop("ORCAPOD_HASH_CACHE_DB", None)

    proc = subprocess.run(
        [sys.executable, "-m", "tests.test_docs._doc_samples", str(REPO_ROOT / page)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode == 0, (
        f"sample runner crashed on {page}:\n{proc.stdout}\n{proc.stderr}"
    )
    stdout_lines = proc.stdout.splitlines()
    assert stdout_lines, f"sample runner printed no results for {page}:\n{proc.stderr}"
    results = json.loads(stdout_lines[-1])

    failures = []
    for r in results:
        if r["status"] != "failed":
            continue
        detail = r[
            "error"
        ] or "printed output is not shown in the sample:\n" + "\n".join(
            f"    {line}" for line in r["unmatched"]
        )
        failures.append(f"{page}:{r['lineno']}\n{detail}")
    assert not failures, "\n\n".join(failures)
