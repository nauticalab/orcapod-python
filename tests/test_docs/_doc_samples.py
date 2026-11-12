"""Extract and run the fenced ``python`` blocks of a Markdown page.

Rules a documentation author needs to know:

* Every fence opened with ``python`` is executed. The blocks of one page share a
  single namespace and run top to bottom, so a later block may use names an
  earlier block defined.
* A fence is three or more backticks or tildes and is closed by a line holding
  exactly the same run. Everything inside a fence in another language is text,
  so a ``python`` fence quoted inside a longer ``markdown`` fence is not run.
* A block is skipped only when the line directly above its fence (blank lines
  allowed) is the HTML comment ``<!-- docs-test: skip -->``. The comment is
  invisible in the rendered site. Use it for samples that need infrastructure
  the test run does not have (a Postgres server), never to hide a sample that
  is simply wrong.
* Every non-empty line a block prints must match one of the comments in that
  block (the text after ``#``). Runs of whitespace are collapsed, and ``...``
  in a comment matches anything, for hashes and other run-dependent values.
  A block that prints nothing needs no comments.

Run as ``python -m tests.test_docs._doc_samples <page.md>`` to execute one page;
the last line of stdout is a JSON list with one result per block.
"""

from __future__ import annotations

import contextlib
import io
import json
import linecache
import re
import sys
import textwrap
import tokenize
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SKIP_MARKER = "<!-- docs-test: skip -->"

_FENCE = re.compile(r"^\s*(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class Block:
    """One fenced ``python`` block.

    Attributes:
        lineno: 1-based line number of the opening fence in the page.
        source: The dedented code, newline-terminated.
        skip: True when the block carries the ``docs-test: skip`` marker.
    """

    lineno: int
    source: str
    skip: bool


def _fence_language(info: str) -> str:
    """Return the language named by a fence's info string, or ``""``.

    Args:
        info: The text after the fence characters on the opening line.

    Returns:
        The first word of ``info`` without a leading ``{`` or ``.``, so
        ``python``, ``python title="x"`` and ``{.python}`` all give ``python``.
    """
    words = info.strip().lstrip("{").split()
    return words[0].strip(".{}") if words else ""


def extract_blocks(text: str) -> list[Block]:
    """Return the fenced ``python`` blocks of a Markdown document, in order.

    Fences are matched the way both renderers of these pages agree on
    (CommonMark for ``README.md`` on GitHub, SuperFences for the MkDocs site):
    a fence is closed by a line holding the same character repeated the same
    number of times. A longer run of that character would close the fence on
    GitHub but not on the site, so it is rejected rather than guessed at.

    Args:
        text: The Markdown source.

    Returns:
        One ``Block`` per ``python`` fence. Fences in any other language, or
        with no language, are ignored, together with everything inside them.

    Raises:
        ValueError: If a ``python`` fence is never closed, or if a fence
            contains a longer run of its own fence character on a line by
            itself.
    """
    blocks: list[Block] = []
    lines = text.splitlines()
    skip_next = False
    i = 0
    while i < len(lines):
        line = lines[i]
        opener = _FENCE.match(line)
        # A backtick fence cannot have a backtick in its info string; such a
        # line is inline code, not a fence.
        if opener and not (opener["fence"][0] == "`" and "`" in opener["info"]):
            fence = opener["fence"]
            is_python = _fence_language(opener["info"]) == "python"
            start = i
            body: list[str] = []
            i += 1
            while i < len(lines) and lines[i].strip() != fence:
                inner = lines[i].strip()
                if len(inner) > len(fence) and set(inner) == {fence[0]}:
                    raise ValueError(
                        f"line {i + 1}: a run of {len(inner)} {fence[0]!r} inside the "
                        f"fence opened at line {start + 1} with {len(fence)}; GitHub "
                        "and MkDocs disagree on whether it closes the fence"
                    )
                body.append(lines[i])
                i += 1
            if is_python:
                if i == len(lines):
                    raise ValueError(
                        f"python fence opened at line {start + 1} is never closed"
                    )
                source = textwrap.dedent("\n".join(body)) + "\n"
                blocks.append(Block(lineno=start + 1, source=source, skip=skip_next))
            skip_next = False
        elif line.strip() == SKIP_MARKER:
            skip_next = True
        elif line.strip():
            # Any other content between the marker and a fence cancels the marker.
            skip_next = False
        i += 1
    return blocks


def expected_patterns(source: str) -> list[str]:
    """Return the text of every comment in ``source``, without the leading ``#``.

    Args:
        source: Python source code.

    Returns:
        Comment texts in source order, stripped. Empty if the source cannot be
        tokenized (it will fail to run anyway).
    """
    patterns: list[str] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(source).readline):
            if tok.type == tokenize.COMMENT:
                patterns.append(tok.string.lstrip("#").strip())
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return []
    return patterns


def _normalize(line: str) -> str:
    """Collapse runs of whitespace in ``line`` and strip its ends."""
    return _WHITESPACE.sub(" ", line).strip()


def _to_regex(pattern: str) -> re.Pattern[str]:
    """Compile a shown-output comment into a regex, with ``...`` as a wildcard."""
    parts = [re.escape(part) for part in _normalize(pattern).split("...")]
    return re.compile(".*".join(parts))


def unmatched_output(stdout: str, source: str) -> list[str]:
    """Return the printed lines that the sample does not show.

    Args:
        stdout: Everything the block printed.
        source: The block's source, whose comments are the shown output.

    Returns:
        The non-empty printed lines (whitespace-collapsed) that match none of
        the block's comments. An empty list means the shown output is true.
    """
    regexes = [_to_regex(p) for p in expected_patterns(source) if p]
    missing: list[str] = []
    for raw in stdout.splitlines():
        line = _normalize(raw)
        if not line:
            continue
        if not any(rx.fullmatch(line) for rx in regexes):
            missing.append(line)
    return missing


def run_blocks(blocks: list[Block], name: str = "<doc>") -> list[dict[str, Any]]:
    """Execute blocks in order in one shared namespace.

    A failing block does not stop the run, so one page reports all of its
    problems at once.

    Args:
        blocks: The blocks to run.
        name: Label used in tracebacks, normally the page path.

    Returns:
        One dict per block with keys ``lineno``, ``status`` (``"passed"``,
        ``"failed"`` or ``"skipped"``), ``error`` (traceback text or ``None``)
        and ``unmatched`` (printed lines the block does not show).
    """
    namespace: dict[str, Any] = {"__name__": "__main__"}
    results: list[dict[str, Any]] = []
    for block in blocks:
        result: dict[str, Any] = {
            "lineno": block.lineno,
            "status": "passed",
            "error": None,
            "unmatched": [],
        }
        if block.skip:
            result["status"] = "skipped"
            results.append(result)
            continue
        captured = io.StringIO()
        filename = f"{name}:{block.lineno}"
        # Register the block's source so ``inspect.getsource`` (used to hash function
        # pods) reads the block itself. Without it, ``linecache`` falls back to the
        # ``__main__`` module's loader, i.e. this runner's own file (seen on 3.13).
        linecache.cache[filename] = (
            len(block.source),
            None,
            block.source.splitlines(keepends=True),
            filename,
        )
        try:
            code = compile(block.source, filename, "exec")
            with contextlib.redirect_stdout(captured):
                exec(code, namespace)  # noqa: S102 - running our own documentation
        except BaseException:  # noqa: BLE001 - report SystemExit etc. as failures too
            result["status"] = "failed"
            result["error"] = traceback.format_exc(limit=-3)
        else:
            result["unmatched"] = unmatched_output(captured.getvalue(), block.source)
            if result["unmatched"]:
                result["status"] = "failed"
        results.append(result)
    return results


def main(argv: list[str]) -> int:
    """Run one page and print its results as a JSON line.

    Args:
        argv: The command line; ``argv[1]`` is the path of the Markdown page.

    Returns:
        The process exit code, always 0: failing blocks are reported in the
        JSON line, not through the exit code.
    """
    page = Path(argv[1])
    blocks = extract_blocks(page.read_text(encoding="utf-8"))
    real_stdout = sys.stdout
    results = run_blocks(blocks, name=page.name)
    print(json.dumps(results), file=real_stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
