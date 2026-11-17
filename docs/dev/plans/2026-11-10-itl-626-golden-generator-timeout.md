# Golden generator subprocess timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A hung golden hash generator fails the test with a clear message instead of
stalling the test run (CodeRabbit comment 4533438815 on PR #284, ITL-626).

**Architecture:** `test_values_independent_of_process_and_hash_seed` runs
`generate_golden_hashes.py --stdout` in a subprocess with no timeout. Move that call into a
small helper, `_run_generator(env)`, that passes `timeout=600` and turns
`subprocess.TimeoutExpired` into `pytest.fail("golden generator timed out ...")`. No golden
value, case or generator code changes.

**Tech Stack:** pytest, `subprocess`.

---

### Task 1: Timeout on the generator subprocess

**Files:**
- Modify: `tests/test_hashing/golden/test_golden_hashes.py` (the seed test near line 90)

- [ ] **Step 1: Write the failing test** (append after the seed test)

```python
def test_generator_timeout_fails_with_message(monkeypatch: pytest.MonkeyPatch) -> None:
    # Exercises the private helper because the test is about its timeout handling.
    def hang(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", hang)
    with pytest.raises(pytest.fail.Exception, match="golden generator timed out"):
        _run_generator(os.environ.copy())
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/test_hashing/golden/test_golden_hashes.py -k timeout -v`
Expected: FAIL, `NameError: name '_run_generator' is not defined`.

- [ ] **Step 3: Implement**

```python
GENERATOR_TIMEOUT_S = 600


def _run_generator(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            [sys.executable, str(GENERATOR), "--stdout"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=GENERATOR_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            f"golden generator timed out after {GENERATOR_TIMEOUT_S}s "
            f"(PYTHONHASHSEED={env.get('PYTHONHASHSEED')})"
        )
```

and in the seed test replace the inline `subprocess.run(...)` with
`result = _run_generator(env)`.

- [ ] **Step 4: Verify**

- `uv run pytest tests/test_hashing/golden/ -v`: all pass.
- `git diff --stat tests/test_hashing/golden/data`: empty.
- `uv run ruff format . && uv run ruff check .`, `uv run --with basedpyright basedpyright`
  (exit code 0), `uv run pytest tests/`.

- [ ] **Step 5: Commit, push, reply**

`test(hashing): time out the golden generator subprocess (ITL-626)`; push; signed reply on
review thread 4533438815; wait for CI and the next CodeRabbit pass.
