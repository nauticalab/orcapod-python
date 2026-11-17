## `src/orcapod/utils/`

### U2 — Bare `except` in `get_git_info()`
**Status:** open
**Severity:** high

`get_git_info()` (`git_utils.py:55`) catches all exceptions including `KeyboardInterrupt` and
`SystemExit`:
```python
except:  # TODO: specify exception
```

Fix: catch `(OSError, subprocess.SubprocessError, FileNotFoundError)` specifically.
