# torch.Tensor value type (ITL-871) Implementation Plan

> **SUPERSEDED (November 12, 2026).** Kept as the record of the first implementation. Do not
> follow it: the storage, the hash and the extension name changed
> (`superpowers/plans/2026-11-12-itl-871-native-storage-strict-hash.md`). In particular Task 7
> below tells you to edit `CHANGELOG.md` and `DESIGN_ISSUES.md`; a pull request never does
> that. The entries are `changelog.d/ITL-871.md` and `design_issues/ITL-871-TT1.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `torch.Tensor` is an Orcapod value type (`orcapod.torch_tensor`, hash label `torch_tensor_v1`) stored in the shared v0.3 array struct, registered lazily so `import orcapod` never imports torch.

**Architecture:** A new `logical_types/torch_type.py` (imports torch) holds the dtype<->tag table, canonicalisation and `LogicalTorchTensor`; `hashing/semantic_hashing/torch_handler.py` holds `TorchTensorHandler`. Both registries (`LogicalTypeRegistry`, `PythonTypeHandlerRegistry`) gain a "deferred types" table, filled from the context JSON, consulted on a lookup miss: by class qualified name (MRO walk, never imports torch) or by extension name (imports torch if installed, else raises an install error).

**Tech Stack:** PyArrow, Polars, NumPy, PyTorch (optional peer dependency, tests via `uv run --with torch`).

**Spec:** `superpowers/specs/2026-11-11-torch-tensor-v0.3.md` (provisional picks at the top override the body).
**Branch:** `eywalker/itl-871-add-pytorch-tensor-support-to-logical-types`, stacked on PR #289 (ITL-888). Draft PR against `main`.

---

## Resume notes (keep updated)

- 2026-11-11: spec approved provisionally by the coordinator (name `orcapod.torch_tensor`, label
  `torch_tensor_v1`, NumPy-shared dtypes + bfloat16, GPU/grad accepted, P1 peer dependency,
  no new CI workflow, draft PR). Plan written. Next: Task 1.
- Tasks 1-7 done (deferred registration, sort key, tests, torch_type + handler, declared-output
  check, golden `torch` group, docs/CHANGELOG/DESIGN_ISSUES). The delta-async store test is
  xfail(strict=False) for ITL-864 / PR #293 (verified it passes with #293 merged locally); remove
  the xfail once #293 is on main. Findings reported: ITL-896 (terminal operator node empty after
  async run), in-memory pass-through of the returned object (raised with the developer).
  Next: Task 8 (full suite, ruff, basedpyright, torch on 3.11-3.13, merge main, draft PR).
- Task 8 done: draft PR #300 (https://github.com/metamorphic-labs/orcapod-python/pull/300),
  CI green, CodeRabbit rounds 1 and 2 answered (round 1 fixes in e118c40c; #289-owned findings
  relayed). Open follow-ups: (1) when #289 gains the recursive MergeJoin ordering key for lists
  of arrays, merge #289's tip and add a colliding list[torch.Tensor] MergeJoin test; (2) remove
  the delta-async xfail once #293 (ITL-864) is on main; (3) merge main after #289 lands;
  (4) apply the developer's decisions on items (a)-(d) if they differ from the provisional picks.

## Commands

- Default suite (no torch): `uv run pytest tests/`
- Torch suite: `uv run --with torch pytest tests/test_torch tests/test_hashing/golden -q`
- Torch on each Python: `uv run --python 3.11 --with torch pytest tests/test_torch -q` (and 3.12, 3.13)
- Lint/type: `uv run --with ruff ruff check .`, `uv run --with ruff ruff format --check .`,
  `uv run --with basedpyright basedpyright` (exit code). basedpyright runs WITHOUT torch: the
  `import torch` lines carry `# pyright: ignore[reportMissingImports]  # optional peer dependency`
  and torch values are typed `Any`.

## File map

| File | Role |
|---|---|
| `src/orcapod/utils/deferred_types.py` (new) | `qualified_name(cls)`, `load_object(dotted)`, `MissingOptionalDependencyError` |
| `src/orcapod/logical_types/registry.py` | `deferred_logical_types` ctor arg; miss hooks in `get_by_python_type`, `get_by_arrow_extension_name` |
| `src/orcapod/hashing/semantic_hashing/type_handler_registry.py` | `deferred_handlers` ctor arg; miss hooks in `get_handler`, `get_handler_for_type` |
| `src/orcapod/contexts/data/v0.1.json` | deferred entries for torch + changelog line |
| `src/orcapod/logical_types/array_format.py` | `array_storage_sort_key` (moved from `LogicalNumpyArray.storage_sort_key`, shared) |
| `src/orcapod/logical_types/torch_type.py` (new) | tags, `canonical_tensor`, refusals, `LogicalTorchTensor` |
| `src/orcapod/hashing/semantic_hashing/torch_handler.py` (new) | `TorchTensorHandler` (`torch_tensor_v1`) |
| `src/orcapod/core/data_function.py` | declared-output check covers `torch.Tensor` |
| `src/orcapod/errors.py` | `MissingOptionalDependencyError(ImportError)` if no suitable error exists |
| `tests/test_torch/` (new) | all torch tests, `pytest.importorskip("torch")` per module |
| `tests/test_torch_absent/` or `tests/test_logical_types/test_deferred_types.py` | no-torch-import + install error (runs in default suite) |
| `tests/test_hashing/golden/{cases.py,generate_golden_hashes.py,test_golden_hashes.py,data/torch.json}` | new `torch` group, additions only |
| `docs/concepts/arrays.md`, `docs/reference/hashing.md` | Tensors section; label note |
| `CHANGELOG.md`, `DESIGN_ISSUES.md` | `[0.3.0]` entry; one appended entry |

## Task 1: deferred-type lookup (no torch involved)

Tests first, `tests/test_logical_types/test_deferred_types.py`, using a fake class defined in a
test helper module `tests/test_logical_types/deferred_fixture.py` with a module-level counter
proving it is imported only on first use:

- `LogicalTypeRegistry(deferred_logical_types=[{"python_type": "<module>.Thing",
  "extension_name": "test.thing", "logical_type": "<module>.LogicalThing",
  "requires": "<module>"}])`: `get_by_python_type(Thing)` returns a `LogicalThing` and registers it;
  a subclass of `Thing` resolves too; `get_by_arrow_extension_name("test.thing")` loads it;
  an unrelated type returns `None` without importing anything.
- `requires` names a module that does not exist -> `get_by_arrow_extension_name` raises
  `MissingOptionalDependencyError` whose message names the extension and the module.
- `PythonTypeHandlerRegistry(deferred_handlers=[{"python_type": ..., "handler": ...}])`:
  `get_handler(Thing())` loads and registers; unrelated returns `None`.

Implementation:

```python
# src/orcapod/utils/deferred_types.py
def qualified_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"

def load_object(dotted: str) -> Any:
    module_name, _, attr = dotted.rpartition(".")
    return getattr(importlib.import_module(module_name), attr)
```

In `LogicalTypeRegistry.__init__`: `self._deferred_by_python_name` and
`self._deferred_by_arrow_name` dicts of entries. `get_by_python_type`: after the existing
misses, walk `__mro__`; on a name hit call `self._load_deferred(entry)` (instantiate
`load_object(entry["logical_type"])()`, `register_logical_type`, drop the entry from both
dicts) and return the fresh lookup. `get_by_arrow_extension_name`: on a miss with a deferred
entry, `importlib.util.find_spec(entry["requires"])` is `None` -> raise
`MissingOptionalDependencyError(f"Arrow extension type {name!r} needs the optional dependency
{requires!r}, which is not installed: install it (for example `pip install torch`) to read
this column")`; else load. Same pattern for the handler registry (instantiate
`load_object(entry["handler"])()` and `register(load_object(entry["python_type"]), handler)`).

Commit: `feat(logical_types): deferred registration of optional value types (ITL-871)`.

## Task 2: shared sort key

Move the body of `LogicalNumpyArray.storage_sort_key` to
`array_format.array_storage_sort_key(storage_value)`; `LogicalNumpyArray.storage_sort_key`
calls it. Existing `tests/test_arrays` MergeJoin tests cover it (run them). Commit
`refactor(array_format): share the stored-row ordering key (ITL-871)`.

## Task 3: tests for the tensor type (failing first)

`tests/test_torch/` modules, each starting `torch = pytest.importorskip("torch")`; mirror
`tests/test_arrays` (`array_cases.py` -> `tensor_cases.py`):

- `test_tensor_value_format.py` (logical type + hasher through the public context API:
  `get_default_context().semantic_hasher.hash_object`, `type_converter`):
  stored row per dtype (all shared tags + bfloat16) equals the independent reference
  (`hashlib` + `struct`) digest; label `torch_tensor_v1`; tensor vs equal ndarray: hashes differ,
  digest bodies equal; invariances (`t.T.contiguous()` vs `t.T`, channels-last, `t.conj()` vs
  resolved, `requires_grad`, `nn.Parameter`, MPS when available, CUDA when available);
  differences (dtype width, shape `(0,3)`/`(3,0)`, `()`/`(1,)`, `-0.0`/`0.0`); refusals with
  the column named (via a pod or `DictSource`): sparse, quantized, nested, meta,
  `complex32`, `float8_e4m3fn`, `int4`; no mutation (`requires_grad`, device, strides,
  conj bit, `data_ptr`, values of the input unchanged after storing/hashing).
- `test_tensor_round_trip.py`: pod `x: torch.Tensor -> torch.Tensor` in/out, chained pods,
  read-back is CPU, contiguous, `requires_grad=False`, exact dtype and shape, plain
  `torch.Tensor`, writeable, does not share memory; 0-d and empty; bfloat16 exact bits;
  declared `-> torch.Tensor` returning an ndarray or float raises `TypeError` naming the key;
  an `np.ndarray` column into a `torch.Tensor` parameter is a type error at graph build.
- `test_tensor_operators.py`: Batch, GroupBy (`list[torch.Tensor]`), MergeJoin with a colliding
  tensor column (order by stored row), SemiJoin, Join, PolarsFilter, Select/Drop, MapData;
  each sync and async (`tests/test_core/operators/test_async_sync_equivalence.py` pattern).
- `test_tensor_pipeline_stores.py`: in-memory and Delta databases, sync and async.
- `test_tensor_cached_rerun.py`: Delta store, run, then a fresh `subprocess` run hits the cache
  (no recompute) and reads equal tensors; cross-process and `PYTHONHASHSEED` stability of hashes.
- `tests/test_logical_types/test_torch_optional.py` (default suite): subprocess
  `import orcapod`, default context, run a small numpy pipeline, assert `"torch" not in
  sys.modules`; with torch absent (skip if present) reading a hand-built `orcapod.torch_tensor`
  column through `ArrowTableSource`/datagram raises `MissingOptionalDependencyError`.

Run `uv run --with torch pytest tests/test_torch -q`: expect failures (`Unsupported Python type`).
Commit (WIP tests).

## Task 4: `torch_type.py` and `TorchTensorHandler`

```python
TORCH_TENSOR_EXTENSION_NAME = "orcapod.torch_tensor"
TORCH_TENSOR_HASH_LABEL = "torch_tensor_v1"
# dtype -> (tag, raw-bit view dtype or None); built at import from getattr(torch, name, None)
_SHARED = ["bool", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64",
           "float16", "float32", "float64", "complex64", "complex128"]
_RAW_BITS = {"bfloat16": "int16"}
```

`canonical_tensor(value) -> tuple[str, tuple[int, ...], memoryview]`:
1. not `torch.Tensor` -> `TypeError`; `layout != torch.strided` -> refuse (sparse/mkldnn,
   "convert with .to_dense()"); `is_quantized`, `is_nested`, `device.type == "meta"`,
   named (`any(n is not None for n in getattr(value, "names", ()) or ())`) -> refuse;
   dtype not in the table -> refuse naming dtype ("not supported in v0.3").
2. `t = value.detach()`; `if t.device.type != "cpu": t = t.to("cpu")`;
   `t = t.resolve_conj().resolve_neg().contiguous()`; raw-bit dtypes `t = t.view(int dtype)`.
3. `arr = t.numpy()`; `_, canon = numpy_type.canonical_array(arr)`; return
   `(tag, tuple(int(d) for d in value.shape), numpy_type.array_bytes(canon))`.

Refusal message: `UnsupportedValueError(f"torch tensors with {what} cannot be stored: {reason}")`
(the converter prefixes the column, as for arrays).

`LogicalTorchTensor(BaseLogicalType)`: same structure as `LogicalNumpyArray`
(`make_arrow_extension_type(TORCH_TENSOR_EXTENSION_NAME, ARRAY_STORAGE_TYPE,
ARRAY_FORMAT_METADATA)`, polars likewise, `python_type = torch.Tensor`);
`python_to_storage` -> `{"dtype", "shape", "data": bytes}`; `storage_to_python`: validate
fields and length like ndarray, `np_dtype = numpy_type.tag_to_dtype(tag or raw int tag)`,
`arr = np.frombuffer(...).reshape(dims).copy()`, native byte order, `torch.from_numpy(arr)`,
`.view(torch dtype)` for raw-bit tags; unknown tag for tensors -> `ValueError`;
`storage_sort_key = array_storage_sort_key`.

`TorchTensorHandler.handle(obj, hasher)` -> `ContentHash("torch_tensor_v1",
array_digest_v1(*canonical_tensor(obj)))`.

Context JSON: `"deferred_logical_types": [{"python_type": "torch.Tensor", "extension_name":
"orcapod.torch_tensor", "logical_type": "orcapod.logical_types.torch_type.LogicalTorchTensor",
"requires": "torch"}]` in the registry `_config`; `"deferred_handlers": [{"python_type":
"torch.Tensor", "handler": "orcapod.hashing.semantic_hashing.torch_handler.TorchTensorHandler"}]`
in the handler registry `_config`; one changelog line. Verify the objectspec parser passes plain
string dicts through unchanged.

Run torch tests until green; run default suite. Commit
`feat(logical_types): torch.Tensor value type orcapod.torch_tensor (ITL-871)`.

## Task 5: declared-output check

Generalise `_declared_ndarray_type` to match `numpy.ndarray` or `torch.Tensor` by MRO names;
message for tensors suggests `torch.as_tensor(...)`. Test in Task 3 goes green. Commit.

## Task 6: golden `torch` group (additions only)

`cases.py`: `GROUPS` gains `"torch"`; torch cases registered only when torch imports (the
values of `ndarray.*` cases as tensors plus bool, bfloat16, zero_dim, non_contiguous;
`type.torch_tensor`; `single_field.torch_tensor.{arrow_type, schema_hash}`;
`torch_tensor.{content_hash, pipeline_hash}` from a datagram source). Test module: entries and
file-present checks for the `torch` group skip without torch. Generator: without torch, keep
`torch.json` untouched (never write it empty). Generate with torch; `git diff` must show only
the new file. Commit `test(golden): pin torch tensor hashes (ITL-871)`.

## Task 7: docs, CHANGELOG, DESIGN_ISSUES

- `docs/concepts/arrays.md`: "PyTorch tensors" section (install as peer dependency, identity
  table, what is not stored, GPU/grad accepted, read-back, dtypes and refusals, distinct from
  ndarray). Code samples are executed by `tests/test_docs` without torch: mark them
  `<!-- docs-test: skip -->` (needs torch, infrastructure the test run lacks) or keep prose.
- `docs/reference/hashing.md`: `torch_tensor_v1` label next to `ndarray_v1`.
- `CHANGELOG.md` `[0.3.0] — unreleased` -> `### Added`: tensor entry.
- `DESIGN_ISSUES.md`: one appended entry for ITL-871 (resolved with Fix note).

## Task 8: finish

Full default suite, torch suite on 3.11/3.12/3.13, ruff, basedpyright; merge `origin/main`
(no rebase); push; draft PR (`Fixes ITL-871`, stacked on #289, items a-d await the developer,
torch results pasted); `orca linear attach`, `orca worktree set --pr`; CodeRabbit rounds;
worker_done.

---

# Phase 2: developer decisions of 2026-11-11 (dispatch task_3ef74b556455)

Decisions: 2a name/label confirmed; 2b add complex32 + float8 family (raw bits, torch names
as tags; a tag the installed torch lacks fails clearly at use, never at import; quantized,
sparse, nested, sub-byte still refused); 2c GPU/grad accepted with a warning (plan below,
needs coordinator OK); 2d `orcapod[torch]` extra, support driven by importability, lazy
registration kept; 2e torch tests run in the REQUIRED CI on every PR (plan below, needs OK).
Also: no-debug-only-parameters over the whole diff; list[torch.Tensor] MergeJoin test after
#289 merges into main; ITL-864 xfail removal after #293 is on main.

## CI facts (checked 2026-11-11)

- Org-required `python-required-ci.yml` -> `ci-pytest.yml`: shared `setup-uv` action (installs
  uv, Python, uv cache keyed on uv.lock), then `uv run pytest tests/`. No `uv sync`, no
  `--all-extras`: `uv run` syncs the project + the default `dev` group only. **An extra alone
  is NOT installed there.**
- `ci-typecheck.yml`: `uv run --with basedpyright basedpyright` -> same default sync (dev group).
- `ci-license-check.yml`: `uv run --no-dev --with pip-licenses` -> project deps only, no extras.
- Repo `run-tests.yml` / `release.yml`: `uv sync --locked --all-extras --dev` -> extras installed.
- Repo `_license-check.yml`: `uv sync --locked --dev` -> dev group (so whatever dev pulls).

## 2e plan

1. `[project.optional-dependencies] torch = ["torch>=2.3"]`; `all` gains `orcapod[torch]`.
2. `dev` group gains `"orcapod[torch]"` (uv self-reference with an extra): the default
   `uv run` sync, i.e. the org-required pytest and typecheck jobs, installs torch; one source of
   truth for the version bound. Users never get torch unless they ask for the extra.
3. CPU wheels: `[[tool.uv.index]] name = "pytorch-cpu", url = "https://download.pytorch.org/whl/cpu",
   explicit = true` and `[tool.uv.sources] torch = [{ index = "pytorch-cpu", marker =
   "sys_platform == 'linux'" }]`; macOS/Windows use PyPI. `tool.uv.sources` is not in the
   published metadata, so `pip install orcapod[torch]` is unaffected.
4. `uv lock`; check no `nvidia-*`/`triton` in the Linux resolution.
5. CI guard: `tests/test_torch/test_ci_has_torch.py` (no importorskip): fails when
   `CI` env var is set (GitHub sets `CI=true`) and torch is not importable.
6. No-torch path keeps running although dev envs now have torch: subprocess tests with a
   `sys.meta_path` blocker for `torch` (not a sys.modules hack): `import orcapod` + run never
   imports torch; reading an `orcapod.torch_tensor` column raises
   `MissingOptionalDependencyError`. The current skipif-torch-installed test is converted.
7. basedpyright now sees torch: fix new errors; drop the now-unneeded
   `reportMissingImports` ignores.
8. Measure: wheel download size and `uv sync` time added per job (cold cache), licenses of
   everything the extra pulls (`pip-licenses` on the torch closure).

## 2c plan (warning)

- Category `orcapod.errors.ValueConversionWarning(UserWarning)` (name to confirm; #299 has no
  warning category as of 2026-11-11).
- Emitted by the callers that have the context (no-debug-only-parameters): a pure helper in
  `torch_type` returns what a store converts (`device 'cuda:0'`, `requires_grad=True`), and
  (a) `PythonDataFunction._build_output_data` warns naming the function and the output key;
  (b) `DictSource`/`ListSource` (Python-value sources) warn naming the source and column.
  Tensors nested in lists/dicts of an output are included.
- Once per (function instance, output key, conversion kinds); one per (source, column) with the
  row count. A 10,000-row run where every row returns a CUDA grad tensor: 1 warning.
- Never for plain CPU tensors without grad. Tests with `pytest.warns` / `warnings.catch_warnings`.

## Phase 2 resume notes

- 2c and 2e plans approved by the coordinator. Shared category `ValueConversionWarning` in
  `orcapod.errors` (exact docstring sent to the coordinator for #299; placed just before
  `FieldNotResolvableError`, where #299 puts its class, so the later merge conflicts visibly).
- Done: packaging (extra, dev group, CPU index, lock), CI guard, no-torch subprocess tests,
  complex32/float8 (+ golden additions), conversion warning (pods + DictSource/ListSource),
  basedpyright with torch (exit 0), docs/CHANGELOG/DESIGN_ISSUES/spec, merged origin/main
  (#287) and #289's tip (recursive list ordering), list[torch.Tensor] MergeJoin tests (fail
  before #289's fix with TypeError dict < dict).
- Measured: Linux CPU torch wheel 209.5 MB (cp311-313), sympy 6.3 MB, mpmath 0.5 MB; macOS
  wheel 138.5 MB. Licenses: torch "Apache-2.0 AND ... BSD-3-Clause ... MIT AND Zlib" (passes
  the repo's pip-licenses check), sympy/mpmath/networkx/jinja2/markupsafe BSD, fsspec
  BSD-3-Clause, filelock Unlicense, typing_extensions PSF-2.0. CI time added: read from the
  CI logs of the next run.
- Left: ITL-864 xfail removal once #293 is on main; merge main again when #289 lands.
  Next: full suite, push, PR description, mark ready, CI, CodeRabbit (read review bodies).
- 2026-11-11 later: merged main (#296, #297). PR #300 marked ready; description rewritten with
  final decisions. CI measured: torch CPU wheel 199.8 MiB, 19-24 s download per required pytest
  job, install of all 199 packages 5.3-6.4 s, torch tests ~35-40 s; required pytest jobs went
  from 2m29-2m50 to 3m23-4m00 (cold cache for the new lock). CodeRabbit on 7384e3e5: 2 actionable
  (torch 2.3-2.6 lack float8_e8m0fnu) fixed in 94564573 (cases.unavailable_entries, generator
  keeps pins); 3 nitpicks on #289 code relayed; review of 94564573 had nothing actionable;
  docstring-coverage pre-merge warning answered (ITL-871 functions all documented).
  Left: remove ITL-864 xfail once #293 is on main; merge main after #289 lands.
