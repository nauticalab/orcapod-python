# Function Defaults Through the Semantic Hasher (ITL-953) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A function's default values enter its hashes as the semantic hasher's hash of each
value instead of `repr(value)`; a default the hasher cannot hash raises, naming the parameter
and the function.

**Architecture:** The signature text stops carrying defaults. Defaults become a separate member
of the hashed description: a list of `[parameter name, hash of the default]` in declared order,
present only when the function has defaults. One helper hashes the defaults and wraps the
hasher's `TypeError` with the parameter and function names; both hash sites call it.

**Tech Stack:** Python 3.11-3.13, pytest via `uv run`.

**Linear:** ITL-953 (In Progress). Branch `eywalker/itl-953-function-default-values-enter-the-function`,
to be created FROM `origin/eywalker/itl-967-v03-hash-step-combine-the-hash-changing` once the
coordinator says the six merges are in. PR against `main`, "Stacked on the ITL-967 pull request".

**Status (November 12, 2026):** complete, PR opened against `main`, stacked on ITL-967.
Merged ITL-967 at `c4b1e4a7` (all six hash-step branches, main with #316, its golden
regeneration). Relative to `c4b1e4a7`:

- golden files `tests/test_hashing/golden/data`: 0 changed, 0 removed, 5 added
  (`semantic:function.value.{plain,defaults,kinds}`,
  `semantic:function.stored_signature.{ratio,scaled}`); a regeneration on the merged head
  gives no diff;
- re-pinned by hand: `test_canonical_type_names.py`
  (`test_function_object_hash`, `test_function_signature_hash`) and the two
  builtin-annotation functions in `hash_samples/type_annotation_golden.json` (a deliberate
  pre-fix snapshot, so not regenerated wholesale);
- checks on the merged head: `uv run pytest tests/` 8608 passed, 101 skipped, 18 xfailed,
  0 failed (3.12; 3.11 and 3.13 were green at the previous merge, 8593 passed each); ruff
  check and format clean; basedpyright exit 0; changelog fragments OK.

**Round 1 (PR #323, CodeRabbit review 5871552520 on `3ed0d418`):** 30 findings, all in the
review body. Two were on this PR's files (missing docstrings in
`test_function_default_hashing.py` and `test_function_info_extractors.py`): fixed. The other
28 are on ITL-967 files and were routed to the coordinator. Then merged ITL-967 at `9743004d`
(main with #317): golden data still differs from that tip by the same 5 added entries only;
3.12 suite 8696 passed, 0 failed; ruff clean; basedpyright exit 0.

**Left:** CI and CodeRabbit on the pushed head, the signed top-level comment, `worker_done`.

**Decisions that supersede the text further down:**

- **D1 = (b)** (developer): one structured description. `parameters` is a list of
  `{name, kind, annotation, default}` entries in declared order; `annotation` / `default`
  absent when there is none. Fixes finding X (parameter kind). Every function-value hash moves.
- **Stored column = `hash_object(function)`** (coordinator, supersedes D2/S2): one description
  of a signature. Every newly written `__pf_var_function_signature_hash` value moves.
  `get_function_signature` is no longer used by `src` (kept public, with the Y fix).
- **D3 = raise** for v0.3 (developer, November 11, 2026); follow-up ITL-971, named in the
  docs, changelog fragment, design-issue file and PR body. `hash_default_value` (in
  `builtin_handlers.py`) is the one function that decides how a default is hashed, so "hash a
  pod parameter's default as the data value it would be in a column" can be added there later.
  Not built.
- **D4** as proposed. With one site left there is no shared helper: `FunctionHandler` hashes
  each `ParameterDefault` the extractor reports and wraps the hasher's `TypeError`.

---

## Step 1 findings (read on `origin/eywalker/itl-967-…` at `c1949b24`, Python 3.12)

### Where the signature is hashed: TWO code paths, not one

The issue says `FunctionSignatureExtractor` feeds both hashes. It feeds only one.

| Hash | Code path | Carries defaults as |
|---|---|---|
| A function used as a **value** (`semantic_hasher.hash_object(fn)`; any function inside an identity structure) | `FunctionHandler.handle` → `FunctionSignatureExtractor.extract_function_info` (`hashing/semantic_hashing/function_info_extractors.py`), registered in `contexts/data/v1.json` for `types.FunctionType`, `BuiltinFunctionType`, `MethodType` | `repr(default)` inside the `params` string (`_format_param`) |
| Stored variation value `function_signature_hash` → column `__pf_var_function_signature_hash` | `PythonDataFunction.__init__` (`core/data_function.py:519`): `semantic_hasher.hash_object(get_function_signature(function))`, where `get_function_signature` is the string builder in `hashing/hash_utils.py` | `repr(default)` inside `str(inspect.Parameter)` |

- `FunctionSignatureExtractor` is referenced only by `v1.json` (component
  `function_semantic_hasher`), by `register_builtin_handlers` (the fallback default) and by
  `FunctionInfoExtractorFactory`. Its only consumer is `FunctionHandler`.
- `__pf_var_function_signature_hash` is written with every result row and read by nothing
  (`grep signature_hash src`: only `data_function.py`).
- A function pod's identity does not use either: the uri is
  `(function name, output-schema hash, major version, type id)`, and paths derive from the uri
  and the pipeline hash. Confirmed by the passing pins in the new test file once added (Task 4).
- `function_content_hash` (`get_function_components`) hashes the source text, where a default
  appears as written; it never calls `repr()` on a value. Not touched.

### How default values hash under the context's strict semantic hasher

| Default | Result |
|---|---|
| `None`, `bool`, `int`, `float`, `str` | hashed as the JSON primitive. `None` differs from "no default" (the entry is absent) |
| `tuple` | tagged `tuple`, order kept; differs from the equal `list` |
| `set` / `frozenset` | tagged `set`, items sorted (by canonical JSON once ITL-942 is merged): seed-independent |
| `dict` | keys sorted; insertion order irrelevant |
| `bytes`, `uuid.UUID`, a function or lambda, a type, `numpy.ndarray` (content hash, `NumpyArrayHandler`), pandas objects, anything with `identity_structure()` | hashed by its handler |
| `IntEnum` member, `str`-mixin enum member, `numpy.float64` | hashed as the `int` / `str` / `float` it subclasses: `IntEnum.A` (value 1) and `1` are one default. Existing hasher behaviour for primitive subclasses, not changed here |
| plain `enum.Enum` member | **raises** |
| dataclass instance | **raises** (the dataclass *type* is a logical type; its *values* have no structure-hasher handler) |
| `pathlib.Path` | **raises** (same: logical type, no value handler) |
| `numpy.int64`, `numpy.float32`, `numpy.bool_` | **raises** |
| `datetime`, `date`, `Decimal`, `complex`, `range`, `...`, `object()` | **raises** |

### Which defaults in the repo would now raise

- `src/`, `docs/`, `README.md`, `examples/`, `notebooks/`, `workshop/` and the tutorial branch
  (PR #274): an AST scan finds no pod-decorated function with any default.
- Test suite and executed docs samples: see "Probe" below.

**Probe.** A pytest plugin recorded every default that reaches either hash site during the full
suite (8325 passed; includes the executed docs samples in `tests/test_docs`) and tried the
strict hasher on it. Seven defaults reach a hash site, all `int` or `str`, all hashable:

- stored signature hash: `func_with_default(y: int = …)` in
  `tests/test_core/data_function/test_data_function.py`, `add_with_default(y: int = …)` (two
  tests) in `tests/test_core/function_pod/test_simple_function_pod.py`;
- function value: four local functions in `tests/test_hashing/test_function_info_extractors.py`
  (direct extractor calls).

**Nothing in the repo, its tests, the docs samples, the workshop tool or the tutorial branch
would raise.** No test or sample hashes a function as a value except by calling the extractor
or the hasher directly.

### Golden files and stored values

- No golden entry pins a function with a default or a signature hash
  (`tests/test_hashing/golden/data/*.json`); golden fixtures `golden_ratio`, `golden_adjust`
  have no defaults. Expected: 0 changed; entries added to pin the new behaviour.
- `contexts/data/v1.json` is not edited (same class, same config), so its pinned sha256 in
  `released_manifests.json` stands.

### Out-of-scope findings (for worker_done)

1. Both signature builders drop the parameter kind: `f(a, *, b)` and `f(a, b)`, and
   `f(a, /, b)` and `f(a, b)`, share a function-value hash and a stored signature hash.
2. `get_function_signature(include_defaults=False)` cuts at the first `=`, truncating an
   annotation such as `Literal["a=b"]` (the extractor already avoids this).
3. There are two parallel signature descriptions (the string in `hash_utils`, the dict in the
   extractor).
4. On `c1949b24` four tests fail without any change of mine (ITL-967 is mid-merge):
   `tests/test_core/sources/test_spiraldb_table_source_fake.py::TestSourceId::test_unrecorded_table_hashes_are_unchanged`
   and `tests/test_core/sources/test_stream_builder.py::TestUnhashableColumnTypes::{test_fixed_size_list_column_is_named,test_map_column_is_named,test_every_unhashable_column_is_named}`.

---

## Decisions sent to the coordinator

**D1. Shape (recommended).** `params` keeps today's text minus the defaults; a new member
`defaults` = `[[name, <hash>], …]` in declared order, only when the function has defaults. A
function without defaults keeps its hash. The hash is embedded as a `ContentHash`, so after
ITL-945 it is the tagged `{"__type__": "hash", "digest": …}` and cannot be spelled by a literal
default; a default is never embedded raw, so no literal can collide with it either way.
Alternative: a fully structured parameter list (name, kind, annotation, default), which also
fixes out-of-scope finding 1 but moves every function-value hash.

**D2. Stored column.** (S2, recommended) same rule on the existing string path: no defaults →
`hash_object(signature_text)` as today (value unchanged); with defaults →
`hash_object({"signature": text without defaults, "defaults": [[name, hash], …]})`. Only
functions with defaults move. (S1) store `hash_object(function)`: one description, but every
newly written value of the column moves, and the pinned value in
`test_canonical_type_names.py::TestFunctionPodGolden::test_function_signature_hash` changes.

**D3. Path / Enum / dataclass defaults raise.** Per the issue (no fallback). Adding value
handlers for them is scope growth; not planned unless asked.

**D4. Error type.** `TypeError` (what the hasher raises), chained from the hasher's error,
message: parameter name, function `module.qualname`, the hasher's text, and the remedy.

---

### Coordinator's answers (November 11, 2026)

- D2: **S2 approved.** D4: **approved.** Finding Y: fix as part of S2, with a test.
- D1 and D3: with the developer (D1 presented as (a) against (b) together with finding X;
  D3 as raise against value handlers for Path / Enum / datetime). **Pending.**
- No branch until the coordinator says the six merges are in.
- Tests extended meanwhile (74 tests: 18 fail, 56 pass on `c1949b24`):
  `TestDefaultsOfCommonTypes` holds under either D3 answer (refused, or hashed by value);
  `TestParameterKind` covers X and stays only if D1 is (b), otherwise it is removed and X is
  reported; `TestSignatureTextWithoutDefaults` covers Y.

## File structure

| File | Change |
|---|---|
| `src/orcapod/hashing/semantic_hashing/function_info_extractors.py` | `_format_param` loses the default; new `parameter_defaults(func)` and `hash_parameter_defaults(func, hasher)`; extractor reports `defaults` when `include_defaults` |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | `FunctionHandler.handle` replaces the reported defaults by their hashes |
| `src/orcapod/hashing/hash_utils.py` | `get_function_signature` builds parameters without `repr()` of defaults when `include_defaults=False` (structured, no `split("=")`) |
| `src/orcapod/core/data_function.py` | stored signature hash per D2 |
| `src/orcapod/protocols/hashing_protocols.py` | docstring of `extract_function_info`: the `defaults` member |
| `tests/test_hashing/test_function_default_hashing.py` | new (written, failing) |
| `tests/test_hashing/test_function_info_extractors.py` | update the two `include_defaults` assertions |
| `tests/test_hashing/golden/cases.py`, `golden/data/semantic.json` | add entries for functions with defaults (additions only) |
| `docs/reference/hashing.md`, `docs/concepts/function-pods.md` | the rule, the consequence, the remedy |
| `changelog.d/ITL-953.md`, `design_issues/ITL-953-FD1.md` | fragment and entry |

## Tasks (ORIGINAL draft for D1 (a) / S2; kept as the Step 1 record, superseded by the decisions at the top)

### Task 1: branch and commit the failing tests

- [ ] `git checkout -b eywalker/itl-953-function-default-values-enter-the-function origin/eywalker/itl-967-v03-hash-step-combine-the-hash-changing`
- [ ] `uv run pytest tests/test_hashing/test_function_default_hashing.py -q` → expect 11 failed,
      34 passed (dict order ×2, seeds, same-repr ×2, six "raises" tests).
- [ ] Commit `test(hashing): failing tests for function defaults through the semantic hasher (ITL-953)` with this plan; push.

### Task 2: function-value hash

- [ ] In `function_info_extractors.py`:

```python
def parameter_defaults(func: Callable[..., Any]) -> list[tuple[str, Any]]:
    """Return ``(name, default)`` for each parameter of ``func`` that has a default, in declared order."""
    return [
        (name, param.default)
        for name, param in inspect.signature(func).parameters.items()
        if param.default is not inspect.Parameter.empty
    ]


def hash_parameter_defaults(
    func: Callable[..., Any], hasher: "SemanticHasherProtocol"
) -> list[list[Any]]:
    """Return ``[name, hash]`` for each default of ``func``, hashed by ``hasher``.

    Raises:
        TypeError: If ``hasher`` cannot hash a default; names the parameter and the function.
    """
    hashed: list[list[Any]] = []
    for name, default in parameter_defaults(func):
        try:
            hashed.append([name, hasher.hash_object(default)])
        except TypeError as exc:
            where = f"{getattr(func, '__module__', '?')}.{getattr(func, '__qualname__', repr(func))}"
            raise TypeError(
                f"Cannot hash the default value of parameter {name!r} of function "
                f"{where}: {exc} A default value is part of the function's hash. Register "
                "a handler for its type, or remove the default and supply the value as "
                "an input."
            ) from exc
    return hashed
```

- [ ] `_format_param(param, canonical_annotation)` drops `include_defaults` and never formats a
      default. `FunctionSignatureExtractor.extract_function_info` (which has no hasher) reports
      the raw defaults when `include_defaults` is true and the function has any:
      `parts["defaults"] = parameter_defaults(func)`. `FunctionHandler.handle` (which has the
      hasher) replaces that member: `if "defaults" in info: info["defaults"] = hash_parameter_defaults(obj, hasher)`.
      The `defaults` member is documented on `FunctionInfoExtractorProtocol.extract_function_info`.
- [ ] Run the `value` tests → pass; `uv run pytest tests/test_hashing -q` → only the two
      `include_defaults` extractor assertions need updating; golden suite unchanged.
- [ ] Commit `fix(hashing): function defaults enter the function-value hash through the semantic hasher (ITL-953)`.

### Task 3: stored signature hash (per D2 answer)

- [ ] `get_function_signature(include_defaults=False)` formats each parameter from its parts
      (name, kind prefix, canonical annotation); `include_defaults=True` output unchanged.
- [ ] `PythonDataFunction.__init__`:

```python
signature_text = get_function_signature(function, include_defaults=False)
defaults = hash_parameter_defaults(function, semantic_hasher)
signature: Any = (
    {"signature": signature_text, "defaults": defaults} if defaults else signature_text
)
self._function_signature_hash = semantic_hasher.hash_object(signature).to_prefixed_digest()
```

- [ ] Run the `stored` tests and `tests/test_core/data_function` → pass, the pinned value in
      `test_canonical_type_names.py` unchanged.
- [ ] Commit `fix(data_function): stored signature hash takes defaults through the semantic hasher (ITL-953)`.

### Task 4: pins, golden additions, identity unaffected

- [ ] Add tests: a pod's `uri`, `pipeline_hash()` and `content_hash()` are equal for two
      functions that differ only in a default; a no-default function's value hash and stored
      hash equal the values computed on the ITL-967 branch before the change (literals).
- [ ] Add golden cases `semantic:function_defaults.*` (primitives, tuple, frozenset, dict,
      nested function, no default); regenerate; `git diff --stat` on the golden data must show
      additions only. List every entry in the PR.

### Task 5: docs, changelog fragment, design issue

- [ ] `docs/reference/hashing.md`: "Function values and default values" (rule, table of what
      hashes and what raises, remedy). `docs/concepts/function-pods.md`: one paragraph at the
      default-parameter text: a default of an unregistered type fails at construction.
- [ ] `changelog.d/ITL-953.md` (`### Breaking changes and migration`, `### Fixed`);
      `uv run python scripts/assemble_changelog.py --check`.
- [ ] `design_issues/ITL-953-FD1.md`.

### Task 6: verify and deliver

- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`, `uv run --with basedpyright basedpyright` (exit code), `uv run pytest tests/`.
- [ ] Merge `origin/main` if it moved; push once; PR per the brief; CodeRabbit round; `worker_done`.
