# Golden hash values

Every hash Orcapod stores or embeds in a name is pinned here and checked on every CI run. The
full description, with the threat model and what each check protects against, is
[`docs/reference/golden-hashes.md`](../../../docs/reference/golden-hashes.md).

In short:

- `data/<folder>/<group>-NN.json` holds the values of the current data context; the folder is
  named by `golden.folder` in `src/orcapod/contexts/data/<version>.json`.
- `golden.history` in that manifest lists every file with the sha256 of its bytes, in a chain
  of revisions; a release appends a revision with `added: {}` and pins its digest in
  `src/orcapod/contexts/released_manifests.json`.
- A file listed by a released revision never changes. New values go into new files, in any
  release. A change to a released value needs a new data context version.

| File | Role |
| --- | --- |
| `cases.py` | The cases. Never move or rename a fixture: they hash with their module path. |
| `generate_golden_hashes.py` | Adds new entries (default), reports drift (`--check`), removes an unreleased entry (`--remove`), records a release (`--release X.Y.Z`). Read its header first. |
| `store.py` | The rules: file names, digests, history, pins. |
| `test_golden_hashes.py` | Computes every entry again; refuses skips but the optional-package rule. |
| `test_golden_store.py` | The rules and the generator, on copies of the tree. |

`tests/test_contexts/test_data_context_v1.py` checks the repository's history and pins, and
`scripts/check_golden_tags.py` (a CI step) checks HEAD against every release tag.
