# Orcapod public release readiness: inventory for the developer (ITL-875)

Prepared 2026-11-11 against `main` at `b197544e`. This document lists what has to be known or
decided before `0.3.0` is published on public PyPI and before the repository
`metamorphic-labs/orcapod-python` is made public. **It makes no decisions.** Each section
gives the facts, then the options with their consequences.

Decided by the developer already, and taken as given here: Orcapod is MIT (#283). `0.3.0` is
published on public PyPI as `orcapod` (target tag date 2026-11-16, workshop 2026-11-17).
Publishing does not require the repository to be public. The developer makes the repository
public later. The developer changes the PyPI trusted publisher.

Summary:

| # | Topic | Blocks the PyPI release? | Blocks making the repository public? |
|---|---|---|---|
| 1 | Secrets in git history and GitHub text | No: none found | No: none found |
| 2 | Internal-only material | No (see 2.9 for what ships in the wheel) | Decision needed: what to remove, and whether to rewrite history or publish a fresh repository |
| 3 | Dependency licenses | Decision advisable on the `spiraldb` and `postgresql` extras | Same |
| 4 | Dependabot alerts (170 open) | Decision advisable on the `ray` pin | Visible once public |
| 5 | Release workflow for `0.3.0` | Trusted publishers on PyPI **and** TestPyPI must exist first | No |
| 6 | Checklists | Section 6.1 | Section 6.2 |

---

## 1. Secrets and credentials

### 1.1 What was scanned

- **Every ref of the GitHub repository**, from a fresh `git clone --mirror`: 36 branches,
  18 tags and 257 pull-request refs for 244 PRs (`refs/pull/*`, which GitHub serves publicly
  once the repository is public and which the owner cannot delete), 4,554 commits in total.
- **GitHub text**: the bodies of all 244 pull requests and 51 issues, 777 issue/PR comments
  and 2,815 review comments.

Tools (downloaded release binaries, checksums verified, run from a scratch directory; nothing
installed on the machine):

- `gitleaks` 8.30.1, `gitleaks git --log-opts=--all --redact`: 4,365 commits with a diff,
  61 MB.
- `trufflehog` 3.99.8, `trufflehog git` over all refs (verified, unverified and unknown
  results), 53,300 chunks, 64 MB.
- Both tools again over the dumped GitHub text (`gitleaks dir`, `trufflehog filesystem`).
- A pattern search over every added line in history (1.5 M lines) for AWS keys, private key
  blocks, GitHub, Slack, OpenAI, Anthropic, Linear, PyPI and Google tokens, Slack webhooks,
  `password|secret|token|api_key = "<literal>"` assignments, and committed `.env`, `.pem`,
  `.key`, `id_rsa`, `credentials`, `.pypirc` or `.netrc` files.

### 1.2 Result: no secret or credential found

Every hit was triaged by reading the line with any credential-shaped substring masked. All are
false positives:

| Tool | Hits | File(s) and first commit | What it is |
|---|---|---|---|
| gitleaks `generic-api-key` | 1 | `src/orcabridge/dj/pod.py` (`7b3b37e5`, 2025-04-30) | Code: `entry.fetch(*outer_self.fp.output_keys, as_dict=True)` |
| trufflehog `Postgres` | 36 | `src/orcapod/databases/postgresql_connector.py`, `core/sources/postgresql_table_source.py`, `hashing/postgres_hash_cacher.py`, `contexts/__init__.py`, `docs/concepts/file-hash-caching.md`, `docs/plans/…`, `docs/superpowers/plans/…`, `superpowers/plans/…`, `superpowers/specs/…` (2026-03-25 to 2026-07-11) | Placeholder DSNs in docstrings and docs (`postgresql://user:<placeholder>@host:5432/dbname`) |
| trufflehog `Postgres` | 2 | `tests/test_hashing/test_postgres_hash_cacher.py` (`5637a661`, `b02e0600`) | Test fixture for password masking, host `db.example.com` |
| trufflehog `Postgres` | 2 | One PR review comment | Placeholder DSN (`…@host`) |
| trufflehog `Box`, `Fmfw` | 3 | `pipeline_creation_playground.ipynb` (`211a81e3`), `pipeline.ipynb` (`f0f50fc1`), both deleted | Base64 PNG notebook output |
| trufflehog `Enigma` | 2 | `uv.lock` (`282bc3a0`, `bea864d3`, 2025-08) | A git dependency URL with a commit SHA (see 2.7: it names a private repository) |
| Pattern search | 2 lines | Docs examples | `UPath(..., key="mykey", secret="mysecret")` placeholders; a computed `token = ... .to_hex()` |
| Pattern search, IPs | 2 | `uv.lock` history; docs | `10.3.5.147` is an `nvidia-curand` version; `192.168.1.100` is an example |

No committed `.env`, key, certificate or credential file exists anywhere in history.

### 1.3 What was not scanned

- **GitHub Actions logs and artifacts** of past runs, which become visible with the
  repository. The workflows print no secret (GitHub masks registered secrets in logs), but
  the logs were not read.
- **Repository secrets themselves** stay private when the repository is public:
  `CODECOV_TOKEN`, `LINEAR_ACCESS_KEY`, `SPIRAL_WORKLOAD_ID` (secrets) and
  `SPIRAL_PROJECT_ID`, `SPIRAL_SERVER_URL` (variables). Workflows trigger on `push`,
  `pull_request` and `workflow_dispatch` only (no `pull_request_target`, no self-hosted
  runner), so a pull request from a fork gets no secret.

### 1.4 Repository security settings today (read only; nothing was changed)

Visibility `internal` (visible to members of the enterprise), 0 forks, no wiki repository.
`code_security` and Dependabot security updates are **enabled**. Secret scanning, push
protection, non-provider patterns and validity checks are **disabled**. On a public
repository GitHub secret scanning is free; enabling it and push protection is the developer's
setting (proposed in 6.2).

---

## 2. Internal-only material

Everything below is on `main` unless the row says "history only". "History only" material
becomes public with the repository anyway, through old commits, branches and `refs/pull/*`,
unless history is rewritten or a fresh repository is published (2.10).

### 2.1 Agent process documents: `superpowers/`

- **What:** implementation plans and design specs written by agents for each Linear issue
  (decision records, task lists, developer quotes, review discussions).
- **Where / how much:** `superpowers/plans/` 120 files, `superpowers/specs/` 92 files,
  `superpowers/scripts/hash_audit_example.py`; 213 files, about 121,700 lines. A further 132
  `superpowers/` paths exist in history only.
- **Sensitivity:** mostly technical. Contains Linear URLs (2.5), Linear issue IDs, the
  developer's name in decision ownership (e.g. `superpowers/specs/2026-07-14-itl-524-side-effect-pods-design.md`),
  the internal repository `metamorphic-brain/axon` (now `metamorphic-labs/devenv-cli`,
  internal) in four release-system files, SpiralDB project details (2.6), and the
  `ENIGMA File System Spec (engmfs)` project name.

### 2.2 Other planning and design documents

| Path | Files | Lines | What |
|---|---|---|---|
| `docs/metamorphic/plans/` | 8 | 4,957 | Agent plans (the Sensei default plan location) |
| `docs/plans/`, `docs/specs/`, `docs/design/` | 6 | 4,067 | Older plans and specs (SQLite, PostgreSQL sources, non-active node semantics) |
| `design/` | 2 | 860 | Async execution design and plan |
| `plan.md`, `function-execution-improvements-plan.md`, `TESTING_PLAN.md` | 3 | 1,810 | Root-level working plans |
| `orcapod-design.md` | 1 | 730 | The design specification; referenced from `CLAUDE.md` as canonical |
| `test-objective/` | 33 | 7,600 | Objective test suite and its notes |

Note: `mkdocs build` renders every Markdown file under `docs/`, including the plan and spec
pages that are not in the navigation, so a hosted documentation site would publish
`docs/metamorphic/`, `docs/plans/`, `docs/specs/` and `docs/design/` too (site hosting is out
of scope for ITL-875).

### 2.3 Agent instructions: `CLAUDE.md`, `.zed/rules`, `AGENT_COORDINATION.md`, `.claude/settings.json`

- `CLAUDE.md` (544 lines) and `.zed/rules` (494 lines): coding rules, release rules and
  dated developer decisions ("developer, November 10, 2026"), Linear workflow, the
  organization's required CI.
- `AGENT_COORDINATION.md` (252 lines): the Orca coordinator/worker runbook (Orca CLI
  commands, the agent signature for GitHub comments, PR readiness pipeline).
- `.claude/settings.json` (5 lines).
- **Sensitivity:** low. They describe the development process and name internal tools
  (Orca, Sensei skills, Kurodo agents, Linear), not credentials or customers. Many open
  source projects publish a `CLAUDE.md`.

### 2.4 `DESIGN_ISSUES.md`

2,247 lines: the log of known design problems, bugs and their fixes. Technical; includes
SpiralDB infrastructure history (2.6) and Linear issue IDs. Comparable to a public issue
tracker.

### 2.5 Linear links and issue IDs

- **`linear.app` URLs on `main`:** 42, all to the workspace `linear.app/enigma-metamorphic`:
  40 in 32 files under `superpowers/`, 2 in `docs/design/specs/2026-03-26-sqlite-table-source-design.md`
  and `docs/specs/2026-03-26-postgresql-table-source-design.md`. Several URLs include the
  issue title slug.
- **GitHub text:** 14 PR/issue bodies, 26 issue comments and 4 review comments contain
  `linear.app` links (Linear's GitHub integration adds some of them).
- **Issue IDs** (`ITL-`, `PLT-`, `ENG-`, `NPIPE-`): 1,979 references in 320 files on `main`,
  including 23 files under `src/` (comments and docstrings, so they ship in the wheel),
  74 test files, `CHANGELOG.md` (about 100 lines) and `pyproject.toml`.
- **Sensitivity:** the links need a Linear login, so they disclose the workspace name and,
  in slugs, issue titles; issue IDs disclose nothing on their own.

### 2.6 Internal hosts, infrastructure and vendor details

| Item | Where on `main` | Notes |
|---|---|---|
| `at-database3.stanford.edu` with user `eywalker` (MySQL, port 3306) | `notebooks/old_tutorials/05_orcabridge_dj_integration.ipynb` lines 160-161 (cell output) | Internal university database host name and a username. History: 6 commits (2025-04-28 to 2025-05-05) in deleted notebooks too |
| `ray://op-pipe-kuberay-head-svc.ray.svc.cluster.local:10001` | `notebooks/tutorials/02_parallel_execution_on_ray.ipynb` line 38 | Internal Kubernetes service name. History also has `raycluster-op-test-kuberay-head-svc…` |
| `api.spiraldb.dev` | `.github/workflows/run-tests.yml:59`, two `src/` docstrings, tests, plans | SpiralDB's development API endpoint (vendor host, not ours) |
| SpiralDB project ID `test-orcapod-362211` | `.github/workflows/run-tests.yml:58`, `tests/test_databases/test_spiraldb_connector_integration.py:48`, one plan | Our SpiralDB test project. Not a credential (the workload ID is a secret), but an internal identifier |
| `t3.storage.dev` | `DESIGN_ISSUES.md` (SP1), two `superpowers/` files | SpiralDB's object store; incident history |
| `metamorphic-brain/axon` | `RELEASING.md:162`, three `superpowers/` files | Internal repository (now `metamorphic-labs/devenv-cli`); `RELEASING.md` says the Linear key is shared with it |
| `orcapod.dev` | `src/orcapod/contexts/data/schemas/context_schema.json` (`$id`) | A JSON Schema identifier; the domain is not ours to verify |

### 2.7 Names of internal systems, customers and datasets

- **Internal systems / organizations:** Linear workspace `enigma-metamorphic`; Orca;
  Sensei; Kurodo; `metamorphic-brain/axon`; `metamorphic-labs/shared-workflows` (only named
  in this document); SpiralDB (a vendor; we hold a commercial agreement, per the
  organization's license workflow); NauticaLab (copyright holder, 41 mentions).
- **History only:** `uv.lock` in 2025-08 (`282bc3a0`, `bea864d3`, 6 commits) depended on
  `git+https://github.com/enigma-brain/selection_pipeline`, a **private** repository; its
  name and a commit SHA are in history.
- **Customers:** none found. Keyword search (customer, client, patient, institution names)
  found only generic examples (`customers_2024` in `orcapod-design.md`, a `patient_id`
  example in a spec). This is a keyword search, not a proof.
- **Datasets:** none found beyond public ones (SpikeInterface examples).

### 2.8 People's names and e-mail addresses

- **Commit metadata** (public with any history): authors and committers include
  `@uw.edu`, `@stanford.edu`, `@gmail.com` and `@metamorphic.com` addresses of several people,
  the Kurodo agent address `kur…@metamorphic.com`, and GitHub noreply addresses. 335 commits
  have an empty author e-mail. This is normal for an open-source repository and is changed
  only by a history rewrite.
- **In content:** the GitHub handle `eywalker` (133 mentions in 69 files, mostly plan files
  and test paths), "Edgar Walker" in one spec, "Edgar" as sample data in
  `notebooks/old_tutorials/02_orcapod_basic_usage copy.ipynb`, the Stanford address above.
  `tests/test_packaging.py` and `LICENSE` name NauticaLab (stays, by decision).
- No other personal e-mail address appears in file content (randomized Hypothesis examples
  and `@example.com` excluded).

### 2.9 What the PyPI package itself contains

Built locally with `uv build` from this branch:

- sdist: `src/orcapod`, `README.md`, `LICENSE`, `pyproject.toml`, `PKG-INFO` only
  (`[tool.hatch.build.targets.sdist] only-include`).
- wheel: `src/orcapod` (194 files).
- So nothing from 2.1-2.4 ships. What ships: Linear issue IDs in 23 source files (no
  `linear.app` URL), `api.spiraldb.dev` in two docstrings, `orcapod.dev` in a JSON Schema
  `$id`, and the README (PyPI long description).

### 2.10 Options for the repository going public

Per category (2.1-2.8), three options:

| Option | What happens | Consequences |
|---|---|---|
| **A. Leave as is** | Flip visibility | Everything above is public: plans, specs, runbooks, Linear URLs, the Stanford host name, the KubeRay service name, the SpiralDB project ID, the private `enigma-brain/selection_pipeline` name. Nothing breaks. |
| **B. Remove from `main`, keep history** | A PR deletes or edits the files (for example `superpowers/`, `docs/metamorphic/`, the root plans, the two notebook outputs), then flip visibility | The current tree is clean, but every removed file stays readable in history, in branches and in `refs/pull/*`. Breaks `CLAUDE.md` and the Sensei skills that write plans into `superpowers/` and `docs/metamorphic/` unless a new location (outside the repository, or a private companion repository) is chosen. Links from Linear to those files stop working on `main`. Low risk, reversible. |
| **C. Rewrite history** (`git filter-repo`) and force-push all branches and tags | Material removed from every reachable commit | Every commit SHA changes: tags `v0.0.x`-`v0.2.0` must be re-created (the `v0.1.x` sdists on PyPI then no longer match a tag's commit); every open PR (#145, #248, #258, #285-#295) and every agent worktree must be rebased or recreated; `hatch-vcs` versions shift until re-tagged. **GitHub keeps `refs/pull/*` and cached views of the old commits**: they are removed only by GitHub Support, so a rewrite alone does not hide the material. Requires bypassing the organization ruleset that forbids force-push (`non_fast_forward`). |
| **C'. Publish a fresh public repository** | Keep `metamorphic-labs/orcapod-python` private as is; create a new public repository from a filtered (or squashed) copy of `main` | Nothing private leaks, no force-push. Loses public issue/PR history (it stays in the private repository); PR links in `CHANGELOG.md` (`#NNN`) point at a private repository; the PyPI trusted publisher, `pyproject.toml` URLs, `RELEASING.md` and CI must name the new repository; two repositories to keep in sync or a cut-over. |

Material that only option C or C' can remove: the Stanford host name and the
`enigma-brain/selection_pipeline` name in history, commit author e-mails, `refs/pull/*`
content, and anything in deleted files.

Not affected by any option: the golden hash values (content-based; function pods hash by
module path, not by git state).

---

## 3. Dependencies and their licenses

Licenses as reported by the installed packages' metadata (`pip-licenses` in a throwaway
virtual environment with every extra installed, Python 3.11, macOS). Every one of the 220
locked packages resolves from `https://pypi.org/simple` (`uv.lock` has no other index and no
git or URL source), so **all dependencies, including every extra, are installable from
public PyPI**.

### 3.1 Default install (62 packages)

All permissive:

| License | Packages |
|---|---|
| MIT | 23, including `polars`, `pydantic`, `typer`, `beartype`, `graphviz`, `universal-pathlib`, `rich`, `urllib3` |
| BSD (2-/3-Clause, "BSD License") | 17, including `numpy`, `pandas`, `networkx`, `s3fs`, `fsspec`, `gitpython`, `uuid-utils`, `xxhash`, `click` |
| Apache-2.0 | 10, including `pyarrow`, `deltalake`, `botocore`, `aiobotocore`, `tzdata` |
| Apache-2.0 and/or MIT, or Apache/BSD | 5: `starfix` (MIT OR Apache-2.0, on PyPI 0.4.0), `arro3-core`, `aiohttp`, `packaging`, `python-dateutil` |
| PSF-2.0 / PSF-style | 4: `matplotlib`, `typing-extensions`, `aiohappyeyeballs`, `pathlib-abc` |
| MIT-CMU (HPND) | `pillow` |
| ISC | `shellingham` |
| BSD (Windows only, not installed on macOS) | `colorama` |

### 3.2 Optional extras

| Extra | Adds | Licenses of what it adds | Note |
|---|---|---|---|
| `redis` | 2 | `redis` MIT, `async-timeout` Apache-2.0 | Permissive |
| `ray` | 57 | Apache-2.0, MIT, BSD, ISC, PSF, `certifi` MPL-2.0, `filelock` Unlicense | Permissive / weak copyleft (MPL-2.0, file-level). Pins `ray[default]==2.48.0` exactly (see 4) |
| `postgresql` | 2 | `psycopg`, `psycopg-binary`: **LGPL-3.0-only** | Weak copyleft |
| `spiraldb` | 26 | `pyspiral`: **Other/Proprietary License** (PyPI: "Proprietary License", homepage spiraldb.com, on public PyPI, 0.16.33); others MIT, BSD, Apache, MPL-2.0 (`certifi`, `orjson`), Unlicense | Proprietary |
| `all` | all of the above | | Pulls both `pyspiral` and `psycopg` |

### 3.3 What an MIT package may declare about them

General points, not legal advice (the developer may want counsel to confirm 3.3.3):

1. **Declaring a dependency does not distribute it.** The wheel and sdist contain only
   Orcapod's own files (2.9); `pip` fetches each dependency from PyPI under its own license.
   An MIT package may therefore name dependencies under any license, permissive or not.
2. **LGPL-3.0 (`psycopg`) is compatible with an MIT package** that imports it as a separate,
   unmodified library: the LGPL obligations (allow replacing the library, keep its notices)
   fall on whoever distributes `psycopg`, which Orcapod does not. Orcapod stays MIT. The
   repository's own `dependency-review` job (`run-tests.yml`) denies LGPL in **new**
   dependency changes in PRs; `psycopg` predates it.
3. **A proprietary optional dependency (`pyspiral`)** is allowed to be named, but has
   practical consequences: users who install `orcapod[spiraldb]` or `orcapod[all]` accept
   SpiralDB's license and need a SpiralDB account to use it; MIT users may not expect a
   proprietary package from `[all]`. Common practice is to keep such an integration as an
   opt-in extra with a clear note in the README/docs, or to move it to a separate package.
4. **The organization's required license check** (`metamorphic-labs/shared-workflows`,
   `ci-license-check.yml`) checks only the default install (`uv run --no-dev`), allows
   `LGPL-3.0-only` and `LicenseRef-Metamorphic-Proprietary`, and ignores `pyspiral` by name
   ("covered by Metamorphic's commercial Spiral agreement"). It is owned by the organization
   and was not changed. The repository's own `_license-check.yml` is changed in this PR (see
   *Part B*).

Options for the developer (no change made here):

| Option | Consequence |
|---|---|
| Keep both extras as they are | Fine under MIT. `orcapod[all]` installs a proprietary package |
| Drop `orcapod[spiraldb]` from `all` | `all` becomes fully open source; SpiralDB users install `orcapod[spiraldb]` explicitly. Packaging change (needs your approval) |
| Document the licenses of the extras in README/docs | Users see it before installing |
| Move the SpiralDB integration to a separate package | Larger change; out of scope for `0.3.0` |

---

## 4. Dependabot alerts on `main`

Read on 2026-11-11 (`gh api …/dependabot/alerts?state=open`): **170 open alerts**, all on
`uv.lock`, 170 distinct advisories, 169 with a patched version available. Oldest opened
2026-07-09.

| Severity | Count |
|---|---|
| Critical | 9 |
| High | 95 |
| Medium | 59 |
| Low | 7 |

Dependabot labels 167 of them "runtime" because every `uv.lock` entry looks like one; mapped
against the actual dependency sets of 3.1/3.2:

| Scope | Alerts | Critical / High / Medium / Low | Packages (locked version, alerts) |
|---|---|---|---|
| **Default install** | 97 | 6 / 62 / 28 / 1 | `pillow` 11.2.1 (51, via `matplotlib`), `gitpython` 3.1.45 (32, direct), `aiohttp` 3.12.15 (11, via `s3fs`), `urllib3` 2.4.0 (2), `fsspec` 2026.3.0 (1) |
| `ray` extra | 31 | 0 / 19 / 9 / 3 | `virtualenv` (19), `pyasn1` (7), `filelock` (2), `msgpack`, `opentelemetry-api`, **`ray` 2.48.0 (1 high, fixed in 2.56.0)** |
| `ray` and `spiraldb` extras | 1 | 1 / 0 / 0 / 0 | `certifi` 2025.4.26 |
| `spiraldb` extra | 21 | 2 / 6 / 12 / 1 | `pyjwt` (13), `anyio` (2), `cryptography` (3), `dnspython`, `h2`, `hpack` |
| Development only | 20 | 0 / 8 / 10 / 2 | `tornado` (11), `hydra-core`, `pymdown-extensions`, `mkdocs-material`, `pip`, `setuptools` |

What this means for users of the published package: `uv.lock` governs CI and development
environments, not what `pip install orcapod` resolves. The package declares lower bounds
(`gitpython>=3.1.45`, `pyarrow>=20.0.0`, …) or nothing (`pillow`, `aiohttp`, `urllib3` come
through other packages), so a fresh install gets current, patched versions. Exceptions:

- `ray[default]==2.48.0` in the `ray` extra is an **exact pin**: every `orcapod[ray]` user
  gets `ray` 2.48.0, which has one open high-severity advisory (patched in 2.56.0).
- `gitpython>=3.1.45` admits versions with known advisories if a user already has one
  installed (the fix is in 3.2.x).

Once the repository is public, the Dependabot alerts list stays private to maintainers, but
the vulnerable versions in `uv.lock` are visible to anyone.

Options (each a dependency change, so for the developer): refresh `uv.lock` (`uv lock
--upgrade`) before `0.3.0` and re-run CI; raise `gitpython` to `>=3.2.1`; relax or bump the
`ray` pin; leave as is and address after `0.3.0`.

---

## 5. The Release workflow as it would run for `0.3.0`

`.github/workflows/release.yml`, manual dispatch only, inputs `version` (`0.3.0`; a leading
`v` is stripped) and `branch` (default `main`).

### 5.1 Jobs, in order

| Job | Needs | What it does |
|---|---|---|
| `validate-branch` | | Confirms the branch exists on origin through the GitHub API |
| `test` (3.11, 3.12, 3.13) | `validate-branch` | `uv sync --locked --all-extras --dev` (installs `pyspiral` and `psycopg` from PyPI), installs Graphviz, runs `pytest -m "not postgres"`. Not the PostgreSQL or SpiralDB integration jobs |
| `license-check` | `validate-branch` | Calls the repository's `_license-check.yml` (default install plus dev group, no extras) |
| `build` | `test`, `license-check` | Checks out the branch with full history, creates the local tag `v0.3.0`, `uv build` (`hatch-vcs` reads the tag, so the version is `0.3.0`), **aborts if `v0.3.0` exists on origin**, pushes the tag, uploads `dist/` |
| `publish-testpypi` | `build` | Environment `testpypi` (no protection rules). `uv publish --publish-url https://test.pypi.org/legacy/ dist/*` with an OIDC token |
| `publish-pypi` | `build`, `publish-testpypi` | Environment `pypi`: **required reviewer `eywalker`, administrators cannot bypass**, so the run pauses for your approval after TestPyPI succeeds. Then `uv publish dist/*` with OIDC and creates the GitHub Release `v0.3.0` with generated notes and the dist files |
| `linear-sync` | `build` | Runs in parallel with publishing: `linear-release-action sync --version v0.3.0` (needs `LINEAR_ACCESS_KEY`, which exists) |
| `linear-complete` | `publish-pypi`, `linear-sync` | Marks the Linear release done (Merged → Done) |

### 5.2 What must exist before the run

| Prerequisite | State on 2026-11-11 | Who |
|---|---|---|
| PyPI trusted publisher for `orcapod`: owner `metamorphic-labs`, repository `orcapod-python`, workflow `release.yml`, environment `pypi` | **Not verifiable from here** (needs a PyPI owner login). PyPI's `orcapod` 0.1.4 metadata still points at `nauticalab/orcapod-python`, so the existing publisher very likely names the old repository | Developer (PyPI owner) |
| TestPyPI trusted publisher, same fields, environment `testpypi` | Not verifiable. TestPyPI has `orcapod` 0.1.0rc1, 0.1.1, 0.1.2 only (0.1.3 and 0.1.4 were not staged there). The TestPyPI project may belong to a different account than the PyPI one | Developer (TestPyPI owner) |
| Environments `pypi` and `testpypi` | **Both exist** (`pypi` with required reviewer `eywalker`; `testpypi` unprotected). Neither has a deployment branch policy | Exists |
| `LINEAR_ACCESS_KEY` secret | Exists | Exists |
| Tag `v0.3.0` absent on origin; `0.3.0` absent on PyPI and TestPyPI | Absent on all three | |
| `CHANGELOG.md` section `[0.3.0]` dated | No `[0.3.0]` section exists yet on `main` | Release prep |
| The repository being public | **Not required.** OIDC trusted publishing works from a private repository | |

If a trusted publisher is missing, `publish-testpypi` fails **after** `build` has pushed the
tag (see `RELEASING.md`, *If a Run Fails Part-Way*: re-run the failed job once the publisher
is fixed; do not start a new run).

### 5.3 Findings

1. **There is no TestPyPI-only rehearsal mode.** Every run that passes `build` pushes a tag
   and uploads to TestPyPI, then goes on to PyPI. ITL-875's success criterion "a rehearsal
   publishes a pre-release to TestPyPI ... and installs it in a clean environment" has no
   dedicated path. Without changing the workflow, the closest is: dispatch a pre-release
   version (e.g. `0.3.0rc1`), let it reach TestPyPI, install it from TestPyPI in a clean
   environment, then either **approve** the `pypi` deployment (publishes `0.3.0rc1` on PyPI,
   which `pip install orcapod` ignores without `--pre`) or **reject** it (the run ends
   failed; tag `v0.3.0rc1` stays on origin; `0.3.0rc1` is burned on TestPyPI; `linear-sync`
   has already synced a Linear release named `v0.3.0rc1`). Adding a real TestPyPI-only
   input to `release.yml` is a behaviour change and was not made.
2. **A hand-made tag blocks publishing that version through the workflow.** The guard in
   `build` aborts when `vX.Y.Z` already exists on origin. `v0.2.0` was created by hand
   (tag-only decision), so `0.2.0` can only be published through the workflow after deleting
   that tag, and the workflow would then tag whatever commit it builds. For `0.3.0`: do not
   create `v0.3.0` by hand.
3. **TestPyPI is ungated, PyPI is gated.** The required-reviewer rule on `pypi` is a natural
   checkpoint: inspect the TestPyPI upload before approving.
4. **While the repository is private, the PyPI page links into it.** The README (the PyPI
   long description) links `CHANGELOG.md` on GitHub, and the Homepage, Repository,
   Changelog and Issues URLs in `pyproject.toml` name `metamorphic-labs/orcapod-python`.
   Until the repository is public these give a logged-out PyPI visitor a 404, so the
   migration notes in `CHANGELOG.md` cannot be reached from PyPI.
5. `uv publish` does not upload PEP 740 attestations (the PyPA publish action does); not
   required.
6. The `test` job installs every extra, so it depends on `pyspiral` staying installable from
   PyPI on release day.

---

## 6. Checklists for the developer

### 6.1 Release day (`0.3.0`), independent of repository visibility

Before the day:

1. [ ] On **pypi.org** → `orcapod` → *Manage* → *Publishing*: add a GitHub trusted publisher
       (owner `metamorphic-labs`, repository `orcapod-python`, workflow `release.yml`,
       environment `pypi`). Remove the `nauticalab` publisher if you no longer want that
       repository to be able to publish.
2. [ ] On **test.pypi.org**: the same with environment `testpypi` (check which account owns
       the TestPyPI project).
3. [ ] Decide 3.3 (the `spiraldb` extra in `all`) and 4 (`uv lock --upgrade`, `ray` pin,
       `gitpython` floor); merge any resulting PR before the cut.
4. [ ] Decide whether to rehearse (5.3, item 1). If yes: dispatch `0.3.0rc1`, check TestPyPI,
       download `orcapod-0.3.0rc1-py3-none-any.whl` from its TestPyPI page into a new empty
       directory, run `uv venv && uv pip install --python .venv ./orcapod-0.3.0rc1-py3-none-any.whl` there
       (dependencies come from PyPI; an `--extra-index-url` install would not reach TestPyPI,
       because uv stops at the first index that has `orcapod`; `--python .venv` keeps an already
       active environment from being used), check `.venv/bin/python -c "import orcapod"`, then approve or
       reject the `pypi` deployment.
5. [ ] Merge the release-prep PR: `CHANGELOG.md` `[0.3.0] — 2026-11-16`, README / docs
       install text (remove "`0.2.0` is not on PyPI, where the latest release is `0.1.4`").
6. [ ] If the repository is still private on release day, decide whether the PyPI page's
       links that 404 for the public (5.3, item 4) are acceptable for the workshop.

On the day:

7. [ ] Confirm `main` is green (`Run Tests`, PostgreSQL tests) on the commit to release.
8. [ ] Confirm `v0.3.0` does not exist on origin (`git ls-remote --tags origin v0.3.0`) and
       `0.3.0` is not on PyPI or TestPyPI.
9. [ ] *Actions → Release → Run workflow*, "Use workflow from" `main`, version `0.3.0`,
       branch `main`.
10. [ ] Wait for `publish-testpypi`; check https://test.pypi.org/project/orcapod/0.3.0/.
11. [ ] Approve the `pypi` deployment.
12. [ ] Check https://pypi.org/project/orcapod/0.3.0/ (license MIT, homepage
        `metamorphic-labs/orcapod-python`, README renders), the GitHub Release `v0.3.0`, and
        `pip install orcapod==0.3.0` in a clean environment.
13. [ ] Check `linear-complete` succeeded (it does not affect publishing).
14. [ ] If anything fails: `RELEASING.md`, *If a Run Fails Part-Way*. Never delete a tag
        whose version is on an index.

### 6.2 Making the repository public (any time; separate from the release)

1. [ ] Decide the option of 2.10 per category (A, B, C or C') and, for B, where plans and
       specs go from now on (that changes `CLAUDE.md`, `.zed/rules` and the Sensei plan
       locations).
2. [ ] At minimum consider clearing the two notebook outputs (Stanford host and username,
       KubeRay service name) and the SpiralDB project ID default in `run-tests.yml` and the
       integration test (B, or C/C' to also clear history).
3. [ ] Review the GitHub text that becomes public: 244 PRs, 51 issues and their comments
       (no secrets found; 44 contain `linear.app` links), and Actions logs (not scanned).
4. [ ] Before flipping: enable secret scanning and push protection (free on public
       repositories), and set *Actions → General → Fork pull request workflows* to require
       approval for outside contributors.
5. [ ] Check the organization allows `internal` → `public` for this repository (an
       enterprise policy may require an owner).
6. [ ] Flip visibility: *Settings → General → Danger Zone → Change visibility → Public*.
7. [ ] Afterward: confirm the README and docs links (PR numbers in `CHANGELOG.md`, the
       PyPI project URLs) resolve for a logged-out user; add `CONTRIBUTING.md` /
       `SECURITY.md` if wanted (not present today).

---

## Part B: cleanups made in this PR

- `.github/workflows/_license-check.yml`: removed `LicenseRef-Metamorphic-Proprietary`
  from the allow-list. The file is owned by this repository (called with `uses: ./…` from
  `run-tests.yml` and `release.yml`); the organization's required license check is a
  different workflow in `metamorphic-labs/shared-workflows` and was not touched. Checked by
  running the workflow's exact `pip-licenses` command in a fresh `uv sync --locked --dev`
  environment with the shortened list: exit 0 (Orcapod itself reports MIT; no package
  declares the proprietary identifier).
- `nauticalab` URLs: **none needed changing.** `RELEASING.md` has no `nauticalab` URL left
  (PR #269 fixed them); its one mention is the historical sentence about the move.
  `README.md:35` and `CHANGELOG.md:342` link `nauticalab/orcapod-extension-spikeinterface`,
  which still exists, public, under `nauticalab` and does not exist under
  `metamorphic-labs`, so the links are correct. `LICENSE` and `tests/test_packaging.py`
  keep the copyright holder NauticaLab. Plans under `superpowers/` and
  `docs/design/plans/` mention `nauticalab` as history and were not edited.
