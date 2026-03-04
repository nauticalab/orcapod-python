# Documentation Site Setup & Maintenance

This guide covers how the orcapod documentation site is built, deployed, and maintained.
Follow these instructions to replicate the setup from scratch, troubleshoot deployment
issues, or make changes to the documentation infrastructure.

---

## Overview

| Component | Technology |
|-----------|-----------|
| Documentation framework | [MkDocs](https://www.mkdocs.org/) with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) |
| API docs generation | [mkdocstrings](https://mkdocstrings.github.io/) (Python handler) |
| Hosting | [GitHub Pages](https://pages.github.com/) |
| Deployment | [GitHub Actions](https://github.com/features/actions) (`.github/workflows/docs.yml`) |
| Custom domain | `orcapod.org` |

---

## Local Development

### Install dependencies

```bash
uv sync --group docs
```

### Live preview

```bash
uv run mkdocs serve
```

Opens a local server at `http://127.0.0.1:8000` with hot-reload on file changes.

### Build static site

```bash
uv run mkdocs build
```

Outputs static HTML to the `site/` directory (gitignored).

---

## File Structure

```
orcapod-python/
├── mkdocs.yml                      # MkDocs configuration (nav, theme, plugins)
├── docs/
│   ├── CNAME                       # Custom domain file (deployed to GitHub Pages root)
│   ├── CONTRIBUTING_DOCS.md        # This file
│   ├── index.md                    # Homepage
│   ├── getting-started/
│   │   ├── installation.md
│   │   ├── quickstart.md
│   │   └── first-pipeline.md
│   ├── concepts/
│   │   ├── architecture.md
│   │   ├── datagrams.md
│   │   ├── streams.md
│   │   ├── identity.md
│   │   ├── provenance.md
│   │   └── schema.md
│   ├── user-guide/
│   │   ├── sources.md
│   │   ├── function-pods.md
│   │   ├── operators.md
│   │   ├── pipelines.md
│   │   ├── caching.md
│   │   └── execution.md
│   └── api/
│       ├── index.md
│       ├── types.md
│       ├── sources.md
│       ├── streams.md
│       ├── datagrams.md
│       ├── function-pods.md
│       ├── packet-functions.md
│       ├── operators.md
│       ├── nodes.md
│       ├── pipeline.md
│       ├── databases.md
│       ├── errors.md
│       └── configuration.md
├── .github/workflows/
│   └── docs.yml                    # GitHub Actions deployment workflow
└── pyproject.toml                  # docs dependency group defined here
```

---

## GitHub Pages Setup (from scratch)

### Step 1: Enable GitHub Pages

1. Go to **https://github.com/walkerlab/orcapod-python/settings/pages**
2. Under **Source**, select **GitHub Actions** (not "Deploy from a branch")
3. Click **Save**

That's all that's needed on the GitHub side. The workflow in `.github/workflows/docs.yml`
handles building and deploying.

### Step 2: Verify the workflow

The workflow triggers on:

- **Push to `main`** — automatic deployment on every merge
- **`workflow_dispatch`** — manual trigger from the Actions tab

To manually trigger:

1. Go to **https://github.com/walkerlab/orcapod-python/actions**
2. Select the **Deploy docs** workflow
3. Click **Run workflow** → **Run workflow**

### Step 3: Verify deployment

After the first successful run:

- The site is live at `https://walkerlab.github.io/orcapod-python/`
- Check the **Environments** section on the repo homepage for the deployment URL

---

## Custom Domain Setup (orcapod.org)

### Step 1: Configure DNS records

At your domain registrar's DNS management panel (e.g., Cloudflare, Namecheap, Route 53),
add the following records:

#### A records (apex domain — `orcapod.org`)

| Type | Name | Value | TTL |
|------|------|-------|-----|
| A | `@` | `185.199.108.153` | 3600 |
| A | `@` | `185.199.109.153` | 3600 |
| A | `@` | `185.199.110.153` | 3600 |
| A | `@` | `185.199.111.153` | 3600 |

These are GitHub Pages' IP addresses. All four are required for redundancy.

#### CNAME record (www subdomain — optional but recommended)

| Type | Name | Value | TTL |
|------|------|-------|-----|
| CNAME | `www` | `walkerlab.github.io` | 3600 |

This redirects `www.orcapod.org` to the GitHub Pages site.

### Step 2: Configure GitHub Pages custom domain

1. Go to **https://github.com/walkerlab/orcapod-python/settings/pages**
2. Under **Custom domain**, enter `orcapod.org`
3. Click **Save**
4. GitHub will run a DNS check — this may take a few minutes
5. Once the DNS check passes, check **Enforce HTTPS**

### Step 3: CNAME file in the repository

The file `docs/CNAME` contains the custom domain (`orcapod.org`). MkDocs copies this file
to the root of the built site, which tells GitHub Pages to serve the site at the custom
domain.

**Important:** Do not delete `docs/CNAME`. If this file is missing, GitHub Pages will revert
to serving at `walkerlab.github.io/orcapod-python/` and the custom domain will stop working
after the next deployment.

### Step 4: Verify

```bash
# Check DNS propagation (may take up to 24 hours, usually minutes)
dig orcapod.org +short
# Should return:
# 185.199.108.153
# 185.199.109.153
# 185.199.110.153
# 185.199.111.153

# Check HTTPS
curl -I https://orcapod.org
# Should return HTTP/2 200
```

---

## Troubleshooting

### Site not updating after push

1. Check **Actions tab** → look for the latest "Deploy docs" run
2. If the run failed, click into it to see the error logs
3. Common issues:
   - **mkdocstrings import error** — a module referenced in an API doc page doesn't exist
     or has an import error. Check the build log for the specific module path.
   - **Missing dependency** — add it to the `docs` group in `pyproject.toml`

### Custom domain shows 404

1. Verify `docs/CNAME` exists and contains `orcapod.org`
2. Check GitHub Pages settings → Custom domain should show `orcapod.org`
3. Re-save the custom domain in settings to re-trigger DNS verification
4. Verify DNS records: `dig orcapod.org +short` should show GitHub's IPs

### Custom domain shows GitHub Pages 404 (not your site)

The CNAME file may have been removed during a deployment. Verify `docs/CNAME` exists in
the repository and redeploy.

### HTTPS not available

- HTTPS is only available after DNS propagation completes and GitHub verifies ownership
- Check **Settings > Pages** — if the DNS check shows a warning, wait and try again
- GitHub provisions TLS certificates via Let's Encrypt, which can take up to 1 hour after
  DNS verification

### API docs page shows "Module not found"

The mkdocstrings directive references a Python module path. If you see an error like:

```
ERROR - mkdocstrings: No module named 'orcapod.some.module'
```

1. Check that the module path in the `.md` file matches the actual Python module path
2. Verify the module has no import-time errors: `uv run python -c "import orcapod.some.module"`
3. Check that `src` is listed in `mkdocstrings` handler paths in `mkdocs.yml`

### Build works locally but fails in CI

1. Check Python version — the CI uses whatever `uv` resolves; ensure compatibility
2. Check for system dependencies — some packages (e.g., `pygraphviz`) need system libraries
   that may not be available in the CI runner

---

## Making Changes

### Adding a new documentation page

1. Create the `.md` file in the appropriate `docs/` subdirectory
2. Add it to the `nav` section in `mkdocs.yml`
3. Preview locally with `uv run mkdocs serve`

### Adding API docs for a new module

Add a mkdocstrings directive in the appropriate `docs/api/` file:

```markdown
## MyNewClass

::: orcapod.module.path.MyNewClass
    options:
      members:
        - method_a
        - method_b
```

The `members` list controls which methods are documented. Omit it to show all public members
that have docstrings.

### Updating the navigation

Edit the `nav` section in `mkdocs.yml`. The structure maps directly to the site's sidebar
navigation.

### Changing the theme or plugins

Edit `mkdocs.yml`. See the [Material for MkDocs documentation](https://squidfunk.github.io/mkdocs-material/)
for available options.

---

## Dependencies

Documentation dependencies are managed in the `docs` dependency group in `pyproject.toml`:

```toml
[dependency-groups]
docs = [
    "mkdocs>=1.6.0",
    "mkdocs-material>=9.5.0",
    "mkdocstrings[python]>=0.27.0",
    "pymdown-extensions>=10.7",
]
```

To update: edit the versions in `pyproject.toml` and run `uv sync --group docs`.
