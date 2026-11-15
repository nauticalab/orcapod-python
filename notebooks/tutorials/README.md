# Tutorials

The Orcapod tutorial now lives in the documentation, where every sample is run by the test
suite: [`docs/tutorial/`](../../docs/tutorial/index.md).

It is a self-paced course in eleven short parts on one small synthetic dataset. To generate
the dataset:

```bash
uv run python docs/tutorial/workshop_data.py
```

The notebooks that used to be in this directory were written against an earlier API and no
longer ran; they are in the git history if you need them. `notebooks/old_tutorials/` is kept
for reference only and is not maintained.
