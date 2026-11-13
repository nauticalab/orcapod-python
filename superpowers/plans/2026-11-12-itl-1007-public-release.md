# ITL-1007: make the repository ready for public release from nauticalab

Scope after the developer's decisions of 2026-11-12: Spiral project ids stay as they are (they
name only the sample dataset and sit behind credentials), and anything workshop related moves
to the ediop repository. So: no id scrub, no guard test; the workshop tool, its tests and its
plan are removed (they stay in git history); URLs, workflow repository guards, `lint.yml`,
`RELEASING.md` and the agent instructions stay.

Facts checked: nauticalab main is 61d0f18e, an ancestor of our main; nauticalab has `tests.yml`,
deleted on our main by 07e5a2ea (#267), so a fast-forward push deletes it.

Order: undo scrub -> drop guard -> remove workshop plan -> instructions and fragment -> full
suite on 3.11/3.12/3.13 -> push once -> CodeRabbit.
