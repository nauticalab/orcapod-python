# ITL-938: bring the design document in line with the post-session decisions

Docs only. File: `superpowers/specs/2026-11-11-itl-938-versioning-system-design.md` (PR #312).
Source of truth for each statement: PR bodies #313 to #318 (read November 11, 2026); the
coordinator's brief where no PR exists (ITL-935, ITL-953, ITL-957).

CodeRabbit on #312: review skipped (file over the size limit); no reviews, no inline comments.
Nothing to plan for.

## Edits, by decision

1. **MergeJoin (E) withdrawn; ITL-942 as in PR #315.** 5.2 row 8; 5.3 (drop the E row); 5.4
   diagram (fold wording); Part 6 (what moves paths: F and N only); 7.2 row 2; Part 8 items 5, 7;
   A.9 rows E, L; Part 9.
2. **Source ids.** 4.1 intro; 5.6 rule 5 + new 5a; 7.1 (label row, source identity row); 7.2
   row 6a + new rows; 7.3 default-id row; Part 8 items 1a, 11; Part 9.
3. **Native containers carry no version.** 5.5 table; 7.1 logical type row; 7.2 row 4; Part 8
   item 4; Part 9.
4. **Old data: prefix scan, warn when a store cannot list (PR #318).** Part 6 paths block and
   "old layout" paragraph; 6.1 resolutions; 7.1; 7.2 row 3; 7.3; 7.3a rows 4, 5; Part 8 item 1;
   Part 9.
5. **GroupBy / operator rule (PR #316, #317).** 5.2 new rows; 7.2 new rows; Part 9.
6. **Issue and PR numbers.** 7.2 State column; Part 8 items; header.

Then: one docs commit, push, update PR body, check CI and CodeRabbit, `worker_done`.
