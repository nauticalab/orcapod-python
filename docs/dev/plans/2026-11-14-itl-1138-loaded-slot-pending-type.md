# ITL-1138: a loaded source slot whose user class is not registered at load

Branch `eywalker/itl-1138-a-job-loaded-before-its-user-class-is`, from PR #386's head
(`4ffdef06`), or `origin/main` if #386 has merged. Draft PR against `main`:
"Stacked on #386: merge right after it. Fixes ITL-1138".

## Cause (measured for ITL-1137, sequence 4)

`Pipeline.load` (`pipeline/graph.py`, `source_type == "node"`) restores a slot's saved schema
with `deserialize_schema`, which returns the raw saved text (`extension<mod.A@…;…>`) for a
column whose extension type is not registered in the loading process. `PipelineJob.load` copies
that schema into the `SourceJobNode`. Its pipeline hash is then the hash of the text; on #386
the bind check compares the text with the source's class and refuses, and #386 names the
system-tag columns from the slot's pipeline hash.

## Plan

1. Failing fresh-process test first (public interface): sequence-4 job (s1 over `C`, s2;
   `Join` -> `g(p: C, r: int)`; `MapData` -> `h(pp: C)`), exact and structural `C`; save; fresh
   process loads without registering `C`, builds the sources, binds, `run(load_only=True)`;
   every node's pipeline hash, content hash, `as_table(all_info=True)` schema (system-tag
   names), rows and stored rows equal the saving process. Plus: pydantic; a class never
   registered -> clear error at bind; a file saved before this change (the job file format
   is unchanged, so an existing file is that file: write it with the base commit? no: the
   saved descriptor already records `pipeline_hash`, so a file from before the change is the
   same file; cover a descriptor without the saved hash if the format allows one).
2. Loaded slot keeps saved `pipeline_hash` (and `content_hash` while unbound) from its
   descriptor, like the function and operator stubs.
3. Pending type text: the slot keeps a column it could not restore as text; at bind (the
   `_fit` check) it restores it again through the data context's converter (the source has
   registered the class by then). Restored -> the slot's schema holds the class and the check
   compares types as usual. Not restored -> `SourceSpecMismatchError` naming the column and the
   saved class identity (name and kind from the extension metadata) and what to do.
4. No hash, golden or stored-table change.
