## `src/orcapod/logical_types/` — identity of user classes (dataclasses, pydantic models)

### UCK1 — A user class was identified by `module.qualname` only; then every class by name, kind and fields
**Status:** resolved
**Severity:** high
**Issue:** ITL-972 (supersedes the model of ITL-881, PR #292)

In v0.2.0 a dataclass or pydantic model was identified by `f"{module}.{qualname}"`. Two
classes with the same name and different fields shared pipeline hashes and tables, so a
changed class read the old table, failed, and the run returned 0 rows with only log
warnings; re-running the notebook cell that defines a class raised
`ValueError: … already bound` until a kernel restart; a subclass was stored as its parent,
dropping its extra fields.

PR #292 (ITL-881, not merged) identified every class by a declared name, kind and ordered
fields, so two different classes with the same description were interchangeable. A
measurement on November 12, 2026 showed the consequence: in memory a pod annotated with one
class received the other class's object unchanged, while through a store it received its
own class.

**Fix:** two explicit kinds (developer, November 12, 2026; spec
`superpowers/specs/2026-11-12-itl-972-user-class-kinds.md`).

- An ordinary dataclass or pydantic model is an **exact** class: named by module path and
  qualified name, never equivalent to another class. Python subclassing still applies at a
  pod input. A value is stored only as its own class.
- `orcapod.dataclass` makes a **structural** class: named by its declared name (default the
  class name); two structural classes with equal identity records are compatible in the
  schema check and their instances compare equal. Orcapod converts nothing: a value is
  passed as it is. `from_instance` is the explicit conversion.
- Every kind is described by an identity record `{kind, name, fields}` with structured
  type tokens; a nested class embeds its whole record and a logical-type field the whole
  logical type identifier. The identity string `name@<SHA-256 of the record>` is the logical
  type name and Arrow extension name (Arrow's and Polars' registries are keyed by name),
  and the record is the extension metadata, so a structural class is rebuilt from metadata
  when no class is defined (`orcapod.synthesized`).
- Kept from PR #292: a subclass has its own identity; field order is identity;
  `TypeIdentityError` is not a `StoreReadError` and fails the run under every error policy;
  the registry binds an identifier to the newest class object with that identity.
  Removed: `op.register_type`, declared names for exact classes, equivalence of exact
  classes.

Arrow-backed data carries the identifier, not a class object: after an exact class is
defined again and used, tables built before are read as the new class.

Not covered: a structural pydantic model (ITL-973), computed fields, Enum/NamedTuple/
TypedDict.

**Known limitation (ITL-894):** reading a stored column whose storage differs from the
registered one only by layout (`string` vs `large_string`) is accepted at any depth, so
Delta-compacted tables stay readable. `__arrow_ext_deserialize__` then returns the
registered large storage type, which is correct for Parquet and Delta reads (the decoder
converts the data) but not over Arrow IPC or the C stream interface, where Arrow reads
32-bit offsets as 64-bit and the arrays are invalid. Top-level columns had this since
ITL-602; nested struct fields since this fix. Orcapod's own writers and Polars round trips
never produce non-large storage. Pinned by a strict xfail in
`tests/test_user_types/test_read_path.py`.
