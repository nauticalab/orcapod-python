# Your Own Data Types

Function pods can take and return your own classes: Python
[dataclasses](https://docs.python.org/3/library/dataclasses.html) and
[pydantic](https://docs.pydantic.dev/) models. Orcapod stores their values in Arrow
columns and caches results computed from them like any other value.

Caching and type checking need to know when two classes are **the same type**. Orcapod
has two kinds of class, and you choose the kind where you define the class.

## Two kinds of class

| | `@op.dataclass` | `@dataclass`, pydantic model |
|---|---|---|
| Identified by | its **name** and its **fields** (names, types, order) | its **exact class**: module path, class name and fields |
| Two classes with the same name and fields | one type | two types |
| The class cell is run again in a notebook | still the same type; pods and stored results keep working | a new class; pods built for the earlier one refuse it |
| The class moves to another module | same type (the module is not part of it) | another type; results are computed again |
| Reading stored values without the class | works: the class is rebuilt from stored metadata | `ImportError` |
| What a pod is promised | the fields | the class itself, with its methods and validators |

**Use `@op.dataclass`** for data you pass between pods, keep in a store and work on in
notebooks: records, measurements, parameters. It is the kind to use while a class is
still being edited.

**Use an ordinary `@dataclass` or a pydantic model** when the pod needs that very class:
its methods, its validators, its place in a library. Orcapod then never accepts another
class in its place.

In both kinds, adding, removing, renaming, retyping or reordering fields makes a **new
type**. Results computed with the old definition stay in the database, and the new type
computes its results into new tables, so a stored value is never read back with fields
it was not written with. Field order counts because your code can see it: a dataclass
compares, unpacks and prints its fields in declared order.

Defaults, methods, properties, validators and docstrings are **not** part of a type,
although a pod can observe them. Changing one of them keeps the type and its stored
results. If such a change alters what a pod computes, bump the pod's version, as you
would for any change to its code.

## `op.dataclass` in a notebook

`op.dataclass` is used like `dataclasses.dataclass` and returns a real dataclass. Define
a class, use it in a pod and run a job:

```python
import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@op.dataclass
class Point:
    x: float
    y: float


calls = []


@function_pod(output_keys="norm")
def norm(p: Point) -> float:
    calls.append(p)  # only here to show when the function really runs
    return (p.x**2 + p.y**2) ** 0.5


db = InMemoryArrowDatabase()


def run(point_class):
    rows = [
        {"k": "a", "p": point_class(x=3.0, y=4.0)},
        {"k": "b", "p": point_class(x=6.0, y=8.0)},
    ]
    job = PipelineJob(name="points", database=db)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="pts")
        norm.pod(source, label="norm")
    job.run()
    return job.nodes["norm"]


print(run(Point).as_table().to_pandas())
#    k  norm
# 0  a   5.0
# 1  b  10.0
print("function calls:", len(calls))
# function calls: 2
```

Running the cell that defines `Point` again creates a new class object with the same
name and fields. It is the same type: the pod defined earlier accepts it, and the next
run finds the stored results.

```python
@op.dataclass
class Point:
    x: float
    y: float


run(Point)
print("function calls:", len(calls))
# function calls: 2
```

Changing the fields makes a new type. Re-run the pod cell too, so that `norm` takes the
new `Point`; the run then computes into new tables:

```python
@op.dataclass
class Point:
    x: float
    y: float
    label: str = ""


@function_pod(output_keys="norm")
def norm(p: Point) -> float:
    calls.append(p)
    return (p.x**2 + p.y**2) ** 0.5


run(Point)
print("function calls:", len(calls))
# function calls: 4
```

### The name

The type is named by the class name, `Point`, wherever the class is defined: in a
notebook, a script or any module. Pass `name=` to choose another name, for example to
keep two unrelated `Point` classes apart or to rename a class without losing its
results:

```python
@op.dataclass(name="Point")
class LabelledPoint:
    x: float
    y: float
    label: str = ""


run(LabelledPoint)
print("function calls:", len(calls))
# function calls: 4
```

A name must not be empty, contain `@`, `;` or spaces, or start with `orcapod.`. Two
classes with the same fields and **different** names are different types.

`op.dataclass` also takes the options of `dataclasses.dataclass` (`frozen=True`,
`slots=True`, `kw_only=True`, `order=True`, ...), except `init=False`. Equality and
hashing always cover all stored fields, so that every class of a type compares alike: a
field with `field(compare=False)` or `field(hash=...)` is refused with `TypeError`.

You can see a type on any column that holds it. The part before `@` is the name; the
rest is a hash of the name, the kind and the fields:

```python
source = DictSource(data=[{"k": "a", "p": LabelledPoint(1.0, 2.0)}], tag_columns=["k"])
print(source.as_table().schema.field("p").type.extension_name)
# Point@...
```

### A structural class promises its fields, not its methods

When two classes are the same type, Orcapod checks that they are compatible and passes
the value **as it is**. It never converts a value from one class to another. A function
annotated with `Point` can therefore receive an instance of another class of the same
type: the object an upstream pod returned, an instance of the class as it was defined
before the cell was re-run, or a class rebuilt from stored metadata.

What you can rely on:

- **The fields.** Every class of the type has the same fields with the same types.
- **Equality.** Instances of two classes of the same type are equal when their field
  values are equal.

What you cannot rely on is a method or a `__post_init__` check of the class named in the
annotation. Keep behavior in functions that take the value, or convert explicitly.

```python
print(LabelledPoint(1.0, 2.0) == Point(1.0, 2.0))
# True
```

### Converting explicitly

`from_instance` builds an instance of an `op.dataclass` from an instance of any other
dataclass, ordinary or `op.dataclass`, that has exactly the same field names with the
same types. It is the only conversion, and Orcapod never calls it for you.

```python
from dataclasses import dataclass


@dataclass
class LegacyPoint:
    x: float
    y: float
    label: str = ""


converted = Point.from_instance(LegacyPoint(1.0, 2.0, "origin"))
print(type(converted).__name__, converted.label)
# Point origin
```

A missing or an extra field raises `TypeError` rather than dropping or inventing data.

## Ordinary dataclasses and pydantic models

An ordinary `@dataclass` and a pydantic model are identified by their **exact class**:
the module path, the class name and the fields.

- A class defined in a **module** is named by its module path: `pkg.models.Reading`.
- A class defined in a **notebook or a script** run directly is named under `__main__`:
  `__main__.Reading`.

Two different classes are never the same type, even with the same name and fields. In a
notebook this shows when a class cell is run again: the new class is a new class, and a
pod built for the earlier one refuses it. Re-run the pod cell as well.

```python
@dataclass
class Reading:
    value: float


@function_pod(output_keys="doubled")
def double(r: Reading) -> float:
    return 2 * r.value


@dataclass
class Reading:  # the cell is run again
    value: float


try:
    double.pod(DictSource(data=[{"k": "a", "r": Reading(1.0)}], tag_columns=["k"]))
except ValueError as error:
    print("refused:", "column 'r'" in str(error))
# refused: True
```

The stored results are not lost: a pod built for the new class finds them, because the
module path, the name and the fields are unchanged.

Python's subclass rule holds: a pod annotated with a class accepts a column of a
subclass. The subclass remains its own type, with its own fields and tables.

A column has its declared logical type. A pod annotated
`-> Child` produces a `Child` column with all of `Child`'s fields, and a downstream
parameter `p: Base` accepts that column. A pod annotated `-> Base` that returns a `Child`
instance is accepted: in memory it remains a `Child`, while the declared `Base` logical
type stores only the base fields. Subclass-only fields do not enter the stored row hash.

A dataclass and a pydantic model are never the same type, and neither is ever the same
type as an `op.dataclass`.

## Classes inside classes

A field whose type is another class carries that class's whole description: its name,
its kind and its fields. Renaming the inner class or changing its fields therefore makes
the outer class a new type too.

Every class inside an `op.dataclass` must itself be an `op.dataclass`, so that the whole
value can be rebuilt from stored metadata. A field typed by an ordinary dataclass or a
pydantic model raises `TypeError` naming the field. An ordinary dataclass or a pydantic
model may hold fields of any kind.

A class that refers back to itself, directly (`next: Node | None`) or through other
classes, cannot be stored: Arrow has no recursive struct type. Orcapod raises
`RecursiveTypeError` naming the path, for example `Node.next -> Node`.

## Reading stored values back

**An `op.dataclass` column** is read with any `op.dataclass` of the stored name and
fields that is defined in the current process. If none is defined, Orcapod builds a
dataclass from the stored description (in the module `orcapod.synthesized`): it has the
stored fields, compares equal to your own class's instances, and converts with
`from_instance`, but it has none of your methods.

**An ordinary dataclass or pydantic column** is read with the class found at the stored
module path (for a notebook or script class: in the running `__main__`).

- If there is no class at that path, reading raises `ImportError` naming the column, the
  class and its stored fields.
- If the class there has other fields or another field order, reading raises
  `TypeIdentityError` (from `orcapod.errors`) naming the column and both sets of fields.

Neither case is reported as missing data, and nothing stored is changed.

## Rules and limits

- An ordinary dataclass or pydantic model defined inside a function cannot be stored: it
  has no module path to be found by. An `op.dataclass` may be defined anywhere.
- Each subclass has its own identity. Declaring a base-class output accepts subclass
  instances but stores only base fields; declare the subclass to store its extra fields.
- There is no structural pydantic model: a pydantic model is always identified by its
  exact class.
- Results stored by Orcapod versions before 0.3 identified classes by module and class
  name only. Version 0.3 does not read those columns; pipelines compute their results again
  into new tables and leave the old ones untouched.

## How it connects to other concepts

- [Function Pods](function-pods.md): the pods that take and return these classes.
- [Identity & Hashing](identity.md): how a type's identity enters pipeline and content
  hashes.

Function outputs accept instances of their declared class and subclasses (ITL-895).
A subclass object is passed unchanged to the next pod in memory. When returned for a
base-class output, only the base fields are stored and read back; extra subclass fields
are not included in the row hash. Two subclass instances differing only in extra fields
therefore have the same stored row hash. Declare the subclass to retain all its fields.
Compatible structural objects also pass unchanged in memory; stored reads use the registry
binding for their structural identity.
