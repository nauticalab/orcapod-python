# This script is used to generate hash examples for testing purposes.
# The resulting hashes are saved in `hash_samples` folder, and are used
# throughout the tests to ensure consistent hashing behavior across different runs
# and revisions of the codebase.
#
# Uses the new BaseSemanticHasher API (get_default_semantic_hasher) rather than
# the legacy hash_to_hex / hash_to_int / hash_to_uuid functions.

import json
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

from orcapod.hashing import get_default_semantic_hasher

# Create the hash_samples directory if it doesn't exist
SAMPLES_DIR = Path(__file__).parent / "hash_samples"
SAMPLES_DIR.mkdir(exist_ok=True)

# Create data_structures subdirectory for the hash examples
DATA_STRUCTURES_DIR = SAMPLES_DIR / "data_structures"
DATA_STRUCTURES_DIR.mkdir(exist_ok=True)

# Format the current date and time for the filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = DATA_STRUCTURES_DIR / f"hash_examples_{timestamp}.json"


def generate_hash_examples():
    """Generate hash examples for various data structures using BaseSemanticHasher."""
    hasher = get_default_semantic_hasher()
    examples = []

    # Basic data types
    basic_examples = [
        None,
        True,
        False,
        0,
        1,
        -1,
        42,
        3.14159,
        -2.71828,
        0.0,
        "",
        "hello",
        "Hello, World!",
        "Special chars: !@#$%^&*()",
        "Unicode: 你好, Привет, こんにちは",
    ]

    # Bytes examples
    bytes_examples = [
        b"",
        b"hello",
        b"\x00\x01\x02\x03",
        bytearray(b"hello world"),
        bytearray([65, 66, 67]),  # ABC
    ]

    # Collection examples
    collection_examples = [
        [],
        [1, 2, 3],
        ["a", "b", "c"],
        [1, "a", True],
        set(),
        {1, 2, 3},
        {"a", "b", "c"},
        frozenset(),
        frozenset([1, 2, 3]),
        (),
        (1, 2, 3),
        {},
        {"a": 1},
        {"a": 1, "b": 2},
        {"b": 1, "a": 2},  # Same keys as above but different insertion order
        {"nested": {"a": 1, "b": 2}},
    ]

    # Complex nested examples
    nested_examples = [
        [1, [2, [3, [4, [5]]]]],
        {"a": {"b": {"c": {"d": {"e": 42}}}}},
        {"a": [1, 2, {"b": [3, 4, {"c": 5}]}]},
        [{"a": 1}, {"b": 2}, {"c": [3, 4, 5]}],
        {"keys": ["a", "b", "c"], "values": [1, 2, 3]},
        [{"a": 1, "b": [2, 3]}, {"c": 4, "d": [5, 6]}],
        {"users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]},
        {
            "data": {
                "points": [[1, 2], [3, 4], [5, 6]],
                "labels": ["A", "B", "C"],
            }
        },
        OrderedDict([("a", 1), ("b", 2), ("c", 3)]),
        [[1, 2], [3, 4], {"a": [5, 6], "b": [7, 8]}],
    ]

    # Combine all examples
    all_examples = (
        basic_examples + bytes_examples + collection_examples + nested_examples
    )

    # Generate hashes for each example
    for value in all_examples:
        try:
            content_hash = hasher.hash_object(value)
            hash_string = content_hash.to_string()

            # Produce a JSON-serialisable representation of the value so the
            # sample file is human-readable and round-trippable by the test.
            if isinstance(value, (bytes, bytearray)):
                serialized_value = f"bytes:{value.hex()}"
            elif isinstance(value, (set, frozenset)):
                type_tag = "frozenset" if isinstance(value, frozenset) else "set"
                serialized_value = {
                    "__type__": type_tag,
                    "items": sorted(value, key=str),
                }
            elif isinstance(value, tuple):
                serialized_value = {"__type__": "tuple", "items": list(value)}
            elif isinstance(value, OrderedDict):
                serialized_value = {"__type__": "OrderedDict", "items": dict(value)}
            else:
                serialized_value = value

            examples.append(
                {
                    "value": serialized_value,
                    "hash": hash_string,
                }
            )
        except Exception as e:
            print(f"Error hashing value {repr(value)}: {e}")

    return examples


if __name__ == "__main__":
    # Generate the hash examples
    hash_examples = generate_hash_examples()

    # Save the examples to a JSON file
    with open(output_file, "w") as f:
        json.dump(hash_examples, f, indent=2, ensure_ascii=False)

    print(f"Generated {len(hash_examples)} hash examples")
    print(f"Saved to {output_file}")
