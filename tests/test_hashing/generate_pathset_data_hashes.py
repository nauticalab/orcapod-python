#!/usr/bin/env python
"""
Generate sample pathsets and data and record their hashes.

This script creates various pathset and data examples using the files from
hash_samples/file_samples, then computes and records their hashes in JSON lookup tables.
"""

import json
import sys
from pathlib import Path

# Add the parent directory to the path to import orcapod
sys.path.append(str(Path(__file__).parent.parent.parent))
from orcapod.hashing import hash_data, hash_pathset

# Create directories if they don't exist
HASH_SAMPLES_DIR = Path(__file__).parent / "hash_samples"
HASH_SAMPLES_DIR.mkdir(exist_ok=True)

# Use file_samples subdirectory with existing sample files
SAMPLE_FILES_DIR = HASH_SAMPLES_DIR / "file_samples"

# Paths for the hash lookup tables
PATHSET_LUT_PATH = HASH_SAMPLES_DIR / "pathset_hash_lut.json"
DATA_LUT_PATH = HASH_SAMPLES_DIR / "data_hash_lut.json"


def create_sample_pathsets():
    """Create sample pathsets and compute their hashes."""
    pathsets_info = []

    # Check if the sample files directory exists
    if not SAMPLE_FILES_DIR.exists():
        print(f"Sample files directory {SAMPLE_FILES_DIR} not found.")
        print("Run generate_file_hashes.py first to create sample files.")
        return []

    # Sample 1: Single file
    sample_files = list(SAMPLE_FILES_DIR.glob("*.txt"))[:1]  # Just take one text file
    if sample_files:
        single_file = sample_files[0]
        rel_path = single_file.relative_to(Path(__file__).parent)
        pathset_hash = hash_pathset(single_file)

        pathsets_info.append(
            {
                "name": "single_file",
                "paths": [str(rel_path)],
                "type": "single_file",
                "hash": pathset_hash,
            }
        )
        print(f"Created pathset from single file: {rel_path}, Hash: {pathset_hash}")

    # Sample 2: Multiple text files
    text_files = list(SAMPLE_FILES_DIR.glob("*.txt"))[:3]  # Take up to 3 text files
    if len(text_files) >= 2:
        rel_paths = [f.relative_to(Path(__file__).parent) for f in text_files]
        pathset_hash = hash_pathset(text_files)

        pathsets_info.append(
            {
                "name": "multiple_text_files",
                "paths": [str(p) for p in rel_paths],
                "type": "collection",
                "hash": pathset_hash,
            }
        )
        print(
            f"Created pathset from {len(text_files)} text files, Hash: {pathset_hash}"
        )

    # Sample 3: Mix of text and binary files
    binary_files = list(SAMPLE_FILES_DIR.glob("*.bin"))[:2]
    mixed_files = text_files[:2] + binary_files[:2]
    if len(mixed_files) >= 3:
        rel_paths = [f.relative_to(Path(__file__).parent) for f in mixed_files]
        pathset_hash = hash_pathset(mixed_files)

        pathsets_info.append(
            {
                "name": "mixed_files",
                "paths": [str(p) for p in rel_paths],
                "type": "collection",
                "hash": pathset_hash,
            }
        )
        print(
            f"Created pathset from {len(mixed_files)} mixed files, Hash: {pathset_hash}"
        )

    # Sample 4: Directory as pathset
    if SAMPLE_FILES_DIR.exists():
        rel_path = SAMPLE_FILES_DIR.relative_to(Path(__file__).parent)
        pathset_hash = hash_pathset(SAMPLE_FILES_DIR)

        pathsets_info.append(
            {
                "name": "directory",
                "paths": [str(rel_path)],
                "type": "directory",
                "hash": pathset_hash,
            }
        )
        print(f"Created pathset from directory: {rel_path}, Hash: {pathset_hash}")

    return pathsets_info


def create_sample_data():
    """Create sample data and compute their hashes."""
    data_info = []

    # Check if the sample files directory exists
    if not SAMPLE_FILES_DIR.exists():
        print(f"Sample files directory {SAMPLE_FILES_DIR} not found.")
        print("Run generate_file_hashes.py first to create sample files.")
        return []

    # Get available text and binary files
    text_files = list(SAMPLE_FILES_DIR.glob("*.txt"))
    binary_files = list(SAMPLE_FILES_DIR.glob("*.bin"))

    # Sample 1: Simple data with one key
    if text_files:
        data = {"data": text_files[0]}
        data_hash = hash_data(data)

        data_info.append(
            {
                "name": "simple_data",
                "structure": {
                    "data": str(text_files[0].relative_to(Path(__file__).parent))
                },
                "hash": data_hash,
            }
        )
        print(f"Created simple data with one key, Hash: {data_hash}")

    # Sample 2: DataProtocol with multiple keys, each pointing to a single file
    if len(text_files) >= 2 and binary_files:
        data = {
            "text": text_files[0],
            "more_text": text_files[1],
            "binary": binary_files[0],
        }
        data_hash = hash_data(data)

        data_info.append(
            {
                "name": "multi_key_data",
                "structure": {
                    "text": str(text_files[0].relative_to(Path(__file__).parent)),
                    "more_text": str(text_files[1].relative_to(Path(__file__).parent)),
                    "binary": str(binary_files[0].relative_to(Path(__file__).parent)),
                },
                "hash": data_hash,
            }
        )
        print(f"Created data with multiple keys, Hash: {data_hash}")

    # Sample 3: DataProtocol with keys pointing to collections of files
    if len(text_files) >= 3 and len(binary_files) >= 2:
        data = {"texts": text_files[:3], "binaries": binary_files[:2]}
        data_hash = hash_data(data)

        data_info.append(
            {
                "name": "collection_data",
                "structure": {
                    "texts": [
                        str(f.relative_to(Path(__file__).parent))
                        for f in text_files[:3]
                    ],
                    "binaries": [
                        str(f.relative_to(Path(__file__).parent))
                        for f in binary_files[:2]
                    ],
                },
                "hash": data_hash,
            }
        )
        print(f"Created data with collections, Hash: {data_hash}")

    # Sample 4: Hierarchical data with directory and files
    if SAMPLE_FILES_DIR.exists() and text_files and binary_files:
        data = {"directory": SAMPLE_FILES_DIR, "specific_file": text_files[0]}
        data_hash = hash_data(data)

        data_info.append(
            {
                "name": "hierarchical_data",
                "structure": {
                    "directory": str(
                        SAMPLE_FILES_DIR.relative_to(Path(__file__).parent)
                    ),
                    "specific_file": str(
                        text_files[0].relative_to(Path(__file__).parent)
                    ),
                },
                "hash": data_hash,
            }
        )
        print(f"Created hierarchical data, Hash: {data_hash}")

    return data_info


def main():
    """Generate sample pathsets and data, and save their hash information."""
    print(f"Generating sample pathsets using files from {SAMPLE_FILES_DIR}")
    pathsets_info = create_sample_pathsets()

    # Convert to the required format for the pathset hash LUT
    pathset_lut = {}
    for info in pathsets_info:
        pathset_lut[info["name"]] = {
            "paths": info["paths"],
            "type": info["type"],
            "hash": info["hash"],
        }

    # Save to the pathset lookup table file
    with open(PATHSET_LUT_PATH, "w", encoding="utf-8") as f:
        json.dump(pathset_lut, f, indent=2)

    print(f"\nGenerated {len(pathsets_info)} sample pathsets")
    print(f"PathSet hash lookup table saved to {PATHSET_LUT_PATH}")

    print(f"\nGenerating sample data using files from {SAMPLE_FILES_DIR}")
    data_info = create_sample_data()

    # Convert to the required format for the data hash LUT
    data_lut = {}
    for info in data_info:
        data_lut[info["name"]] = {
            "structure": info["structure"],
            "hash": info["hash"],
        }

    # Save to the data lookup table file
    with open(DATA_LUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data_lut, f, indent=2)

    print(f"\nGenerated {len(data_info)} sample data")
    print(f"DataProtocol hash lookup table saved to {DATA_LUT_PATH}")


if __name__ == "__main__":
    main()
