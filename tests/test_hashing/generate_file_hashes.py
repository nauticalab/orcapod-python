#!/usr/bin/env python
"""
Generate sample files with random content and record their hashes.

This script creates sample text and binary files with random content,
then computes and records their hashes in a JSON lookup table file.
"""

import json
import random
import string
import sys
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path to import orcapod
sys.path.append(str(Path(__file__).parent.parent.parent))
from orcapod.hashing import hash_file

# Create directories if they don't exist
HASH_SAMPLES_DIR = Path(__file__).parent / "hash_samples"
HASH_SAMPLES_DIR.mkdir(exist_ok=True)

# Create file_samples subdirectory for sample files
SAMPLE_FILES_DIR = HASH_SAMPLES_DIR / "file_samples"
SAMPLE_FILES_DIR.mkdir(exist_ok=True)

# Path for the hash lookup table
HASH_LUT_PATH = HASH_SAMPLES_DIR / "file_hash_lut.json"


def generate_random_text(size_kb):
    """Generate random text content of approximate size in KB."""
    # Each character is roughly 1 byte, so size_kb * 1024 characters
    chars = string.ascii_letters + string.digits + string.punctuation + " \n\t"
    return "".join(random.choice(chars) for _ in range(size_kb * 1024))


def generate_random_binary(size_kb):
    """Generate random binary content of approximate size in KB."""
    return bytes(random.getrandbits(8) for _ in range(size_kb * 1024))


def create_sample_files():
    """Create sample files with random content."""
    files_info = []

    # Generate text files of various sizes
    text_sizes = [1, 5, 10, 50, 100]  # sizes in KB
    for size in text_sizes:
        filename = f"sample_text_{size}kb.txt"
        filepath = SAMPLE_FILES_DIR / filename
        rel_filepath = Path("hash_samples/file_samples") / filename

        # Generate and write random text content
        content = generate_random_text(size)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        # Compute the hash
        file_hash = hash_file(filepath)

        files_info.append(
            {
                "file": str(rel_filepath),
                "hash": file_hash.to_hex(),
                "size_kb": size,
                "type": "text",
            }
        )
        print(f"Created text file: {filename} ({size} KB), Hash: {file_hash.to_hex()}")

    # Generate binary files of various sizes
    binary_sizes = [1, 5, 10, 50, 100]  # sizes in KB
    for size in binary_sizes:
        filename = f"sample_binary_{size}kb.bin"
        filepath = SAMPLE_FILES_DIR / filename
        rel_filepath = Path("hash_samples/file_samples") / filename

        # Generate and write random binary content
        content = generate_random_binary(size)
        with open(filepath, "wb") as f:
            f.write(content)

        # Compute the hash
        file_hash = hash_file(filepath)

        files_info.append(
            {
                "file": str(rel_filepath),
                "hash": file_hash.to_hex(),
                "size_kb": size,
                "type": "binary",
            }
        )
        print(
            f"Created binary file: {filename} ({size} KB), Hash: {file_hash.to_hex()}"
        )

    # Create a structured file (JSON)
    json_filename = "sample_structured.json"
    json_filepath = SAMPLE_FILES_DIR / json_filename
    rel_filepath = Path("hash_samples/file_samples") / json_filename
    json_content = {
        "name": "Example Data",
        "created": datetime.now().isoformat(),
        "values": [random.random() for _ in range(100)],
        "metadata": {
            "description": "Sample data for hash testing",
            "version": "1.0",
            "tags": ["test", "hash", "sample"],
        },
    }

    with open(json_filepath, "w", encoding="utf-8") as f:
        json.dump(json_content, f, indent=2)

    # Compute the hash
    json_hash = hash_file(json_filepath)

    files_info.append(
        {"file": str(rel_filepath), "hash": json_hash.to_hex(), "type": "json"}
    )
    print(f"Created JSON file: {json_filename}, Hash: {json_hash.to_hex()}")

    return files_info


def main():
    """Generate sample files and save their hash information."""
    print(f"Generating sample files in {SAMPLE_FILES_DIR}")
    files_info = create_sample_files()

    # Convert to the required format for the hash LUT
    hash_lut = {}
    for info in files_info:
        filename = Path(info["file"]).name
        hash_lut[filename] = {"file": info["file"], "hash": info["hash"]}

    # Save to the lookup table file
    with open(HASH_LUT_PATH, "w", encoding="utf-8") as f:
        json.dump(hash_lut, f, indent=2)

    print(f"\nGenerated {len(files_info)} sample files")
    print(f"Hash lookup table saved to {HASH_LUT_PATH}")


if __name__ == "__main__":
    main()
