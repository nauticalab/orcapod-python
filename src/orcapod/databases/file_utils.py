# # file_ops.py - Atomic file operations module

# import builtins
# import contextlib
# import inspect
# import logging
# import os
# from pathlib import Path

# from orcapod.types import PathLike, PathSet, DataLike
# from collections.abc import Collection, Callable


# logger = logging.getLogger(__name__)


# def atomic_write(file_path: PathLike, content: str) -> Path:
#     """
#     Atomically write content to a file.

#     This function writes content to a temporary file and then atomically
#     renames it to the target file path, ensuring that other processes never
#     see a partially-written file.

#     Args:
#         file_path: Target file path
#         content: Content to write

#     Returns:
#         Path object to the written file

#     Raises:
#         OSError: If the file cannot be written
#     """
#     file_path = Path(file_path)
#     temp_path = file_path.with_name(f"{file_path.name}.tmp{os.getpid()}")

#     # Ensure parent directory exists
#     file_path.parent.mkdir(parents=True, exist_ok=True)

#     try:
#         # Write content to a temporary file
#         with open(temp_path, "w") as f:
#             f.write(content)
#             f.flush()
#             os.fsync(f.fileno())  # Force flush to disk

#         # Atomic rename
#         os.rename(temp_path, file_path)
#         return file_path
#     except Exception as e:
#         logger.error(f"Error writing file {file_path}: {str(e)}")
#         raise
#     finally:
#         # Clean up the temporary file if it exists
#         if temp_path.exists():
#             temp_path.unlink(missing_ok=True)


# def atomic_write_bytes(file_path: PathLike, content: bytes) -> Path:
#     """
#     Atomically write binary content to a file.

#     This function writes binary content to a temporary file and then atomically
#     renames it to the target file path.

#     Args:
#         file_path: Target file path
#         content: Binary content to write

#     Returns:
#         Path object to the written file

#     Raises:
#         OSError: If the file cannot be written
#     """
#     file_path = Path(file_path)
#     temp_path = file_path.with_name(f"{file_path.name}.tmp{os.getpid()}")

#     # Ensure parent directory exists
#     file_path.parent.mkdir(parents=True, exist_ok=True)

#     try:
#         # Write content to a temporary file
#         with open(temp_path, "wb") as f:
#             f.write(content)
#             f.flush()
#             os.fsync(f.fileno())  # Force flush to disk

#         # Atomic rename
#         os.rename(temp_path, file_path)
#         return file_path
#     except Exception as e:
#         logger.error(f"Error writing file {file_path}: {str(e)}")
#         raise
#     finally:
#         # Clean up the temporary file if it exists
#         if temp_path.exists():
#             temp_path.unlink(missing_ok=True)


# def atomic_copy(source_path: PathLike, dest_path: PathLike) -> Path:
#     """
#     Atomically copy a file.

#     This function copies a file to a temporary location and then atomically
#     renames it to the target path, ensuring that other processes never
#     see a partially-copied file.

#     Args:
#         source_path: Source file path
#         dest_path: Destination file path

#     Returns:
#         Path object to the copied file

#     Raises:
#         OSError: If the file cannot be copied
#         FileNotFoundError: If the source file does not exist
#     """
#     import shutil

#     source_path = Path(source_path)
#     dest_path = Path(dest_path)
#     temp_path = dest_path.with_name(f"{dest_path.name}.tmp{os.getpid()}")

#     # Check if source exists
#     if not source_path.exists():
#         raise FileNotFoundError(f"Source file does not exist: {source_path}")

#     # Ensure parent directory exists
#     dest_path.parent.mkdir(parents=True, exist_ok=True)

#     try:
#         # Copy to temporary file
#         shutil.copy2(source_path, temp_path)

#         # Ensure the data is written to disk
#         with open(temp_path, "a") as f:
#             os.fsync(f.fileno())

#         # Atomic rename
#         os.rename(temp_path, dest_path)
#         return dest_path
#     except Exception as e:
#         logger.error(f"Error copying file from {source_path} to {dest_path}: {str(e)}")
#         raise
#     finally:
#         # Clean up the temporary file if it exists
#         if temp_path.exists():
#             temp_path.unlink(missing_ok=True)


# def atomic_append(file_path: PathLike, content: str) -> Path:
#     """
#     Atomically append content to a file.

#     This function reads the existing content, appends the new content,
#     and then atomically writes the result back to the file.

#     Args:
#         file_path: Target file path
#         content: Content to append

#     Returns:
#         Path object to the appended file

#     Raises:
#         OSError: If the file cannot be written
#     """
#     file_path = Path(file_path)

#     # Read existing content if file exists
#     existing_content = ""
#     if file_path.exists():
#         try:
#             with open(file_path, "r") as f:
#                 existing_content = f.read()
#         except Exception as e:
#             logger.error(f"Error reading file {file_path} for append: {str(e)}")
#             raise

#     # Write the combined content atomically
#     return atomic_write(file_path, existing_content + content)


# def atomic_replace(
#     file_path: PathLike, pattern: str, replacement: str, count: int = -1
# ) -> tuple[Path, int]:
#     """
#     Atomically replace text in a file.

#     This function reads the existing content, performs the replacement,
#     and then atomically writes the result back to the file.

#     Args:
#         file_path: Target file path
#         pattern: Pattern to replace
#         replacement: Replacement text
#         count: Maximum number of replacements (default: unlimited)

#     Returns:
#         Tuple of (Path object to the file, number of replacements made)

#     Raises:
#         OSError: If the file cannot be read or written
#         FileNotFoundError: If the file does not exist
#     """
#     file_path = Path(file_path)

#     # Check if file exists
#     if not file_path.exists():
#         raise FileNotFoundError(f"File does not exist: {file_path}")

#     # Read existing content
#     try:
#         with open(file_path, "r") as f:
#             existing_content = f.read()
#     except Exception as e:
#         logger.error(f"Error reading file {file_path} for replacement: {str(e)}")
#         raise

#     # Perform replacement
#     new_content, num_replacements = existing_content, 0
#     if count == -1:
#         # Replace all occurrences
#         new_content = existing_content.replace(pattern, replacement)
#         num_replacements = existing_content.count(pattern)
#     else:
#         # Replace only up to count occurrences
#         new_content = ""
#         remaining = existing_content
#         for _ in range(count):
#             if pattern not in remaining:
#                 break
#             pos = remaining.find(pattern)
#             new_content += remaining[:pos] + replacement
#             remaining = remaining[pos + len(pattern) :]
#             num_replacements += 1
#         new_content += remaining

#     # Write the new content atomically
#     return atomic_write(file_path, new_content), num_replacements


# def is_file_locked(file_path: PathLike) -> bool:
#     """
#     Check if a file is locked.

#     This function attempts to open the file for writing in non-blocking mode
#     and checks if it fails with a "resource temporarily unavailable" error.

#     Args:
#         file_path: File path to check

#     Returns:
#         True if the file is locked, False otherwise
#     """
#     import errno
#     import fcntl

#     file_path = Path(file_path)

#     # If file doesn't exist, it's not locked
#     if not file_path.exists():
#         return False

#     try:
#         # Try to open the file and get an exclusive lock in non-blocking mode
#         with open(file_path, "r+") as f:
#             fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
#             # If we get here, the file is not locked
#             fcntl.flock(f, fcntl.LOCK_UN)
#             return False
#     except IOError as e:
#         if e.errno == errno.EAGAIN:
#             # Resource temporarily unavailable = the file is locked
#             return True
#         # Some other error - assume not locked
#         return False
#     except Exception:
#         # Any other exception - assume not locked
#         return False


# @contextlib.contextmanager
# def redirect_open(
#     mapping: dict[str, str] | Callable[[str], str | None],
# ):
#     """
#     Context manager to intercept file opening operations.

#     Args:
#         mapping: Either a dictionary mapping original paths to their replacements,
#                 or a function that takes a path string and returns a replacement path
#                 (or None to indicate the file should not be opened).

#     Raises:
#         FileNotFoundError: If using a dictionary and the path is not found in it.
#     """
#     # Track all places that might store an open() function
#     places_to_patch = []

#     # 1. Standard builtins.open
#     original_builtin_open = builtins.open
#     places_to_patch.append((builtins, "open", original_builtin_open))

#     # 2. __builtins__ (could be different in some contexts, especially IPython)
#     if isinstance(__builtins__, dict) and "open" in __builtins__:
#         places_to_patch.append((__builtins__, "open", __builtins__["open"]))

#     # 3. Current module's globals (for the calling namespace)
#     current_frame = inspect.currentframe()
#     if current_frame is not None:
#         caller_globals = current_frame.f_back.f_globals if current_frame.f_back else {}
#         if "open" in caller_globals:
#             places_to_patch.append((caller_globals, "open", caller_globals["open"]))

#     # 4. Check for IPython user namespace
#     try:
#         import IPython

#         ip = IPython.get_ipython()  # type: ignore
#         if ip and "open" in ip.user_ns:
#             places_to_patch.append((ip.user_ns, "open", ip.user_ns["open"]))
#     except (ImportError, AttributeError):
#         pass

#     def patched_open(file, *args, **kwargs):
#         # Convert PathLike objects to string if needed
#         if hasattr(file, "__fspath__"):
#             file_path = os.fspath(file)
#         else:
#             file_path = str(file)

#         if isinstance(mapping, dict):
#             if file_path in mapping:
#                 redirected_path = mapping[file_path]
#                 print(f"Redirecting '{file_path}' to '{redirected_path}'")
#                 return original_builtin_open(redirected_path, *args, **kwargs)
#             else:
#                 raise FileNotFoundError(
#                     f"Path '{file_path}' not found in redirection mapping"
#                 )
#         else:  # mapping is a function
#             redirected_path = mapping(file_path)
#             if redirected_path is not None:
#                 print(f"Redirecting '{file_path}' to '{redirected_path}'")
#                 return original_builtin_open(redirected_path, *args, **kwargs)
#             else:
#                 raise FileNotFoundError(f"Path '{file_path}' could not be redirected")

#     # Apply the patch to all places
#     for obj, attr, _ in places_to_patch:
#         if isinstance(obj, dict):
#             obj[attr] = patched_open
#         else:
#             setattr(obj, attr, patched_open)

#     try:
#         yield
#     finally:
#         # Restore all original functions
#         for obj, attr, original in places_to_patch:
#             if isinstance(obj, dict):
#                 obj[attr] = original
#             else:
#                 setattr(obj, attr, original)


# def virtual_mount(
#     data: DataLike,
# ) -> tuple[DataLike, dict[str, str], dict[str, str]]:
#     """
#     Visit all pathset within the data, and convert them to alternative path
#     representation. By default, full path is mapped to the file name. If two or
#     more paths have the same file name, the second one is suffixed with "_1", the
#     third one with "_2", etc. This is useful for creating a virtual mount point
#     for a set of files, where the original paths are not important, but the file
#     names can be used to identify the files.
#     """
#     forward_lut = {}  # mapping from original path to new path
#     reverse_lut = {}  # mapping from new path to original path
#     new_data = {}

#     for key, value in data.items():
#         new_data[key] = convert_pathset(value, forward_lut, reverse_lut)  # type: ignore

#     return new_data, forward_lut, reverse_lut


# # TODO: re-assess the structure of PathSet and consider making it recursive
# def convert_pathset(pathset: PathSet, forward_lut, reverse_lut) -> PathSet:
#     """
#     Convert a pathset to a new pathset. forward_lut and reverse_lut are updated
#     with the new paths. The new paths are created by replacing the original paths
#     with the new paths in the forward_lut. The reverse_lut is updated with the
#     original paths. If name already exists, a suffix is added to the new name to avoid
#     collisions.
#     """
#     if isinstance(pathset, (str, bytes)):
#         new_name = Path(pathset).name
#         if new_name in reverse_lut:
#             # if the name already exists, add a suffix
#             i = 1
#             while f"{new_name}_{i}" in reverse_lut:
#                 i += 1
#             new_name = f"{new_name}_{i}"
#         forward_lut[pathset] = new_name
#         reverse_lut[new_name] = pathset
#         return new_name
#     elif isinstance(pathset, Collection):
#         return [convert_pathset(p, forward_lut, reverse_lut) for p in pathset]  # type: ignore
#     else:
#         raise ValueError(
#             f"Unsupported pathset type: {type(pathset)}. Expected str, bytes, or Collection."
#         )


# class WrappedPath:
#     def __init__(self, path, name=None):
#         self.path = Path(path)
#         if name is None:
#             name = self.path.name
#         self.name = name

#     def __fspath__(self) -> str | bytes:
#         return self.path.__fspath__()

#     def __str__(self) -> str:
#         return self.name

#     def __repr__(self) -> str:
#         return f"WrappedPath({self.path}): {self.name}"
