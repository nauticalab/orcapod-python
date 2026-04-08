from __future__ import annotations

import inspect
from typing import Any
from orcapod.utils.lazy_module import LazyModule

git = LazyModule("git")


def is_git_repo(path):
    """Check if path is a git repository"""
    try:
        git.Repo(path, search_parent_directories=True)
        return True
    except git.exc.InvalidGitRepositoryError:
        return False


def get_root_git_dir(path):
    """Get the root .git directory for a path"""
    try:
        repo = git.Repo(path, search_parent_directories=True)
        return repo.git.rev_parse("--show-toplevel")
    except git.exc.InvalidGitRepositoryError:
        return None


def get_git_info(path):
    """Get comprehensive git information for a path"""
    try:
        # This will search parent directories for .git
        repo = git.Repo(path, search_parent_directories=True)

        # Get current commit hash
        commit_hash = repo.head.commit.hexsha
        short_hash = repo.head.commit.hexsha[:7]

        # Check if repository is dirty (staged or unstaged changes only;
        # untracked_files=False avoids a slow git ls-files subprocess call)
        is_dirty = repo.is_dirty(untracked_files=False)

        # Get current branch name
        try:
            branch_name = repo.active_branch.name
        except TypeError:
            # Handle detached HEAD state
            branch_name = "HEAD (detached)"

        # Check for untracked files (separate from is_dirty which only
        # covers staged/unstaged changes to tracked files).
        has_untracked_files = len(repo.untracked_files) > 0

        return {
            "is_repo": True,
            "commit_hash": commit_hash,
            "short_hash": short_hash,
            "is_dirty": is_dirty,
            "has_untracked_files": has_untracked_files,
            "branch": branch_name,
            "repo_root": repo.working_dir,
        }

    except:  # TODO: specify exception
        return None


def get_git_info_for_python_object(python_object, try_cwd:bool=False) -> dict[str, Any] | None:
    """Get git info for the file where the python object is defined"""
    try:
        file_path = inspect.getfile(python_object)
        git_info = get_git_info(file_path)
        git_source = "function"
        if git_info is None:
                # If the file isn't in a git repo, optionally try the current working directory
            if try_cwd:
                git_info = get_git_info(".")

            if git_info is None:
                return None
        
            git_source = "cwd"
            
        env_info = {}
        env_info["git_commit_hash"] = git_info.get("commit_hash")
        env_info["git_repo_status"] = "dirty" if git_info.get("is_dirty") else "clean"
        env_info["has_untracked_files"] = (
            "true" if git_info.get("has_untracked_files") else "false"
        )
        env_info["git_source"] = git_source
        return env_info
    except TypeError:
        return None
