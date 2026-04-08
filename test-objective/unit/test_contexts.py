"""Specification-derived tests for DataContext resolution and validation.

Tests based on the documented context management API.
"""

from __future__ import annotations

import pytest

from orcapod.contexts import (
    get_available_contexts,
    get_default_context,
    resolve_context,
)
from orcapod.contexts.core import ContextResolutionError, DataContext


class TestResolveContext:
    """Per the documented API, resolve_context handles None, str, and
    DataContext inputs."""

    def test_none_returns_default(self):
        ctx = resolve_context(None)
        assert isinstance(ctx, DataContext)

    def test_string_version_resolves(self):
        ctx = resolve_context("v0.1")
        assert isinstance(ctx, DataContext)
        assert "v0.1" in ctx.context_key

    def test_datacontext_passthrough(self):
        original = get_default_context()
        result = resolve_context(original)
        assert result is original

    def test_invalid_version_raises(self):
        with pytest.raises((ContextResolutionError, KeyError, ValueError)):
            resolve_context("v999.999")


class TestGetAvailableContexts:
    """Per the documented API, returns sorted list of version strings."""

    def test_returns_list(self):
        contexts = get_available_contexts()
        assert isinstance(contexts, list)
        assert len(contexts) > 0

    def test_includes_v01(self):
        contexts = get_available_contexts()
        assert "v0.1" in contexts


class TestDefaultContextComponents:
    """Per the design, the default context has type_converter, arrow_hasher,
    and semantic_hasher."""

    def test_has_type_converter(self):
        ctx = get_default_context()
        assert ctx.type_converter is not None

    def test_has_arrow_hasher(self):
        ctx = get_default_context()
        assert ctx.arrow_hasher is not None

    def test_has_semantic_hasher(self):
        ctx = get_default_context()
        assert ctx.semantic_hasher is not None

    def test_has_context_key(self):
        ctx = get_default_context()
        assert isinstance(ctx.context_key, str)
        assert len(ctx.context_key) > 0
