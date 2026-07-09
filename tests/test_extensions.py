"""Tests for OrcapodExtension protocol and register_extension() (ITL-473)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orcapod.extensions import OrcapodExtension, register_extension


class _MockExtension:
    """Minimal concrete implementation of OrcapodExtension for testing."""

    name = "mock"

    def register(self, context) -> None:
        pass


def test_mock_extension_satisfies_protocol():
    """A class with name and register() satisfies OrcapodExtension at runtime."""
    ext = _MockExtension()
    assert isinstance(ext, OrcapodExtension)


def test_register_extension_passes_explicit_context():
    """register_extension passes an explicit context directly to extension.register."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    register_extension(ext, context=mock_context)

    ext.register.assert_called_once_with(mock_context)


def test_register_extension_resolves_default_context_when_none():
    """register_extension calls get_default_context() and passes result when context=None."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    with patch("orcapod.extensions.get_default_context", return_value=mock_context) as mock_get:
        register_extension(ext, context=None)

    mock_get.assert_called_once()
    ext.register.assert_called_once_with(mock_context)


def test_register_extension_context_defaults_to_none():
    """register_extension resolves default context when called without context kwarg."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    with patch("orcapod.extensions.get_default_context", return_value=mock_context):
        register_extension(ext)  # no context arg

    ext.register.assert_called_once_with(mock_context)


def test_register_extension_does_not_call_get_default_context_when_context_provided():
    """register_extension never calls get_default_context when a context is supplied."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    with patch("orcapod.extensions.get_default_context") as mock_get:
        register_extension(ext, context=mock_context)

    mock_get.assert_not_called()
