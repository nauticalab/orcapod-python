"""Semantic hash handler for ``torch.Tensor`` (ITL-871).

Loaded on first use through the ``deferred_value_handlers`` entry of
``contexts/logical_types.json``, so importing Orcapod never imports torch.
"""

from __future__ import annotations

from typing import Any

from orcapod.logical_types.ignore_list import IgnoreListHandler
from orcapod.logical_types.torch_type import (
    OPTIONS,
    TORCH_TENSOR_HASH_LABEL,
    tensor_digest,
)


class TorchTensorHandler(IgnoreListHandler):
    """Hasher for ``torch.Tensor``: the value hash ``D`` under ``torch_tensor_v1``.

    The hash is strict by default: dtype, shape and every element bit always
    count, and so do ``requires_grad``, the memory layout (strides), the lazy
    conjugate and negative bits and the class (``torch.Tensor`` or
    ``torch.nn.Parameter``) unless the ignore list names them. Device and
    autograd history never count: storage does not restore them. The formula is
    in ``orcapod.logical_types.torch_type``. The preimage starts with the
    extension name, so a tensor never hashes like an ndarray with the same
    elements, also inside an untyped Python structure.
    """

    OPTIONS = OPTIONS
    HASH_LABEL = TORCH_TENSOR_HASH_LABEL

    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the value hash of a tensor, ignoring the named properties.

        Args:
            obj: A ``torch.Tensor`` or ``torch.nn.Parameter``.
            ignore: The property names to ignore; a subset of ``OPTIONS``.

        Returns:
            The 32-byte digest (``torch_type.tensor_digest``).

        Raises:
            TypeError: If ``obj`` is not a ``torch.Tensor``.
            UnsupportedValueError: If the tensor cannot be stored.
        """
        return tensor_digest(obj, ignore)
