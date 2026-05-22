from typing import TypeAlias

from .function_node import FunctionNode
from .operator_node import OperatorNode
from .source_node import SourceJobNode, SourceNode, SourceNodeBase

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode

__all__ = [
    "FunctionNode",
    "GraphNode",
    "OperatorNode",
    "SourceJobNode",
    "SourceNode",
    "SourceNodeBase",
]
