from typing import TypeAlias

from .function_node import FunctionJobNode, FunctionNode, FunctionNodeBase
from .operator_node import OperatorJobNode, OperatorNode, OperatorNodeBase
from .source_node import SourceJobNode, SourceNode, SourceNodeBase

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode
JobNode: TypeAlias = SourceJobNode | FunctionJobNode | OperatorJobNode

__all__ = [
    "FunctionJobNode",
    "FunctionNode",
    "FunctionNodeBase",
    "GraphNode",
    "JobNode",
    "OperatorJobNode",
    "OperatorNode",
    "OperatorNodeBase",
    "SourceJobNode",
    "SourceNode",
    "SourceNodeBase",
]
