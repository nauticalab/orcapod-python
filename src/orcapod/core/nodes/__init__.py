from typing import TypeAlias

from .function_node import FunctionJobNode, FunctionNode, FunctionNodeBase
from .operator_node import OperatorNode
from .source_node import SourceJobNode, SourceNode, SourceNodeBase

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode
JobNode: TypeAlias = SourceJobNode | FunctionJobNode

__all__ = [
    "FunctionJobNode",
    "FunctionNode",
    "FunctionNodeBase",
    "GraphNode",
    "JobNode",
    "OperatorNode",
    "SourceJobNode",
    "SourceNode",
    "SourceNodeBase",
]
