# Nodes

Nodes are the elements of a compiled pipeline graph. The plain node classes describe the
graph of a `Pipeline` blueprint; the job node classes below add database-backed execution.

::: orcapod.core.nodes.FunctionNode

::: orcapod.core.nodes.OperatorNode

::: orcapod.core.nodes.SourceNode

## Job nodes

The database-backed nodes a `PipelineJob` builds; these are what `job.nodes[label]` returns.

::: orcapod.core.nodes.FunctionJobNode

::: orcapod.core.nodes.OperatorJobNode

::: orcapod.core.nodes.SourceJobNode
