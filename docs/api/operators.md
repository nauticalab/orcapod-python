# Operators

Structural transformers that reshape streams without synthesizing new values.

## Join

::: orcapod.core.operators.join.Join

## MergeJoin

::: orcapod.core.operators.merge_join.MergeJoin

## SemiJoin

::: orcapod.core.operators.semijoin.SemiJoin

## Batch

::: orcapod.core.operators.batch.Batch

## SelectTagColumns

::: orcapod.core.operators.column_selection.SelectTagColumns

## SelectPacketColumns

::: orcapod.core.operators.column_selection.SelectPacketColumns

## DropTagColumns

::: orcapod.core.operators.column_selection.DropTagColumns

## DropPacketColumns

::: orcapod.core.operators.column_selection.DropPacketColumns

## MapTags

::: orcapod.core.operators.mappers.MapTags

## MapPackets

::: orcapod.core.operators.mappers.MapPackets

## PolarsFilter

::: orcapod.core.operators.filters.PolarsFilter

## Base Classes

### UnaryOperator

::: orcapod.core.operators.base.UnaryOperator
    options:
      members:
        - validate_unary_input
        - unary_static_process
        - unary_output_schema

### BinaryOperator

::: orcapod.core.operators.base.BinaryOperator
    options:
      members:
        - validate_binary_inputs
        - binary_static_process
        - binary_output_schema
        - is_commutative

### NonZeroInputOperator

::: orcapod.core.operators.base.NonZeroInputOperator
    options:
      members:
        - validate_nonzero_inputs

### StaticOutputPod

::: orcapod.core.static_output_pod.StaticOutputPod
    options:
      members:
        - process
        - validate_inputs
        - argument_symmetry
        - output_schema
        - static_process
        - async_execute
