# Operator & Function Nodes

Database-backed execution nodes for persistent computation.

## FunctionNode

::: orcapod.core.function_pod.FunctionNode
    options:
      members:
        - iter_packets
        - as_table
        - output_schema
        - keys
        - clear_cache
        - content_hash
        - pipeline_hash
        - run

## PersistentFunctionNode

::: orcapod.core.function_pod.PersistentFunctionNode
    options:
      members:
        - iter_packets
        - as_table
        - output_schema
        - keys
        - run
        - process_packet
        - add_pipeline_record
        - get_all_records
        - pipeline_path
        - as_source

## OperatorNode

::: orcapod.core.operator_node.OperatorNode
    options:
      members:
        - iter_packets
        - as_table
        - output_schema
        - keys
        - run
        - clear_cache
        - content_hash
        - pipeline_hash

## PersistentOperatorNode

::: orcapod.core.operator_node.PersistentOperatorNode
    options:
      members:
        - run
        - get_all_records
        - as_source
        - cache_mode
        - pipeline_path
