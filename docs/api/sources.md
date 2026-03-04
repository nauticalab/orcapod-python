# Sources

Data source implementations for loading external data into orcapod streams.

## ArrowTableSource

::: orcapod.core.sources.arrow_table_source.ArrowTableSource
    options:
      members:
        - resolve_field
        - output_schema
        - keys
        - as_table
        - as_stream
        - iter_packets
        - content_hash
        - pipeline_hash
        - source_id
        - pipeline_identity_structure

## DictSource

::: orcapod.core.sources.dict_source.DictSource

## ListSource

::: orcapod.core.sources.list_source.ListSource

## DataFrameSource

::: orcapod.core.sources.data_frame_source.DataFrameSource

## DeltaTableSource

::: orcapod.core.sources.delta_table_source.DeltaTableSource

## CSVSource

::: orcapod.core.sources.csv_source.CSVSource

## DerivedSource

::: orcapod.core.sources.derived_source.DerivedSource

## RootSource (Abstract Base)

::: orcapod.core.sources.base.RootSource
    options:
      members:
        - resolve_field
        - pipeline_identity_structure
        - source_id

## PersistentSource

::: orcapod.core.sources.persistent_source.PersistentSource

## SourceRegistry

::: orcapod.core.sources.source_registry.SourceRegistry
