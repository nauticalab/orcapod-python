# Sources

Source classes provide the entry point for external data into Orcapod pipelines.
All sources convert their input to a PyArrow Table and use `SourceStreamBuilder` for
enrichment (provenance columns, system tags, hashing).

::: orcapod.core.sources.ArrowTableSource

::: orcapod.core.sources.DictSource

::: orcapod.core.sources.ListSource

::: orcapod.core.sources.DataFrameSource

::: orcapod.core.sources.CSVSource

::: orcapod.core.sources.DeltaTableSource

::: orcapod.core.sources.DerivedSource

::: orcapod.core.sources.CachedSource

::: orcapod.core.sources.SourceProxy

::: orcapod.core.sources.SourceRegistry

::: orcapod.core.sources.RootSource
