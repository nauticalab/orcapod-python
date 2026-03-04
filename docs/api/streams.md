# Streams

Immutable stream implementations for carrying (Tag, Packet) pairs.

## ArrowTableStream

::: orcapod.core.streams.arrow_table_stream.ArrowTableStream
    options:
      members:
        - output_schema
        - keys
        - iter_packets
        - as_table
        - content_hash
        - pipeline_hash
        - clear_cache

## StreamBase (Abstract Base)

::: orcapod.core.streams.base.StreamBase
    options:
      members:
        - output_schema
        - keys
        - iter_packets
        - as_table
        - content_hash
        - pipeline_hash
        - producer
        - upstreams
        - last_modified
        - is_stale
