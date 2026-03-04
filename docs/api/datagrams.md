# Datagrams

Immutable data containers with lazy dict/Arrow conversion.

## Datagram

::: orcapod.core.datagrams.datagram.Datagram
    options:
      members:
        - keys
        - schema
        - as_dict
        - as_table
        - content_hash
        - select
        - drop
        - rename
        - update
        - with_columns
        - copy
        - get_meta_value
        - datagram_id

## Tag

::: orcapod.core.datagrams.tag_packet.Tag
    options:
      members:
        - keys
        - schema
        - as_dict
        - as_table
        - system_tags
        - as_datagram
        - copy

## Packet

::: orcapod.core.datagrams.tag_packet.Packet
    options:
      members:
        - keys
        - schema
        - as_dict
        - as_table
        - source_info
        - with_source_info
        - rename
        - as_datagram
        - copy
