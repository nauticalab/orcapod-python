# Packet Functions

Stateless computations that transform individual packets.

## PythonPacketFunction

::: orcapod.core.packet_function.PythonPacketFunction
    options:
      members:
        - direct_call
        - direct_async_call
        - call
        - async_call
        - input_packet_schema
        - output_packet_schema
        - canonical_function_name
        - is_active
        - set_active
        - identity_structure
        - pipeline_identity_structure

## PacketFunctionBase (Abstract Base)

::: orcapod.core.packet_function.PacketFunctionBase
    options:
      members:
        - call
        - async_call
        - direct_call
        - direct_async_call
        - executor
        - major_version
        - packet_function_type_id
        - canonical_function_name
        - output_packet_schema_hash
        - uri

## CachedPacketFunction

::: orcapod.core.packet_function.CachedPacketFunction
    options:
      members:
        - get_cached_output_for_packet
        - record_packet
        - get_all_cached_outputs
