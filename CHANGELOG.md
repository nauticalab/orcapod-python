# Changelog

## [Unreleased]

### Breaking Changes

#### `tag` → `key` rename (hard break)

All identifiers containing `tag`/`tags`/`Tag` have been renamed to
`key`/`keys`/`Key`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Tag` | `Key` |
| `TagProtocol` | `KeyProtocol` |
| `TagValue` | `KeyValue` |
| `DuplicateTagError` | `DuplicateKeyError` |
| `SelectTagColumns` | `SelectKeyColumns` |
| `DropTagColumns` | `DropKeyColumns` |
| `MapTags` | `MapKeys` |
| `system_tags()` | `system_keys()` |
| `map_tags()` | `map_keys()` |
| `select_tag_columns()` | `select_key_columns()` |
| `drop_tag_columns()` | `drop_key_columns()` |
| `sort_by_tags` | `sort_by_keys` |
| `SYSTEM_TAG_PREFIX` | `SYSTEM_KEY_PREFIX` |
| `SYSTEM_TAG_PREFIX_NAME` (`"tag"`) | `SYSTEM_KEY_PREFIX_NAME` (`"key"`) |
| `SYSTEM_TAG_SOURCE_ID_PREFIX` | `SYSTEM_KEY_SOURCE_ID_PREFIX` |
| `SYSTEM_TAG_RECORD_ID_PREFIX` | `SYSTEM_KEY_RECORD_ID_PREFIX` |
| `SYSTEM_TAG_SOURCE_ID_FIELD` | `SYSTEM_KEY_SOURCE_ID_FIELD` |
| `SYSTEM_TAG_RECORD_ID_FIELD` | `SYSTEM_KEY_RECORD_ID_FIELD` |
| `ColumnConfig(system_tags=...)` | `ColumnConfig(system_keys=...)` |
| Column prefix `_tag_` | `_key_` (e.g. `_tag_source_id` → `_key_source_id`) |
| Column prefix `_tag::` | `_key::` (e.g. `_tag::source:abc` → `_key::source:abc`) |
| `src/orcapod/core/datagrams/tag_data.py` | `key_data.py` |
| `test-objective/unit/test_tag.py` | `test_key.py` |

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to
`data`/`Data`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Packet` | `Data` |
| `PacketProtocol` | `DataProtocol` |
| `PacketFunction` | `DataFunction` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketLogger` | `DataLogger` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `iter_packets()` | `iter_data()` |
| `process_packet()` | `process_data()` |
| `async_process_packet()` | `async_process_data()` |
| `execute_packet()` | `execute_data()` |
| `map_packets()` | `map_data()` |
| `select_packet_columns()` | `select_data_columns()` |
| `drop_packet_columns()` | `drop_data_columns()` |
| `on_packet_start()` | `on_data_start()` |
| `on_packet_end()` | `on_data_end()` |
| `on_packet_crash()` | `on_data_crash()` |
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |
