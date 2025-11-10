# Constants used for source info keys
SYSTEM_COLUMN_PREFIX = "__"
DATAGRAM_PREFIX = "_"
SOURCE_INFO_PREFIX = "source_"
POD_ID_PREFIX = "pod_id_"
DATA_CONTEXT_KEY = "context_key"
INPUT_PACKET_HASH = "input_packet_hash"
PACKET_RECORD_ID = "packet_id"
SYSTEM_TAG_PREFIX = "tag"
POD_VERSION = "pod_version"
EXECUTION_ENGINE = "execution_engine"
POD_TIMESTAMP = "pod_ts"
FIELD_SEPARATOR = ":"
BLOCK_SEPARATOR = "::"
ENV_INFO = "env_info"


class SystemConstant:
    def __init__(self, global_prefix: str = ""):
        self._global_prefix = global_prefix

    @property
    def BLOCK_SEPARATOR(self) -> str:
        return BLOCK_SEPARATOR

    @property
    def FIELD_SEPARATOR(self) -> str:
        return FIELD_SEPARATOR

    @property
    def META_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}"

    @property
    def DATAGRAM_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}"

    @property
    def SOURCE_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SOURCE_INFO_PREFIX}"

    @property
    def CONTEXT_KEY(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{DATA_CONTEXT_KEY}"

    @property
    def POD_ID_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_ID_PREFIX}"

    @property
    def INPUT_PACKET_HASH(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{INPUT_PACKET_HASH}"

    @property
    def PACKET_RECORD_ID(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PACKET_RECORD_ID}"

    @property
    def SYSTEM_TAG_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SYSTEM_TAG_PREFIX}{self.BLOCK_SEPARATOR}"

    @property
    def POD_VERSION(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_VERSION}"

    @property
    def EXECUTION_ENGINE(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{EXECUTION_ENGINE}"

    @property
    def POD_TIMESTAMP(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_TIMESTAMP}"

    @property
    def ENV_INFO(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{ENV_INFO}"


constants = SystemConstant()
