# Constants used for source info keys
SYSTEM_COLUMN_PREFIX = "__"
DATAGRAM_PREFIX = "_"
SOURCE_INFO_PREFIX = "source_"
POD_ID_PREFIX = "pod_id_"
PF_VARIATION_PREFIX = "pf_var_"
PF_EXECUTION_PREFIX = "pf_exec_"
DATA_CONTEXT_KEY = "context_key"
INPUT_PACKET_HASH_COL = "input_packet_hash"
PACKET_RECORD_ID = "packet_id"
SYSTEM_TAG_PREFIX_NAME = "tag"
SYSTEM_TAG_SOURCE_ID_FIELD = "source_id"
SYSTEM_TAG_RECORD_ID_FIELD = "record_id"
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
    def PF_VARIATION_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PF_VARIATION_PREFIX}"

    @property
    def PF_EXECUTION_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PF_EXECUTION_PREFIX}"

    @property
    def INPUT_PACKET_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{INPUT_PACKET_HASH_COL}"

    @property
    def PACKET_RECORD_ID(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PACKET_RECORD_ID}"

    @property
    def SYSTEM_TAG_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SYSTEM_TAG_PREFIX_NAME}_"

    @property
    def SYSTEM_TAG_SOURCE_ID_PREFIX(self) -> str:
        return f"{self.SYSTEM_TAG_PREFIX}{SYSTEM_TAG_SOURCE_ID_FIELD}"

    @property
    def SYSTEM_TAG_RECORD_ID_PREFIX(self) -> str:
        return f"{self.SYSTEM_TAG_PREFIX}{SYSTEM_TAG_RECORD_ID_FIELD}"

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
