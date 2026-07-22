##! Add detailed docstring to summarize key categories of constants and how they are used/what do they control


# Constants used for source info keys
#! Must add appropriate comment to clarify the use of each & every constant
SYSTEM_COLUMN_PREFIX = "__"
DATAGRAM_PREFIX = "_"
SOURCE_INFO_PREFIX = "source_"

#! Indeed it's not clear what this was meant to be used for -- I'm imagining things that used to be part of the fucntion
#! data is now captured by the data function's own e.g. variation datagram
POD_ID_PREFIX = "pod_id_"  #!? dead: 0 uses outside this file (its property below is also unused). Wire or remove.
PF_VARIATION_PREFIX = "pf_var_" #! Needs comment to clarify what this is -- applies to everything
PF_EXECUTION_PREFIX = "pf_exec_" #! Very likely PF = packet_function which is actually an old naming for data function
DATA_CONTEXT_KEY = "context_key"
INPUT_DATA_HASH_COL = "input_data_hash" #! There is constant naming inconsistency -- generally remove "col" and have hash named after it's content
OUTPUT_DATA_HASH_COL = "output_data_hash"
NODE_CONTENT_HASH_COL = "node_content_hash"
DATA_RECORD_ID = "data_id"  #! Add more explanation as to what this is about
SYSTEM_TAG_PREFIX_NAME = "tag"
SYSTEM_TAG_SOURCE_ID_FIELD = "source_id"
SYSTEM_TAG_RECORD_ID_FIELD = "record_id"

#! This is indeed very likely no longer relevant -- corresponding but more refined versioning measure of pod occurs now through versioning of data function
#! concretely, noting version info of the OSS code, running config, etc

#! This variable is likely no longer relevant -- verify it's not used anywhere and just delete this constant
POD_VERSION = "pod_version"  #!? dead: intended pod-version system column never wired. Version lives only in the table path + pf_var_* observational cols (Area 2). Decide: wire as a real column or remove.

EXECUTION_ENGINE = "execution_engine"  #!? dead: intended executor-identity column never wired. Executor info only in pf_exec_* observational cols; never in pipeline/tag records (Area 2, X2).
POD_TIMESTAMP = "pod_ts" #! If this is still used, it's awkward as to why POD_TIMESTAMP is left alone

#! Must add explanation as to what is the semantic differentiation between the field and block separator with concrete examples
FIELD_SEPARATOR = ":"
BLOCK_SEPARATOR = "::"

#! If completed outdated/not used, we should consider fully dropping this
ENV_INFO = "env_info"  #!? dead: 0 uses. Env/provenance stamping never implemented (overlaps PLT-1950).
IS_EPHEMERAL_COL = "is_ephemeral" #! Give short description

#! The following are highly related and should be given its own subsection
PIPELINE_DB_SCHEMA_VERSION = "pdb_v1"
RESULT_DB_SCHEMA_VERSION = "rdb_v1"
TRACKING_DB_SCHEMA_VERSION = "tdb_v1"


#! Every constant should be further investigated to ensure that the use of particular prefix
#! makes sense and it actually happens through the use of proper prefix as provided by the class's property, rather
#! than skipping and obtaining information directly from the constants as found in this file
#! E.g., use self.BLOCK_SEPARATOR instead of the bare module-level constant/variable BLOCK_SEPARATOR
class SystemConstant:
    def __init__(self, global_prefix: str = ""):
        #!? global_prefix is never set non-empty anywhere — only `constants = SystemConstant()` exists.
        #!? Inert namespacing hook: all the f-string prefixing below is dead flexibility. Keep only if a
        #!? multi-tenant/namespaced column scheme is actually planned for v0.2; otherwise simplify.
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
    def INPUT_DATA_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{INPUT_DATA_HASH_COL}"

    @property
    def OUTPUT_DATA_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{OUTPUT_DATA_HASH_COL}"

    @property
    def DATA_RECORD_ID(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{DATA_RECORD_ID}"

    @property
    def NODE_CONTENT_HASH_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{NODE_CONTENT_HASH_COL}"

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

    @property
    def IS_EPHEMERAL_COL(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{IS_EPHEMERAL_COL}"


# create a singleton instance for use everywhere
#! We are going to stick to singleton design IN CASE there will ever be a future where we want to model closely
#! that cannot be covered by the use of fetch API per se
constants = SystemConstant()
