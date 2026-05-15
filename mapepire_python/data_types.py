# Protocol type definitions for the Mapepire WebSocket protocol.
#
# Generation strategy: hand-written (Option B)
# These types are manually maintained to match the canonical definitions in
# https://github.com/Mapepire-IBMi/mapepire-protocol. If the protocol changes,
# update this file by hand. A future improvement (Option A) would auto-generate
# these from the protocol's JSON Schema files using datamodel-codegen, which
# would prevent drift. See issue #96 for details.

import os
from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from dataclasses_json import dataclass_json

from mapepire_python.authentication.kerberosTokenProvider import KerberosTokenProvider


def dict_to_dataclass(data: Dict[str, Any], dataclass_type: Any) -> Any:
    field_names = {f.name for f in fields(dataclass_type)}
    filtered_data = {k: v for k, v in data.items() if k in field_names}
    return dataclass_type(**filtered_data)


class JobStatus(Enum):
    NotStarted = "notStarted"
    Ready = "ready"
    Busy = "busy"
    Ended = "ended"


class ExplainType(Enum):
    Run = 0
    DoNotRun = 1


class TransactionEndType(Enum):
    COMMIT = 0
    ROLLBACK = 1


class ServerTraceLevel(Enum):
    OFF = "OFF"
    ON = "ON"
    ERRORS = "ERRORS"
    DATASTREAM = "DATASTREAM"
    INPUT_AND_ERRORS = "INPUT_AND_ERRORS"


class ServerTraceDest(Enum):
    FILE = "FILE"
    IN_MEM = "IN_MEM"

class MessageType(Enum):
    CONNECT = "connect"
    SQL = "sql"
    PREPARE_SQL = "prepare_sql"
    PREPARE_SQL_EXECUTE = "prepare_sql_execute"
    EXECUTE = "execute"
    SQL_MORE = "sqlmore"
    SQL_CLOSE = "sqlclose"
    CL = "cl"
    PING = "ping"
    GET_DB_JOB = "getdbjob"
    GET_VERSION = "getversion"
    SET_CONFIG = "setconfig"
    GET_TRACE_DATA = "gettracedata"
    EXIT = "exit"

class ConnectionTechnique(Enum):
    TCP = "tcp"
    CLI = "cli"

class ParameterMode(Enum):
    IN = "IN"
    OUT = "OUT"
    INOUT = "INOUT"
    UNKNOWN = "UNKNOWN"
    


@dataclass
class DaemonServer:
    host: str
    user: str
    password: Union[str, KerberosTokenProvider]
    port: Optional[Union[str, int]]
    ignoreUnauthorized: Optional[bool] = False
    ca: Optional[Union[str, bytes]] = None

    def get_password(self) -> str:
        if isinstance(self.password, KerberosTokenProvider):
            return self.password.get_token()
        return self.password

    @classmethod
    def from_env(cls) -> "DaemonServer":
        """Create a DaemonServer from environment variables.

        Reads MAPEPIRE_HOST, MAPEPIRE_USER, MAPEPIRE_PASSWORD (required),
        MAPEPIRE_PORT (default 8076), and MAPEPIRE_CA_PATH (optional).
        """
        host = os.environ.get("MAPEPIRE_HOST")
        user = os.environ.get("MAPEPIRE_USER")
        password = os.environ.get("MAPEPIRE_PASSWORD")
        port = os.environ.get("MAPEPIRE_PORT", "8076")
        ca_path = os.environ.get("MAPEPIRE_CA_PATH")

        missing = [
            name
            for name, val in [
                ("MAPEPIRE_HOST", host),
                ("MAPEPIRE_USER", user),
                ("MAPEPIRE_PASSWORD", password),
            ]
            if not val
        ]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        assert host is not None
        assert user is not None
        assert password is not None

        ca: Optional[bytes] = None
        if ca_path:
            try:
                with open(ca_path, "rb") as f:
                    ca = f.read()
            except FileNotFoundError:
                raise ValueError(f"CA certificate file not found: {ca_path!r}")
            except PermissionError:
                raise ValueError(f"Cannot read CA certificate file (permission denied): {ca_path!r}")

        return cls(host=host, user=user, password=password, port=port, ca=ca)


@dataclass_json
@dataclass
class ServerResponse:
    id: str
    success: bool
    sql_rc: int = 0
    sql_state: str = ""
    error: Optional[str] = None
    execution_time: Optional[int] = None

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        return hasattr(self, key) if isinstance(key, str) else False


@dataclass_json
@dataclass
class ConnectionResult(ServerResponse):
    job: str = ""
    


@dataclass_json
@dataclass
class VersionCheckResult(ServerResponse):
    build_date: str = ""
    version: str  = ""


@dataclass_json
@dataclass
class PingResponse(ServerResponse):
    alive: Optional[bool] = None
    db_alive: Optional[bool] = None


@dataclass_json
@dataclass
class PrepareSqlResponse(ServerResponse):
    parameter_count: Optional[int] = None


@dataclass_json
@dataclass
class SqlMoreResponse(ServerResponse):
    data: List[Any] = field(default_factory=list)
    is_done: bool = field(default=False)


@dataclass_json
@dataclass
class SqlCloseResponse(ServerResponse):
    pass


@dataclass_json
@dataclass
class GetDbJobResponse(ServerResponse):
    job: str = field(default="")


@dataclass_json
@dataclass
class ExitResponse(ServerResponse):
    pass


@dataclass
class ColumnMetaData:
    display_size: int
    label: str
    name: str
    type: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    autoIncrement: Optional[bool] = None
    nullable: Optional[bool] = None
    readOnly: Optional[bool] = None
    writeable: Optional[bool] = None
    table: Optional[str] = None


@dataclass
class QueryMetaData:
    column_count: int = 0
    columns: List[ColumnMetaData] = field(default_factory=list)
    job: str = ""


@dataclass
class ParameterDetail:
    name: str
    type: str
    mode: ParameterMode
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class ParameterResult:
    index: int
    type: str
    precision: int
    scale: int
    name: str
    ccsid: Optional[int] = None
    value: Optional[Union[str, int, float, bool]] = None


@dataclass_json
@dataclass
class QueryResult:
    is_done: bool = False
    has_results: bool = False
    update_count: int = 0
    data: List[Any] = field(default_factory=list)
    metadata: Optional[QueryMetaData] = None
    parameter_count: Optional[int] = None
    output_parms: Optional[List[ParameterResult]] = None
    id: str = field(default="")
    success: bool = field(default=False)
    sql_rc: int = field(default=0)
    sql_state: str = field(default="")
    error: Optional[str] = field(default=None)
    execution_time: Optional[int] = field(default=None)

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        return hasattr(self, key) if isinstance(key, str) else False


@dataclass
class ExplainResults(QueryResult):
    vemetadata: Optional[QueryMetaData] = None
    vedata: Optional[Any] = None


@dataclass_json
@dataclass
class GetTraceDataResult(ServerResponse):
    tracedata: str = ""
    jtopentracedata: Optional[str] = None


@dataclass
class JobLogEntry:
    MESSAGE_ID: str
    SEVERITY: int
    MESSAGE_TIMESTAMP: str
    FROM_LIBRARY: str
    FROM_PROGRAM: str
    MESSAGE_TYPE: str
    MESSAGE_TEXT: str
    MESSAGE_SECOND_LEVEL_TEXT: str


@dataclass_json
@dataclass
class CLCommandResult(ServerResponse):
    joblog: List[JobLogEntry] = field(default_factory=list)


@dataclass
class QueryOptions:
    isTerseResults: Optional[bool] = None
    isClCommand: Optional[bool] = None
    parameters: Optional[Union[Sequence[Any], Mapping[Union[str, int], Any]]] = None
    autoClose: Optional[bool] = None


@dataclass_json
@dataclass
class SetConfigResult(ServerResponse):
    tracedest: ServerTraceDest = ServerTraceDest.FILE  # type: ignore
    tracelevel: ServerTraceLevel = ServerTraceLevel.OFF  # type: ignore
    jtopentracedest: Optional[ServerTraceDest] = None
    jtopentracelevel: Optional[ServerTraceLevel] = None


@dataclass
class BaseRequest:
    id: str
    type: str


@dataclass
class ConnectRequest(BaseRequest):
    technique: str = field(default=ConnectionTechnique.TCP.value)
    application: str = "Python Client"
    props: str = ""
    type: str = field(default=MessageType.CONNECT.value, init=False)


@dataclass
class SqlRequest(BaseRequest):
    sql: str = ""
    rows: int = 0
    terse: Optional[bool] = None
    parameters: Optional[Union[Sequence[Any], Mapping[Union[str, int], Any]]] = None
    type: str = field(default=MessageType.SQL.value, init=False)


@dataclass
class PrepareSqlRequest(BaseRequest):
    sql: str = ""
    type: str = field(default=MessageType.PREPARE_SQL.value, init=False)


@dataclass
class PrepareSqlExecuteRequest(BaseRequest):
    sql: str = ""
    rows: int = 0
    terse: Optional[bool] = None
    parameters: Optional[Union[Sequence[Any], Mapping[Union[str, int], Any]]] = None
    type: str = field(default=MessageType.PREPARE_SQL_EXECUTE.value, init=False)


@dataclass
class ExecuteRequest(BaseRequest):
    cont_id: str = ""
    rows: int = 0
    parameters: Optional[Union[Sequence[Any], Mapping[Union[str, int], Any]]] = None
    type: str = field(default=MessageType.EXECUTE.value, init=False)


@dataclass
class SqlMoreRequest(BaseRequest):
    cont_id: str = ""
    sql: str = ""
    rows: int = 0
    type: str = field(default=MessageType.SQL_MORE.value, init=False)


@dataclass
class SqlCloseRequest(BaseRequest):
    cont_id: str = ""
    type: str = field(default=MessageType.SQL_CLOSE.value, init=False)


@dataclass
class ClRequest(BaseRequest):
    cmd: str = ""
    terse: Optional[bool] = None
    type: str = field(default=MessageType.CL.value, init=False)


@dataclass
class PingRequest(BaseRequest):
    type: str = field(default=MessageType.PING.value, init=False)


@dataclass
class GetDbJobRequest(BaseRequest):
    type: str = field(default=MessageType.GET_DB_JOB.value, init=False)


@dataclass
class GetVersionRequest(BaseRequest):
    type: str = field(default=MessageType.GET_VERSION.value, init=False)


@dataclass
class SetConfigRequest(BaseRequest):
    tracedest: Optional[str] = None
    tracelevel: Optional[str] = None
    type: str = field(default=MessageType.SET_CONFIG.value, init=False)


@dataclass
class GetTraceDataRequest(BaseRequest):
    type: str = field(default=MessageType.GET_TRACE_DATA.value, init=False)


@dataclass
class ExitRequest(BaseRequest):
    type: str = field(default=MessageType.EXIT.value, init=False)


@dataclass_json
@dataclass
class UnparsableError(ServerResponse):
    pass


@dataclass_json
@dataclass
class IncompleteError(ServerResponse):
    pass


@dataclass_json
@dataclass
class UnknownError(ServerResponse):
    pass


@dataclass_json
@dataclass
class BadRequestError(ServerResponse):
    pass


@dataclass
class JDBCOptions:
    naming: Optional[str] = None
    date_format: Optional[str] = None
    date_separator: Optional[str] = None
    decimal_separator: Optional[str] = None
    time_format: Optional[str] = None
    time_separator: Optional[str] = None
    full_open: Optional[bool] = None
    access: Optional[str] = None
    autocommit_exception: Optional[bool] = None
    bidi_string_type: Optional[str] = None
    bidi_implicit_reordering: Optional[bool] = None
    bidi_numeric_ordering: Optional[bool] = None
    data_truncation: Optional[bool] = None
    driver: Optional[str] = None
    errors: Optional[str] = None
    extended_metadata: Optional[bool] = None
    hold_input_locators: Optional[bool] = None
    hold_statements: Optional[bool] = None
    ignore_warnings: Optional[str] = None
    keep_alive: Optional[bool] = None
    key_ring_name: Optional[str] = None
    key_ring_password: Optional[str] = None
    metadata_source: Optional[str] = None
    proxy_server: Optional[str] = None
    remarks: Optional[str] = None
    secondary_URL: Optional[str] = None
    secure: Optional[bool] = None
    server_trace: Optional[str] = None
    thread_used: Optional[bool] = None
    toolbox_trace: Optional[str] = None
    trace: Optional[bool] = None
    translate_binary: Optional[bool] = None
    translate_boolean: Optional[bool] = None
    libraries: Optional[List[str]] = None
    auto_commit: Optional[bool] = None
    concurrent_access_resolution: Optional[str] = None
    cursor_hold: Optional[bool] = None
    cursor_sensitivity: Optional[str] = None
    database_name: Optional[str] = None
    decfloat_rounding_mode: Optional[str] = None
    maximum_precision: Optional[str] = None
    maximum_scale: Optional[str] = None
    minimum_divide_scale: Optional[str] = None
    package_ccsid: Optional[str] = None
    transaction_isolation: Optional[str] = None
    translate_hex: Optional[str] = None
    true_autocommit: Optional[bool] = None
    XA_loosely_coupled_support: Optional[str] = None
    big_decimal: Optional[bool] = None
    block_criteria: Optional[str] = None
    block_size: Optional[str] = None
    data_compression: Optional[bool] = None
    extended_dynamic: Optional[bool] = None
    lazy_close: Optional[bool] = None
    lob_threshold: Optional[str] = None
    maximum_blocked_input_rows: Optional[str] = None
    package: Optional[str] = None
    package_add: Optional[bool] = None
    package_cache: Optional[bool] = None
    package_criteria: Optional[str] = None
    package_error: Optional[str] = None
    package_library: Optional[str] = None
    prefetch: Optional[bool] = None
    qaqqinilib: Optional[str] = None
    query_optimize_goal: Optional[str] = None
    query_timeout_mechanism: Optional[str] = None
    query_storage_limit: Optional[str] = None
    receive_buffer_size: Optional[str] = None
    send_buffer_size: Optional[str] = None
    variable_field_compression: Optional[bool] = None
    sort: Optional[str] = None
    sort_language: Optional[str] = None
    sort_table: Optional[str] = None
    sort_weight: Optional[str] = None
