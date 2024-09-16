from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from dataclasses_json import dataclass_json


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


class ServerTraceDest(Enum):
    FILE = "FILE"
    IN_MEM = "IN_MEM"


@dataclass
class DaemonServer:
    host: str
    user: str
    password: str
    port: Optional[Union[str, int]]
    ignoreUnauthorized: Optional[bool] = True
    ca: Optional[Union[str, bytes]] = None


@dataclass_json
@dataclass
class ServerResponse:
    id: str
    success: bool
    sql_rc: int
    sql_state: str
    error: Optional[str] = None


@dataclass_json
@dataclass
class ConnectionResult(ServerResponse):
    job: str  # type: ignore
    id: str = field(init=False)
    success: bool = field(init=False)
    sql_rc: int = field(init=False)
    sql_state: str = field(init=False)
    error: Optional[str] = field(default=None, init=False)


@dataclass_json
@dataclass
class VersionCheckResult(ServerResponse):
    build_date: str  # type: ignore
    version: str  # type: ignore
    id: str = field(init=False)
    success: bool = field(init=False)
    sql_rc: int = field(init=False)
    sql_state: str = field(init=False)
    error: Optional[str] = field(default=None, init=False)


@dataclass
class ColumnMetaData:
    display_size: int
    label: str
    name: str
    type: str


@dataclass
class QueryMetaData:
    column_count: int
    columns: List[ColumnMetaData]
    job: str


@dataclass
class QueryResult:
    metadata: QueryMetaData
    is_done: bool
    has_results: bool
    update_count: int
    data: List[Any]


@dataclass
class ExplainResults(QueryResult):
    vemetadata: QueryMetaData
    vedata: Any


@dataclass_json
@dataclass
class GetTraceDataResult(ServerResponse):
    tracedata: str  # type: ignore
    id: str = field(init=False)
    success: bool = field(init=False)
    sql_rc: int = field(init=False)
    sql_state: str = field(init=False)
    error: Optional[str] = field(default=None, init=False)


@dataclass
class JobLogEntry:
    MESSAGE_ID: str
    SEVERITY: str
    MESSAGE_TIMESTAMP: str
    FROM_LIBRARY: str
    FROM_PROGRAM: str
    MESSAGE_TYPE: str
    MESSAGE_TEXT: str
    MESSAGE_SECOND_LEVEL_TEXT: str


@dataclass_json
@dataclass
class CLCommandResult(ServerResponse):
    joblog: List[JobLogEntry]  # type: ignore
    id: str = field(init=False)
    success: bool = field(init=False)
    sql_rc: int = field(init=False)
    sql_state: str = field(init=False)
    error: Optional[str] = field(default=None, init=False)


@dataclass
class QueryOptions:
    isTerseResults: Optional[bool] = None
    isClCommand: Optional[bool] = None
    parameters: Optional[List[Any]] = None
    autoClose: Optional[bool] = None


@dataclass_json
@dataclass
class SetConfigResult(ServerResponse):
    tracedest: ServerTraceDest  # type: ignore
    tracelevel: ServerTraceLevel  # type: ignore
    id: str = field(init=False)
    success: bool = field(init=False)
    sql_rc: int = field(init=False)
    sql_state: str = field(init=False)
    error: Optional[str] = field(default=None, init=False)


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
