import base64
import ssl
from functools import wraps
from typing import Callable, TypeVar

from websockets import InvalidHandshake, InvalidURI

from mapepire_python.data_types import DaemonServer

ReturnType = TypeVar("ReturnType")


class BaseConnection:
    def __init__(self, db2_server: DaemonServer) -> None:
        self.uri = f"wss://{db2_server.host}:{db2_server.port}/db/"
        self.headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{db2_server.user}:{db2_server.password}".encode()).decode("ascii")
        }
        self.db2_server = db2_server

    def _create_ssl_context(self, db2_server: DaemonServer):
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if db2_server.ignoreUnauthorized:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        elif db2_server.ca:
            ssl_context.load_verify_locations(cadata=db2_server.ca)
        return ssl_context


def _parse_ws_error(error: RuntimeError, connection: BaseConnection):
    if not isinstance(error, RuntimeError):
        return error
    if isinstance(error, InvalidURI):
        raise InvalidURI(f"The provided URI is not a valid WebSocket URI.")
    elif isinstance(error, OSError):
        raise OSError(f"The TCP connection failed to connect to Mapepire server")
    elif isinstance(error, InvalidHandshake):
        raise InvalidHandshake("The opening handshake failed.")
    elif isinstance(error, TimeoutError):
        raise TimeoutError("The opening handshake timed out.")
    else:
        return error


def handle_ws_errors(function: Callable[..., ReturnType]) -> Callable[..., ReturnType]:

    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except RuntimeError as err:
            raise _parse_ws_error(err) from err

    return wrapper
