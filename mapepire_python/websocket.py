import base64
import inspect
import ssl
from functools import wraps
from typing import Callable, TypeVar

from pep249 import OperationalError
from websockets import WebSocketException

from mapepire_python.data_types import DaemonServer
from mapepire_python.ssl import get_certificate

ReturnType = TypeVar("ReturnType")


class BaseConnection:
    def __init__(self, db2_server: DaemonServer) -> None:
        self.uri = f"wss://{db2_server.host}:{db2_server.port}/db/"
        self.headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{db2_server.user}:{db2_server.get_password()}".encode()).decode(
                "ascii"
            )
        }
        self.db2_server = db2_server

    def _create_ssl_context(self, db2_server: DaemonServer):
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if db2_server.ignoreUnauthorized:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            if db2_server.ca:
                ssl_context.load_verify_locations(cadata=db2_server.ca)
            else:
                cert = get_certificate(db2_server)
                if cert:
                    ssl_context.load_verify_locations(cadata=cert)
                else:
                    raise ssl.SSLError("Failed to retrieve server certificate")

                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED
        return ssl_context


CONNECTION_ERRORS = (OSError, WebSocketException)


def _connection_error(error: Exception) -> OperationalError:
    return OperationalError(f"Connection to Mapepire server failed: {error}")


def handle_ws_errors(function: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    """Wrap a sync or async function, raising `OperationalError` from socket and websocket errors."""

    if inspect.iscoroutinefunction(function):

        @wraps(function)
        async def async_wrapper(*args, **kwargs):
            try:
                return await function(*args, **kwargs)
            except CONNECTION_ERRORS as err:
                raise _connection_error(err) from err

        return async_wrapper  # type: ignore[return-value]

    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except CONNECTION_ERRORS as err:
            raise _connection_error(err) from err

    return wrapper
