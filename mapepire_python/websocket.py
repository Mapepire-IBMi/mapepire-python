import base64
import ssl
from functools import wraps
from typing import Any, Callable, TypeVar

from websockets import ConcurrencyError, ConnectionClosed, InvalidHandshake, InvalidURI

from mapepire_python.data_types import DaemonServer
from mapepire_python.ssl import get_certificate
from mapepire_python.ssl_cache import cache_ssl_context, get_cached_ssl_context

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
        # Check cache first
        cached_context = get_cached_ssl_context(db2_server)
        if cached_context is not None:
            return cached_context

        # Create new SSL context
        if db2_server.ignoreUnauthorized:
            ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            if db2_server.ca:
                # Create a context that only trusts the provided CA
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                # Don't load any default CAs, only trust the provided one
                ssl_context.load_verify_locations(cadata=db2_server.ca)
                # Set minimum protocol version for security
                ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
            else:
                ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED

                cert = get_certificate(db2_server)
                if cert:
                    ssl_context.load_verify_locations(cadata=cert)
                else:
                    raise ssl.SSLError("Failed to retrieve server certificate")

        # Cache the new context
        cache_ssl_context(db2_server, ssl_context)
        return ssl_context


def _parse_ws_error(error: Exception, driver: Any = None):
    to_str = str(driver)

    if isinstance(error, ssl.SSLError):
        # Re-raise SSL errors as-is so they can be caught by tests
        raise error
    elif isinstance(error, InvalidURI):
        raise InvalidURI(f"The provided URI is not a valid WebSocket URI: {to_str}")
    elif isinstance(error, OSError):
        raise OSError(f"The TCP connection failed to connect to Mapepire server: {to_str}")
    elif isinstance(error, InvalidHandshake):
        raise InvalidHandshake("The opening handshake failed.")
    elif isinstance(error, TimeoutError):
        raise TimeoutError("The opening handshake timed out.")
    elif isinstance(error, ConnectionClosed):
        raise ConnectionClosed("The Conection was closed.")
    elif isinstance(error, ConcurrencyError):
        raise ConcurrencyError("Connection is sending a fragmented message")
    elif isinstance(error, TypeError):
        raise TypeError("Message doesn't have a supported type")
    else:
        return error


def handle_ws_errors(function: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    import inspect
    
    if inspect.iscoroutinefunction(function):
        # Async function - create async wrapper
        @wraps(function)
        async def async_impl(self, *args, **kwargs):
            try:
                return await function(self, *args, **kwargs)
            except RuntimeError as err:
                raise _parse_ws_error(err, driver=self) from err
        return async_impl
    else:
        # Sync function - create sync wrapper
        @wraps(function)
        def sync_impl(self, *args, **kwargs):
            try:
                return function(self, *args, **kwargs)
            except RuntimeError as err:
                raise _parse_ws_error(err, driver=self) from err
        return sync_impl
