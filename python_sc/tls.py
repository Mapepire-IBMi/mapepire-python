import asyncio
import socket
import ssl
from dataclasses import dataclass
from typing import Optional, Union

from python_sc.types import DaemonServer


async def get_certificate(server: DaemonServer):
    """ Asynchronously get the peer's certificate from a secure TLS connection. """
    # Create a default SSL context
    context = ssl.create_default_context()
    
    if server.ignoreUnauthorized is False:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    if server.ca:
        context.load_verify_locations(server.ca)

    # Create a non-blocking socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)

    # Wrap the socket with the SSL context
    wrapped_socket = context.wrap_socket(sock, server_hostname=server.host)

    # Connect using asyncio's event loop
    await asyncio.get_event_loop().sock_connect(wrapped_socket, (server.host, server.port))

    try:
        # Perform the handshake to establish the secure connection
        await asyncio.get_event_loop().sock_do_handshake(wrapped_socket)
        # Obtain the certificate
        cert = wrapped_socket.getpeercert(True)
        return cert
    except Exception as e:
        print(f"Error: {e}")
        raise e
    finally:
        wrapped_socket.close()