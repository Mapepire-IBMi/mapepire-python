import socket
import ssl

from python_wsdb.types import DaemonServer


def get_certificate(creds: DaemonServer) -> (bytes | None):
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    if creds.ca:
        context.load_verify_locations(
            cadata=creds.ca if isinstance(creds.ca, str) else creds.ca.decode()
        )
    with socket.create_connection((creds.host, creds.port)) as sock:
        with context.wrap_socket(sock, server_hostname=creds.host) as ssock:
            return ssock.getpeercert(binary_form=True)
