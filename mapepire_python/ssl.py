import socket
import ssl
from typing import Optional

from .data_types import DaemonServer
from .ssl_cache import cache_certificate, get_cached_certificate


def get_certificate(creds: DaemonServer) -> Optional[bytes]:
    # Check cache first
    cached_cert = get_cached_certificate(creds)
    if cached_cert is not None:
        return cached_cert

    # Fetch certificate from server
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    if creds.ca:
        context.load_verify_locations(
            cadata=creds.ca if isinstance(creds.ca, str) else creds.ca.decode()
        )
    with socket.create_connection((creds.host, creds.port)) as sock:
        with context.wrap_socket(sock, server_hostname=creds.host) as ssock:
            try:
                ssock.do_handshake()
                cert = ssock.getpeercert(binary_form=True)
                pem_cert = ssl.DER_cert_to_PEM_cert(cert)

                # Cache the certificate
                cache_certificate(creds, pem_cert)

                return pem_cert
            except ssl.SSLError as er:
                raise er
