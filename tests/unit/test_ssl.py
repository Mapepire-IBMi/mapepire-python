"""Unit tests for ssl.get_certificate.

The real function opens a TLS socket to the server; here ``socket`` and the
``ssl`` helpers are patched so we can assert how the SSL context is configured,
how the CA material is loaded, and how the peer certificate is returned —
without any network I/O.
"""
import ssl as real_ssl
from unittest.mock import MagicMock, patch

import pytest

from mapepire_python.data_types import DaemonServer
from mapepire_python.ssl import get_certificate


@pytest.fixture
def ssl_env():
    """Patch socket/ssl and yield the mocks used by get_certificate."""
    with patch("mapepire_python.ssl.ssl.create_default_context") as create_ctx, \
         patch("mapepire_python.ssl.socket.create_connection") as create_conn, \
         patch("mapepire_python.ssl.ssl.DER_cert_to_PEM_cert") as der_to_pem:
        context = MagicMock()
        create_ctx.return_value = context

        sock = MagicMock()
        create_conn.return_value.__enter__.return_value = sock

        ssock = MagicMock()
        context.wrap_socket.return_value.__enter__.return_value = ssock
        ssock.getpeercert.return_value = b"DER-CERT-BYTES"

        der_to_pem.return_value = "-----BEGIN CERTIFICATE-----\nPEM\n-----END CERTIFICATE-----"

        yield {
            "create_ctx": create_ctx,
            "create_conn": create_conn,
            "der_to_pem": der_to_pem,
            "context": context,
            "sock": sock,
            "ssock": ssock,
        }


def _creds(ca=None):
    return DaemonServer(
        host="cert.example.com", user="u", password="p", port=8076, ca=ca
    )


# ---------------------------------------------------------------------------
# Happy path / return value
# ---------------------------------------------------------------------------

class TestReturnValue:
    def test_returns_pem_from_der_conversion(self, ssl_env):
        result = get_certificate(_creds())
        assert result == ssl_env["der_to_pem"].return_value

    def test_peer_cert_fetched_in_binary_form(self, ssl_env):
        get_certificate(_creds())
        ssl_env["ssock"].getpeercert.assert_called_once_with(binary_form=True)

    def test_der_to_pem_called_with_peer_cert(self, ssl_env):
        get_certificate(_creds())
        ssl_env["der_to_pem"].assert_called_once_with(b"DER-CERT-BYTES")

    def test_handshake_performed(self, ssl_env):
        get_certificate(_creds())
        ssl_env["ssock"].do_handshake.assert_called_once()


# ---------------------------------------------------------------------------
# SSL context configuration
# ---------------------------------------------------------------------------

class TestContextConfiguration:
    def test_hostname_check_disabled(self, ssl_env):
        get_certificate(_creds())
        assert ssl_env["context"].check_hostname is False

    def test_verify_mode_is_cert_none(self, ssl_env):
        get_certificate(_creds())
        assert ssl_env["context"].verify_mode == real_ssl.CERT_NONE

    def test_connects_to_host_and_port(self, ssl_env):
        get_certificate(_creds())
        ssl_env["create_conn"].assert_called_once_with(("cert.example.com", 8076))

    def test_wrap_socket_uses_server_hostname(self, ssl_env):
        get_certificate(_creds())
        _, kwargs = ssl_env["context"].wrap_socket.call_args
        assert kwargs["server_hostname"] == "cert.example.com"


# ---------------------------------------------------------------------------
# CA handling
# ---------------------------------------------------------------------------

class TestCaHandling:
    def test_no_ca_skips_load_verify_locations(self, ssl_env):
        get_certificate(_creds(ca=None))
        ssl_env["context"].load_verify_locations.assert_not_called()

    def test_string_ca_passed_as_cadata(self, ssl_env):
        get_certificate(_creds(ca="CA-STRING"))
        ssl_env["context"].load_verify_locations.assert_called_once_with(cadata="CA-STRING")

    def test_bytes_ca_is_decoded_before_load(self, ssl_env):
        get_certificate(_creds(ca=b"CA-BYTES"))
        ssl_env["context"].load_verify_locations.assert_called_once_with(cadata="CA-BYTES")


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------

class TestErrorPropagation:
    def test_ssl_error_during_handshake_propagates(self, ssl_env):
        ssl_env["ssock"].do_handshake.side_effect = real_ssl.SSLError("handshake failed")
        with pytest.raises(real_ssl.SSLError, match="handshake failed"):
            get_certificate(_creds())
