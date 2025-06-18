"""
Tests for TLS/SSL functionality (migrated from tls_test.py).
Simple TLS tests using real IBM i server.
"""

import ssl

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer
from mapepire_python.ssl import get_certificate


@pytest.mark.tls
def test_get_certificate(ibmi_credentials):
    """Test getting server certificate."""
    creds = DaemonServer(**ibmi_credentials)
    cert = get_certificate(creds)
    assert cert is not None
    assert isinstance(cert, str)
    assert "-----BEGIN CERTIFICATE-----" in cert
    assert "-----END CERTIFICATE-----" in cert


@pytest.mark.tls
def test_verify_certificate_with_ca(ibmi_credentials):
    """Test certificate verification with provided CA."""
    creds = DaemonServer(**ibmi_credentials)
    cert = get_certificate(creds)
    creds.ignoreUnauthorized = False
    creds.ca = cert

    job = SQLJob()
    result = job.connect(creds)
    assert result["success"]
    job.close()


@pytest.mark.tls
def test_verify_certificate_default(ibmi_credentials):
    """Test certificate verification with default behavior."""
    creds = DaemonServer(**ibmi_credentials)
    creds.ignoreUnauthorized = False

    job = SQLJob()
    result = job.connect(creds)
    assert result["success"]
    job.close()


@pytest.mark.tls
def test_ignore_unauthorized_certificate(ibmi_credentials):
    """Test connecting with ignoreUnauthorized=True."""
    creds = DaemonServer(**ibmi_credentials)
    creds.ignoreUnauthorized = True

    job = SQLJob()
    result = job.connect(creds)
    assert result["success"]
    job.close()


@pytest.mark.tls
def test_bad_certificate_fails(ibmi_credentials):
    """Test that bad certificate causes connection failure."""
    bad_cert = """-----BEGIN CERTIFICATE-----
MIIDhTCCAm2gAwIBAgIEYRpOADANBgkqhkiG9w0BAQsFADBzMRAwDgYDVQQIEwdV
bmtub3duMRAwDgYDVQQGEwdVbmtub3duMRYwFAYDVQQKEw1EYjIgZm9yIElCTSBp
MRowGAYDVQQLExFXZWIgU29ja2V0IFNlcnZlcjEZMBcGA1UEAxMQT1NTQlVJTEQu
UlpLSC5ERTAeFw0yNDA4MjMxODE2MDJaFw0zNDA4MjUxODE2MDJaMHMxEDAOBgNV
BAgTB1Vua25vd24xEDAOBgNVBAYTB1Vua25vd24xFjAUBgNVBAoTDURiMiBmb3Ig
SUJNIGkxGjAYBgNVBAsTEVdlYiBTb2NrZXQgU2VydmVyMRkwFwYDVQQDExBPU1NC
VUlMRC5SWktILkRFMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhKBx
5KoTsBs3dHibT/j8ycApa6teJOclaiCl9fX5IwKP0dli5qZ91t5sZ+51qS3mgLny
zMWBCaSIQYsuDEE374lHYpYB6wh/00VE1NseJpHbqbCQz1GSUz/d4tK4R1qx0Gv0
lpKOd8/oMLUZ24FCEUKaqeQBxTzlQxkI9t2DbIRwS6U6oc4uj5DN2EIU+mfLb17y
j8iA7VMKsRmoke2vyOLXJJYJeASNI02AbHcbYkd6BaoyNeb3BlpssEhgZribWmdy
FhrJldpGtJyirvABaZQaEFelEqmSVbdPWccX3JWQdorZoNVXCypxJatxOZAhCg6f
iu3AceHUr+dMAS8z4QIDAQABoyEwHzAdBgNVHQ4EFgQU6VvyCjQ5574xtCg0oypV
zHP0vAMwDQYJKoZIhvcNAQELBQADggEBAD4bKhansD+uuUPYaIvPwyclr4zPvuyg
QAFu5oILqddzgPGIwogbxTxQkNjEGyorFJj1vJBCVIq4zJJ0DIv57BK/oVMy4Byl
6zMhJTjS74assgjCq1pVjIBtc2PCfiWxzo0wQCOEL8gsNCy/w5EaIATKfLtx6+Fd
CHsadf7fvJnLnK3FXOStAnN31ISSTwsvsRobdXX70nlYM/2OaZQsIlndftVRbI39
2+94KHciPSwo/4fu+FLuvOm37GS+/ST3BKDSvwRJRxUc0r8lo1STiQz0cXC6uqDd
79/VBUN4NLZ3mBVk2FGuazIu9n80+o0fI5sg1ucQ/h
-----END CERTIFICATE-----"""

    creds = DaemonServer(**ibmi_credentials)
    creds.ca = bad_cert
    creds.ignoreUnauthorized = False

    job = SQLJob()
    # Test that connection fails - might raise different types of errors or return failure
    try:
        result = job.connect(creds)
        # If connection succeeds, check that it's actually failed in the response
        if result and result.get("success"):
            pytest.fail("Connection should fail with bad certificate but succeeded")
    except (ssl.SSLError, Exception):
        # Any exception is acceptable - certificate validation should prevent connection
        pass  # Expected behavior


@pytest.mark.tls
def test_tls_connection_parameters(ibmi_credentials):
    """Test various TLS connection parameters."""
    creds = DaemonServer(**ibmi_credentials)

    # Test with explicit TLS settings
    creds.ignoreUnauthorized = True  # For testing purposes

    job = SQLJob()
    result = job.connect(creds)
    assert result["success"]

    # Test basic query to ensure TLS connection works
    query_result = job.query_and_run("select 1 from sample.employee")
    assert query_result["success"]

    job.close()


@pytest.mark.tls
def test_certificate_validation_strictness(ibmi_credentials):
    """Test different levels of certificate validation."""
    creds = DaemonServer(**ibmi_credentials)

    # Test strict validation (default)
    creds.ignoreUnauthorized = False
    job1 = SQLJob()
    result1 = job1.connect(creds)
    assert result1["success"]
    job1.close()

    # Test relaxed validation
    creds.ignoreUnauthorized = True
    job2 = SQLJob()
    result2 = job2.connect(creds)
    assert result2["success"]
    job2.close()
