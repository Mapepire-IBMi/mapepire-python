import os
import ssl

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.ssl import get_certificate

from .test_setup import *


def test_get_cert():
    cert = get_certificate(creds)
    print(cert)
    assert cert != None


def test_verify_cert():
    cert = get_certificate(creds)
    creds.ignoreUnauthorized = False
    creds.ca = cert
    job = SQLJob()
    result = job.connect(creds)
    assert result["success"]


def test_verify_cert_not_provided():
    creds.ignoreUnauthorized = False
    job = SQLJob()
    result = job.connect(creds)
    assert result["success"]


def test_bad_cert():
    badCert = """-----BEGIN CERTIFICATE-----
mIIDhTCCAm2gAwIBAgIEYRpOADANBgkqhkiG9w0BAQsFADBzMRAwDgYDVQQIEwdV
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
79/VBUN4NLZ3mBVk2FGuazIu9n80+o0fI5sg1ucQ/hBt8WR8iQ6sZUc=
-----END CERTIFICATE-----"""

    creds.ca = badCert
    creds.ignoreUnauthorized = False
    job = SQLJob()
    with pytest.raises(ssl.SSLError) as err:
        res = job.connect(creds)
