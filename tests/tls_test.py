import os

from mapepire_python.data_types import DaemonServer

# Fetch environment variables
server = os.getenv("VITE_SERVER")
user = os.getenv("VITE_DB_USER")
password = os.getenv("VITE_DB_PASS")
port = os.getenv("VITE_DB_PORT")

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError("One or more environment variables are missing.")

creds = DaemonServer(host=server, port=port, user=user, password=password, ignoreUnauthorized=False)


# def test_get_cert():
#     cert = get_certificate(creds)
#     print(cert)
#     assert cert != None


# def test_verify_cert():
#     cert = get_certificate(creds)
#     creds.ca = cert
#     job = SQLJob()
#     result = job.connect(creds)
#     print(result)
