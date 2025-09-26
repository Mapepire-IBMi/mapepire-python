# Fetch environment variables
import os
import platform

from mapepire_python.authentication.kerberosTokenProvider import KerberosTokenProvider
from mapepire_python.data_types import DaemonServer
__all__ = ["creds", "server", "user", "password", "use_kerberos", "realm", "realm_user", "krb5_path", "port", "ignoreUnauthorized", "ca"]

# Necessary environment variables
server = os.getenv("VITE_SERVER")
user = os.getenv("VITE_DB_USER")
password = os.getenv("VITE_DB_PASS")

# Kerberos environment variables
use_kerberos = os.getenv("VITE_USE_KERBEROS", "false").lower() == "true"
realm = os.getenv("VITE_KRB_REALM", None)
realm_user = os.getenv("VITE_KRB_USER", None)
krb5_path = os.getenv("VITE_KRB5_PATH", None)

# Optional environemnt variables
port = os.getenv("VITE_DB_PORT")
ignoreUnauthorized = os.getenv("VITE_IGNORE_UNATH", "false").lower() == "true"
ca = os.getenv("VITE_CA", None)

# Check if necessary environment variables are set
if not server or not user or not password:
    raise ValueError("One or more environment variables are missing.")

# Check if necessary kerberos environment variables are set
if use_kerberos:
    if not realm or not realm_user or not krb5_path:
        raise ValueError("One or more Kerberos environment variables are missing.")
    
    if platform.system() == "Windows":
        token_provider = KerberosTokenProvider(host=server)
    else:
        token_provider = KerberosTokenProvider (
            realm=realm,
            realm_user=realm_user,
            host=server,
            krb5_path=krb5_path
        )

    creds = DaemonServer(
        host=server,
        user=user,
        password=token_provider,
        port=port,
        ignoreUnauthorized=ignoreUnauthorized,
        ca=ca
    )
    
else:
    creds = DaemonServer(host=server, port=port, user=user, password=password, ignoreUnauthorized=ignoreUnauthorized, ca=ca)