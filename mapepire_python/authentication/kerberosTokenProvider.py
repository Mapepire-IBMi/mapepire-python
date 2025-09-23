import subprocess
import base64
import os
import time
import platform

from typing import Optional

try:
    import gssapi
except OSError:
    gssapi = None

# For Windows SSPI token generation:
# if platform.system() == "Windows":
#     try:
#         import sspi
#         import sspicon
#     except ImportError:
#         kerberos_sspi = None
import sspi

class KerberosTokenProvider:
    def __init__(
        self,
        realm: str,
        realm_user: str,
        host: str,
        krb5_path: str,
        ticket_cache: Optional[str] = None,
        krb5_mech: Optional[str] = None,
        token_lifetime_seconds: int = 600  # default: 10 minutes
    ):
        self.realm = realm
        self.realm_user = realm_user
        self.host = host
        self.krb5_path = krb5_path
        self.ticket_cache = ticket_cache
        self.krb5_mech = krb5_mech
        self.token_lifetime = token_lifetime_seconds

        self._token = None
        self._token_expiry = 0
        self._token_used = False

    def get_token(self) -> str:
        if self._token is None or self._token_used or self._is_expired():
            self._refresh_token()
        self._token_used = True
        return self._token # type: ignore

    def _is_expired(self) -> bool:
        return time.time() >= self._token_expiry
    
    def _refresh_token(self):
        if platform.system() == "Windows":
            self._refresh_token_windows()
        else:
            self._refresh_token_unix()


    def _refresh_token_windows(self):
        # Prefer using kerberos-sspi if available
        target = f"krbsvr400/{self.host}"
        client = sspi.ClientAuth("Kerberos", targetspn=target)


        err, out_buffer = client.authorize(None)
        if err != 0:
            raise RuntimeError(f"SSPI error: {hex(err)}")

        token = out_buffer[0].Buffer

        if isinstance(token, str):
            raise Exception
        ticket_b64 = base64.b64encode(token).decode("utf-8")
        print(base64.b64decode(ticket_b64)[:10]) 


        self._token = "_KERBEROSAUTH_" + ticket_b64
        self._token_expiry = time.time() + self.token_lifetime
    
    def _refresh_token_unix(self):
        if gssapi is None:
            raise RuntimeError("gssapi module not installed, cannot generate Kerberos token on Unix")

        os.environ["KRB5_CONFIG"] = self.krb5_path
        if self.ticket_cache:
            os.environ["KRB5CCNAME"] = self.ticket_cache

        mech = (
            gssapi.OID.from_int_seq("1.2.840.113554.1.2.2")
            if self.krb5_mech is None
            else gssapi.OID.from_int_seq(self.krb5_mech)
        )

        user_name = gssapi.Name(
            f"{self.realm_user}@{self.realm}", name_type=gssapi.NameType.user
        )
        cred = gssapi.Credentials(name=user_name, usage="initiate", mechs=[mech])
        server_name = gssapi.Name(
            f"krbsvr400@{self.host}", name_type=gssapi.NameType.hostbased_service
        )
        ctx = gssapi.SecurityContext(name=server_name, mech=mech, creds=cred, usage="initiate")

        token = ctx.step(b"")
        ticket_b64 = base64.b64encode(token).decode("utf-8")
        self._token = "_KERBEROSAUTH_" + ticket_b64

        lifetime = cred.lifetime
        indefinite_lifetimes = [0xFFFFFFFF, -1]
        if lifetime is None or lifetime in indefinite_lifetimes:
            self._token_expiry = time.time() + self.token_lifetime
        else:
            self._token_expiry = time.time() + lifetime

        self._token_used = False
