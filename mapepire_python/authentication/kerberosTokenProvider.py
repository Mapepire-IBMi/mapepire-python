import subprocess
import base64
import os
import time
import platform

from typing import Optional, Final

sspi = None
gssapi = None

# For Windows SSPI token generation:
if platform.system() == "Windows":
    try:
        import sspi
    except ImportError:
        pass
else:
    try:
        import gssapi
    except ImportError:
        pass

TOKEN_PREFIX: Final = "_KERBEROSAUTH_"

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


    def _set_token(self, token: bytes, lifetime: float):
        token_b64 = base64.b64encode(token).decode("utf-8")

        self._token = TOKEN_PREFIX + token_b64
        self._token_expiry = time.time() + lifetime
        self._token_used = False


    def _refresh_token_windows(self):
        target = f"krbsvr400/{self.host}"
        client = sspi.ClientAuth("Kerberos", targetspn=target)

        err, out_buffer = client.authorize(None)
        if err != 0:
            raise RuntimeError(f"Windows SSPI error when attempting Kerberos login: {hex(err)}")

        token = out_buffer[0].Buffer
        self._set_token(token, self.token_lifetime)
    
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

        try:
            user_name = gssapi.Name(
                f"{self.realm_user}@{self.realm}", name_type=gssapi.NameType.user
            )
            cred = gssapi.Credentials(name=user_name, usage="initiate", mechs=[mech])
            server_name = gssapi.Name(
                f"krbsvr400@{self.host}", name_type=gssapi.NameType.hostbased_service
            )
            ctx = gssapi.SecurityContext(name=server_name, mech=mech, creds=cred, usage="initiate")

            token = ctx.step(b"")
            if token is None:
                raise RuntimeError("Failed to generate Kerberos token. No token returned from GSSAPI context.")
        except gssapi.exceptions.GSSError as e: # type: ignore
            raise RuntimeError(f"Kerberos token generation error when attempting Kerberos login: {str(e)}")

        lifetime = cred.lifetime
        if not lifetime or lifetime in (0xFFFFFFFF, -1):
            lifetime = self.token_lifetime
        
        self._set_token(token, lifetime)
