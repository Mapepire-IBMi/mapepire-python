import base64
import logging
import os
import platform
from typing import Final, Optional

logger = logging.getLogger(__name__)

sspi = None
gssapi = None
PLATFORM = platform.system()
TOKEN_PREFIX: Final = "_KERBEROSAUTH_"


# For Windows SSPI token generation:
if PLATFORM == "Windows":
    import sspi
else:
    import gssapi


class KerberosTokenProvider:
    def __init__(
        self,
        host: str,
        realm: Optional[str] = None,
        realm_user: Optional[str] = None,
        krb5_path: Optional[str] = None,
        ticket_cache: Optional[str] = None,
        krb5_mech: Optional[str] = None,
    ):
        self.host = host
        self.realm = realm
        self.realm_user = realm_user
        self.krb5_path = krb5_path
        self.ticket_cache = ticket_cache
        self.krb5_mech = krb5_mech

        if PLATFORM != "Windows":
            missing = []
            if not self.realm:
                missing.append("realm")
            if not self.realm_user:
                missing.append("realm_user")
            if not self.krb5_path:
                missing.append("krb5_path")
            if missing:
                raise ValueError(f"Missing required parameters: {', '.join(missing)}")

    def get_token(self) -> str:
        logger.debug("Requesting Kerberos token for host=%s", self.host)
        return self._refresh_token()

    def _refresh_token(self) -> str:
        if PLATFORM == "Windows":
            return self._refresh_token_windows()
        else:
            return self._refresh_token_unix()

    def _format_token(self, token: bytes) -> str:
        token_b64 = base64.b64encode(token).decode("utf-8")
        return TOKEN_PREFIX + token_b64

    def _refresh_token_windows(self) -> str:
        logger.debug("Generating Kerberos token via Windows SSPI for host=%s", self.host)
        target = f"krbsvr400/{self.host}"
        client = sspi.ClientAuth("Kerberos", targetspn=target)

        err, out_buffer = client.authorize(None)
        if err != 0:
            logger.error("Windows SSPI Kerberos authentication failed: error_code=%s", hex(err))
            raise RuntimeError(f"Windows SSPI error when attempting Kerberos login: {hex(err)}")

        token = out_buffer[0].Buffer
        logger.debug("Kerberos token generated successfully via Windows SSPI")
        return self._format_token(token)

    def _refresh_token_unix(self) -> str:
        logger.debug("Generating Kerberos token via GSSAPI for host=%s realm=%s", self.host, self.realm)
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
                raise RuntimeError(
                    "Failed to generate Kerberos token. No token returned from GSSAPI context."
                )
        except gssapi.exceptions.GSSError as e:
            if "No credentials were supplied" in str(e) or "Unavailable" in str(e):
                logger.error("Kerberos authentication failed: no valid TGT in credential cache")
                raise RuntimeError("No valid TGT found in credential cache.")
            logger.error("Kerberos GSSAPI authentication failed for host=%s", self.host)
            raise RuntimeError(
                f"Kerberos token generation error when attempting Kerberos login: {str(e)}"
            )

        logger.debug("Kerberos token generated successfully via GSSAPI")
        return self._format_token(token)
