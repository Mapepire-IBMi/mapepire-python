"""Unit tests for KerberosTokenProvider.

All external dependencies (gssapi, sspi, platform.system, os.environ) are
patched so the suite runs without a Kerberos KDC, a credential cache, or
Windows.  Tests are grouped into four classes:

  TestFormatToken      – _KERBEROSAUTH_ prefix + base64 encoding
  TestInitValidation   – ValueError on missing realm/realm_user/krb5_path (non-Windows)
  TestUnixTokenPath    – gssapi happy-path, GSSError → RuntimeError translations,
                         None-token guard, and env-var side-effects
  TestWindowsTokenPath – sspi happy-path and SSPI error → RuntimeError
"""
import base64
import os
from unittest.mock import MagicMock, patch

import pytest

from mapepire_python.authentication import kerberosTokenProvider as mod
from mapepire_python.authentication.kerberosTokenProvider import KerberosTokenProvider

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gssapi_mock() -> MagicMock:
    """Return a MagicMock shaped like the gssapi module."""
    gss = MagicMock(name="gssapi")

    # OID factory
    oid = MagicMock(name="OID")
    gss.OID.from_int_seq.return_value = oid

    # Name / Credentials / SecurityContext
    gss.Name.return_value = MagicMock(name="gss.Name")
    gss.NameType.user = "user"
    gss.NameType.hostbased_service = "hostbased_service"
    gss.Credentials.return_value = MagicMock(name="gss.Credentials")

    ctx = MagicMock(name="gss.SecurityContext")
    ctx.step.return_value = b"FAKE_TOKEN"
    gss.SecurityContext.return_value = ctx

    # GSSError must be a real Exception subclass for pytest.raises to work
    class _GSSError(Exception):
        pass

    gss.exceptions.GSSError = _GSSError
    return gss


def _make_sspi_mock(err: int = 0) -> MagicMock:
    """Return a MagicMock shaped like the sspi module."""
    sspi = MagicMock(name="sspi")
    buf = MagicMock()
    buf.Buffer = b"SSPI_TOKEN"
    client = MagicMock()
    client.authorize.return_value = (err, [buf])
    sspi.ClientAuth.return_value = client
    return sspi


# ---------------------------------------------------------------------------
# TestFormatToken
# ---------------------------------------------------------------------------


class TestFormatToken:
    """_format_token encodes raw bytes and prepends _KERBEROSAUTH_.

    _format_token has no dependency on PLATFORM or any external library —
    it only uses base64 and the module-level TOKEN_PREFIX constant.  We
    construct a provider once (patching PLATFORM so __init__ validation
    passes) and call _format_token directly across all test methods.
    """

    @pytest.fixture(autouse=True)
    def _make_provider(self):
        with patch.object(mod, "PLATFORM", "Linux"):
            self.provider = KerberosTokenProvider(
                host="h", realm="R", realm_user="u", krb5_path="/k"
            )

    @pytest.mark.parametrize(
        "raw, expected_b64",
        [
            (b"hello", base64.b64encode(b"hello").decode()),
            (b"\x00\x01\x02\x03", base64.b64encode(b"\x00\x01\x02\x03").decode()),
            (b"", base64.b64encode(b"").decode()),
            (b"any-token", base64.b64encode(b"any-token").decode()),
        ],
    )
    def test_format_is_prefix_plus_base64(self, raw, expected_b64):
        result = self.provider._format_token(raw)
        assert result == "_KERBEROSAUTH_" + expected_b64

    def test_prefix_is_present(self):
        assert self.provider._format_token(b"x").startswith("_KERBEROSAUTH_")

    def test_body_round_trips_to_original_bytes(self):
        raw = b"\xde\xad\xbe\xef"
        result = self.provider._format_token(raw)
        encoded_part = result[len("_KERBEROSAUTH_"):]
        assert base64.b64decode(encoded_part) == raw


# ---------------------------------------------------------------------------
# TestInitValidation
# ---------------------------------------------------------------------------


class TestInitValidation:
    """On non-Windows platforms, realm/realm_user/krb5_path are all required."""

    def _make(self, **kwargs):
        with patch.object(mod, "PLATFORM", "Linux"):
            return KerberosTokenProvider(**kwargs)

    def test_all_required_params_provided_succeeds(self):
        provider = self._make(
            host="ibmi.example.com",
            realm="REALM.COM",
            realm_user="user",
            krb5_path="/etc/krb5.conf",
        )
        assert provider.realm == "REALM.COM"

    @pytest.mark.parametrize("missing_param", ["realm", "realm_user", "krb5_path"])
    def test_missing_param_raises_value_error(self, missing_param):
        required = dict(host="h", realm="R", realm_user="u", krb5_path="/k")
        required.pop(missing_param)
        with pytest.raises(ValueError, match=missing_param):
            self._make(**required)

    def test_error_message_names_all_missing_params(self):
        with pytest.raises(ValueError) as exc_info:
            self._make(host="h")  # all three missing
        msg = str(exc_info.value)
        assert "realm" in msg
        assert "realm_user" in msg
        assert "krb5_path" in msg

    def test_windows_skips_required_param_check(self):
        """On Windows no ValueError is raised even if realm/realm_user/krb5_path are absent."""
        with patch.object(mod, "PLATFORM", "Windows"):
            provider = KerberosTokenProvider(host="ibmi.example.com")
        assert provider.host == "ibmi.example.com"


# ---------------------------------------------------------------------------
# TestUnixTokenPath
# ---------------------------------------------------------------------------


class TestUnixTokenPath:
    """_refresh_token_unix via gssapi (patched)."""

    def _run(self, gss_mock, **provider_kwargs):
        """Patch gssapi on the module, call get_token(), return the result."""
        with patch.object(mod, "PLATFORM", "Linux"), patch.object(mod, "gssapi", gss_mock):
            provider = KerberosTokenProvider(
                host="ibmi.example.com",
                realm="REALM.COM",
                realm_user="testuser",
                krb5_path="/etc/krb5.conf",
                **provider_kwargs,
            )
            return provider.get_token()

    def test_happy_path_returns_formatted_token(self):
        gss = _make_gssapi_mock()
        result = self._run(gss)
        assert result.startswith("_KERBEROSAUTH_")
        assert base64.b64decode(result[len("_KERBEROSAUTH_"):]) == b"FAKE_TOKEN"

    def test_krb5_config_env_var_is_set(self, monkeypatch):
        # monkeypatch.setenv takes ownership of the key so it is restored on teardown
        monkeypatch.setenv("KRB5_CONFIG", "sentinel")
        self._run(_make_gssapi_mock())
        assert os.environ["KRB5_CONFIG"] == "/etc/krb5.conf"

    def test_ticket_cache_env_var_set_when_provided(self, monkeypatch):
        monkeypatch.setenv("KRB5CCNAME", "sentinel")
        self._run(_make_gssapi_mock(), ticket_cache="/tmp/krb5cc_1000")
        assert os.environ["KRB5CCNAME"] == "/tmp/krb5cc_1000"

    def test_ticket_cache_env_var_not_set_when_omitted(self, monkeypatch):
        monkeypatch.delenv("KRB5CCNAME", raising=False)
        self._run(_make_gssapi_mock())
        assert "KRB5CCNAME" not in os.environ

    def test_default_mech_is_krb5_oid(self):
        gss = _make_gssapi_mock()
        self._run(gss)
        gss.OID.from_int_seq.assert_called_once_with("1.2.840.113554.1.2.2")

    def test_custom_mech_is_used_when_provided(self):
        gss = _make_gssapi_mock()
        self._run(gss, krb5_mech="1.3.6.1.5.5.2")
        gss.OID.from_int_seq.assert_called_once_with("1.3.6.1.5.5.2")

    def test_no_credentials_gss_error_raises_runtime_error_with_tgt_message(self):
        gss = _make_gssapi_mock()
        gss.SecurityContext.return_value.step.side_effect = gss.exceptions.GSSError(
            "No credentials were supplied"
        )
        with pytest.raises(RuntimeError, match="No valid TGT"):
            self._run(gss)

    def test_unavailable_gss_error_raises_runtime_error_with_tgt_message(self):
        gss = _make_gssapi_mock()
        gss.SecurityContext.return_value.step.side_effect = gss.exceptions.GSSError(
            "Unavailable"
        )
        with pytest.raises(RuntimeError, match="No valid TGT"):
            self._run(gss)

    def test_generic_gss_error_raises_runtime_error_with_original_message(self):
        gss = _make_gssapi_mock()
        gss.SecurityContext.return_value.step.side_effect = gss.exceptions.GSSError(
            "Some other GSSAPI failure"
        )
        with pytest.raises(RuntimeError, match="Kerberos token generation error"):
            self._run(gss)

    def test_gss_error_during_credential_acquisition_is_also_translated(self):
        """GSSError from gssapi.Credentials() (e.g. expired TGT before ctx.step)
        must also be caught and re-raised as RuntimeError."""
        gss = _make_gssapi_mock()
        gss.Credentials.side_effect = gss.exceptions.GSSError(
            "No credentials were supplied"
        )
        with pytest.raises(RuntimeError, match="No valid TGT"):
            self._run(gss)

    def test_none_token_raises_runtime_error(self):
        gss = _make_gssapi_mock()
        gss.SecurityContext.return_value.step.return_value = None
        with pytest.raises(RuntimeError, match="No token returned from GSSAPI"):
            self._run(gss)

    def test_server_name_uses_krbsvr400_principal(self):
        gss = _make_gssapi_mock()
        self._run(gss)
        positional_args = [c.args[0] for c in gss.Name.call_args_list]
        assert "krbsvr400@ibmi.example.com" in positional_args

    def test_user_name_combines_realm_user_and_realm(self):
        gss = _make_gssapi_mock()
        self._run(gss)
        positional_args = [c.args[0] for c in gss.Name.call_args_list]
        assert "testuser@REALM.COM" in positional_args


# ---------------------------------------------------------------------------
# TestWindowsTokenPath
# ---------------------------------------------------------------------------


class TestWindowsTokenPath:
    """_refresh_token_windows via sspi (patched)."""

    def _run(self, sspi_mock):
        with patch.object(mod, "PLATFORM", "Windows"), patch.object(mod, "sspi", sspi_mock):
            provider = KerberosTokenProvider(host="ibmi.example.com")
            return provider.get_token()

    def test_happy_path_returns_formatted_token(self):
        sspi = _make_sspi_mock(err=0)
        result = self._run(sspi)
        assert result.startswith("_KERBEROSAUTH_")
        assert base64.b64decode(result[len("_KERBEROSAUTH_"):]) == b"SSPI_TOKEN"

    def test_target_spn_uses_krbsvr400(self):
        sspi = _make_sspi_mock(err=0)
        self._run(sspi)
        _, kwargs = sspi.ClientAuth.call_args
        assert kwargs["targetspn"] == "krbsvr400/ibmi.example.com"

    def test_sspi_error_raises_runtime_error(self):
        sspi = _make_sspi_mock(err=0x80090302)  # SEC_E_NO_CREDENTIALS
        with pytest.raises(RuntimeError, match="Windows SSPI error"):
            self._run(sspi)

    def test_sspi_error_message_includes_hex_code(self):
        sspi = _make_sspi_mock(err=0x80090302)  # SEC_E_NO_CREDENTIALS
        with pytest.raises(RuntimeError, match="0x80090302"):
            self._run(sspi)
