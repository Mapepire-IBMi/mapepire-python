"""Unit tests for DaemonServer config parsing & validation.

Covers two layers, neither of which touches the network:

  DaemonServer
    - construction defaults & required-field validation
    - dict_to_dataclass coercion
    - from_env (success, missing-var validation, CA-file handling)
    - get_password for plain-string credentials

  BaseJob._parse_connection_input  (the public entry point that accepts
    every input shape and normalises it to a DaemonServer)
    - DaemonServer  -> returned as-is
    - dict          -> coerced via dict_to_dataclass
    - str / Path    -> read from an INI config file (with/without section)
    - None          -> falls back to DaemonServer.from_env
    - invalid path / wrong type -> raises
"""
from pathlib import Path

import pytest

from mapepire_python.base_job import BaseJob
from mapepire_python.data_types import DaemonServer, dict_to_dataclass


# ===========================================================================
# DaemonServer
# ===========================================================================

class TestConstruction:
    def test_required_fields_are_set(self):
        srv = DaemonServer(host="h", user="u", password="p", port=8076)
        assert (srv.host, srv.user, srv.password, srv.port) == ("h", "u", "p", 8076)

    def test_optional_fields_default(self):
        srv = DaemonServer(host="h", user="u", password="p", port=8076)
        assert srv.ignoreUnauthorized is False
        assert srv.ca is None

    def test_missing_required_field_raises_type_error(self):
        with pytest.raises(TypeError):
            DaemonServer(host="h", user="u")  # type: ignore[call-arg]


class TestDictToDataclass:
    def test_coerces_matching_keys(self):
        srv = dict_to_dataclass(
            {"host": "h", "user": "u", "password": "p", "port": 8076}, DaemonServer
        )
        assert isinstance(srv, DaemonServer)
        assert srv.host == "h"

    def test_drops_unknown_keys(self):
        srv = dict_to_dataclass(
            {"host": "h", "user": "u", "password": "p", "port": 8076, "extra": 1},
            DaemonServer,
        )
        assert not hasattr(srv, "extra")

    def test_carries_optional_keys(self):
        srv = dict_to_dataclass(
            {"host": "h", "user": "u", "password": "p", "port": 8076,
             "ignoreUnauthorized": True},
            DaemonServer,
        )
        assert srv.ignoreUnauthorized is True

    def test_missing_required_key_raises(self):
        with pytest.raises(TypeError):
            dict_to_dataclass({"host": "h"}, DaemonServer)


class TestFromEnv:
    def _set_required(self, monkeypatch):
        monkeypatch.setenv("MAPEPIRE_HOST", "env.example.com")
        monkeypatch.setenv("MAPEPIRE_USER", "envuser")
        monkeypatch.setenv("MAPEPIRE_PASSWORD", "envpass")
        monkeypatch.delenv("MAPEPIRE_CA_PATH", raising=False)

    def test_reads_required_vars(self, monkeypatch):
        self._set_required(monkeypatch)
        monkeypatch.delenv("MAPEPIRE_PORT", raising=False)
        srv = DaemonServer.from_env()
        assert srv.host == "env.example.com"
        assert srv.user == "envuser"
        assert srv.get_password() == "envpass"

    def test_port_defaults_to_8076(self, monkeypatch):
        self._set_required(monkeypatch)
        monkeypatch.delenv("MAPEPIRE_PORT", raising=False)
        assert DaemonServer.from_env().port == "8076"

    def test_port_override(self, monkeypatch):
        self._set_required(monkeypatch)
        monkeypatch.setenv("MAPEPIRE_PORT", "9999")
        assert DaemonServer.from_env().port == "9999"

    @pytest.mark.parametrize("missing", ["MAPEPIRE_HOST", "MAPEPIRE_USER", "MAPEPIRE_PASSWORD"])
    def test_missing_required_var_raises_naming_the_var(self, monkeypatch, missing):
        self._set_required(monkeypatch)
        monkeypatch.delenv(missing, raising=False)
        with pytest.raises(ValueError, match=missing):
            DaemonServer.from_env()

    def test_ca_path_is_read_into_ca_bytes(self, monkeypatch, tmp_path):
        self._set_required(monkeypatch)
        ca_file = tmp_path / "ca.pem"
        ca_file.write_bytes(b"-----CERT-----")
        monkeypatch.setenv("MAPEPIRE_CA_PATH", str(ca_file))
        srv = DaemonServer.from_env()
        assert srv.ca == b"-----CERT-----"

    def test_missing_ca_file_raises_value_error(self, monkeypatch, tmp_path):
        self._set_required(monkeypatch)
        monkeypatch.setenv("MAPEPIRE_CA_PATH", str(tmp_path / "absent.pem"))
        with pytest.raises(ValueError, match="CA certificate file not found"):
            DaemonServer.from_env()


class TestGetPassword:
    def test_returns_plain_string_password(self):
        srv = DaemonServer(host="h", user="u", password="secret", port=8076)
        assert srv.get_password() == "secret"


# ===========================================================================
# BaseJob._parse_connection_input
# ===========================================================================

@pytest.fixture
def job() -> BaseJob:
    return BaseJob()


@pytest.fixture
def ini_file(tmp_path) -> Path:
    """A two-section INI config file written to a temp dir."""
    content = (
        "[mysystem]\n"
        "host = ini.example.com\n"
        "user = iniuser\n"
        "password = inipass\n"
        "port = 8076\n"
        "\n"
        "[other]\n"
        "host = other.example.com\n"
        "user = otheruser\n"
        "password = otherpass\n"
        "port = 9000\n"
    )
    path = tmp_path / "config.ini"
    path.write_text(content)
    return path


class TestParseDaemonServer:
    def test_daemon_server_returned_unchanged(self, job, mock_creds):
        assert job._parse_connection_input(mock_creds) is mock_creds

    def test_daemon_server_is_stored_on_job(self, job, mock_creds):
        job._parse_connection_input(mock_creds)
        assert job.creds is mock_creds


class TestParseDict:
    def test_dict_is_coerced_to_daemon_server(self, job):
        result = job._parse_connection_input(
            {"host": "h", "user": "u", "password": "p", "port": 8076}
        )
        assert isinstance(result, DaemonServer)
        assert result.host == "h"
        assert result.port == 8076

    def test_extra_dict_keys_are_ignored(self, job):
        result = job._parse_connection_input(
            {"host": "h", "user": "u", "password": "p", "port": 8076, "bogus": "x"}
        )
        assert not hasattr(result, "bogus")

    def test_missing_required_dict_key_raises_type_error(self, job):
        with pytest.raises(TypeError):
            job._parse_connection_input({"host": "h"})


class TestParseIniFile:
    def test_first_section_used_when_no_section_kwarg(self, job, ini_file):
        result = job._parse_connection_input(str(ini_file))
        assert result.host == "ini.example.com"
        assert result.user == "iniuser"

    def test_named_section_is_selected(self, job, ini_file):
        result = job._parse_connection_input(str(ini_file), section="other")
        assert result.host == "other.example.com"
        assert result.port == "9000"  # INI values are strings

    def test_unknown_section_falls_back_to_first(self, job, ini_file):
        result = job._parse_connection_input(str(ini_file), section="does-not-exist")
        assert result.host == "ini.example.com"

    def test_path_object_is_accepted(self, job, ini_file):
        result = job._parse_connection_input(ini_file)
        assert isinstance(result, DaemonServer)
        assert result.host == "ini.example.com"

    def test_nonexistent_path_raises_value_error(self, job, tmp_path):
        with pytest.raises(ValueError, match="not a valid file"):
            job._parse_connection_input(str(tmp_path / "nope.ini"))


class TestParseFromEnv:
    def test_none_reads_from_environment(self, job, monkeypatch):
        monkeypatch.setenv("MAPEPIRE_HOST", "env.example.com")
        monkeypatch.setenv("MAPEPIRE_USER", "envuser")
        monkeypatch.setenv("MAPEPIRE_PASSWORD", "envpass")
        monkeypatch.delenv("MAPEPIRE_CA_PATH", raising=False)
        result = job._parse_connection_input(None)
        assert result.host == "env.example.com"
        assert result.user == "envuser"

    def test_none_with_missing_env_raises(self, job, monkeypatch):
        for var in ("MAPEPIRE_HOST", "MAPEPIRE_USER", "MAPEPIRE_PASSWORD"):
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(ValueError, match="Missing required environment variables"):
            job._parse_connection_input(None)


class TestParseInvalidType:
    def test_non_supported_type_raises_type_error(self, job):
        with pytest.raises(TypeError, match="must be of type DaemonServer"):
            job._parse_connection_input(12345)  # type: ignore[arg-type]
