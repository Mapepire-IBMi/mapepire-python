# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
- ci: update GitHub Actions to current majors — `actions/checkout` v3/v4→v6, `actions/setup-python` v5→v6, `actions/upload-artifact` v4→v7, `actions/download-artifact` v4→v8, `softprops/action-gh-release` v2→v3 (consolidates Dependabot PRs #105–#109)
- perf(pool): parallel pool init/teardown and load-aware connection fan-out so concurrent queries spread across all connections instead of serializing onto one; switch async response routing from `pyee` to a `dict[str, Future]` (O(1) routing, accurate in-flight accounting) and drop the `pyee` dependency (#115)
- remove outdated Sphinx/ReadTheDocs documentation (superseded by https://mapepire-ibmi.github.io)
- patch dependency security vulnerabilities by regenerating `uv.lock` (urllib3, cryptography, requests, idna, wheel, virtualenv, filelock, marshmallow, python-dotenv, pytest, black, pygments; tornado dropped with the docs toolchain)
- correct declared `requires-python` to `>=3.10` to match the supported Python versions
- fix: do not force a connection in `SQLJob.__enter__` / `AsyncSQLJob.__aenter__` when no credentials are provided, restoring the `with SQLJob() as job: job.connect(creds)` pattern (regression from #111)
- add TLS support
- enable server certificate verification by default
- enable Kerberos authentication
- adopt official Mapepire protocol types using dataclasses (#96)
- Native async support for PEP 249 interface (replace to_thread wrapper) #95
- Improve public API surface and top-level exports #94

## [v0.2.0](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.2.0) - 2024-11-26
- replace `websocket-client` with `websockets`
- add support for permessage deflate (RFC 7692)
- improve error handling

## [v0.1.8](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.8) - 2024-11-11
- bump minimum python version to 3.10

- fix tests
- remove python 3.9 tests

## [v0.1.7](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.7) - 2024-09-16

## [v0.1.6](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.6) - 2024-09-16
- Add PEP 249 support
- add pep249abc dependency
- add support for config.ini file for connection details
- update min Python version to 3.10


## [v0.1.5](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.5) - 2024-08-30
- add cl tests
- add query manager

## TP1
- Add query manager
- Add async support for pooling
- Add context managers for SQLJob and PoolJob
- update docs and usage

## [v0.1.4](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.4) - 2024-08-23

## [v0.1.3](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.3) - 2024-08-23

## [v0.1.4] - 2025-08-09
Rename references from python_wsdb and python-wsdb to mapepire_python and mapepire-python

## [v0.1.3] - 2025-08-02
Add workflow to run test suite on PR's
Update requirements-dev.txt to include isort, black, ruff, mypy

## [v0.1.2](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.2) - 2024-04-22

## [v0.1.1](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.1) - 2024-04-19

## [v0.1.0](https://github.com/Mapepire-IBMi/mapepire-python/releases/tag/v0.1.0) - 2024-04-19
Add initial release

## [0.1.2] 
### Added 
- pre-commit hooks
- repo formatting
- PEP PEP 563 style annotations
