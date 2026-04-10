# PR #98 Review Report — Feature/issue-95: Native Async PEP 249 Support

**PR:** [Mapepire-IBMi/mapepire-python#98](https://github.com/Mapepire-IBMi/mapepire-python/pull/98)
**Author:** Vnajeb
**Branch:** `feature/issue-95` → `main`
**Reviewer requested:** VinodTech92
**Date reviewed:** 2026-04-10
**CI status:** Tests **FAILED** (lint/style/build/docs all passed)

---

## PR Summary

This PR adds native async WebSocket I/O to the PEP 249 interface, replacing the
previous `to_thread` wrapper approach. Key changes:

- New `AsyncBaseJob` (shared async WebSocket lifecycle) and `AsyncSQLJob` entry point
- Refactored `PoolJob` to inherit from `AsyncBaseJob` (eliminates ~270 lines of duplication)
- Refactored `AsyncConnection` / `AsyncCursor` to use native async queries via `PoolQuery`
- Updated result handling to use protocol dataclasses with dict-style access
- Fixed mutable default arguments across multiple constructors
- Replaced `ast.literal_eval` with `json.loads` in error parsing
- Deleted `asyncio/utils.py` (the `to_thread` shim, no longer needed)

**Scope:** 24 files changed, 751 insertions, 788 deletions

---

## CI Status

| Check                | Status   |
|----------------------|----------|
| CHANGELOG            | Passed   |
| Python 3.10 - Build  | Passed   |
| Python 3.10 - Lint   | Passed   |
| Python 3.10 - Style  | Passed   |
| Python 3.10 - Docs   | Passed   |
| Python 3.10 - Test   | **FAILED** |
| Release              | Skipped  |
| Copilot Agent        | Passed   |

**Action required:** Test failures must be investigated and resolved before merge.

---

## Findings

### CRITICAL (3 issues)

#### C1. Fire-and-forget `asyncio.create_task` — no reference, no error handling
**File:** `async_base_job.py:117`

```python
asyncio.create_task(self.message_handler())
```

The task is created but never stored. If it raises an unexpected exception, it is
silently swallowed. The task reference can also be garbage-collected and cancelled.
This is a well-known asyncio anti-pattern.

**Fix:** Store as `self._message_handler_task`, cancel and await it in `close()`/
`dispose()`, and add a `done_callback` to log unexpected failures.

**Manual verification:**
- [ ] Search for `create_task` in `async_base_job.py` and confirm the task is not assigned to any variable
- [ ] Write a test that closes a connection while a query is in-flight and verify no hanging or silent errors

---

#### C2. `close()` does not cancel message_handler or resolve pending futures
**File:** `async_base_job.py:207-210`

```python
async def close(self) -> None:
    self.status = JobStatus.Ended
    if self.socket:
        await self.socket.close()
```

Compare with `dispose()` (lines 147-152) which clears listeners and nulls the socket.
When `close()` is called, pending `wait_for_response` futures will hang forever — they
are never cancelled. `self.socket` is also never set to `None`, so repeat `close()`
calls attempt to close an already-closed socket.

**Fix:** Have `close()` delegate to or share cleanup with `dispose()`. Cancel all
pending futures and the message handler task.

**Manual verification:**
- [ ] Read `close()` vs `dispose()` side by side in `async_base_job.py` — confirm `close()` only does 2 things
- [ ] Check if any code path calls `close()` without also calling `dispose()`

---

#### C3. `json.loads` cannot parse Python dict reprs — breaks SQL error classification
**File:** `core/exceptions.py:63-69`

`ast.literal_eval` was replaced with `json.loads`. But errors are raised as
`RuntimeError(error_dict)` where `error_dict` is a Python dict. `str(dict)` produces
single-quoted strings (`{'error': '...'}`) which `json.loads` cannot parse. All SQL
errors will silently fall through to generic `DatabaseError` instead of being classified
as `ProgrammingError`, `DataError`, etc.

**Fix:** Either revert to `ast.literal_eval`, or change the `raise RuntimeError()`
calls in `query.py`/`pool_query.py` to pass `json.dumps(error_dict)` instead of the
raw dict.

**Manual verification:**
- [ ] Open `core/exceptions.py` line 63, confirm `json.loads(str(error))` is used
- [ ] Open `client/query.py` lines 84-87, confirm `raise RuntimeError(error_list)` passes a Python dict
- [ ] Try running an invalid SQL query and check whether the error type is `ProgrammingError` or generic `DatabaseError`

---

### MAJOR (10 issues)

#### M1. Single bad JSON message kills the message_handler loop
**File:** `async_base_job.py:154-173`

Inside the `async for message in self.socket` loop, a `json.JSONDecodeError` raises
`ValueError` which escapes the loop entirely (not caught by `ConnectionClosedError`).
This kills the handler — all subsequent messages are lost and all pending futures hang.

**Fix:** Log and skip bad messages instead of re-raising.

**Manual verification:**
- [ ] Read the `message_handler` method and trace what happens on `json.JSONDecodeError`

---

#### M2. `wait_for_response` has no timeout — can hang forever
**File:** `async_base_job.py:85-100`

If the server never responds or the message_handler dies, the future hangs indefinitely.

**Fix:** Use `asyncio.wait_for(future, timeout=...)` with a configurable timeout.

---

#### M3. `list(parameters)` corrupts dict-style parameters in async cursor
**File:** `asyncio/cursor.py:85`

```python
opts = QueryOptions(parameters=list(parameters) if parameters else None)
```

PEP 249 allows `parameters` to be a mapping (dict). `list(dict)` extracts only the
keys, silently corrupting the parameter values.

**Fix:** Pass `parameters` through unchanged, or only convert tuples to lists.

**Manual verification:**
- [ ] Check if the sync `Cursor.execute()` does any similar conversion
- [ ] Try `await cursor.execute("SELECT * FROM t WHERE id = :id", {"id": 1})` and verify it works

---

#### M4. `update_count == 0` treated as "unknown" (rowcount = -1)
**Files:** `asyncio/cursor.py:91`, `core/cursor.py:112`

```python
self._rowcount = result.update_count if result.update_count else -1
```

When `update_count` is `0` (e.g., `DELETE FROM t WHERE 1=0`), this falsely reports
`rowcount = -1`. Should check `is not None` instead of truthiness.

**Fix:** `self._rowcount = result.update_count if result.update_count is not None else -1`

**Manual verification:**
- [ ] Execute a DELETE/UPDATE that affects 0 rows and check `cursor.rowcount`

---

#### M5. Re-executing on same cursor leaks server-side query resources
**File:** `asyncio/cursor.py:82-92`

If `execute()` is called twice on the same cursor, the previous `self._query` is
silently overwritten without sending `SqlCloseRequest` to the server.

**Fix:** If `self._query is not None`, call `await self._query.close()` before assigning.

---

#### M6. `commit()` / `rollback()` are silent no-ops (connection + cursor)
**Files:** `asyncio/connection.py:81-85`, `asyncio/cursor.py:153-157`

These are `pass` stubs. The `__aexit__` from the PEP 249 base class calls `commit()` on
success and `rollback()` on error, which means transaction boundaries are silently
ignored. If the server is in manual commit mode, data changes could be lost.

**Fix:** Implement by sending `COMMIT`/`ROLLBACK` via the underlying job, or document
that the connection assumes auto-commit mode.

**Manual verification:**
- [ ] Confirm sync `Connection.commit()` and `Connection.rollback()` actually do work (parity check)
- [ ] Try an INSERT inside `async with connect(...) as conn:` and verify the data persists after exit

---

#### M7. Broken `__all__` exports in `__init__.py`
**File:** `mapepire_python/__init__.py:32-33`

`CONNECTION_CLOSED` and `convert_runtime_errors` are listed in `__all__` but never
imported. `from mapepire_python import CONNECTION_CLOSED` will raise `ImportError`.

**Manual verification:**
- [ ] Run `python -c "from mapepire_python import CONNECTION_CLOSED"` and confirm it fails

---

#### M8. `creds` not passed to `AsyncSQLJob` in `AsyncConnection.__init__`
**File:** `asyncio/connection.py:44`

```python
self._job = AsyncSQLJob(options=opts)  # creds not passed
```

The job's `self.creds` is `None` at init. Connection happens later via
`_ensure_connected()`. This works but is fragile — if the job is accidentally used
directly before `_ensure_connected()`, it will be broken.

---

#### M9. No async error-path tests
**File:** `tests/pep249_async_test.py`

13 tests exist but none cover:
- Invalid SQL via async PEP 249 interface
- Operating on a closed connection/cursor
- `fetchone`/`fetchall` after cursor is exhausted
- `executemany` with mismatched parameter counts

---

#### M10. `Exception` vs `RuntimeError` inconsistency between `PoolQuery` and `Query`
**Files:** `pool/pool_query.py:106`, `client/query.py:87`

`Query` raises `RuntimeError(error_list)` (caught by `convert_runtime_errors`), but
`PoolQuery` raises `Exception(error_list)` which bypasses PEP 249 error classification
entirely. Pool-originated errors won't be converted to proper `DatabaseError` subtypes.

---

### MINOR (9 issues)

| # | File | Issue |
|---|------|-------|
| m1 | `async_base_job.py:82-83` | Bare `except Exception as e: raise e` — pointless, remove |
| m2 | `asyncio/cursor.py:107` | `list.pop(0)` is O(n), making sequential `fetchone()` O(n^2). Use `collections.deque` |
| m3 | `asyncio/cursor.py:144-148` | `callproc` passes `procname` as raw SQL instead of constructing `CALL` statement per PEP 249 |
| m4 | `data_types.py:221-226` | `QueryResult` fields changed from `init=False` to init-capable — subtle API change |
| m5 | `pool/pool_query.py:31` | `_SQLJobProtocol.send()` return type `Dict[Any, Any]` vs actual `Dict[str, Any]` |
| m6 | `tests/async_pool_test.py:162-172` | `test_query_edge_cases` never calls `await job.close()` — leaks WebSocket |
| m7 | `tests/pep249_async_test.py:43-55` | `test_pep249_async_for` has no assertion on results; loop may execute zero times |
| m8 | `tests/pep249_async_test.py:49` | Uses deprecated `asyncio.get_event_loop()` (deprecated since 3.10) |
| m9 | `tests/async_pool_test.py:78-79` | Duplicate `"port": port` dict key |

### NIT (6 issues)

| # | File | Issue |
|---|------|-------|
| n1 | `core/exceptions.py:53` | Docstring says "DuckDB" — should say "DB2" or "Mapepire" |
| n2 | `tests/pep249_async_test.py:55` | Typo: `fime` → `time` |
| n3 | `CHANGELOG.md:13-14` | Inconsistent formatting between entries |
| n4 | `pool/pool_query.py:30` | Protocol requires private method `_get_unique_id` — unusual |
| n5 | `pool/pool_query.py:41`, `client/query.py:39` | `parameters` typed `Optional[List[str]]` but actually accepts `List[Any]` |
| n6 | `asyncio/cursor.py:20-25` | `_DB_TYPE_MAP` missing DATE, TIME, TIMESTAMP, BLOB, XML, etc. (defaults to `str`) |

---

## Completeness Assessment

### What this PR does well
- Successfully extracts shared async logic from `PoolJob` into reusable `AsyncBaseJob`
- Clean removal of the `to_thread` wrapper approach
- Mutable default fixes across the codebase are correct
- Deleted `asyncio/utils.py` cleanly — no dangling imports
- `PoolJob` refactor preserves all existing functionality faithfully

### What is missing or incomplete

| Area | Status | Details |
|------|--------|---------|
| Transaction support | **Not implemented** | `commit()`/`rollback()` are no-ops. PR claims PEP 249 compliance but this violates the transaction contract |
| Resource cleanup | **Incomplete** | `close()` vs `dispose()` divergence; no query cleanup on re-execute; leaked message handler task |
| Error handling | **Broken** | `json.loads` change breaks error classification; message_handler crashes on bad JSON |
| Timeout/resilience | **Missing** | No timeout on `wait_for_response`; no retry or recovery on connection loss |
| Test coverage (error paths) | **Missing** | No tests for invalid SQL, closed connections, exhausted cursors via async PEP 249 |
| Type safety | **Weak** | Protocol mismatches, incorrect parameter type annotations |
| Documentation | **Partially done** | README updated but auto-commit assumption undocumented; `callproc` semantics unclear |

### Pre-existing issues surfaced by this PR (not regressions)

- `PoolJob(self.options.opts)` passes `JDBCOptions` as `creds` in `pool_client.py:89`
- `_is_tracing_channel_data` set in `connect()` but never declared in `__init__`
- `callproc` semantics were already wrong in sync cursor

---

## Manual Verification Checklist

Use this checklist to validate findings hands-on before providing feedback to the author.

### Critical path verification
- [ ] **CI test failure:** Go to the [failed test run](https://github.com/Mapepire-IBMi/mapepire-python/actions/runs/24206226809/job/70662645595) and check which tests are failing and why
- [ ] **Error classification:** Run an invalid SQL query through both sync and async cursors. Verify sync gets `ProgrammingError` and async gets generic `DatabaseError` (confirms C3)
- [ ] **Connection cleanup:** Open an async connection, run a query, close the connection. Verify no warnings about pending tasks or unclosed WebSockets

### Functional verification
- [ ] **Basic async path:** Run the happy-path examples from the updated README against a real IBM i system
- [ ] **Async iteration:** Test `async for row in cursor` with a multi-row result set
- [ ] **Pool compatibility:** Verify pool tests still pass with the `PoolJob` → `AsyncBaseJob` refactor
- [ ] **Parameter binding:** Test `execute()` with dict-style parameters (e.g., `{"id": 1}`) through the async cursor
- [ ] **Zero-row updates:** Execute `UPDATE ... WHERE 1=0` and check `cursor.rowcount` returns `0` not `-1`
- [ ] **Re-execute on same cursor:** Call `execute()` twice on the same async cursor, verify no server-side resource leaks

### Stress/edge cases
- [ ] **Concurrent queries:** Send multiple queries concurrently on the same `AsyncSQLJob` and verify all responses are correctly routed
- [ ] **Connection drop:** Kill the WebSocket server mid-query and verify the client does not hang indefinitely
- [ ] **Large result set:** Fetch 10,000+ rows via `fetchone()` loop and check performance (O(n^2) concern from `list.pop(0)`)

### Code inspection
- [ ] **`__all__` exports:** Run `python -c "from mapepire_python import CONNECTION_CLOSED"` — should raise `ImportError`
- [ ] **Deleted utils.py:** Grep for `asyncio.utils` or `from .utils` in the `asyncio/` directory — should find nothing
- [ ] **Copilot comments:** Review the [9 unresolved Copilot comments](https://github.com/Mapepire-IBMi/mapepire-python/pull/98) and cross-reference with this report

---

## Recommendation

**Do not merge in current state.** The PR has significant issues that need addressing:

1. **Must fix before merge:** C1, C2, C3, M1, M4, M6, M7 — these are bugs or resource leaks that will affect production use
2. **Should fix before merge:** M2, M3, M5, M9, M10 — these affect correctness or robustness
3. **Can fix in follow-up:** Everything else (minor/nit items, performance, type annotations)

The architecture (extracting `AsyncBaseJob`, removing `to_thread`) is sound and the
right direction. The main gaps are in lifecycle management, error handling, and
transaction support.
