# Globus Test Fixtures

This document identifies the real-world API payloads needed to test `downloader/as_globus.py` against realistic Globus Transfer service responses. Fixtures should be captured from a live session and stored as JSON files in a `tests/fixtures/globus/` directory.

---

## Source / destination collection problems

### a) Source collection does not exist
`submit_transfer()` raises `GlobusAPIError` immediately at submission. The exception propagates out of `_setup()` unhandled; the orchestrator worker catches it as `TaskStatus.FAIL`. Behavior is correct.

**Fixture:** `globus_submit_error_invalid_endpoint.json` — `GlobusAPIError` response body for an unknown collection UUID (HTTP 404 or Globus error code `EndpointNotFound`).

### b) Source path does not exist
`_make_transfer_data` sets `skip_source_errors=True`, so Globus silently skips missing files and the transfer **succeeds** with `subtasks_skipped_errors > 0`. The error surfaces only after completion via `_check_skipped_errors()`. Users get no feedback at submission time that a path was wrong.

**Fixtures:**
- `globus_task_succeeded_with_skips.json` — `get_task` response with `SUCCEEDED` and `subtasks_skipped_errors > 0`
- `globus_skipped_errors_page1.json` — `task_skipped_errors` paginated response; `source_path` values must match `file.globus_fn` format

### c) Source collection — permissions denied
Globus typically accepts the submission and fails the transfer post-start. The task ends up `FAILED`.

**Fixtures:**
- `globus_submit_error_permission_denied.json` — `GlobusAPIError` body for ACL rejection at submission (if Globus validates eagerly)
- `globus_task_failed.json` — `FAILED` status response with error detail (for post-submission failure path)

### d) Destination collection does not exist
Same code path as (a): `GlobusAPIError` at submission.

**Fixture:** `globus_submit_error_invalid_endpoint.json` (same shape, different error code — capture separately if the payload differs).

### e) Destination path does not exist
Not an error case — Globus creates destination directories automatically. No fixture needed.

### f) Destination collection — permissions denied
Same post-submission failure path as (c).

**Fixture:** `globus_task_failed.json` (same as c).

---

## Transfer outcome cases

### g) Partial success (some files complete)
Transfer `SUCCEEDED` but with `subtasks_skipped_errors > 0` and `files_skipped > 0`. The skipped files map to `FileStatus.Error` via `_check_skipped_errors()`. The `source_path` values in the skip response must match `file.globus_fn` for the comparison to work correctly.

**Fixtures:**
- `globus_task_succeeded_with_skips.json`
- `globus_skipped_errors_page1.json`

### h) Full success
All files transfer, `subtasks_skipped_errors == 0`.

**Fixtures:**
- `globus_submit_success.json` — `submit_transfer()` "Accepted" response (`task_id`, `submission_id`)
- `globus_task_succeeded_clean.json` — `SUCCEEDED` with representative file/byte counts and zero skips.

### i) Fails at first submission
`GlobusAPIError` from `client.submit_transfer()`. Root causes include quota exceeded, invalid configuration, or auth failure at submission time.

**Fixture:** `globus_submit_error_permission_denied.json` (quota/config errors share the same propagation path; capture at least one).

### j) Fails after polling
Task transitions `ACTIVE` → `FAILED`.

**Fixtures:**
- `globus_task_active.json` — one or more intermediate `ACTIVE` responses
- `globus_task_failed.json` — terminal `FAILED` response with error detail

---

## Reliability and resilience cases

### k) Transient service outage during polling
`client.get_task()` raises `GlobusAPIError` (HTTP 503 or rate-limit). **Current behavior is wrong**: the exception propagates out of `_poll_for_completion()` unhandled, and the worker marks the task `FAIL` permanently. A retry wrapper with backoff around `get_task()` is needed before this can be tested correctly.

**Fixture:** `globus_task_503.json` — `GlobusAPIError` response body for a service-unavailable error.

**Implementation gap:** No retry logic for transient errors during polling.

### l) Collection is paused (INACTIVE)
A paused collection puts the transfer in `INACTIVE` status. `GlobusTransferStatus.INACTIVE` is in `running()`, so the current code keeps polling — which is correct. However, `INACTIVE` is indistinguishable from `ACTIVE` in the task output; the heartbeat fires on each poll but `bytes_checksummed` will not change. Consider surfacing `INACTIVE` differently in `_get_extra()` or logs so a UI can display a "collection paused, waiting" state.

**Fixture:** `globus_task_inactive.json` — `get_task` response with `INACTIVE` status.

---

## Additional cases

### m) External cancellation of the transfer
If a transfer is canceled via the Globus web UI or another client, `get_task()` returns `status: "CANCELED"`. `GlobusTransferStatus["CANCELED"]` raises `KeyError` because `CANCELED` is not in the enum. **This is an unhandled crash path.**

**Fix needed:** Add `CANCELED` to `GlobusTransferStatus`, or handle it explicitly in `_check_transfer_status`.

**Fixture:** `globus_task_canceled.json` — `get_task` response with `CANCELED` status.

### n) Authentication expiry mid-session
Credentials expire between `submit_transfer()` and a later `get_task()` poll. Raises `GlobusAPIError` 401. Currently treated the same as a 503 (permanent `FAIL`). Correct given the program cannot re-authenticate automatically, but should produce a meaningful error message.

**Fixture:** `globus_task_401.json` — `GlobusAPIError` body for an expired-credential error.

### o) Checksum verification failure
`verify_checksum=True` is set in `_make_transfer_data`. A checksum mismatch causes a skipped error (`subtasks_skipped_errors > 0`) rather than a task failure. Same code path as (b/g). Worth a distinct fixture to confirm the payload shape for checksum-specific skip reasons.

**Fixture:** `globus_skipped_errors_checksum.json` — `task_skipped_errors` response where items have a checksum-failure reason field.

### p) Paginated skipped errors spanning multiple pages
`_check_skipped_errors()` uses `client.paginated.task_skipped_errors()`. Tests should verify that files beyond the first page are also classified as `FileStatus.Error`.

**Fixtures:**
- `globus_skipped_errors_page1.json`
- `globus_skipped_errors_page2.json`

---

## Implementation gaps exposed by this review

These should be resolved before or alongside the tests:

| Gap | Affected cases | Notes |
|-----|---------------|-------|
| `CANCELED` not in `GlobusTransferStatus` | m | `KeyError` crash on externally-canceled transfers |
| No retry for transient API errors during polling | k | 503/rate-limit permanently fails the task |
| `INACTIVE` indistinguishable from `ACTIVE` in output | l | Consider surfacing in `_get_extra()` or logs |

---

## Fixture summary

| File | Cases | Content |
|------|-------|---------|
| `globus_submit_success.json` | h | `submit_transfer` "Accepted" response — `task_id`, `submission_id` |
| `globus_submit_error_invalid_endpoint.json` | a, d | `GlobusAPIError` body for unknown collection UUID |
| `globus_submit_error_permission_denied.json` | c, f, i | `GlobusAPIError` body for ACL rejection |
| `globus_task_active.json` | h, j, k, l | `get_task` response — `ACTIVE` status |
| `globus_task_inactive.json` | l | `get_task` response — `INACTIVE` status |
| `globus_task_succeeded_clean.json` | h | `get_task` response — `SUCCEEDED`, no skips |
| `globus_task_succeeded_with_skips.json` | b, g, o | `get_task` response — `SUCCEEDED` with `subtasks_skipped_errors > 0` |
| `globus_task_failed.json` | c, f, j | `get_task` response — `FAILED` with error detail |
| `globus_task_canceled.json` | m | `get_task` response — `CANCELED` status |
| `globus_task_503.json` | k | `GlobusAPIError` body for service outage |
| `globus_task_401.json` | n | `GlobusAPIError` body for expired credentials |
| `globus_skipped_errors_page1.json` | b, g, o, p | `task_skipped_errors` first page; paths must match `file.globus_fn` format |
| `globus_skipped_errors_page2.json` | p | `task_skipped_errors` second page |
| `globus_skipped_errors_checksum.json` | o | `task_skipped_errors` with checksum-failure reason |