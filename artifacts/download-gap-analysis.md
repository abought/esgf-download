# Download Feature: Requirements Analysis & Gap Analysis

This document records the requirements of the existing HTTPS download pipeline and evaluates
how well the new `HttpsDownloadTask` (in `esgpull/downloader/as_https.py`) satisfies them.

## Scope

**Existing implementation (in scope):**
- `esgpull/cli/download.py` — CLI entry point
- `esgpull/esgpull.py:download()` and `iter_results()` — orchestration and DB/event logic
- `esgpull/downloader/pipeline.py` — `Task`, `Processor` classes
- `esgpull/downloader/as_https.py` — legacy helpers only: `DownloadCtx`, `BaseDownloader`, `Simple`
- `esgpull/downloader/fs.py` — `Filesystem`, `FilePath`, `FileObject`, `Digest`, `FileCheck`
- `esgpull/config.py` — `Download` config model

**New refactor (gap analysis target):**
- `esgpull/downloader/as_https.py:HttpsDownloadTask`
- `esgpull/downloader/base.py` — `DownloadTask`, `TaskResult`, `FileResult`, `TaskHeartbeat`, `TaskStartInfo`
- `esgpull/downloader/orchestrator.py` — `Orchestrator`
- `esgpull/downloader/factory.py` — `make_https_tasks`, `make_globus_tasks`

---

## Part 1: Requirements of the Existing System

### a) Security & Configuration

**SSL/TLS handling** (`pipeline.py:25-36`, `config.py:83`):
- A module-level singleton `default_ssl_context` is lazily initialized by `load_default_ssl_context()` on first `Processor` creation, and its initialization is logged.
- On OpenSSL 3+, a custom context is built with option flag `0x4` (`OP_LEGACY_SERVER_CONNECT`) to accommodate older ESGF nodes with non-RFC-compliant TLS. On OpenSSL 1, Python's default (`True`) is used.
- `config.download.disable_ssl = True` bypasses all TLS verification (`verify=False`). Settable per-run via `--disable-ssl`.

**Checksum verification** (`config.py:84`, `fs.py:155-170`):
- If `config.download.disable_checksum` is `False` (default), a `Digest` object (SHA-256) is created per `Task` and updated per chunk during streaming.
- Verification happens after streaming completes, inside `iter_results` via `fs.finalize()`.

**Error handling** (`pipeline.py:109-117`):
- `Simple.stream()` calls `resp.raise_for_status()` once at stream start.
- Per-chunk overrun detection: `DownloadCtx.error` (`completed > file.size`) triggers `DownloadSizeError`.
- `Task.stream()` catches specific exception types: `HTTPError`, `DownloadSizeError`, `GeneratorExit`, `ssl.SSLError`, `FileNotFoundError`. All become named `Err` results surfaced to the user.
- No retry logic anywhere in the existing path.

**Key config options** (`config.py:79-85`):

| Setting | Default | Purpose |
|---|---|---|
| `chunk_size` | 64 MiB (1<<26) | HTTPX streaming chunk size |
| `http_timeout` | 20s | HTTPX client timeout |
| `max_concurrent` | 5 | `asyncio.Semaphore` limiting parallel downloads |
| `disable_ssl` | `False` | Skip TLS verification |
| `disable_checksum` | `False` | Skip SHA-256 verification |
| `show_filename` | `False` | Optional extra progress bar column |

---

### b) Database Updates

Status transitions are managed in `esgpull.py:download()` and gated by `use_db=True` (always true from the CLI):

1. **Before download** (`esgpull.py:384-436`): All files set to `FileStatus.Starting` in-memory; `self.db.add(*processor.files)` persists this for files that will actually be downloaded.
2. **After each file result** (`esgpull.py:457-496`): Per result from `iter_results`:
   - `Ok` → `file.status = FileStatus.Done`
   - `Err` → `file.status = FileStatus.Error`
   - `self.db.add(result.data.file)` called immediately after each result.
3. **On cancellation** (`esgpull.py:497-506`): Remaining files (not yet processed) set to `FileStatus.Cancelled` and batch-written in the `finally` block.

**`FileStatus` lifecycle:** `New` → `Queued` → `Starting` → (`Done` | `Error` | `Cancelled`)

---

### c) Progress Communication

Progress is driven by Rich and operates at two levels:

**Main progress bar** (overall queue, `esgpull.py:386-391`):
- Spinner, M-of-N count, time remaining, error count.
- Advances by 1 on each `Ok` result. On `Err`, total is decremented and error count incremented.

**Per-file progress bar** (transient, `esgpull.py:392-413`):
- SHA prefix, percentage, byte bar, transfer speed, data node, optionally filename.
- Tasks start `visible=False, start=False`.
- Become visible via a `start_callback` (`partial(file_progress.start_task, task_id)`) that fires when the semaphore is acquired and download begins (`pipeline.py:89-91`).
- Updated per-chunk via `progress.update(task.id, completed=result.data.completed)` in `iter_results`.
- On finish: bar hidden, summary line (sha · size · speed · data_node) printed to console and logged.

**`DownloadCtx` as the progress carrier** (`as_https.py:162-211`):
- `Simple` yields the same mutable `ctx` instance each chunk, with `completed` accumulating bytes and `chunk` holding the latest data.
- `pipeline.py:Task.stream()` clears `ctx.chunk = None` after each write to avoid retaining chunk data.
- `ctx.start_time` is set inside `Simple.stream()` just before the HTTP request; used for `Event.file_complete` timing.
- `ctx.finished` (`completed == file.size`) and `ctx.error` (`completed > file.size`) are the completion/overrun signals.

**`--quiet` flag**: `show_progress=False` disables the Rich `Live` display; the final summary count is still printed.

---

### d) Filesystem Interactions, Temp Storage & Validation

**Path resolution** (`fs.py:83-89`): `Filesystem.__getitem__(file)` returns a `FilePath` with:

| Path | Location | Purpose |
|---|---|---|
| `tmp` | `config.paths.tmp / "{sha}.part"` | Active download write target |
| `done` | `config.paths.tmp / "{sha}.done"` | Completed but not yet verified/moved |
| `drs` | `config.paths.data / file.local_path / file.filename` | Final installed location |

Paths are content-addressed (keyed by sha), making concurrent downloads of the same file safe.

**Download write sequence** (`pipeline.py:89-108`):
1. `fs.open(file)` opens `{sha}.part` for binary write.
2. Each chunk written to `.part` immediately.
3. When `ctx.finished`, `file_obj.to_done()` closes the buffer and renames `.part` → `.done`.

**Pre-download check** (`pipeline.py:151-155`): If `fs[file].drs.is_file()`, the file is excluded from download tasks.

**Validation and finalization** (`esgpull.py:344`, `fs.py:195-207`):
- `fs.finalize(file, digest=digest)` is called once `task.finished` in `iter_results`.
- `check()` verifies: (1) `st_size == file.size`, then (2) SHA-256 checksum — using the running `Digest` from streaming if available, or re-reading the `.done` file otherwise.
- On `FileCheck.Done` (passes both): `move_to_drs()` renames `.done` → final DRS path, with `shutil.copyfile` fallback for cross-filesystem moves.
- On `FileCheck.BadSize` or `FileCheck.BadChecksum`: returns `Err`; the `.done` file is **left on disk** (no cleanup in the legacy path for this case).
- When `disable_checksum=True`: `compute_checksum()` returns `file.checksum` as-is, making the comparison always pass. The size check still runs independently.

---

## Part 2: Gap Analysis — `HttpsDownloadTask` vs. Requirements

### a) Security & Configuration

| Requirement | Satisfied? | Notes |
|---|---|---|
| `disable_ssl` | ✅ | `_make_ssl_context(disable_ssl)` applies same OpenSSL version logic |
| `disable_checksum` | ✅ | `digest = Digest(file) if not self._disable_checksum else None` per file |
| `chunk_size` | ✅ | Constructor parameter |
| `http_timeout` | ✅ | Passed to `httpx.AsyncClient` |
| `max_concurrent` | ✅ (delegated) | Correctly left to `Orchestrator` worker pool; no semaphore needed in the task |
| HTTP/SSL error propagation | ❌ | `except Exception: pass` silently discards all exceptions; no error type is recorded or surfaced |
| SSL context info logging | ⚠️ | `_make_ssl_context()` is stateless and silent; legacy logged which OpenSSL version was in use |

---

### b) Database Updates

The task itself has no DB interaction (intentional separation of concerns), but the following responsibilities are currently unassigned to any caller:

| Requirement | Satisfied? | Notes |
|---|---|---|
| `FileStatus.Starting` before download | ❌ | Nothing currently transitions files out of `Queued` in the new path |
| Per-file status update on completion | ⚠️ | `TaskResult` contains `FileResult(status, file)` — sufficient data for a caller to update the DB, but no caller currently does so |
| `FileStatus.Cancelled` on interrupt | ❌ | No equivalent to the legacy `finally` block that writes `Cancelled` on keyboard interrupt |
| `Event.file_complete` / `Event.file_error` / `Event.dataset_complete` | ❌ | None emitted anywhere in the new path |

---

### c) Progress Communication

| Requirement | Satisfied? | Notes |
|---|---|---|
| Notification that a file started | ✅ | `_emit_start(TaskStartInfo(...))` called at top of `_run`, includes `to_download` and `already_done` lists |
| Per-chunk byte-level progress | ✅ | `_emit_heartbeat(files_completed, bytes_completed)` called after every chunk |
| Heartbeat after file completes | ✅ | Final heartbeat with incremented `files_completed` emitted after `Done` (line 129) |
| Per-file identifying info (sha, data_node, filename) | ⚠️ | `TaskHeartbeat` carries only aggregate counts and `task_label`; sha is embedded in label by `factory.py` convention (`"https:{file.sha}"`), but `data_node` and `filename` are not present |
| `start_time` for speed/elapsed display | ⚠️ | Set in `DownloadTask.run()` before `_pre_check`, not at HTTP connection time; slightly less accurate than legacy `ctx.start_time` |
| Suppression of progress (`--quiet`) | ✅ (by design) | Callback-driven; registering no heartbeat callbacks is equivalent to `--quiet` |

---

### d) Filesystem Interactions & Validation

| Requirement | Satisfied? | Notes |
|---|---|---|
| Write to `.part` temp path | ✅ | `aiofiles.open(file_path.tmp, 'wb')` — same path as legacy |
| Rename `.part` → `.done` on completion | ✅ | `file_path.tmp.rename(file_path.done)` |
| Move `.done` → final DRS path | ✅ | `_cleanup` calls `self._fs.move_to_drs()` including cross-filesystem fallback |
| Delete temp files on failure | ✅ | `_cleanup` deletes both `.done` and `.tmp` for non-`Done` results — an **improvement** over legacy, which left `.done` files on disk after a failed `finalize()` |
| Skip already-downloaded files | ✅ | `_pre_check` checks `drs.is_file()` |
| Checksum verification | ✅ | Inline: `digest.hexdigest() == file.checksum` after stream completes |
| Size validation when checksum disabled | ❌ | When `disable_checksum=True`, `digest is None` and `if digest is None or ...` is unconditionally `True` — a truncated download is silently marked `Done`. Legacy `fs.check_impl()` always checked size independently of checksum setting. |
| Distinguish `BadSize` vs `BadChecksum` | ❌ | New code only knows "passed" or "failed"; legacy `FileCheck` enum distinguished these for diagnostics |
| Exception details preserved on failure | ❌ | `except Exception: pass` — all exception information is lost |

---

## Summary of Gaps

| Priority | Gap | Location |
|---|---|---|
| High | `except Exception: pass` silently discards all error information | `as_https.py:_run` |
| High | No size check when `disable_checksum=True` — truncated downloads silently accepted | `as_https.py:_run` |
| High | `FileStatus.Starting` never set; `FileStatus.Cancelled` never set on interrupt | unassigned |
| High | No `Event.file_complete` / `Event.file_error` / `Event.dataset_complete` emissions | unassigned |
| Medium | DB status updates unassigned to any caller in the new path | unassigned |
| Medium | `data_node` / `filename` not available in `TaskHeartbeat` for progress display | `base.py:TaskHeartbeat` |
| Low | `start_time` captured before `_pre_check`, not at HTTP connection establishment | `base.py:DownloadTask.run` |
| Low | SSL context initialization no longer logged | `as_https.py:_make_ssl_context` |

---

## `FileStatus` Enumeration Notes

Documented for future reference; no changes planned at this time.

| Status | Assessment |
|---|---|
| `New` | Effectively unused at rest — ORM column default, immediately overwritten to `Queued` in normal flows |
| `Queued` | Core state; well-used |
| `Starting` | Transient; semantically wrong for long Globus transfers (may persist for hours) |
| `Started` | Used for Globus fire-and-forget and synda migration only; never read as a filter |
| `Pausing` | **Completely unused** — no pause/resume mechanism exists |
| `Paused` | **Completely unused** — no pause/resume mechanism exists |
| `Error` | Core terminal state; retryable |
| `Cancelled` | Set only in legacy path on interrupt; retryable |
| `Done` | Core terminal state |

**Notable absence:** There is no permanent failure state. `Error` and `Cancelled` are the only
non-done terminal statuses, and both are retryable via `retry`. A file that repeatedly fails
will cycle between `Error` and `Queued` indefinitely. Scenarios where a permanent state would
be useful include known-dead URLs, repeated checksum failures suggesting a corrupt source, or
explicit Globus `FAILED` results. A future `Abandoned` status (excluded from `retryable()`)
would address this, but is out of scope for the current PR.
