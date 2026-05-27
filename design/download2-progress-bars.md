# Plan: Add Progress Bars to `download2()` and Replace `download()`

## Context

`download2()` in `esgpull/esgpull.py` is a refactored replacement for `download()` that introduces Globus batch transfer support alongside HTTPS. The core logic (DB status updates, cancellation, error batching) is implemented, but the entire UI layer is absent — no progress bars, no completion logging, no `Live` context. This plan adds that UI layer and then retires the old `download()`.

The new system uses a callback/event architecture:
- `orchestrator.on_task_start(cb)` → fires per task after `_pre_check`, before data moves
- `orchestrator.on_heartbeat(cb)` → fires per chunk (HTTPS, ~1 MB granularity) or per poll (Globus, ~60 s)
- `async for result in orchestrator.iter_results()` → yields one `TaskResult` per task when complete

Key data facts:
- HTTPS tasks: `task_label = file.file_id`, one file per task, heartbeat `bytes_completed` is cumulative bytes (starts at 0 for a fresh single-file task), `bytes_expected = file.size`
- Globus tasks: `task_label = origin_id` (source collection), N files per task, heartbeat `bytes_completed = bytes_checksummed` (not bytes transferred — misleading), `n_files_completed/n_files_expected` is more meaningful
- `TaskStartInfo.files` = files that will actually be downloaded (post `_pre_check`); `start_info.already_done` = files already on disk (skipped)
- `TaskResult.files` = newly downloaded files only (omits `already_done`)

---

## Critical Files

| File | Role | Change |
|---|---|---|
| `esgpull/esgpull.py` | `download2()` | Primary change target |
| `esgpull/downloader/as_https.py` | `HttpsDownloadTask._run()` | Pre-implementation fix (exception handling) |
| `esgpull/downloader/as_globus.py` | `GlobusDownloadTask` | Pre-implementation fixes + imports |
| `esgpull/downloader/factory.py` | Result handler factories | New `_make_globus_result_handler` |
| `esgpull/downloader/base.py` | `TaskHeartbeat`, `TaskStartInfo` | Read-only; add imports |
| `esgpull/downloader/pipeline.py` | Old `Processor` (HTTPS only) | Delete after rename |
| `esgpull/tui.py` | `UI.make_progress`, `ErrorCountColumn` | Read-only; already has everything needed |

---

## Pre-implementation fixes required

These bugs in the downloader layer corrupt the status values that `_process_result` writes to the
database and emits as plugin events. They must be fixed before the plan's main work begins.

### Fix 1 — `as_https.py`: bare `except:` swallows `CancelledError`

**Location**: `HttpsDownloadTask._run()`, the except clause around the chunk-download loop.

**Problem**: `except:` catches `asyncio.CancelledError` (a `BaseException`). The exception is logged
as a download failure and the file is marked `FileStatus.Error`. The outer `except (CancelledError,
KeyboardInterrupt)` in `base.py`'s `run()` never fires, so `to_cancel()` is never called. The
orchestrator receives a normal Error result rather than a cancellation signal; `_process_result` then
emits `Event.file_error` for a file that was merely interrupted.

**Fix**: `except Exception:` — lets `CancelledError` and `KeyboardInterrupt` propagate to `base.py`.

### Fix 2 — `as_globus.py`: `to_fail()` uses `FileStatus.Error` for submitted transfers

**Location**: `GlobusDownloadTask.to_fail()` (lines 140–149).

**Problem**: `to_fail()` is called by the orchestrator when `task.run()` raises any unhandled
`Exception` (e.g., a network error during status polling). It marks all files `FileStatus.Error`
unconditionally. If the transfer was already submitted, the Globus service is still running it —
those files should be `FileStatus.Started` so `check_globus_transfers()` can resolve them on the
next run. Instead, `_process_result` writes Error to the DB and emits `Event.file_error`, and the
next run may re-submit a duplicate transfer.

**Fix**: mirror `to_cancel()` — if `self._transfer_id is not None`, return `FileStatus.Started`;
otherwise `FileStatus.Error`.

### Fix 3 — `as_globus.py`: `INACTIVE` transfer status loops forever

**Location**: `_check_transfer_status()`, the `ACTIVE | INACTIVE` match arm.

**Problem**: `INACTIVE` means the Globus transfer is paused and requires user intervention
(expired credential, endpoint offline). The current code treats it identically to `ACTIVE` and
continues polling indefinitely, producing no signal.

**Fix**: break out `INACTIVE` into its own arm; log a warning and return a terminal failure result
so the files are marked and the user is notified rather than the process hanging silently.

### Fix 4 — `as_globus.py`: poll backoff formula grows without bound

**Location**: `_run()`, lines 258–260.

**Problem**: `self._poll_time = max(self._poll_time + 15, max_poll_time)` should be `min`. With
`max_poll_time = 600` and initial `poll_time = 60`: the first adjustment gives `max(75, 600) = 600`,
then `615`, `630`, etc. — unbounded growth rather than a cap at 10 minutes.

**Fix**: `self._poll_time = min(self._poll_time + 15, max_poll_time)`.

---

## Known follow-on issues (out of scope for this plan)

- **Orchestrator hang**: if a worker exits before placing a result in `_task_results` (e.g., due to
  a bug in code before `task.run()`), `iter_results()` blocks forever on `_task_results.get()`.
  The worker try/except only wraps `task.run()`, not the callback-wiring above it.
- **Blocking Globus SDK calls**: `submit_transfer()` and `get_task()` are synchronous HTTP calls
  inside `async def _run()`, blocking the event loop during each Globus poll cycle.

---

## Decomposition

`download2()` after the full plan is implemented would own five distinct concerns inline: pre-flight,
task construction, UI setup, DB/event persistence, and orchestration. The decomposition below separates
these so `download2()` reads as a flat sequence of high-level steps.

### Responsibility map

| Concern | Owner |
|---|---|
| Task creation + task-level DB callbacks | `factory.py` (unchanged) |
| Task-level UI callback wiring | `download2()`, post-factory |
| Progress bar state + per-result UI updates | `DownloadProgressUI` class |
| DB writes + plugin event emission | `_process_result` inner function |
| Orchestration loop + cancellation | `download2()` skeleton |

### `DownloadProgressUI` — new class (in `esgpull.py` or `esgpull/downloader/progress.py`)

Owns all Rich state and exposes three methods. `download2()` never touches Progress objects directly.

```python
class DownloadProgressUI:
    def __init__(self, ui: UI, queue_size: int, show_filename: bool):
        self.main_progress = ui.make_progress(
            SpinnerColumn(), MofNCompleteColumn(),
            TimeRemainingColumn(compact=True, elapsed_when_finished=True),
            ErrorCountColumn(),
        )
        file_columns = [...]   # byte-level HTTPS columns
        if show_filename:
            file_columns += [...]
        self.https_progress = ui.make_progress(*file_columns, transient=True)
        self.globus_progress = ui.make_progress(
            SpinnerColumn(),
            TextColumn("[cyan]Globus [{task.fields[origin_id]}]"),
            MofNCompleteColumn(), TimeRemainingColumn(compact=True),
            transient=True,
        )
        self._main_task_id = self.main_progress.add_task("", total=queue_size, nb_errors=0)
        self._https_task_ids: dict[str, TaskID] = {}
        self._globus_task_ids: dict[str, TaskID] = {}
        self._error_count = 0
        self._show_filename = show_filename

    def register_callbacks(
        self,
        orchestrator: Orchestrator,
        https_tasks: list[HttpsDownloadTask],
        globus_tasks: list[GlobusDownloadTask],
    ) -> None:
        # Orchestrator-level: shared across all task types
        orchestrator.on_task_start(self._on_any_start)
        # Per-task-type: only where display logic diverges
        for task in https_tasks:
            task.on_start(self._on_https_start)
            task.on_heartbeat(self._on_https_heartbeat)
        for task in globus_tasks:
            task.on_start(self._on_globus_start)
            task.on_heartbeat(self._on_globus_heartbeat)

    def on_result(self, result: TaskResult, live: Live | DummyLive) -> None:
        """Update bars and print completion lines. NOT called from the cancel path."""
        ...  # advance main_progress, log lines, hide/remove per-task bars

    # Private callback implementations (_on_any_start, _on_https_start, etc.)
```

### Factory-created result handlers — two new factory functions

`_process_result` currently has three distinct jobs: set file statuses, emit plugin events, and
update the Globus transfer record. The latter two can each become a factory-created callable,
following the same pattern as `_make_globus_on_start` in `factory.py`.

**`_make_globus_result_handler(app)`** — lives in `factory.py` alongside `_make_globus_on_start`;
the two functions are symmetric lifecycle hooks for the same Globus task.

```python
# factory.py
def _make_globus_result_handler(app: 'Esgpull') -> Callable[[GlobusTaskResult], list]:
    def handler(result: GlobusTaskResult) -> list:
        transfer = app.db.session.get(GlobusTransfer, result.globus_task_id)
        if transfer is None:
            return []
        transfer.status = result.globus_task_status
        transfer.last_updated = datetime.now(timezone.utc)
        transfer.completion_time = result.end_time
        return [transfer]   # caller accumulates; handler does not mutate pending directly
    return handler
```

Returning items rather than mutating `pending` keeps the handler free of shared mutable state.
`make_globus_tasks` builds a `dict[task_label, handler]` for the caller to use.

**`_make_file_event_emitter(app)`** — lives in `esgpull.py` (needs `app.fs` and `app.db`; not a
task-creation concern, so not in `factory.py`):

```python
# esgpull.py
def _make_file_event_emitter(app: 'Esgpull') -> Callable[[FileResult, TaskResult], None]:
    def emitter(fr: FileResult, result: TaskResult) -> None:
        match fr.status:
            case FileStatus.Done:
                emit(Event.file_complete, file=fr.file,
                     destination=app.fs[fr.file].drs,
                     start_time=result.start_time, end_time=result.end_time)
                if fr.file.dataset is not None:
                    if app.db.scalars(sql.dataset.is_complete(fr.file.dataset))[0]:
                        emit(Event.dataset_complete, dataset=fr.file.dataset)
            case FileStatus.Error:
                emit(Event.file_error, file=fr.file, exception=result.msg)
            case FileStatus.Cancelled:
                pass
    return emitter
```

### `_process_result` — narrow inner function in `download2()`

With the two handlers factory-created, `_process_result` closes over only `pending`,
`file_event_emitter`, and `globus_handlers`. No Esgpull-specific members (`db`, `fs`, `sql`,
`GlobusTransfer`, `datetime`, `timezone`) are needed directly.

```python
# In download2():
file_event_emitter = _make_file_event_emitter(self)
globus_handlers = {
    task._task_label: _make_globus_result_handler(self)
    for task in globus_tasks
}

def _process_result(result: TaskResult) -> None:
    for fr in result.files:
        fr.file.status = fr.status      # Done, Error, Cancelled — all persisted
        pending.append(fr.file)
        file_event_emitter(fr, result)
    if handler := globus_handlers.get(result.task_label):
        pending.extend(handler(result))
```

**Retryability note**: verify `sql.file.ready_for_download()` picks up both `FileStatus.Error`
(transient failure) and `FileStatus.Cancelled` (interrupted) on the next run. If the enum has no
permanent-error value, that is a pre-existing limitation.

### `download2()` orchestration skeleton

After decomposition `download2()` reads as:

```python
async def download(self, transfer_client, show_progress=True):
    await self.check_globus_transfers(transfer_client)

    queue = self.db.scalars(sql.file.ready_for_download())
    if not queue:
        return

    by_url, by_globus = partition_by_transfer_method(queue)
    https_tasks = make_https_tasks(list(by_url), self)
    globus_tasks = make_globus_tasks(by_globus, self, transfer_client)

    progress_ui = DownloadProgressUI(self.ui, len(queue), self.config.download.show_filename)
    orchestrator = Orchestrator(max_concurrent_local=self.config.download.max_concurrent)
    orchestrator.on_task_start(_on_task_start)   # DB: file → Starting

    progress_ui.register_callbacks(orchestrator, https_tasks, globus_tasks)

    all_tasks = https_tasks + globus_tasks
    for task in https_tasks:
        orchestrator.add_local_task(task)
    for task in globus_tasks:
        orchestrator.add_remote_task(task)
    for file in (f for t in all_tasks for f in t._files):
        file.status = FileStatus.Starting
    self.db.add(*[f for t in all_tasks for f in t._files])

    pending: list[File | GlobusTransfer] = []

    def _process_result(result): ...   # as above

    try:
        with self.ui.live(*progress_ui.progress_bars(), disable=not show_progress) as live:
            async for result in orchestrator.iter_results():
                _process_result(result)
                progress_ui.on_result(result, live)
                if len(pending) >= _BATCH_SIZE:
                    self.db.add(*pending); pending.clear()
    except (asyncio.CancelledError, KeyboardInterrupt):
        for result in await orchestrator.collect_cancels():
            _process_result(result)   # DB only — no UI update after Live exits
        raise
    finally:
        if pending:
            self.db.add(*pending)
```

---

## Friction Points and Concern Bleed

These are the places where the separation is imperfect or requires explicit care:

**1. Globus DB callback (factory) and Globus UI callback (`download2`) are wired separately**
`make_globus_tasks` registers `_make_globus_on_start` (DB persistence) directly on each task.
`download2()` registers `_on_globus_start` (progress bar) afterward. Two on_start callbacks on the
same Globus task registered in two different places — functional but a reader must look in both
`factory.py` and `download2()` to understand the full start sequence. Document this explicitly.

**2. Error count is owned by `DownloadProgressUI` but also affects the return value**
`DownloadProgressUI` tracks `_error_count` internally to update `main_progress`. The caller
(`download2()`) may also need a final error count for its return value or logging. These should not
diverge — either `DownloadProgressUI.error_count` is the single source of truth and the caller reads
it, or the caller tracks errors independently and passes the count in. Decide one owner.

**3. Plugin events still cross-cut, but now isolated in a factory**
`_make_file_event_emitter` encapsulates the `emit()` calls and the `app.fs`/`app.db` dependencies.
`_process_result` calls it without knowing the details — the mixing is pushed into the emitter
factory rather than eliminated. The remaining concern: `_make_file_event_emitter` is not in
`factory.py` (it belongs to the broader app, not task construction) so it lives in `esgpull.py`
as a module-level private function. This is a reasonable home but worth documenting.

**4. Cancellation path must not call `progress_ui.on_result`**
In the `except` block, `_process_result` is called for cancel results to flush DB state. Calling
`progress_ui.on_result` there would crash (the `Live` context has already exited). This is an
implicit contract between the orchestration loop and the cancellation handler. Make it explicit:
name the pattern in a comment, or accept a `skip_ui: bool` param on `_process_result`.

**5. `_process_result` closure is now narrow**
After factory extraction it closes over only `pending`, `file_event_emitter`, and `globus_handlers`.
The wide `self.*` dependencies have moved into the factory-created callables. If unit tests are still
needed, the three closed-over values can be passed as explicit parameters to make it a module-level
function — but this is no longer urgent given the reduced scope.

**6. Pre-existing: Globus task_id may be None at `on_start`**
`GlobusTaskStartInfo.globus_task_id` is None for new transfers when `_make_globus_on_start` fires
(the FIXME in `as_globus.py`). The DB record is written with a null task_id. This bleed predates
this plan and is not introduced here, but it means the Globus DB callback and Globus UI callback
both silently handle a partially-initialised state at start time.

---

## Implementation Steps

### Step 1 — Add imports to `esgpull.py`

```python
from esgpull.downloader.base import TaskHeartbeat, TaskStartInfo
from esgpull.downloader.as_globus import GlobusTaskResult, GlobusTaskStartInfo, GlobusTaskHeartbeat
```

### Step 2 — Implement `DownloadProgressUI`

Implement as described in the Decomposition section above. Place in `esgpull.py` (private) or
`esgpull/downloader/progress.py` if it grows large. Key methods:
- `__init__(ui, queue_size, show_filename)`
- `register_callbacks(orchestrator, https_tasks, globus_tasks)`
- `on_result(result, live)` — advances bars, prints completion lines, hides finished bars
- `progress_bars()` — returns the three Progress objects in display order for `ui.live()`

### Step 3 — Add factory functions for result handlers

Add `_make_globus_result_handler(app)` to `factory.py` alongside `_make_globus_on_start`.
Add `_make_file_event_emitter(app)` as a module-level private function in `esgpull.py`.
Update `make_globus_tasks` to return a `dict[task_label, handler]` alongside the task list,
or build the dict in `download2()` by calling `_make_globus_result_handler` per task.

### Step 4 — Write narrow `_process_result` as inner function

Implement as shown in the Decomposition section: three lines of generic logic, two factory-call
dispatches. No direct `self.*` references.

### Step 5 — Rewrite `download2()` as the orchestration skeleton

Wire tasks, register callbacks, run the loop. No inline Rich or DB logic in the method body itself.

### Step 6 — Rename `download2()` → `download()`

1. Delete old `download()` (lines 386–518) and `iter_results()` helper
2. Rename `download2` → `download`
3. Update CLI callers in `esgpull/cli/*.py`

### Step 7 — Clean up `pipeline.py`

Delete `esgpull/downloader/pipeline.py`. Remove `DownloadCtx`, `BaseDownloader`, `Simple` from
`as_https.py`.

---

## Known Gotchas

- `task_label` for HTTPS is `file.file_id` (not `file.sha`) — all mappings in `DownloadProgressUI` must key on `file_id`
- Globus `bytes_completed` = `bytes_checksummed` (not transferred); use `n_files_completed` for Globus bars
- `start_info.files` may be empty if all files were already on disk; guard in `_on_https_start` before creating a bar
- `TaskResult.files` omits `already_done`; `_on_any_start` must credit those to `main_progress` immediately, not at result time
- `DownloadProgressUI._error_count` must be the single source of truth for error tracking; do not maintain a parallel count in `download2()` (see Friction Point 2)

---

## Verification

1. HTTPS-only download: per-file bars appear, fill, disappear; completion line printed above
2. Globus download: per-batch N/M file bar advances per poll (~60 s); no misleading byte display
3. Mixed queue (HTTPS + Globus): `main_progress` total is correct; both bar types visible simultaneously
4. Ctrl-C mid-download: cancellation logged, DB updated, no crash
5. File already on disk: counts in `main_progress` total; no spurious progress bar created
6. `show_progress=False`: no output, no crash
