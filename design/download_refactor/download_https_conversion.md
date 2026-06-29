# Refactor of existing url-based functionality

## Purpose
The original download path of esgpull used the legacy `dpownloader/as_https.py:Simple()` class. This is implemented in `esgpull.py:download()` on the `main` branch.

We wish to convert this to use the new `downloader/as_https.py:HttpsDownloadTask()` approach, orchestrated by the new `downloader/orchestrator.py:Orchestrator()` class.

We will begin by decomposing the original function into a set of system requirements, captured in this document.


### Limitations of scope
In order to break this work down into manageable pieces, this ONLY covers the existing functionality: files that are downloaded directly from an HTTPS URL. The new globus-based download mechanisms are NOT IN SCOPE.

## Requirements
### Handle failure modes of a local filesystem
URL-based downloads copy to a local filesystem.

* This is a BIG DATA repository, downloading to shared systems. 
  * Warn the user if the designated copy destination does not have space for all files
  * Check before download begins
  * Catch errors and log if a file fails due to disk space at runtime. If the disk is out of space, all downloads should be stopped immediately.

### Must be backwards-compatible
Existing progress bars and state tracking are intended to work the same way after this change. We are changing _how_ the operation runs, but we wish to preserve compatibility with existing installs. We should identify relevant tests and feature requirements for the new https download function.

As part of this change, the work will be implemented in a NEW FUNCTION: `esgpull.py:download2_https()`. After all changes have been made, we will eventually remove the original download function in a subsequent phase of work.  

#### Known exceptions
This introduces a change to how errors are reported, including exit status code. We MAY choose to alter the behavior of file cancels (eg whether they must be explicitly requeued as `esgpull retry`- that can sort of be explained in an interactive tool, but doesn't make sense for a cron job that could be interrupted by system maintenance)

### UI
* Create a progress bar UI object to encapsulate status updates for various events (start, stop, end)
  1. For each file downloaded, show current and total amount downloaded. Hide the progress bar for a single file once that file is done downloading.
  2. Show a progress bar for TOTAL download. This progress bar is always shown



### Download phases
The below captures key phases of the download process, based on study of the pre-existing download code:

* Identify files to download

* Database: State tracking mechanism
  * The task
    * File tasks are a thin wrapper around individual files. URL-based downloads are not tracked in the database separately.
    * Task-level status is only used for internal book keeping / error reporting at runtime. Individual file status is the unit we track in the database.
    * It is possible for a task to COMPLETE even if the file fails to download.
  * The file
    * Set to queued by `esgpull update` or `esgpull retry` commands.
    * Set to `FileStatus.Started` when the task actually begins to run (not just when it is scheduled).
    * Set to `FileStatus.Error` or `FileStatus.Done` when file  
      * **Resolved, with a known gap:** tracked states are the existing `FileStatus` values (`Queued`, `Starting`, `Started`, `Error`, `Cancelled`, `Done`) — no new states were added. On a *clean* interrupt (Ctrl-C / `SIGTERM` → `CancelledError`), in-flight and still-queued files are explicitly written to `Cancelled` by `_drain_cancels()`. On an *unclean* termination (`SIGKILL`, power loss, OOM-kill) files left in `Starting`/`Started` are **not** automatically detected or recovered by `download2_https()` itself — recovery depends on a subsequent `esgpull retry` invocation, which already accepts `Starting`/`Started` as a valid source status. So "correctly handled if terminated unexpectedly" today means *recoverable via a manual/scripted follow-up command*, not self-healing on the next plain `download` run. Flag as a follow-up if self-healing is required.


### Logging and reporting
* This tool is expected to run in the background, such as a cron job. State transitions (start, retry, success, error) must be captured in logs. Consider mechanisms for a sysadmin to be alerted when download errors occur, such as exiting the program with  specific error status codes if:
  * one or more files fail to download (possible temporary error with remote servers)
  * all files fail to download (possibly a problem with the local system or usage)
* **Resolved:** `cli/download.py` exits 0 (success), 1 (some files failed — exception, but the queue made progress overall, possible transient remote-server issue), or 2 (all files failed, or a local problem: lockfile conflict / insufficient disk space). See the "Compatibility gap" note above re: loss of per-file detail in this same output.
* **Resolved:** existing plugin `Event` enum (`file_complete`, `file_error`, `dataset_complete` — `esgpull/plugin.py`) was not changed; `download2_https()` reuses all three unmodified, so existing plugins keep working without modification. One behavior worth flagging for plugin authors: `Event.file_error` is now emitted for both genuine download errors and user-cancellations, distinguished only by the exception instance passed (`DownloadCancelled` vs a generic `RuntimeError`) — a plugin wanting different handling for "cancelled" vs "actually failed" must `isinstance()`-check the exception rather than relying on a separate event.

## Follow-up items (deferred beyond this phase)

These were identified while designing `download2_https()` but are intentionally
out of scope for this phase. Captured here so they aren't lost.

### Shared lockfile across download/update/retry
`download2_https()` introduces a lockfile to detect concurrent download runs
(see "Check for conflicts" above), but `esgpull update` and `esgpull retry` can
still mutate a `File.status` row that a long-running download is actively
working on (concretely: `cli/update.py` force-resets non-Done files to `Queued`
when a query is re-applied; `cli/retry.py` accepts `Starting`/`Started` as a
source status with no guard). `download2_https()` mitigates this with
`session.refresh()` immediately before each status write (logs a warning on a
detected external change, but lets the download's own outcome win, since it has
ground truth on whether bytes were transferred). This narrows the race window
but does not eliminate it.

A more complete fix: have `update` and `retry` acquire the same lockfile
(non-blocking) before mutating `File.status`, mirroring how a second `download`
invocation is rejected today — i.e. fail fast with a clear error ("a download is
currently in progress; try again once it completes") rather than blocking for
the (possibly multi-day) duration of the download. Recommend doing this as a
follow-up phase, since it changes CLI UX for two already-shipped commands and
deserves its own review.

### `file_id`-keyed task labels
HTTPS tasks are labeled by `file.file_id` (`factory.py:make_https_tasks`), not
`file.sha`, to keep UI/log labels human-readable. If two *queued* files ever
share the same `file_id` but a different `sha` in the same run, their progress
bars/results would collide under one label. Not currently reachable (`file_id`
is effectively unique per dataset+filename in practice) but worth a guard or a
switch to `sha`-based internal keys (orthogonal to the user-facing label) if it
ever surfaces.

### Task-level status is not a proxy for per-file outcome
`TaskResultEvent.status` (`TaskStatus`: ACTIVE/CANCELED/SUCCESS/FAIL/UNKNOWN)
describes whether the *task* ran to completion without crashing, not whether
each file it carried succeeded. `HttpsDownloadTask._run()` bundles exactly one
`File` per task, but it still returns `TaskStatus.SUCCESS` for that task even
when the one file's download failed (eg DNS resolution error) — the per-file
outcome is recorded separately on `FileResult.status` (`FileStatus.Done` /
`FileStatus.Error`), and `TaskStatus.FAIL` is reserved for task-level failures
that abort before producing a normal result (eg `ENOSPC`). Code that needs a
file's outcome must read `FileResult.status` from `TaskResultEvent.files`, never
infer it from `event.status`. This bit `download2_https()`'s DB-write helper,
the file-collection logic, and `HttpsDownloadUI`'s progress rendering during
implementation — all three originally branched on `event.status` and had to be
corrected to branch on each `FileResult.status` instead, since
`tests/cli/test_download.py::test_download_errors_in_progress` (a single failing
file) silently no-oped on the error path until the fix was applied. Worth
double-checking for the same mistake in a future Globus phase, since
`GlobusTransferTask` batches many files per task (keyed by `origin_id`) and so
has even more files-per-task than the HTTPS case to get wrong.

### Legacy `download()` / `Simple` removal
Per "Must be backwards-compatible" above, `download2_https()` was deliberately
added alongside the legacy `download()`/`as_https.py:Simple` path rather than
replacing it in place. `cli/download.py` now calls `download2_https()`
exclusively, so the legacy path is dead code reachable only by direct API
callers. Tracking its removal explicitly so it isn't forgotten now that the
new path is the only one wired into the CLI: delete `Esgpull.download()`,
`as_https.py:Simple`/`BaseDownloader`, and `downloader/pipeline.py` (the old
`Processor`), and drop the now-unneeded `exceptiongroup` import in
`cli/download.py` callers if no longer referenced elsewhere — note it is still
used by `tui.py` and `context.py`, so the dependency itself stays either way.

### NFS/multi-host lockfile reliability (see "Check for conflicts" above)
Restated here as a trackable item: confirm `fcntl.flock()` behaves correctly
for the actual deployment's `config.paths.tmp` location before relying on it
to prevent overlapping downloads launched from different cluster nodes.

### End-to-end test coverage for the live ENOSPC-abort path (see "Failure happens" above)
Restated here as a trackable item: add a test that exercises `_is_disk_full()`
firing during a real `download2_https()` run (not just the predicate or the
preflight check in isolation) and asserts the rest of the queue is cancelled
and reported correctly.
