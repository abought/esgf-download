# Refactoring download functionality for esgpull
## Purpose
Historically, this tool downloaded files only via https URL. We would like to add a new download method (async globus transfer).

The old downloader system was very tightly coupled to UI, and had poorly documented behaviors that may not translate to a CLI driven world.

The new system introduces a shared abstraction, and separation of concerns to make it flexible for different download types.

## Plan of work
Phase 1: Refactor existing HTTPS / url based download functionality (`esgpull.py:download()`). See [spec](design/download_refactor/download_https_conversion.md)
Phase 2: Introduce a separate path for globus-based downloads ([spec](design/download_refactor/download_globus_transfer.md))
Phase 3: Unify these into a new single download method ([spec](design/download_refactor/download-combined.md))
Phase 4: Reconcile design changes with prior `download` function, and validate using a mix of manual and automated strategies
Phase 5: Clean up PR, and merge final version in


## Key shared requirements
### Failure happens
This tool is used by a federated network of data repositories, covering many TB of data, typically hosted on academic HPC systems with shared filesystems and unpredictable uptime characteristics. 

* It is possible that each individual file comes from a different server, and not all servers will have reliable uptime. Retries of one or many files are to be expected.

### Lockfile to avoid conflicts
If this is run as a cron job, it is possible that two versions of the tool could be running at once- eg if the first run takes more than one full day. Ensure there is a lockfile to detect downloads in progress, and warn the user.

The lockfile should be shared and checked across all commands that affect the download queue, including `retry`, `download`, and `update`. It should use a simple method (such as a sentinel file in the esgpull config directory) that is guaranteed to work across multiple hosts sharing a common filesystem (such as would be the case for an HPC environment). The lockfile creation/deletion should be handled via some simple means, such as a decorator on the individual command definitions.

A new `esgpull unlock` command should be created that forcibly deletes this lockfile.

### The download lifecycle
Orchestrator level callbacks should be used for common behavior, such as updating the DB when a file begins downloading.

TASK level callbacks should be used for type-specific behavior, like updating UI progress bars or the DB state of a globuas transfer task.

* Identify files eligible for download
  * The active query is filtered to `File`s with `status == FileStatus.Queued`, deduplicated by `sha`. "Error/retry status" files only re-enter the queue via the separate `esgpull retry` command, which resets their status to `Queued`.
* Partition downloads by type (globus transfer or url-based https; the strategy will be described in PHASE 3).
* Create download tasks, orchestrator, and UI objects
  * Wire up appropriate heartbeat callbacks events at each stage for file transfer status, based on how updates are currently applied. Note appropriate state tracking instructions by download type: see PHASE 2 and PHASE 3 documents.
    * Heartbeats are used for database state tracking, error logging, and UI elements
* Execute downloads. Handle errors and track database status from events where appropriate.
  * Files for URL based download, and also globus transfer tasks for globus files.

### Generic UI
UI (including "m of n total") will be managed separately across download types. This is because some concepts (like time remaining / transfer speed) do not map well to async Globus transfers. We may re-evaluate and unify later.

A UI will only be rendered if there are relevant tasks of that type. (url vs globus)

### Generic database status tracking rules

* The Task
  * File tasks are a thin wrapper around individual files. URL-based downloads are not tracked in the database separately.
  * Task-level status is _only_ used for internal book keeping / error reporting at runtime. Individual file status is the unit we track in the database.
  * It is possible for a task to COMPLETE even if the file fails to download.
* The File
  * Set to queued by `esgpull update` or `esgpull retry` commands.
  * Set to `FileStatus.Started` when the task actually begins to run (not just when it is scheduled).
  * Set to `FileStatus.Error` or `FileStatus.Done` when file done
  * Set to `FileStatus.Cancelled` when program is interrupted, such as due to user intervention (SIGTERM), or because of an outside issue such as running out of disk space. Some scenarios, like server power loss, may leave file stuck in `FileStatus.Started`.


### Logging and reporting
* This tool is expected to run in the background, such as a cron job. State transitions (start, retry, success, error) must be captured in logs. Consider mechanisms for a sysadmin to be alerted when download errors occur, such as exiting the program with  specific error status codes if:
  * one or more files fail to download (possible temporary error with remote servers): `EXIT 1`
  * all files fail to download (possibly a problem with the local system or usage): `EXIT 2`
