# Refactoring download functionality for esgpull
## Purpose
Historically, this tool downloaded files only via https URL. We would like to add a new download method (async globus transfer).

The old downloader system was very tightly coupled to UI, and had poorly documented behaviors that may not translate to a CLI driven world.

The new system introduces a shared abstraction, and separation of concerns to make it flexible for different download types.

## Plan of work
Phase 1: Refactor existing HTTPS / url based download functionality (`esgpull.py:download()`). See [spec](design/download_refactor/download_https_conversion.md)
Phase 2: Introduce a separate path for globus-based downloads ([spec](design/download_refactor/download_globus_transfer.md))
Phase 3: Unify these into a new single download method
Phase 4: Reconcile design changes with prior `download` function, and validate using a mix of manual and automated strategies
Phase 5: Clean up PR, and merge final version in


## Key shared requirements
### Failure happens
This tool is used by a federated network of data repositories, covering many TB of data, typically hosted on academic HPC systems with shared filesystems and unpredictable uptime characteristics. 

* It is possible that each individual file comes from a different server, and not all servers will have reliable uptime. Retries of one or many files are to be expected.

### Check for conflicts
If this is run as a cron job, it is possible that two versions of the tool could be running at once- eg if the first run takes more than one full day. Ensure there is a lockfile to detect downloads in progress, and warn the user.

The lockfile should be shared and checked across all commands that affect the download queue, including `retry`, `download`, and `update`. It should use a simple method (such as a sentinel file in the esgpull config directory) that is guaranteed to work across multiple hosts sharing a common filesystem- such as would be the case for an HPC environment.

A new `esgpull unlock` command

### The download lifecycle
* Identify files eligible for download
  * The active query is filtered to `File`s with `status == FileStatus.Queued`, deduplicated by `sha`. "Error/retry status" files only re-enter the queue via the separate `esgpull retry` command, which resets their status to `Queued`.
* Partition downloads by type (globus transfer or url-based https; the strategy will be described in PHASE 3).
* Create download tasks, orchestrator, and UI objects
  * Wire up appropriate heartbeat callbacks events at each stage for file transfer status, based on how updates are currently applied. Note appropriate state tracking instructions by download type: see PHASE 2 and PHASE 3 documents.
    * Heartbeats are used for database state tracking, error logging, and UI elements
* Execute downloads. Handle errors and track database status when appropriate.
