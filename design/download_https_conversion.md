# Refactoring download functionality for esgpull
## Purpose
Historically, this tool downloaded files only via https, using the legacy functionality `dpownloader/as_https.py:Simple()` class. This is implemented in `esgpull.py:download()` on the `main` branch.

We wish to convert this to use the new `downloader/as_https.py:HttpsDownloadTask()` approach, orchestrated by the new `downloader/orchestrator.py:Orchestrator()` class.

We will begin by decomposing the original function into a set of system requirements.


### Limitations of scope
In order to break this work down into manageable pieces, this ONLY covers the existing functionality: files that are downloaded directly from an HTTPS URL. The new globus-based download mechanisms are NOT IN SCOPE.

## Requirements
### Failure happens
This tool is used by a federated network of data repositories, covering many TB of data, typically hosted on academic HPC systems with unpredictable uptime characteristics. 

* It is possible that each individual file comes from a different server, and not all servers will have reliable uptime. Retries of one or many files are to be expected.
* This is a BIG DATA repository, downloading to shared systems. Warn the user if the designated copy destination does not have space for all files
  * Check before download begins
  * Catch errors and log if a file fails due to disk space at runtime. If the disk is out of space, all downloads should be stopped immediately.

### Must be backwards-compatible
Existing progress bars and state tracking are intended to work the same way after this change. We are changing _how_ the operation runs, but we wish to preserve compatibility with existing installs. We should identify relevant tests and feature requirements for the new https download function.

As part of this change, the work will be implemented in a NEW FUNCTION: `esgpull.py:download2_https()`. After all changes have been made, we will eventually remove the original download function in a subsequent phase of work.  

### Check for conflicts
If this is run as a cron job, it is possible that two versions of the tool could be running at once- eg if the first run takes more than one full day. Ensure there is a lockfile to detect downloads in progress, and warn the user. Also, in cases where a `File` instance needs to be updated, be sure to reload the file from the database before changing state in case data has changed.

### Download phases
The below captures key phases of the download process, based on study of the pre-existing download code:

* Identify files eligible for download
  * TODO: designate criteria for database query, including which files and error/retry status
    * Zero or minimal changes should be made to the database at this time.
* Create download tasks and orchestrator
  * Wire up appropriate heartbeat callbacks events at each stage for file transfer status, based on how updates are currently applied
* Execute downloads. Handle errors and track database status when appropriate.
* Update progress bars (TODO double check this against current implementation, and write a specification for fields in the progress bar)
  1. For each file downloaded, show current and total amount downloaded
  2. Show a progress bar for TOTAL download.
* Database: State tracking mechanism
  * TODO: Identify what states are tracked, including success, failure, and in-progress states. Be sure that states are correctly handled if the program is terminated unexpectedly in the middle of an https download.


### Logging and reporting
* This tool is expected to run in the background, such as a cron job. State transitions (start, retry, success, error) must be captured in logs. Consider mechanisms for a sysadmin to be alerted when download errors occur, such as exiting the program with  specific error status codes if:
  * one or more files fail to download (possible temporary error with remote servers)
  * all files fail to download (possibly a problem with the local system or usage)
* TODO: Evaluate any existing internal notification mechanisms, such as plugins, and determine whether they need updating to work with the new event mechanism. Characterize what current plugin events are defined.

