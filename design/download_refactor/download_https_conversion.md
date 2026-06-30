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

### Task specific unique behavior: UI
* Create a progress bar UI object to encapsulate status updates for various events (start, stop, end). The fields and format of the progress bar should be drawn from the legacy implementation (`esgpull.py:download()`)
  1. For each file downloaded, show current and total amount downloaded. Hide the progress bar for a single file once that file is done downloading.
  2. Update the shared progress bar for total download progress (`m of n complete`)
