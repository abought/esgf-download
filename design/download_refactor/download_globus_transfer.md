# Implementing Globus transfor mode

## Purpose
We will add a new function, `esgpull.py:download3_globus()`, that allows downloading of a provided list of files via globus transfer. 


## Limitations of scope
This does not cover url based https downloads; those are implemented in a separate phase. All files passed to this function are assumed to be globus-specific.


## Requirements
### Decompose a list of files into multiple transfer tasks
Partition the file list into a map of `{collection_id: [files in that collection}`

Generate one `GlobusTransferTask` per source id (globus collection).

### Configuration options
Should respect configuration options:
* `config.globus` for connection information
* `config.download.{prefer_globus, poll_globus, poll_time_max}`

### UI
TODO: Propose how to handle `config.download.poll_globus = False` (maybe show n tasks submitted, instead of bytes download progress? And just the overall bar, not showing per-task progress at all?)

* One progress bar for overall completion of all globus-related downloads (for now- this may be merged into overall download progress barl later, so keep design similar to url-based downloads)
* One progress bar per globus transfer, showing the globus collection id (storage location) as the aggregate unit, with percent complete. 


Both should mimic the appearance and format codes of the file progress bar, but note that a single globus transfer can be multiple files: do not show individual filenames, and instead show the globus collection ID. (equivalent to data node) in url based downloads).

Do not attempt to show estimated time remaining, because globus transfer tasks might be queued, and this would make the rich estimate incorrect.

### Globus specific state tracking
* Save a record of the globus transfer task ID when it starts
* Update file record to associate it with the globus transfer
* From there, normal file status callbacks apply (the generic common rules)
* When a task is complete, update task status and file status
* When the program is terminated, DO NOT update the globus or file task status. This is because globus tasks can continue after the program starts running. File are not canceled/in error if `esgpull` exits unexpectedly, and neither are globus transfer tasks.

* The `retry` command should be extended to reset the globus task IDs associated with a file, in addition to other status fields.

### Logging and monitoring
* Exit with code 2 (program-level issue) if globus authentication fails, or any transfer fails with a permission error. (this is because we expect source to be public, and a permissions error implies issue with the dest source collection, or the user's credentials on the dest)

## Future work
* Before the list of eligible files is computed, we need an additional function to check all existing globus transfer status, and update accordingly.
