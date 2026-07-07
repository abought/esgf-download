# Implementing Globus transfer mode

## Purpose
We will add a new function, `esgpull.py:download4_combined()`, that allows downloading of a provided list of files via EITHER files OR Globus Transfer.

This is the unification of two prior phases of work.

## Key requirements:
* Assume that the cli download command handles the check for existing globus transfer statuses, and that the `esgpull.download4_combined`  method receives only files that are verified eligible for download 
  * The CLI should query for existing globus transfers status AFTER running `check_globus_transfers` method. This should have its own progress UI, and reuse the globus state callbacks where possible.
  * If `prefer_globus` is disabled, the `retry` command should mark any unfinished transfers as FAILED instead of checking their status, and re-queue files for whatever download mechanisms are currently allowed. In this case, it should clear the association between a file and any prior globus transfers. 
* After open questions about file download state have been resolved, generate a list of files eligible for download and pass them to `download4_combined`
* Run download as normal and report status at end using the existing UI/logging mechanisms.
  
### Design guidelines
* Common operations (like updating file status) should live in orchestrator level callbacks
* Operations specific to a task type should live in task-level callbacks or task-type creation factories, where possible (the major exception: `on_result`, because the orchestrator handles unexpected cancels)
* Identify a way to streamline ui handling of on_results. The current approach will be clunky when tasks are unified. (because type-specific result handlers are suddenly all inlined in the main loop)
* Keep the main common download function as clean and generic as possible. The old `download()` method was very long (100+ lines) and had a lot of nested conditionals that made logic hard to follow. Note cases where separation of concerns could not be enforced.
* Handle common errors from both transfer paths (insufficient disk space, authentication)
* Ensure that all download paths respect `config.download.*` and `config.globus.*` options.


## Stretch goals after initial implementation
Currently, errors are returned as a batch from functions. Can we migrate error rendering to a more streaming-oriented approach? (while keeping things decoupled to keep the main loop from being too complex)

## EDGE CASES to clean up later
What do we do if globus mode was on, then later disabled? When the `prefer_globus` config setting changes, does this create the possibility of any "stuck" files that wouldn't be picked up for download queue or resolution? 

(eg globus transfers left in ACTIVE status)

How do we handle the situation where the program launches an async globus transfer, and then does not run for a long time? If a globus transfer "final result" record (from the api) is no longer available, what state do file transfers end up in? They should end up eligible to requeue/retry. 
