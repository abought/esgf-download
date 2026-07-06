# Implementing Globus transfer mode

## Purpose
We will add a new function, `esgpull.py:download4_combined()`, that allows downloading of a provided list of files via EITHER files OR Globus Transfer.

This is the unification of two prior phases of work.

## Key requirements:
* Assume that the cli download command handles the check for existing globus transfer statuses, and that the `esgpull.download4_combined`  method receives only files that are verified eligible for download (eg, query for existing download status AFTER running `check_globus_transfers` method). Consider whether the external "check globus transfers" feature needs its own UI progress bar elements to communicate work to the user.
  * If globus credentials are available, always check existing downloads, even if `prefer_globus` is not currently enabled, because maybe globus transfer mode used to be on
  * If the check fails, eg due to credential errors:
    * If `prefer_globus` is disabled, DO NOT stop the program, because credentials may be intentionally revoked after the globus option was disabled. DO print a message about the failure to the console. ("Prior globus transfers could not be resolved"), and mark those transfers and their associated files as failed.
      * Consider how `check_existing_globus_transfers` and `download4_combined()` might reuse event listener code for the purpose of database state updates when a status transfer task completes. Factories may be a helpful abstractiuon here, esp with a generic UI interface. (the "precheck" ui vs the "download" ui)
    * If `prefer_globus` is enabled, and any status check fails, DO stop the program, because globus credentials are required in this mode, and basic program functionality cannot be used until the user fixes this.
  * If existing pending globus transfers are in the db, and `prefer_globus` is off, AND no credentials are provided, print a warning to the console that file status could not be resolved
  * If a file is associated with a failed/unresolvable globus transfer, `esgpull retry` should re-queue it for download. The extra rule with the new globus feature is that the retry mechanism should clear the existing transfer association.  
* After open questions about file download state have been resolved, generate a list of files eligible for download and pass them to `download4_combined`
* Run download as normal and report status at end using the existing UI/logging mechanisms.
  
### Design guidelines
* Common operations (like updating file status) should live in orchestrator level callbacks
* Operations specific to a task type should live in task-level callbacks or task-type creation factories, where possible
* Identify a way to streamline ui handling of on_results. The current approach will be clunky when tasks are unified. (because type-specific result handlers are suddenly all inlined in the main loop)
* Keep the main common download function as clean and generic as possible. The old `download()` method was very long (100+ lines) and had a lot of nested conditionals that made logic hard to follow. Note cases where separation of concerns could not be enforced.
* Handle common errors from both transfer paths (insufficient disk space, authentication)
* Ensure that all download paths respect `config.download` and `config.globus` options.


## Stretch goals after initial implementation
Currently, errors are returned as a batch from functions. Can we migrate error rendering to a more streaming-oriented approach? (while keeping things decoupled to keep the main loop from being too complex)

## EDGE CASES to clean up later
What do we do if globus mode was on, then later disabled? When the `prefer_globus` config setting changes, does this create the possibility of any "stuck" files that wouldn't be picked up for download queue or resolution? 

(eg globus transfers left in ACTIVE status)

How do we handle the situation where the program launches an async globus transfer, and then does not run for a long time? If a globus transfer "final result" record (from the api) is no longer available, what state do file transfers end up in? They should end up eligible to requeue/retry. 
