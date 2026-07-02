# Implementing Globus transfer mode

## Purpose
We will add a new function, `esgpull.py:download4_combined()`, that allows downloading of a provided list of files via EITHER files OR Globus Transfer.

This is the unification of two prior phases of work.

## Key requirements:
* Assume that the cli download command handles the check for existing globus transfer statuses, before generating a list of files eligible for download (always do this if auth allows, even if pref not enabled, because maybe globus transfer mode used to be on)
* Receives a list of any files eligible for download (query after globus files resolved!)
  
### Design guidelines
* Common operations (like updating file status) should live in orchestrator level callbacks
* Operations specific to a task type should live in task-level callbacks or task-type creation factories, where possible
* Identify a way to streamline ui handling of on_results. The current approach will be clunky when tasks are unified.
* Keep the main common download function as clean and generic as possible. The old `download()` method was very long (100+ lines) and had a lot of nested conditionals that made logic hard to follow. Note cases where separation of concerns could not be enforced.
* Ensure that all download paths respect `config.download` and `config.globus` options.


## EDGE CASES to clean up later
What do we do if globus mode was on, then later disabled? Does this change in preferred download method create the possibility of any "stuck" files that wouldn't be picked up for download queue or resolution? (eg globus transfers left in ACTIVE status)
