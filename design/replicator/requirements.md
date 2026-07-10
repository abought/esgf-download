# Replicator: Requirements (Draft v0)

## Purpose

A new, independent program that automates the same three-stage workflow `esgpull` provides interactively:

1. **Discover** — query the ESGF Search API to find datasets/files of interest, and register a persistent, named definition of that search (`esgpull query --track` equivalent).
2. **Sync** — periodically re-run saved searches, detect new results, and enqueue newly-discovered files for download (`esgpull update` equivalent).
3. **Fetch** — download queued files (HTTPS and/or Globus transfer), and durably record what has been fetched so re-runs don't re-download (`esgpull download` equivalent).

`esgpull` is the reference/inspiration, not a dependency. This is a **new codebase**, sharing concepts and (where genuinely decoupled) possibly small amounts of ESGF client code, but not `esgpull`'s DB schema, CLI, or UI layer.

## Why not just extend esgpull

`esgpull`'s command implementations interleave three concerns that need to be separable for headless/cron operation:

- **Decision logic** — what counts as a match, what's eligible to download, what changed since last sync.
- **Persistence** — the DB as source of truth for saved queries, known files, and their status.
- **Interactive UI** — `rich` progress bars, tables, and *inline confirmation prompts* (`esg.ui.ask(...)`, `esg.ui.choice(...)`) that are triggered *by* the decision logic, not just used to report its results.

Example of the coupling this program must avoid: `esgpull update` (`esgpull/cli/update.py`) decides which files become downloadable, but literally cannot finish that decision without a human answering "y/n/show" at a terminal when a query would issue >50 requests, or before linking newly-found files to the download queue at all. There is no clean seam to call "just the decision logic" from a cron job today — `yes: bool` flags were bolted on to bypass prompts, but the control flow still assumes a TTY is present.

`esgpull`'s download path has already been through a similar decoupling effort (see `design/download_refactor/`) — orchestrator/task-level callbacks, DB updates as side effects of callbacks rather than inline UI code, a shared process lockfile. That precedent is the pattern to follow, applied first to discovery/sync rather than download.

## Deployment model

- **Primary mode: headless.** Runs unattended via cron (or similar) on a shared HPC filesystem, potentially from multiple hosts pointed at the same state store. No step in the primary sync/download path may block on stdin.
- **Secondary mode: thin ops CLI.** A human operator can invoke the same underlying operations manually (e.g. to save a new query, force a re-sync, inspect state, retry failures). This CLI *may* prompt for confirmation on destructive/expensive one-off actions initiated by a human, but must also support a fully-flagged non-interactive form of every command so it stays scriptable.
- Target catalog: **ESGF Search API only**, matching esgpull's current scope. No requirement to generalize to other catalogs at this time.

## Cross-cutting requirements (apply to all three stages)

These carry over directly from the existing download-refactor design doc (`design/download_refactor/README.md`), since they're not specific to download and the team has already validated them:

- **Failure is expected, not exceptional.** Individual files/requests may fail due to flaky federated servers. Partial failure of a batch must not abort the whole run.
- **Shared process lock.** A single lockfile (sentinel file in the program's state/config directory, safe over a shared HPC filesystem) must guard any command that mutates the download queue or saved-query state, so two overlapping cron runs (e.g. a run that takes >24h) can't corrupt state. Needs a force-unlock command for operators.
- **Logging + exit codes for unattended runs.** State transitions (query synced, file queued, file started, file done/error) must be logged. Process exit code must let a sysadmin's monitoring distinguish "some files failed" from "total failure" (esgpull's doc proposes `EXIT 1` partial, `EXIT 2` total — reasonable starting point).
- **Idempotency.** Re-running discover/sync/fetch with no new upstream data must be a safe no-op — critical for cron.

## Capability 1: Discover & Track (priority for this pass)

### Concepts carried over from esgpull, kept
- **A saved query is a persistent, named object**: a set of facet filters (`selection`) + search options (`distrib`, `latest`, `replica`, `retracted`) that gets re-run on every sync.
- **Options must be fully resolved before saving.** esgpull's `Query.trackable()` / `track()` validation — refusing to save a query with unset distrib/latest/replica/retracted — is real business logic (ambiguous options would make re-sync results non-deterministic), not a UI nicety. Keep it, but surface failure as a hard error/log line, never a prompt.
- **Content-addressed identity (SHA over options+selection+tags).** Keep this — it gives free deduplication ("is this exact query already saved?") and change detection ("did someone edit this query's filters since last sync?") without inventing a separate mechanism.

### Concepts carried over, explicitly dropped
- **Hierarchical query inheritance** (`require` / parent-child / the `<<` merge operator in `esgpull/models/query.py`). This exists to let a human incrementally refine a search at a terminal ("start broad, layer on more facets"). A headless system authors complete query definitions up front; there's no interactive refinement session to support. Dropping this removes a meaningful chunk of the current data model's complexity (`Graph.expand`, `resolve_require`, requires-tree rendering).
- **Fuzzy SHA-prefix name lookup** (`Graph._expand_name`, `matching_shas`). CLI ergonomics for humans typing partial hashes; not needed if queries are always referenced by an operator-assigned name.
- **Tags** (`esgpull/models/tag.py`, `query_tag_proxy`). Existed for ad-hoc grouping/filtering in an interactive tree view. Open question below — may still be useful for grouping in logs/ops CLI output, but not load-bearing for sync logic.
- **The tracked/untracked distinction as a toggleable state on a saved query.** This is an esgpull misfeature, not a requirement to port. esgpull's `add` (create, untracked by default) vs. `track`/`untrack` (flip a bit on a query that already exists in the DB) split only makes sense in an interactive tool where a human wants to stage a query, inspect it, then decide later. In the replicator: **there is no "saved but untracked" state.** A query is either a throwaway preview (see below) or it is saved, and saving *is* tracking — sync always acts on every saved query. This removes an entire state dimension (and the `esgpull update` prompt path that depends on it) from the data model.

### Preview vs. save
- A human operator can **preview** a query — run it against ESGF and see result counts/sample hits — without persisting anything. This is the replicator's answer to esgpull's incremental "add, inspect, decide" workflow, but it's explicitly ephemeral: no DB row, no identity, nothing for sync to pick up.
- **Saving** a query definition is the only way it becomes persistent, and a saved query is always in scope for the next sync run. There is no intermediate state.

### Functional requirements (draft)
- FR1.1: An operator can preview a query (facets + options) against ESGF and see match counts, with no persistent side effects — useful for iterating on facet selection before committing.
- FR1.2: An operator can save a query via CLI, fully non-interactively (all facets/options passed as flags or a query-definition file), no confirmation prompt required to complete. Saving immediately puts the query in scope for the next sync.
- FR1.3: Saving a query that is options-incomplete (not "trackable" per esgpull's validation logic) must fail fast with a clear error and non-zero exit code — never block on a prompt to fix it.
- FR1.4: Saving a query identical (by content hash) to an already-saved query must be idempotent — no duplicate, clear message either way.
- FR1.5: An operator can remove a saved query. Needs a decision (see open questions) on what happens to files already discovered/downloaded under it.
- FR1.6: The set of currently-saved queries must be inspectable (list/show) without mutating anything — needed for both the ops CLI and for monitoring/alerting tooling.
- FR1.7: Query definitions must be stored durably (DB) and survive process restarts — this is the persistent memory that makes "remember prior queries" possible across cron invocations.

### Open questions
- Do we need multiple independent "profiles"/state-stores in one deployment (esgpull supports multiple named installs via `InstallConfig`), or is one saved-query set per deployment sufficient?
- Are tags worth keeping in any form (e.g., for grouping queries in alerting/reporting), or fully dropped?
- Now that there's no separate untrack step: when a saved query is removed, do its already-discovered files stay on disk and in the DB (orphaned from any query but still present), or does removal cascade? Is there a need for a "paused" state (skip on next sync, but still saved) distinct from full removal?
- Should a saved query's definition be mutable in place (edit facets on an existing name) or immutable-by-hash (editing implies removing the old one and saving a new one, preserving the old one's file history under its own identity)? This interacts directly with the "keep SHA-based dedup" decision above and needs to be settled before the data model is finalized.
- Does the replicator need esgpull's index-node discovery / distributed-search-optimization logic (`Esgpull.fetch_index_nodes`, `fetch_facets`, `use_custom_distribution_algorithm` in `context.py`), or is a simpler fixed-index-node search sufficient for the target use case?

## Capability 2: Sync (esgpull's `update`) — sketch, to be detailed next pass

Re-runs each saved query against ESGF, diffs results against known files, links newly-discovered files to the query, and marks them eligible for download. Must happen with **zero interactive prompts** — every decision `esgpull update` currently asks a human about (proceed despite >50 requests, link new files to queue) needs a config-driven default or a hard limit/error instead of a question.

Known follow-up per the existing `esgpull.py` TODO (`download2_https`, line ~541): decide how "eligible for download" should be (re)computed in cron mode — e.g. should previously-cancelled files silently re-enter the queue on sync, or only via an explicit retry step?

## Capability 3: Fetch (esgpull's `download`) — sketch, to be detailed next pass

Closest to already being replicator-ready. `esgpull`'s download-refactor effort (`design/download_refactor/`) already establishes: orchestrator + task-level callbacks, DB status updates as callback side effects (not inline UI code), per-download-type UI kept separate from orchestration, shared lockfile. Main open item is whether/how much of that design (Globus transfer support, partial-success reporting, disk-space checks) is directly portable vs. needs its own pass — see existing project memory `download2_https Followup` and `Globus Partial Success Reporting` for known unresolved details in the current implementation.

## Non-goals (for now)

- synda import/migration path
- Plugin system / event emission for third-party extensibility (esgpull's `esgpull/plugin.py`) — no known consumer yet in the replicator context
- Interactive query composition/tree browsing
- Multi-catalog support beyond ESGF