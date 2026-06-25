## Development

### How migrations are applied
* On every startup, the database engine upgrades to the current migration head automatically.
* During development (with a `+dev` version), manually-created hash-named migrations (see below) are applied this way without any extra steps.

### Creating a new migration
New model changes must be captured in a migration file before they will be applied:

```
alembic -c alembic.ini revision --autogenerate -m "Feature migration"
```

This generates a hash-named file under `migrations/versions/`. Commit it alongside the model changes.

CAUTION: If two feature branches both generate migrations, this may cause an unclear revision chain, and alembic will fail. Rebase before merging and verify that the revision chain has no forks.

### The `+dev` version suffix
`pyproject.toml`'s version must include `+dev` during active development (e.g. `0.9.7+dev`). Otherwise, the upgrade mechanism may fail due to conflicts.

Steps to activate:
1. Append `+dev` to `project.version` in `pyproject.toml`
2. Run `uv sync` to propagate the version into installed metadata

## Release process
On release, `_update()` auto-generates an empty stamp migration named after the new version (e.g. `0.9.7_update_tables.py`) the first time any command opens a database. This marks the version as the new migration head.

Release steps:
1. Remove `+dev`, bump to the final version in `pyproject.toml`
2. `uv sync`
3. Run any esgpull command that opens the database — the stamp migration file is auto-generated
4. Commit the generated migration file together with the version bump