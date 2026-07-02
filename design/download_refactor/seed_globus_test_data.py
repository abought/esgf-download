#!/usr/bin/env python3
"""
Seed synthetic Globus test data into the configured esgpull database.

Creates two files (valid_file and error_file) pointing at real paths on a
caller-supplied Globus source collection. If the files already exist, resets
them to a download-eligible state (Queued, no transfer association) so the
script can be re-run between test iterations.

Files are linked to the LegacyQuery so they appear in `esgpull status`.

Usage:
    python design/download_refactor/seed_globus_test_data.py \\
        --collection-id <UUID> \\
        --collection-path /path/on/collection \\
        [--valid-filename actual_good_file.nc] \\
        [--error-filename actual_bad_file.nc]
"""
import argparse

import sqlalchemy as sa

from esgpull import Esgpull
from esgpull.cli.utils import init_esgpull
from esgpull.models import File, FileStatus, GlobusStorage
from esgpull.models.dataset import Dataset
from esgpull.models.query import LegacyQuery, Query
from esgpull.tui import Verbosity

# ---------------------------------------------------------------------------
# Fixture constants — non-download fields are synthetic
# ---------------------------------------------------------------------------
DATASET_ID = "globus.test.fixture.data.v1"
DATA_NODE = "test.esgf.fixture"
VERSION = "v1"
LOCAL_PATH = "globus/test/fixture/data/v1"


def _build_file_spec(collection_path: str, filename: str, checksum_fill: str) -> dict:
    return {
        "file_id": f"{DATASET_ID}.{filename}",
        "filename": filename,
        "master_id": f"{DATASET_ID}.{filename}",
        # url is used only for HTTPS fallback; synthetic is fine here
        "url": f"https://test.esgf.fixture/thredds/{filename}",
        "version": VERSION,
        "local_path": LOCAL_PATH,
        "data_node": DATA_NODE,
        "checksum": checksum_fill * 64,
        "checksum_type": "SHA256",
        "size": 0,
        "dataset_id": DATASET_ID,
    }


def _ensure_legacy_query(esg: Esgpull) -> Query:
    query = esg.db.session.get(Query, "LEGACY")
    if query is not None:
        print("LegacyQuery exists")
        return query
    esg.db.add(LegacyQuery)
    print("Created LegacyQuery")
    return LegacyQuery


def _ensure_globus_storage(esg: Esgpull, collection_id: str, collection_path: str) -> GlobusStorage:
    existing = esg.db.session.execute(
        sa.select(GlobusStorage).where(
            GlobusStorage.origin_id == collection_id,
            GlobusStorage.origin_path == collection_path,
        )
    ).scalar_one_or_none()

    if existing is not None:
        print(f"GlobusStorage exists:  {collection_id}:{collection_path}")
        return existing

    storage = GlobusStorage(origin_id=collection_id, origin_path=collection_path)
    storage.compute_sha()
    esg.db.add(storage)
    print(f"Created GlobusStorage: {collection_id}:{collection_path}")
    return storage


def _ensure_dataset(esg: Esgpull, n_files: int) -> Dataset:
    existing = esg.db.session.get(Dataset, DATASET_ID)
    if existing is not None:
        print(f"Dataset exists:        {DATASET_ID}")
        return existing

    dataset = Dataset(dataset_id=DATASET_ID, total_files=n_files)
    esg.db.add(dataset)
    print(f"Created Dataset:       {DATASET_ID}")
    return dataset


def _seed_file(
    esg: Esgpull,
    spec: dict,
    storage: GlobusStorage,
    query: Query,
) -> None:
    existing = esg.db.session.execute(
        sa.select(File).where(File.file_id == spec["file_id"])
    ).scalar_one_or_none()

    if existing is not None:
        existing.status = FileStatus.Queued
        existing.globus_transfer_task_id = None
        existing.globus_storage = storage
        esg.db.add(existing)
        print(f"Reset:   {spec['filename']} → Queued")
        return

    file = File.fromdict(spec)
    file.compute_sha()
    file.status = FileStatus.Queued
    file.globus_storage = storage
    esg.db.add(file)
    with esg.db.commit_context():
        esg.db.link(query=query, file=file)
    print(f"Created: {spec['filename']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--collection-id", required=True, metavar="UUID",
                        help="Globus source collection UUID")
    parser.add_argument("--collection-path", required=True, metavar="PATH",
                        help="Base path of test files on the source collection")
    parser.add_argument("--valid-filename", default="valid_file.nc", metavar="FILENAME",
                        help="Actual filename expected to transfer successfully (default: valid_file.nc)")
    parser.add_argument("--error-filename", default="error_file.nc", metavar="FILENAME",
                        help="Actual filename expected to fail or be absent (default: error_file.nc)")
    args = parser.parse_args()

    file_specs = [
        _build_file_spec(args.collection_path, args.valid_filename, "a"),
        _build_file_spec(args.collection_path, args.error_filename, "b"),
    ]

    esg = init_esgpull(Verbosity.Detail)
    query = _ensure_legacy_query(esg)
    storage = _ensure_globus_storage(esg, args.collection_id, args.collection_path)
    _ensure_dataset(esg, n_files=len(file_specs))
    for spec in file_specs:
        _seed_file(esg, spec, storage, query)
    print("Done.")


if __name__ == "__main__":
    main()