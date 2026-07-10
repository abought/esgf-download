"""
Integration smoke test for Esgpull.download4_combined: verifies HTTPS and Globus
tasks can run through one shared orchestrator (and one combined Live view) without
cross-contaminating each other's UI/DB state tracking.
"""
import asyncio
import json
import os
from pathlib import Path

import pytest
import responses as responses_lib
from globus_sdk import TransferClient
from globus_sdk.testing import RegisteredResponse, load_response

from esgpull import Esgpull
from esgpull.config import Config
from esgpull.models import File, FileStatus, GlobusStorage, Query

pytestmark = pytest.mark.skip(reason="New integration test, pending review before enabling")


def _load_captured(fn: str) -> dict:
    base = os.path.join(os.path.dirname(__file__), "downloader", "fixtures")
    with open(os.path.join(base, fn)) as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def mocked_responses(monkeypatch):
    """Activate globus SDK mocking (same approach as tests/downloader/test_globus.py)."""
    responses_lib.start()
    monkeypatch.setitem(os.environ, "GLOBUS_SDK_ENVIRONMENT", "production")
    yield
    responses_lib.stop()
    responses_lib.reset()


@pytest.fixture
def transfer_client():
    client = TransferClient()
    with client.retry_config.tune(max_retries=0):
        yield client


def _make_https_file() -> File:
    f = File(
        file_id="https_file",
        dataset_id="dataset",
        master_id="master_https",
        url="https://nonexistent.path/to/file.nc",
        version="v0",
        filename="https_file.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=100,
        status=FileStatus.Queued,
    )
    f.compute_sha()
    return f


def _make_globus_file() -> File:
    f = File(
        file_id="globus_file",
        dataset_id="dataset",
        master_id="master_globus",
        url="https://irrelevant.example/globus_file.nc",
        version="v0",
        filename="globus_file.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=100,
        status=FileStatus.Queued,
    )
    f.globus_storage = GlobusStorage(origin_id="source-collection-uuid", origin_path="/some/path")
    f.compute_sha()
    return f


def test_combined_download_https_and_globus(root: Path, config: Config, transfer_client):
    """
    One https file (bad URL, will fail) and one globus file (fire-and-forget submit,
    will end up Started) processed in a single download4_combined call. Regression
    guard for the UI-routing/live-combination logic: a bug there would either crash,
    or silently miscount one type's progress using the other's UI.
    """
    config.download.poll_globus = False
    config.globus.destination_collection_uuid = "dest-collection-uuid"
    config.globus.destination_collection_root = "/dest/root"
    config.generate(overwrite=True)

    esg = Esgpull(root)

    query = Query()
    query.compute_sha()
    query.track(query.options.default())

    https_file = _make_https_file()
    globus_file = _make_globus_file()
    query.files.append(https_file)
    query.files.append(globus_file)
    esg.db.add(query)

    load_response(transfer_client.get_submission_id, case="default")
    load_response(
        RegisteredResponse(
            service="transfer",
            method="POST",
            path="/v0.10/transfer",
            json=_load_captured("globus_submit_success.json"),
        )
    )

    files, errors = asyncio.run(
        esg.download4_combined(
            [https_file, globus_file],
            transfer_client=transfer_client,
            show_progress=True,
        )
    )

    assert any(err.data.file_id == "https_file" for err in errors)
    assert not any(f.file_id == "globus_file" for f in files)
    assert not any(err.data.file_id == "globus_file" for err in errors)

    esg.db.session.refresh(https_file)
    esg.db.session.refresh(globus_file)
    assert https_file.status == FileStatus.Error
    assert globus_file.status == FileStatus.Started
    assert globus_file.globus_transfer_task_id is not None