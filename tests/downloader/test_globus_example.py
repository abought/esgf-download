"""Unit tests for globus transfer task behavior"""



import asyncio
import json
import os

import pytest
import requests.exceptions
import responses as responses_lib

from globus_sdk import TransferClient
from globus_sdk.testing import RegisteredResponse, load_response

from esgpull.downloader.as_globus import GlobusStatusTask, GlobusTransferTask
from esgpull.downloader.base import TaskStatus
from esgpull.models.file import FileStatus
from tests.downloader.fakes import make_file

TASK_ID = "00000000-0000-0000-0000-000000000001"


def _load_captured(fn: str) -> dict:
    base = os.path.dirname(__file__)
    p = os.path.join(base, 'fixtures', fn)
    with open(p, 'r') as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def mocked_responses(monkeypatch):
    """Activate globus SDK mocking"""
    responses_lib.start()
    monkeypatch.setitem(os.environ, "GLOBUS_SDK_ENVIRONMENT", "production")
    yield
    responses_lib.stop()
    responses_lib.reset()

@pytest.fixture
def client():
    """Per SDK docs, adjust client options"""
    client = TransferClient()
    with client.retry_config.tune(max_retries=0):
        yield client


class TestGlobusTransferTask:
    def test_task_submit_ok_no_status_check(self, client):
        """Async task is submitted, but we don't wait for the result."""
        load_response(client.get_submission_id, case='default')

        load_response(
            RegisteredResponse(
                service="transfer",
                method="POST",
                path="/v0.10/transfer",
                json=_load_captured("globus_submit_success.json"),
            )
        )

        files = [make_file(100, "a")]
        task = GlobusTransferTask(
            task_label="test submit async",
            files=files,
            client=client,
            source_collection_id="dummy",
            dest_collection_id="dummy",
            dest_root_path="/test_dummy/",
            wait_until_resolved=False
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.ACTIVE

        assert all(fr.status == FileStatus.Started for fr in result.files)


    def test_submit_ok_then_wait_until_complete(self, client):
        """Tests the full process of submit -> status check final result"""
        load_response(client.get_submission_id, case='default')

        load_response(
            # Task submit
            RegisteredResponse(
                service="transfer",
                method="POST",
                path="/v0.10/transfer",
                json=_load_captured("globus_submit_success.json"),
            )
        )

        load_response(
            # Task monitoring: instant success response
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}",
                json=_load_captured("globus_task_succeeded_clean.json"),
            )
        )

        files = [make_file(100, "a")]
        task = GlobusTransferTask(
            task_label="test submit async",
            files=files,
            client=client,
            source_collection_id="dummy",
            dest_collection_id="dummy",
            dest_root_path="/test_dummy/",
            wait_until_resolved=True
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.SUCCESS

        assert all(fr.status == FileStatus.Done for fr in result.files)


class TestGlobusStatusTask:
    def test_task_resolved(self, client):
        """Able to read a success response and report file results"""
        load_response(
            # Task monitoring: instant success response
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}",
                json=_load_captured("globus_task_succeeded_clean.json"),
            )
        )

        files = [make_file(100, "a")]
        task = GlobusStatusTask(
            task_label="test status",
            files=files,
            client=client,
            transfer_task_id=TASK_ID,
            wait_until_resolved=True
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.SUCCESS

        assert all(fr.status == FileStatus.Done for fr in result.files)


    def test_task_not_found(self, client):
        """Expired tasks are unknowable and should kick all files into the error/retry queue"""
        load_response(
            # Task monitoring: task not found (expired or other error)
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}",
                json={
                  "code": "ClientError.NotFound",
                  "message": "No task found with id '00000000-0000-0000-0000-000000000001'",
                  "request_id": "REDACTED",
                  "resource": "/task/00000000-0000-0000-0000-000000000001"
                },
                status=404
            )
        )

        files = [make_file(100, "a")]
        task = GlobusStatusTask(
            task_label="test status async",
            files=files,
            client=client,
            transfer_task_id=TASK_ID,
            wait_until_resolved=True
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.FAIL

        assert all(fr.status == FileStatus.Error for fr in result.files)

    def test_network_error_try_later(self, client):
        load_response(
            # Task monitoring: task not found (expired or other error)
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}",
                body=requests.exceptions.ConnectionError("Simulated network issue"),
                status=404
            )
        )

        files = [make_file(100, "a")]
        task = GlobusStatusTask(
            task_label="test status check fails",
            files=files,
            client=client,
            transfer_task_id=TASK_ID,
            wait_until_resolved=False
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.UNKNOWN

        assert all(fr.status == FileStatus.Started for fr in result.files)


    def test_task_with_skips(self, client):
        """Two files submitted: one is skipped by Globus, one succeeds. Overall task SUCCESS,
        but each file's result must reflect its own outcome."""
        load_response(
            # Task monitoring: success with skips
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}",
                json=_load_captured("files_skipped/globus_task_succeeded_with_skips.json"),
            )
        )

        load_response(
            # Task monitoring: skip logs details
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}/skipped_errors",
                json=_load_captured("files_skipped/globus_skipped_errors_page1.json"),
            )
        )

        # The skipped_errors fixture's source_path is "/home/u_24weaxffujbulbpdjk42up2a5u/fake.txt".
        # globus_fn = origin_path + filename, so this file must reproduce that path exactly to be
        # recognized as the skipped one; "present" shares the origin but not the filename.
        origin_path = "/home/u_24weaxffujbulbpdjk42up2a5u"
        skipped = make_file(100, "missing", filename="fake.txt", globus_origin_path=origin_path)
        ok = make_file(100, "present", globus_origin_path=origin_path)

        files = [skipped, ok]
        task = GlobusStatusTask(
            task_label="test status",
            transfer_task_id=TASK_ID,
            files=files,
            client=client,
            wait_until_resolved=True
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.SUCCESS

        by_id = {fr.file.file_id: fr.status for fr in result.files}
        assert by_id["missing"] == FileStatus.Error
        assert by_id["present"] == FileStatus.Done

    def test_401_status_unknown(self, client):
        load_response(
            # Task monitoring: success with skips
            RegisteredResponse(
                service="transfer",
                method="GET",
                path=f"/v0.10/task/{TASK_ID}",
                status=401,
            )
        )

        files = [make_file(100, "a")]
        task = GlobusStatusTask(
            task_label="test status",
            transfer_task_id=TASK_ID,
            files=files,
            client=client,
            wait_until_resolved=True
        )
        result = asyncio.run(task.run())

        assert result.status == TaskStatus.UNKNOWN

        assert all(fr.status == FileStatus.Started for fr in result.files)
