"""Unit tests for globus transfer task behavior"""



import asyncio
import json
import os

import pytest
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
    def test_task_submit_ok_then_stop(self, client):
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


    def test_submit_ok_no_status_found(self, client):
        """
        If a task is expired and no status can be determined, report unknown status

        FIXME move this to globusStatusTask tests; we don't need the submit step at all
        """
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
        assert result.status == TaskStatus.FAIL

        assert all(fr.status == FileStatus.Error for fr in result.files)


class TestGlobusStatusTask:
    def test_task_not_found(self, client):
        # TODO: Move task from above
        pass


    def test_task_with_skips(self, client):
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
                path=f"/v0.10/task/{TASK_ID}/skipper_errors",
                json=_load_captured("files_skipped/globus_skipped_errors_page1.json"),
            )
        )

        files = [make_file(100, "a")]  # TODO: file list needs to do a better job of matching skip logs (file list factory needs names)
        task = GlobusStatusTask(
            task_label="test status",
            transfer_task_id=TASK_ID,
            files=files,
            client=client,
            wait_until_resolved=True
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.SUCCESS

        assert all(fr.status == FileStatus.Done for fr in result.files)

#
#
# class TestStatusTask:
#     def test_succeeded_response(self, client):
#         # json= will be loaded from tests/fixtures/globus/globus_task_succeeded_clean.json
#         load_response(client.get_submission_id, case='default')
#         # TODO> write submit task with transfer payload, no wait, by hand, captured fixtures!!!
#
#         r = load_response(
#             RegisteredResponse(
#                 service="transfer",
#                 path=f"/v0.10/transfer",
#                 json={
#                     "task_id": TASK_ID,
#                     "status": "SUCCEEDED",
#                     "files_transferred": 2,
#                     "files_skipped": 0,
#                     "bytes_checksummed": 1024,
#                     "subtasks_skipped_errors": 0,
#                 },
#                 status=200,
#         ))
#
#         files = [make_file(100, "a")]
#
#         task = GlobusStatusTask(task_label="a test", files=files, client=client, transfer_task_id=TASK_ID)
#         result = asyncio.run(task.run())
#
#         assert result.status == TaskStatus.ACTIVE
#         assert all(fr.status == FileStatus.Done for fr in result.files)
#
#     def test_api_error_returns_unknown(self):
#         # status= and json= come from the {"http_status": N, "body": {...}} format
#         # captured by capture_auth_error.py → tests/fixtures/globus/globus_task_401.json
#         RegisteredResponse(
#             service="transfer",
#             path=f"/v0.10/task/{TASK_ID}",
#             status=401,
#             json={
#                 "code": "AuthenticationFailed",
#                 "message": "Token is not valid",
#                 "request_id": "abc123",
#                 "resource": f"/v0.10/task/{TASK_ID}",
#             },
#         ).add()
#
#         files = [make_file("a")]
#         result = asyncio.run(make_status_task(files)._run(files, []))
#
#         assert result.status == TaskStatus.UNKNOWN
#         assert all(fr.status == FileStatus.Started for fr in result.files)
#
#     def test_skipped_errors_across_two_pages(self):
#         # get_task response — from tests/fixtures/globus/globus_task_succeeded_with_skips.json
#         RegisteredResponse(
#             service="transfer",
#             path=f"/v0.10/task/{TASK_ID}",
#             json={
#                 "task_id": TASK_ID,
#                 "status": "SUCCEEDED",
#                 "files_transferred": 1,
#                 "files_skipped": 2,
#                 "bytes_checksummed": 512,
#                 "subtasks_skipped_errors": 2,
#             },
#         ).add()
#
#         # Paginated skipped errors — ResponseList queues page1 then page2 for the same URL.
#         # The SDK Paginator consumes them in order, stopping when next_marker is absent.
#         # page json= will come from globus_skipped_errors_page1.json / page2.json
#         ResponseList(
#             RegisteredResponse(
#                 service="transfer",
#                 path=f"/v0.10/task/{TASK_ID}/skipped_errors",
#                 json={
#                     "DATA_TYPE": "task_skipped_errors_list#1.0.0",
#                     "DATA": [{"source_path": "/esgf/data/file_a.nc", "error_code": "FILE_NOT_FOUND"}],
#                     "next_marker": "page2marker",
#                 },
#             ),
#             RegisteredResponse(
#                 service="transfer",
#                 path=f"/v0.10/task/{TASK_ID}/skipped_errors",
#                 json={
#                     "DATA_TYPE": "task_skipped_errors_list#1.0.0",
#                     "DATA": [{"source_path": "/esgf/data/file_b.nc", "error_code": "FILE_NOT_FOUND"}],
#                     "next_marker": None,
#                 },
#             ),
#         ).add()
#
#         files = [make_file("a"), make_file("b")]
#         result = asyncio.run(make_status_task(files)._run(files, []))
#
#         assert result.status == TaskStatus.SUCCESS
#         assert all(fr.status == FileStatus.Error for fr in result.files)