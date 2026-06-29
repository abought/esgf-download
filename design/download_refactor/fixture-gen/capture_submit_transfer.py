"""
Capture submit_transfer() response payloads: the successful "Accepted" result
and the GlobusAPIError bodies for failed submissions.

--source-path and --dest-path are full file paths (not directories) so this
can be pointed at a specific real or deliberately-missing source file.

TransferData is built with the same options as
esgpull.downloader.as_globus.GlobusTransferTask._make_transfer_data
(skip_source_errors, verify_checksum, encrypt_data, sync_level) so captured
payloads match what the real downloader actually submits.

Produces:
  design/fixtures/globus_submit_success.json                          (requires --dest-id)
  design/fixtures/globus_task_active.json                             (requires --dest-id; best-effort —
                                                                         only saved if the task is still
                                                                         ACTIVE when checked right after submit)
  design/fixtures/globus_submit_error_invalid_endpoint.json
  design/fixtures/globus_submit_error_invalid_source_endpoint.json    (requires --dest-id)
  design/fixtures/globus_submit_error_permission_denied.json          (requires --no-permission-dest-id)

Usage:
  python design/capture_submit_transfer.py \\
    --client-id <UUID> \\
    --client-secret <SECRET> \\
    --source-collection-id <UUID> \\
    --source-path /full/path/to/source/file \\
    [--dest-id <UUID> --dest-path /full/path/to/dest/file] \\
    [--no-permission-dest-id <UUID>  --no-permission-dest-path /full/path/on/dest]
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _capture_auth import FIXTURES_DIR, make_transfer_client

from globus_sdk import GlobusAPIError, TransferData


def make_transfer_data(source_collection_id: str, dest_collection_id: str) -> TransferData:
    """Mirror GlobusTransferTask._make_transfer_data so captures match production behavior."""
    return TransferData(
        source_collection_id,
        dest_collection_id,
        skip_source_errors=True,
        verify_checksum=True,
        encrypt_data=True,
        sync_level="checksum",
    )


def save_error(e: GlobusAPIError, filename: str) -> None:
    data = {"http_status": e.http_status, "body": e.raw_json}
    path = FIXTURES_DIR / filename
    path.write_text(json.dumps(data, indent=2, default=str))
    print(f"Saved {path}  (HTTP {e.http_status}, code={e.code!r})")


def save_response(data: dict, filename: str) -> None:
    path = FIXTURES_DIR / filename
    path.write_text(json.dumps(data, indent=2, default=str))
    print(f"Saved {path}  (task_id={data.get('task_id')!r})")


def capture_submit_success(
    client, source_collection_id: str, source_path: str, dest_id: str, dest_path: str
) -> None:
    td = make_transfer_data(source_collection_id, dest_id)
    td.add_item(source_path, dest_path)
    resp = client.submit_transfer(td)
    save_response(dict(resp.data), "globus_submit_success.json")
    task_id = resp.data["task_id"]

    # Race against the transfer completing: check status immediately, before it can resolve.
    status_data = dict(client.get_task(task_id).data)
    if status_data["status"] == "ACTIVE":
        save_response(status_data, "globus_task_active.json")
    else:
        print(
            f"NOTE: task was already {status_data['status']!r} by the time we checked — "
            "too fast/small to catch ACTIVE.\n"
            "      Re-run to try again, ideally with a larger source file."
        )

    print(
        f"NOTE: this queued a real transfer task ({task_id}).\n"
        "      Use capture_task_status.py to follow it to completion."
    )


def capture_invalid_endpoint(client, source_collection_id: str, source_path: str) -> None:
    bogus_dest_id = str(uuid.uuid4())
    td = make_transfer_data(source_collection_id, bogus_dest_id)
    td.add_item(source_path, "/nonexistent/dest/fixture_probe.nc")
    try:
        client.submit_transfer(td)
        print("WARNING: submit succeeded unexpectedly — no invalid-endpoint error to capture")
    except GlobusAPIError as e:
        save_error(e, "globus_submit_error_invalid_endpoint.json")


def capture_invalid_source_endpoint(client, dest_collection_id: str, dest_path: str) -> None:
    """Same failure shape as capture_invalid_endpoint, but with the bogus ID on the source
    side — captured separately in case the error code/message differs by which side is invalid."""
    bogus_source_id = str(uuid.uuid4())
    td = make_transfer_data(bogus_source_id, dest_collection_id)
    td.add_item("/nonexistent/source/fixture_probe.nc", dest_path)
    try:
        client.submit_transfer(td)
        print("WARNING: submit succeeded unexpectedly — no invalid-source-endpoint error to capture")
    except GlobusAPIError as e:
        save_error(e, "globus_submit_error_invalid_source_endpoint.json")


def capture_permission_denied(
    client, source_collection_id: str, source_path: str, dest_id: str, dest_path: str
) -> None:
    td = make_transfer_data(source_collection_id, dest_id)
    td.add_item(source_path, dest_path)
    try:
        client.submit_transfer(td)
        # Globus often validates permissions lazily — accepts the task and fails post-start
        print(
            "NOTE: submit accepted (Globus validates dest permissions post-start).\n"
            "      Use capture_task_status.py on the returned task once it fails\n"
            "      to capture globus_task_failed.json instead."
        )
    except GlobusAPIError as e:
        save_error(e, "globus_submit_error_permission_denied.json")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument("--source-collection-id", required=True, metavar="UUID",
                        help="A public source collection the service account can read")
    parser.add_argument("--source-path", required=True,
                        help="Full path to a file on the source collection "
                             "(e.g. /home/u_.../real.txt — may be missing, to capture a skip)")
    parser.add_argument("--dest-id", default=None, metavar="UUID",
                        help="A destination collection the service account can write to")
    parser.add_argument("--dest-path", default=None, metavar="PATH",
                        help="Full destination file path (e.g. /deleteme/<test_id>/real.txt); "
                             "required if --dest-id is given")
    parser.add_argument("--no-permission-dest-id", default=None, metavar="UUID",
                        help="A destination collection the service account cannot write to")
    parser.add_argument("--no-permission-dest-path", default="/fixture_probe.nc", metavar="PATH",
                        help="Full path on the no-permission destination "
                             "(default: /fixture_probe.nc)")
    args = parser.parse_args()

    if args.dest_id and not args.dest_path:
        parser.error("--dest-path is required when --dest-id is given")

    client = make_transfer_client(args.client_id, args.client_secret)
    client.add_app_data_access_scope(args.source_collection_id)

    if args.dest_id:
        print("--- submit success ---")
        capture_submit_success(
            client, args.source_collection_id, args.source_path,
            args.dest_id, args.dest_path,
        )

        print("--- invalid source endpoint ---")
        capture_invalid_source_endpoint(client, args.dest_id, args.dest_path)
    else:
        print("Skipping success capture (pass --dest-id and --dest-path to enable)")
        print("Skipping invalid-source-endpoint capture (pass --dest-id and --dest-path to enable)")

    print("--- invalid endpoint ---")
    capture_invalid_endpoint(client, args.source_collection_id, args.source_path)

    if args.no_permission_dest_id:
        print("--- permission denied ---")
        capture_permission_denied(
            client,
            args.source_collection_id,
            args.source_path,
            args.no_permission_dest_id,
            args.no_permission_dest_path,
        )
    else:
        print("Skipping permission-denied capture (pass --no-permission-dest-id to enable)")


if __name__ == "__main__":
    main()