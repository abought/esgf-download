"""
Capture a real FAILED get_task() response by submitting a normal, healthy transfer
and canceling it immediately, before it has a chance to complete.

Globus task cancellation is asynchronous: the task may stay ACTIVE/INACTIVE for a
moment after the cancel request before flipping to a terminal status. This is a
race against the transfer actually finishing first (especially for a tiny file),
so you may need to run this more than once to land on FAILED instead of SUCCEEDED.

Produces:
  design/fixtures/globus_task_failed.json   (only written if the task ends up FAILED)

Usage:
  python design/capture_task_failed.py \\
    --client-id <UUID> \\
    --source-collection-id <UUID> \\
    --source-path /full/path/to/real/source/file \\
    --dest-id <UUID> \\
    --dest-path /full/path/to/dest/file
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _capture_auth import FIXTURES_DIR, make_transfer_client

from globus_sdk import TransferData

TERMINAL_STATUSES = ("SUCCEEDED", "FAILED")


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument("--source-collection-id", required=True, metavar="UUID",
                        help="A real source collection the service account can read")
    parser.add_argument("--source-path", required=True,
                        help="Full path to a real (existing) file on the source collection")
    parser.add_argument("--dest-id", required=True, metavar="UUID",
                        help="A real destination collection the service account can write to")
    parser.add_argument("--dest-path", required=True,
                        help="Full destination file path (e.g. /deleteme/<test_id>/real.txt)")
    parser.add_argument("--max-attempts", type=int, default=20,
                        help="How many times to poll get_task() before giving up (default: 20)")
    parser.add_argument("--poll-interval", type=float, default=1.0,
                        help="Seconds to wait between polls (default: 1.0)")
    args = parser.parse_args()

    client = make_transfer_client(args.client_id, args.client_secret)
    client.add_app_data_access_scope(args.source_collection_id)

    td = make_transfer_data(args.source_collection_id, args.dest_id)
    td.add_item(args.source_path, args.dest_path)
    submit_resp = client.submit_transfer(td)
    task_id = submit_resp.data["task_id"]
    print(f"Submitted task {task_id}, canceling immediately...")

    client.cancel_task(task_id)

    status = None
    data = None
    for attempt in range(1, args.max_attempts + 1):
        resp = client.get_task(task_id)
        data = dict(resp.data)
        status = data["status"]
        print(f"  poll {attempt}/{args.max_attempts}: status={status!r}")
        if status in TERMINAL_STATUSES:
            break
        time.sleep(args.poll_interval)

    if status == "FAILED":
        path = FIXTURES_DIR / "globus_task_failed.json"
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"Saved {path}  (status={status!r})")
    elif status == "SUCCEEDED":
        print(
            "Lost the race: the transfer completed before cancellation took effect.\n"
            "Try again — a larger or slower-to-transfer source file may help."
        )
    else:
        print(
            f"Gave up after {args.max_attempts} polls; task is still {status!r} "
            "(cancellation may still be processing). Re-run capture_task_status.py\n"
            "on this task ID later, or increase --max-attempts/--poll-interval."
        )


if __name__ == "__main__":
    main()