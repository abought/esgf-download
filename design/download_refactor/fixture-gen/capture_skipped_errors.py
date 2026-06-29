"""
Capture paginated task_skipped_errors() responses for a completed Globus transfer.

The task must have status SUCCEEDED with subtasks_skipped_errors > 0.
Each page of the paginated response is saved as a separate fixture file.

To capture checksum-specific skip reasons (case o), run against a transfer where
verify_checksum=True caused failures, then rename the output to
globus_skipped_errors_checksum.json (or pass --output-prefix globus_skipped_errors_checksum).

Produces:
  design/fixtures/<prefix>_page1.json
  design/fixtures/<prefix>_page2.json   (if more than one page)
  ...
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _capture_auth import FIXTURES_DIR, make_transfer_client


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument("--task-id", required=True,
                        help="Globus transfer task UUID (must be SUCCEEDED with skipped errors)")
    parser.add_argument("--output-prefix", default="globus_skipped_errors", metavar="PREFIX",
                        help="Filename prefix; pages saved as <prefix>_page1.json etc. "
                             "(default: globus_skipped_errors)")
    args = parser.parse_args()

    client = make_transfer_client(args.client_id, args.client_secret)

    # Verify preconditions before paginating
    task_resp = client.get_task(args.task_id)
    status = task_resp.data.get("status")
    n_skipped = task_resp.data.get("subtasks_skipped_errors", 0)

    if status != "SUCCEEDED":
        print(f"WARNING: task status is {status!r}, expected SUCCEEDED")
    if n_skipped == 0:
        print("WARNING: subtasks_skipped_errors == 0; no skipped errors to capture")
        return

    print(f"Task {args.task_id}: status={status!r}, subtasks_skipped_errors={n_skipped}")

    pager = client.paginated.task_skipped_errors(args.task_id)
    for i, page in enumerate(pager.pages(), start=1):
        data = dict(page.data)
        path = FIXTURES_DIR / f"{args.output_prefix}_page{i}.json"
        path.write_text(json.dumps(data, indent=2, default=str))
        n_items = len(data.get("DATA", []))
        print(f"Saved {path}  ({n_items} items)")


if __name__ == "__main__":
    main()