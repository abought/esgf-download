"""
Snapshot the current get_task() response for a Globus transfer task.

Run this at each lifecycle stage to capture the fixture for that state.
The --output argument controls the filename written to design/fixtures/.

Produces:
  design/fixtures/<output>.json

Typical capture sequence:

  # While transfer is running:
  python design/capture_task_status.py --task-id <ID> --output globus_task_active     ...
  python design/capture_task_status.py --task-id <ID> --output globus_task_inactive   ...  (if collection pauses)

  # After transfer completes:
  python design/capture_task_status.py --task-id <ID> --output globus_task_succeeded_clean       ...
  python design/capture_task_status.py --task-id <ID> --output globus_task_succeeded_with_skips  ...
  python design/capture_task_status.py --task-id <ID> --output globus_task_failed                ...
  python design/capture_task_status.py --task-id <ID> --output globus_task_canceled              ...
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
    parser.add_argument("--task-id", required=True, help="Globus transfer task UUID")
    parser.add_argument("--output", required=True, metavar="FILENAME",
                        help="Output filename without .json extension (e.g. globus_task_active)")
    args = parser.parse_args()

    client = make_transfer_client(args.client_id, args.client_secret)

    resp = client.get_task(args.task_id)
    data = dict(resp.data)

    path = FIXTURES_DIR / f"{args.output}.json"
    path.write_text(json.dumps(data, indent=2, default=str))
    print(f"Saved {path}  (status={data.get('status')!r})")


if __name__ == "__main__":
    main()