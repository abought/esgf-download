"""
Capture the GlobusAPIError response body for a 401 (invalid/expired credentials) error.

Uses a deliberately bad access token to trigger a 401 from get_task() without
requiring any valid credentials. The --task-id only needs to be a plausible UUID;
the 401 is returned before Globus looks up the task.

Produces:
  design/fixtures/globus_task_401.json

Note on 503 (service unavailable):
  503 errors cannot be triggered on demand. To obtain
  design/fixtures/globus_task_503.json, either:
    - Watch https://status.globus.org and run capture_task_status.py during an outage
    - Hand-craft the fixture using the Globus API error format:
        {"http_status": 503, "body": {"code": "ServiceUnavailable", "message": "..."}}
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _capture_auth import FIXTURES_DIR

from globus_sdk import AccessTokenAuthorizer, GlobusAPIError, TransferClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--task-id", required=True,
                        help="Any plausible task UUID (401 fires before task lookup)")
    args = parser.parse_args()

    FIXTURES_DIR.mkdir(exist_ok=True)

    bad_client = TransferClient(
        authorizer=AccessTokenAuthorizer("invalid_token_for_fixture_capture")
    )

    try:
        bad_client.get_task(args.task_id)
        print("ERROR: expected a 401 but the request succeeded — bad token was somehow accepted")
    except GlobusAPIError as e:
        if e.http_status != 401:
            print(f"WARNING: expected HTTP 401, got {e.http_status}")
        data = {"http_status": e.http_status, "body": e.raw_json}
        path = FIXTURES_DIR / "globus_task_401.json"
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"Saved {path}  (HTTP {e.http_status})")


if __name__ == "__main__":
    main()