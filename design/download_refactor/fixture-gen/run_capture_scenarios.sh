#!/usr/bin/env bash
# Manual fixture-capture run against real Globus assets.
# Fill in the placeholders below, then run each block by hand (not meant to be
# executed unattended — each submit_transfer queues a REAL transfer task).
set -euo pipefail

# --- auth ---------------------------------------------------------------
GLOBUS_CLIENT_ID="<your-globus-app-client-id>"        # required by every script
# GLOBUS_CLIENT_SECRET="<secret>"                      # only for a confidential/service client;
                                                         # omit for native-app browser login

# --- assets ---------------------------------------------------------------
SOURCE_COLLECTION_ID="<CORRECTED-SOURCE-UUID>"          # Globus Tutorial Collection 1 — the UUID you
                                                         # pasted (6c54cade-bde5-45c1-bdea-f4bd71dbloa2cc)
                                                         # has invalid hex chars (l, o) and a 14-char
                                                         # final segment; please re-paste the real one
SOURCE_DIR="/home/u_24weaxffujbulbpdjk42up2a5u"
DEST_COLLECTION_ID="dba0d7c0-1f63-44d1-bcd0-76865d3d44a0"   # abought-js2 - GUEST

NO_PERMISSION_DEST_ID="<NO-PERMISSION-DEST-UUID>"       # a collection you can auth to but not write to
NO_PERMISSION_DEST_PATH="/fixture_probe.nc"

# ===========================================================================
# Scenario A: full success (case h) — real.txt moves cleanly
# ===========================================================================
TEST_ID="success"
python design/capture_submit_transfer.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --source-collection-id "$SOURCE_COLLECTION_ID" \
  --source-path "$SOURCE_DIR/real.txt" \
  --dest-id "$DEST_COLLECTION_ID" \
  --dest-path "/deleteme/$TEST_ID/real.txt"
# ^ also re-captures globus_submit_error_invalid_endpoint.json (runs every time)
# Prints a task_id — copy it below.

TASK_ID="<paste-task-id-from-scenario-A>"

# Optional: catch it mid-flight for an ACTIVE snapshot (case h/j/k/l) —
# run this quickly after submission, before it finishes.
python design/capture_task_status.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --task-id "$TASK_ID" \
  --output globus_task_active

# After it reaches SUCCEEDED (check the Globus web UI, or re-run the above
# with a different --output until status flips):
python design/capture_task_status.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --task-id "$TASK_ID" \
  --output globus_task_succeeded_clean

# ===========================================================================
# Scenario B: "task ran but file did not move" (case b/g) — missing source file
# ===========================================================================
TEST_ID="skip-missing-file"
python design/capture_submit_transfer.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --source-collection-id "$SOURCE_COLLECTION_ID" \
  --source-path "$SOURCE_DIR/fake.txt" \
  --dest-id "$DEST_COLLECTION_ID" \
  --dest-path "/deleteme/$TEST_ID/fake.txt"

TASK_ID="<paste-task-id-from-scenario-B>"

# Wait for it to reach SUCCEEDED with subtasks_skipped_errors > 0, then:
python design/capture_task_status.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --task-id "$TASK_ID" \
  --output globus_task_succeeded_with_skips

python design/capture_skipped_errors.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --task-id "$TASK_ID"
# -> design/fixtures/globus_skipped_errors_page1.json (and _page2 if it paginates)

# ===========================================================================
# Scenario C: permission denied (case c/f/i) — no write access of any kind
# ===========================================================================
python design/capture_submit_transfer.py \
  --client-id "$GLOBUS_CLIENT_ID" \
  --source-collection-id "$SOURCE_COLLECTION_ID" \
  --source-path "$SOURCE_DIR/real.txt" \
  --no-permission-dest-id "$NO_PERMISSION_DEST_ID" \
  --no-permission-dest-path "$NO_PERMISSION_DEST_PATH"
# If Globus accepts the submission and fails post-start instead of rejecting
# at submit time, follow up with capture_task_status.py on the printed
# task_id (--output globus_task_failed) once it reaches FAILED.

# ===========================================================================
# Scenario D: expired/invalid credentials (case n, HTTP 401) — asset-independent
# ===========================================================================
python design/capture_auth_error.py --task-id 00000000-0000-0000-0000-000000000000