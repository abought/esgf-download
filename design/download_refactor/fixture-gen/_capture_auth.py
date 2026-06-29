"""Shared auth helper for fixture capture scripts."""
from pathlib import Path

from globus_sdk import GlobusAppConfig, TransferClient, UserApp
from globus_sdk.token_storage import JSONTokenStorage

FIXTURES_DIR = Path(__file__).parent / "fixtures"
_TOKENS_FILE = FIXTURES_DIR / ".tokens.json"


def make_transfer_client(client_id: str, client_secret: str | None) -> TransferClient:
    FIXTURES_DIR.mkdir(exist_ok=True)
    app = UserApp(
        "esgpull-fixture-capture",
        client_id=client_id,
        client_secret=client_secret,
        config=GlobusAppConfig(token_storage=JSONTokenStorage(_TOKENS_FILE)),
    )
    return TransferClient(app=app)