from globus_sdk import TransferClient

from esgpull.config import Config
from esgpull.globus.auth import get_globus_app


def get_transfer_client(config: Config) -> TransferClient:
    app = get_globus_app(config)
    return TransferClient(app=app)
