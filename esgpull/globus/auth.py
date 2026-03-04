from globus_sdk import AuthClient, ClientApp, GlobusApp, GlobusAppConfig
from globus_sdk.token_storage import JSONTokenStorage

from esgpull.config import Config

_GLOBUS_TOKENS_FILENAME = "globus-tokens.json"


def get_globus_app(config: Config) -> GlobusApp:
    """Get an appropriately configured Globus app for the given esgpull workspace"""
    # TODO future: if client secret omitted, support UserApp mode using a bundled or self-registered client_id
    storage = JSONTokenStorage(config.paths.auth / _GLOBUS_TOKENS_FILENAME)

    options = GlobusAppConfig(auto_redrive_gares=True, token_storage=storage)
    return ClientApp(
        client_id=config.globus.client_id,
        client_secret=config.globus.client_secret,
        config=options,
    )


def get_user_string(app: GlobusApp) -> str:
    """Return a human readable representation of the current logged in user"""
    ac = AuthClient(app=app)
    resp = ac.userinfo()
    return f'{resp.data["name"]} ({resp.data["sub"]})'
