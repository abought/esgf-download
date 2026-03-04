"""
Commands for managing globus transfer features

The actual transfer process will be handled transparently using existing download commands.

These commands are focused on ensuring that the user is logged in, and diagnosing common
 permission issues.
"""

import click
from click.exceptions import Abort, Exit
from globus_sdk.services.auth.errors import AuthAPIError

from esgpull.cli.utils import init_esgpull
from esgpull.cli.decorators import opts
from esgpull.globus.auth import get_globus_app, get_user_string
from esgpull.tui import Verbosity


@click.group
def globus():
    """Manage globus identities"""
    pass


# @globus.command
# @opts.verbosity
# def login(verbosity: Verbosity):
#     """Log in to the provided Globus identity.
#     TODO: Revisit this when user login is needed. Client credentials will be used to get a token seamlessly.
#      """
#     esg = init_esgpull(verbosity, load_db=False)
#
#     with esg.ui.logging("globus-login", onraise=Abort):
#         client_id = esg.config.globus.client_id
#         client_secret = esg.config.globus.client_secret
#         app = get_globus_app(client_id, client_secret)
#
#         # These are present for their side effects. They automatically set minimum scopes needed on the request.
#         ac = AuthClient(app=app)
#         tc = TransferClient(app=app)
#
#         app.login()
#         user = get_user_string(app)
#
#         esg.ui.print(f"Successfully logged in as: {user}")


@globus.command
@opts.verbosity
def whoami(verbosity: Verbosity):
    """Print the name of the current Globus user."""
    esg = init_esgpull(verbosity, load_db=False)

    with esg.ui.logging("globus-whoami", onraise=Abort):
        client_id = esg.config.globus.client_id
        client_secret = esg.config.globus.client_secret
        if not (client_id and client_secret):
            # Stub. This branch will be more useful when UserApp implemented.
            esg.ui.print("No globus identity was provided", err=True)
            esg.ui.raise_maybe_record(Exit(1))

        app = get_globus_app(esg.config)

        try:
            user = get_user_string(app)
            esg.ui.print(user)
        except AuthAPIError:
            esg.ui.print("User cannot be authenticated", err=True)
            esg.ui.raise_maybe_record(Exit(1))


@globus.command("check-permissions")
@opts.verbosity
def check():
    """
    Check the permissions associated with the logged-in identity. This may be used as a diagnostic command to ensure
    that the provided identity has read and write access on the destination collection.
    """
    pass
