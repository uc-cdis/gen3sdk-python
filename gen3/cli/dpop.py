"""CLI commands for DPoP proxy management."""

import threading

import click
from cdislogging import get_logger

from gen3.auth import Gen3Auth
from gen3.dpop import dpop_proxy_context, USER_FACING_ERRORS

logging = get_logger(__name__)


@click.group()
def dpop() -> None:
    """DPoP proxy management commands."""
    pass


@dpop.group()
def proxy() -> None:
    """Manage the DPoP proxy server."""
    pass


@proxy.command(name="start")
@click.option(
    "--port",
    type=int,
    default=0,
    envvar="GEN3_DPOP_PROXY_PORT",
    show_envvar=True,
    help="Port to run proxy on. Set ENV VAR, otherwise defaults to an OS-assigned free port.",
)
@click.option(
    "--task-token-type",
    type=str,
    default="WORKFLOW",
    show_default=True,
    help="Type of task token to request.",
)
@click.option(
    "--task-token-expiration",
    type=int,
    default=None,
    help="Task token expiration time in seconds. Server default if omitted.",
)
@click.option(
    "--tes-endpoint",
    type=str,
    default=None,
    help="Will default to standard location in the Gen3 instance of your API key.",
)
@click.option(
    "--s3-endpoint",
    type=str,
    default=None,
    help="Will default to standard location in the Gen3 instance of your API key.",
)
@click.pass_context
def proxy_start_command(
    ctx: click.Context,
    port: int,
    task_token_type: str,
    task_token_expiration: int | None,
    tes_endpoint: str | None,
    s3_endpoint: str | None,
) -> None:
    """
    Start the DPoP proxy server in the foreground.

    Point Nextflow (or another client) at the port and stop the proxy
    with Ctrl+C. See docs/nextflow.md for the Nextflow configuration.
    """
    stop_event = threading.Event()
    try:
        auth: Gen3Auth = ctx.obj.get("auth_factory").get()

        with dpop_proxy_context(
            auth=auth,
            port=port or None,
            task_token_type=task_token_type,
            task_token_expiration=task_token_expiration,
            tes_endpoint=tes_endpoint,
            s3_endpoint=s3_endpoint,
        ) as (_, proxy_port):
            logging.warning(
                f"***  Starting Gen3 DPoP proxy on http://127.0.0.1:{proxy_port} (Press Ctrl+C to stop)...  ***"
            )
            try:
                stop_event.wait()
            except KeyboardInterrupt:
                pass
            finally:
                logging.info("***  Stopped Gen3 DPoP proxy.  ***")
    except USER_FACING_ERRORS as exc:
        # These say what the user should do
        raise click.ClickException(str(exc)) from exc
