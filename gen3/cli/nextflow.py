"""
CLI commands for running Nextflow workflows with DPoP-bound task tokens.
"""

import click
from cdislogging import get_logger

from gen3.auth import Gen3Auth
from gen3.dpop import USER_FACING_ERRORS
from gen3.dpop_nextflow import run_gen3_nextflow

logging = get_logger(__name__)


@click.group()
def nextflow():
    """Nextflow usage in Gen3"""
    pass


@nextflow.command(
    name="run",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
@click.argument("nextflow_args", nargs=-1, type=click.UNPROCESSED)
# The following options mirror `gen3 dpop proxy start`; both hand them to dpop_proxy_context().
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
@click.option(
    "--no-generated-config",
    is_flag=True,
    help="Do not generate the config that redirects Nextflow to the proxy. Your own "
    "config then has to point tes.endpoint and aws.client.endpoint at "
    "127.0.0.1:$GEN3_DPOP_PROXY_PORT itself, and use $GEN3_DPOP_BOUND_TASK_TOKEN as "
    "tes.oauthToken and aws.accessKey; see the Nextflow guide in the docs.",
)
@click.pass_context
def nextflow_run(
    ctx: click.Context,
    nextflow_args: tuple,
    port: int,
    task_token_type: str,
    task_token_expiration: int | None,
    tes_endpoint: str | None,
    s3_endpoint: str | None,
    no_generated_config: bool,
) -> None:
    """
    Run Nextflow with a Gen3 DPoP proxy.

    Requires Gen3Auth credentials (API key or refresh token) configured. TES and
    S3 endpoints are determined from the credentials.

    Keep your normal Gen3 Nextflow config, the one pointing at your commons. A
    small override redirecting TES and S3 to the proxy is generated and passed with
    `-c` last, so nothing else in your config changes.

    Anything not listed below is passed through to `nextflow run`. If your
    pipeline takes a parameter named like one of these options, put `--` first:
    everything after it goes to Nextflow untouched.

    Examples: `gen3 -v nextflow run hello.nf`, `gen3 -v nextflow run main.nf -resume`,
    `gen3 -v nextflow run main.nf -- --port 8080`
    """
    logging.info("Running Nextflow with DPoP proxy...")

    try:
        auth: Gen3Auth = ctx.obj.get("auth_factory").get()
        return_code = run_gen3_nextflow(
            nf_args=list(nextflow_args),
            auth=auth,
            port=port or None,
            task_token_type=task_token_type,
            task_token_expiration=task_token_expiration,
            tes_endpoint=tes_endpoint,
            s3_endpoint=s3_endpoint,
            generate_config=not no_generated_config,
        )
    except (*USER_FACING_ERRORS, FileNotFoundError) as exc:
        # These say what the user should do
        raise click.ClickException(str(exc)) from exc

    if return_code != 0:
        logging.error(
            f"!!! Nextflow errored. It exited with a non-zero exit code: {return_code}. "
            f"See above logs for more error info from Nextflow. !!!"
        )

    # Click discards whatever a command returns, so the exit code has to be set
    # explicitly or a failed pipeline would look successful to the caller.
    ctx.exit(return_code)
