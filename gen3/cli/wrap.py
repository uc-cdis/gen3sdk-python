import click

from cdislogging import get_logger

from gen3.tools.wrap import Gen3Wrap


logger = get_logger("__name__")


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="A wrapper command that forwards COMMAND_ARGS as-is with GEN3_TOKEN set to Gen3Auth's access token",
)
@click.argument("command_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def wrap(ctx, command_args):
    auth = ctx.obj["auth_factory"].get()
    gen3Wrap_object = Gen3Wrap(auth, command_args)
    logger.info(
        f"Running the command {command_args} with gen3 access token in environment variable"
    )
    gen3Wrap_object.run_command()
