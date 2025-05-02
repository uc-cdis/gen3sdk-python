import click


from gen3.tools.wrap import Gen3Wrap


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="A wrapper command that forwards COMMAND_ARGS as-is after setting the environment variable GEN3_TOKEN",
)
@click.argument("command_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def run(ctx, command_args):
    auth = ctx.obj["auth_factory"].get()
    gen3Wrap_object = Gen3Wrap(auth, command_args)
    gen3Wrap_object.run_command()
