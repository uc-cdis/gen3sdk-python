import asyncio

import click

from gen3.tools.metadata.discovery import (
    publish_discovery_metadata,
    output_expanded_discovery_metadata,
    try_delete_discovery_guid,
)


@click.group()
def discovery():
    """Commands for reading and editing discovery metadata"""
    pass


@click.command()
@click.argument("file", required=False)
@click.option(
    "--default-file",
    "use_default_file",
    is_flag=True,
    help="Publishes {commons}-discovery_metadata.tsv from current directory",
    show_default=True,
)
@click.option(
    "--omit-empty",
    "omit_empty",
    is_flag=True,
    help="omit fields from empty columns if set",
    show_default=True,
)
@click.pass_context
def discovery_publish(ctx, file, use_default_file, omit_empty):
    """
    Run a discovery metadata ingestion on a given metadata TSV file with guid column.
    If [FILE] is omitted and --default-file not set, prompts for TSV file name.
    """
    auth = ctx.obj["auth_factory"].get()
    if not file and not use_default_file:
        file = click.prompt("Enter discovery metadata TSV file to publish")

    loop = asyncio.get_event_loop()
    endpoint = ctx.obj.get("endpoint")
    loop.run_until_complete(
        publish_discovery_metadata(
            auth, file, endpoint=endpoint, omit_empty_values=omit_empty
        )
    )


@click.command()
@click.option(
    "--limit",
    "limit",
    help="max number of metadata records to fetch",
    default=500,
    show_default=True,
)
@click.option(
    "--agg",
    is_flag=True,
    help="use aggregate metadata service instead of the metadata service",
    show_default=True,
)
@click.pass_context
def discovery_read(ctx, limit, agg):
    """
    Download the metadata used to populate a commons' discovery page into a TSV.
    Outputs the TSV filename with format {commons-url}-discovery_metadata.tsv
    """
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()
    endpoint = ctx.obj.get("endpoint")
    output_file = loop.run_until_complete(
        output_expanded_discovery_metadata(
            auth, endpoint=endpoint, limit=limit, use_agg_mds=agg
        )
    )

    click.echo(output_file)


@click.command()
@click.argument("guid")
@click.pass_context
def discovery_delete(ctx, guid):
    """
    Delete all discovery metadata for the provided guid
    """
    auth = ctx.obj["auth_factory"].get()
    try_delete_discovery_guid(auth, guid)


discovery.add_command(discovery_read, name="read")
discovery.add_command(discovery_publish, name="publish")
discovery.add_command(discovery_delete, name="delete")
