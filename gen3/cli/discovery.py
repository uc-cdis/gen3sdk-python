import asyncio
import click
import csv

from gen3.tools.metadata.discovery import (
    publish_discovery_metadata,
    output_expanded_discovery_metadata,
    try_delete_discovery_guid,
    combine_discovery_metadata,
)
from gen3.utils import get_or_create_event_loop_for_thread


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

    loop = get_or_create_event_loop_for_thread()
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
    loop = get_or_create_event_loop_for_thread()
    endpoint = ctx.obj.get("endpoint")
    output_file = loop.run_until_complete(
        output_expanded_discovery_metadata(
            auth, endpoint=endpoint, limit=limit, use_agg_mds=agg
        )
    )

    click.echo(output_file)


@click.command()
@click.argument("file", required=False)
@click.option(
    "--output-filename",
    "output_filename",
    help="filename for final combined output",
    default="combined_discovery_metadata.tsv",
    show_default=True,
)
@click.option(
    "--max-number-discovery-records",
    "max_number_discovery_records",
    help="max number of metadata records to fetch. Initially defaulted to something very high. If you start to miss records, you may need to bump this up.",
    default=8192,
    type=int,
    show_default=True,
)
@click.option(
    "--discovery-column-to-map-on",
    "discovery_column_to_map_on",
    help="The column in the current discovery metadata to use to",
    default="guid",
    show_default=True,
)
@click.option(
    "--metadata-column-to-map",
    "metadata_column_to_map",
    help="The column in the provided metadata file to use to map/merge into the current Discovery metadata",
    default="guid",
    show_default=True,
)
@click.option(
    "--metadata-prefix",
    "metadata_prefix",
    help="Prefix to add to the column names in the provided metadata file before final output",
    default="",
    show_default=True,
)
@click.option(
    "--agg",
    is_flag=True,
    help="use aggregate metadata service instead of the metadata service",
    show_default=True,
)
@click.pass_context
def discovery_read_and_combine(
    ctx,
    file,
    output_filename,
    discovery_column_to_map_on,
    metadata_column_to_map,
    max_number_discovery_records,
    metadata_prefix,
    agg,
):
    """
    Combine provided metadata from file with current commons' discovery page metadata.
    Outputs a TSV with the original discovery metadata and provided metadata.
    """
    auth = ctx.obj["auth_factory"].get()
    loop = get_or_create_event_loop_for_thread()
    endpoint = ctx.obj.get("endpoint")
    current_discovery_metadata_file = loop.run_until_complete(
        output_expanded_discovery_metadata(
            auth, endpoint=endpoint, limit=max_number_discovery_records, use_agg_mds=agg
        )
    )

    output_file = combine_discovery_metadata(
        current_discovery_metadata_file,
        file,
        discovery_column_to_map_on,
        metadata_column_to_map,
        output_filename,
        metadata_prefix=metadata_prefix,
    )

    click.echo(f"{output_file}")


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
discovery.add_command(discovery_read_and_combine, name="combine")
discovery.add_command(discovery_publish, name="publish")
discovery.add_command(discovery_delete, name="delete")
