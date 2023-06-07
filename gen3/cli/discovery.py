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
    help="Publishes {commons}-{guid_type}.tsv from current directory",
    show_default=True,
)
@click.option(
    "--omit-empty",
    "omit_empty",
    is_flag=True,
    help="omit fields from empty columns if set",
    show_default=True,
)
@click.option(
    "--guid-type",
    "guid_type",
    default="discovery_metadata",
    help=(
        "The value of this gets set as _guid_type in the root level metadata. "
        "discovery_metadata is the default that enables the Gen3 Discovery Page to visualize the results."
    ),
    show_default=True,
)
@click.option(
    "--guid_field",
    "guid_field",
    help=(
        'The column / field name within the metadata that will be used as GUIDs, if not specified, will try to find a column \ field named "guid" from the metadata.'
        "If that field doesn't exists in a certain metadata record, that record will be skipped from publishing."
    ),
    default=None,
    show_default=True,
)
@click.pass_context
def discovery_publish(ctx, file, use_default_file, omit_empty, guid_type, guid_field):
    """
    Run a discovery metadata ingestion on a given metadata TSV / JSON file with guid column / field.
    If [FILE] is omitted and --default-file not set, prompts for TSV / JSON file name.
    """
    auth = ctx.obj["auth_factory"].get()
    if not file and not use_default_file:
        file = click.prompt("Enter discovery metadata TSV / JSON file to publish")

    loop = get_or_create_event_loop_for_thread()
    endpoint = ctx.obj.get("endpoint")
    loop.run_until_complete(
        publish_discovery_metadata(
            auth,
            file,
            endpoint=endpoint,
            omit_empty_values=omit_empty,
            guid_type=guid_type,
            guid_field=guid_field,
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
@click.option(
    "--guid_type",
    "guid_type",
    help="value of intended GUID type for query",
    default="discovery_metadata",
    show_default=True,
)
@click.option(
    "--output_format",
    "output_format",
    help="format of output file (can only be either tsv or json)",
    default="tsv",
    show_default=True,
)
@click.option(
    "--output_filename_suffix",
    "output_filename_suffix",
    help="additional suffix for the output file name",
    default="",
    show_default=True,
)
@click.pass_context
def discovery_read(ctx, limit, agg, guid_type, output_format, output_filename_suffix):
    """
    Download the metadata used to populate a commons' discovery page into a TSV or JSON file.
    Outputs the TSV / JSON filename with format {commons-url}-{guid_type}.tsv/.json
    If "output_filename_suffix" exists, file name will be something like {commons-url}-{guid_type}-{output_filename_suffix}
    """
    auth = ctx.obj["auth_factory"].get()
    loop = get_or_create_event_loop_for_thread()
    endpoint = ctx.obj.get("endpoint")
    output_file = loop.run_until_complete(
        output_expanded_discovery_metadata(
            auth,
            endpoint=endpoint,
            limit=limit,
            use_agg_mds=agg,
            guid_type=guid_type,
            output_format=output_format,
            output_filename_suffix=output_filename_suffix,
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
