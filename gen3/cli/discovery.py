import asyncio
import click
import csv

from gen3.tools.metadata.discovery import (
    publish_discovery_metadata,
    output_expanded_discovery_metadata,
    try_delete_discovery_guid,
    combine_discovery_metadata,
)
from gen3.tools.metadata.discovery_objects import (
    output_discovery_objects,
    publish_discovery_object_metadata,
    try_delete_discovery_objects,
    try_delete_discovery_objects_from_dict,
    is_valid_object_manifest,
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
    if is_valid_object_manifest(file):
        click.confirm(
            "WARNING! It appears like you are attempting to publish discovery **objects** (based on the file provided) but you are attempting to use the non-object, dataset-level command `gen3 discovery publish`. Perhaps you meant to use `gen3 discovery objects publish`. Are you sure you want proceed with publishing the dataset-level discovery metadata using the object file?",
            abort=True,
        )

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


@discovery.group()
def objects():
    """For ingesting dataset-level files"""
    pass


@click.command(
    help="""
    Outputs a TSV with populated information. [DATASET_GUIDS] argument is
    a variable number of datasets, e.g. 'phs000001.v1.p1.c1 phs000002.v1.p1.c1'
    """
)
@click.argument(
    "dataset_guids",
    nargs=-1,
)
@click.option(
    "--only-object-guids",
    help="""
    Outputs object guids to the command line in the following format:
        drs://dg.4503:943200c3-271d-4a04-a2b6-040272239a64
        drs://dg.4503:58818753-e8b9-4225-99bd-635a36ac41d9
        drs://dg.4503:72a9051e-5afa-4eb1-8faf-2fb28369f99b
        drs://dg.4503:034e406d-f8f1-4ee8-bbb3-3818473174a6
    Output can be piped into another command
    """,
    is_flag=True,
)
@click.option(
    "--template",
    help="""
    Output a TSV with required columns for `publish`. Will NOT contain any actual existing values for datasets or objects.
    If you want the full output of what's in the commons, don't supply this flag.
    """,
    is_flag=True,
)
@click.option(
    "--output_format",
    help="format of output file (can only be either tsv or json)",
    default="tsv",
    show_default=True,
)
@click.pass_context
def discovery_objects_read(
    ctx, dataset_guids, only_object_guids, template, output_format
):
    """
    Download the discovery objects from dataset(s) from a commons into TSV file.
    Outputs the TSV / JSON filename with format {commons-url}-discovery_objects.tsv
    If --only-object-guids is set, output object guids to the command line.
    If the --template option is set, output a TSV with just the required columns with
    format {commons-url}-discovery_objects-TEMPLATE.tsv
    """
    auth = ctx.obj["auth_factory"].get()
    loop = get_or_create_event_loop_for_thread()
    endpoint = ctx.obj.get("endpoint")
    output = loop.run_until_complete(
        output_discovery_objects(
            auth,
            dataset_guids=dataset_guids,
            endpoint=endpoint,
            template=template,
            output_format=output_format,
            only_object_guids=only_object_guids,
        )
    )
    if not only_object_guids:
        click.echo(output)
    else:
        for obj in output:
            click.echo(obj["guid"])


@click.command(
    help="""
    Takes a TSV as input and writes the specified objects GUIDs and defined metadata into Gen3's
    Metadata API, under a Discovery Metadata record for the dataset. The specified content from
    the TSV goes into an 'objects' block of the dataset's metadata.
    If dataset_guid already exists, update, if it doesn't already exist, create it.
    Use 'discovery objects read --template' to get a TSV with the minimum required columns
    to publish.
    """
)
@click.argument(
    "file",
    required=True,
)
@click.option(
    "--overwrite",
    help="""
    Replaces all guids in the 'objects' field of a dataset instead of appending (which is the default)
    """,
    is_flag=True,
)
@click.pass_context
def discovery_objects_publish(
    ctx,
    file,
    overwrite,
):
    """
    Run a discovery objects ingestion on a given metadata TSV file. If --overwrite is set, objects for
    each dataset are replaced instead of appeding by default.
    """
    auth = ctx.obj["auth_factory"].get()

    loop = get_or_create_event_loop_for_thread()
    endpoint = ctx.obj.get("endpoint")
    loop.run_until_complete(
        publish_discovery_object_metadata(
            auth,
            file,
            endpoint=endpoint,
            overwrite=overwrite,
        )
    )


@click.command(
    help="Delete objects related to datasets in TSV file or list of dataset_guids"
)
@click.argument(
    "guids",
    nargs=-1,
)
@click.option(
    "--file",
    help="""
    Delete objects from a file
    """,
    type=click.File("r"),
)
@click.pass_context
def discovery_objects_delete(
    ctx,
    guids,
    file,
):
    """
    Delete all discovery objects for the provided guid(s), or from the the file provided by the --file option.
    [GUIDS] argument is a variable number of dataset GUIDs, e.g. 'phs000001.v1.p1.c1 phs000002.v1.p1.c1'
    """
    auth = ctx.obj["auth_factory"].get()
    if guids:
        try_delete_discovery_objects(auth, guids)
    if file:
        if is_valid_object_manifest(file.name):
            delete_objs = {}
            tsv_reader = csv.DictReader(file, delimiter="\t")
            for row in tsv_reader:
                dataset_guid = row["dataset_guid"]
                guid = row["guid"]
                delete_objs.setdefault(dataset_guid, set()).add(guid)
            try_delete_discovery_objects_from_dict(auth, delete_objs)
        else:
            click.echo("Invalid objects file")


objects.add_command(discovery_objects_read, name="read")
objects.add_command(discovery_objects_publish, name="publish")
objects.add_command(discovery_objects_delete, name="delete")
