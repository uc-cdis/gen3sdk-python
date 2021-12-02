import asyncio
import click
import csv
import logging
import os
import sys
from urllib.parse import urlparse

from gen3.tools import indexing
from gen3.tools.indexing import (
    is_valid_manifest_format,
    index_object_manifest,
    delete_all_guids,
)
from gen3.tools.indexing.verify_manifest import manifest_row_parsers


@click.group()
def objects():
    """Commands for reading and editing objects"""
    pass


@objects.group()
def manifest():
    """For working with minimal object metadata manifests"""
    pass


@click.command(
    help="Creates an object manifest of current data by reading from the Gen3 instance."
)
@click.option(
    "--output-file",
    "output_file",
    default="object-manifest.csv",
    help="filename for output",
    type=click.Path(writable=True),
)
@click.option(
    "--num-processes",
    "num_processes",
    default=4,
    help="Number of parallel Python processes to start",
    type=int,
)
@click.option(
    "--max-concurrent-requests",
    "max_concurrent_requests",
    help="Maximum concurrent requests to Gen3",
    default=24,
    type=int,
)
@click.pass_context
def objects_manifest_read(ctx, output_file, num_processes, max_concurrent_requests):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()
    click.echo(f"Getting minimal object metadata from {auth.endpoint}")
    loop.run_until_complete(
        indexing.async_download_object_manifest(
            auth.endpoint,
            output_filename=output_file,
            num_processes=num_processes,
            max_concurrent_requests=max_concurrent_requests,
        )
    )
    click.echo(output_file)


@click.command(
    help="Verifies that a Gen3 instance contains the data in the specified object manifest."
)
@click.argument("file", required=False)
@click.option(
    "--max-concurrent-requests",
    "max_concurrent_requests",
    help="Maximum concurrent requests to Gen3",
    default=24,
    type=int,
)
@click.pass_context
def objects_manifest_verify(ctx, file, max_concurrent_requests):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()

    if not file:
        file = click.prompt("Enter Discovery metadata file path to publish")

    click.echo(f"Verifying {file}...\n    Against: {auth.endpoint}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        indexing.async_verify_object_manifest(
            auth.endpoint,
            manifest_file=file,
            max_concurrent_requests=max_concurrent_requests,
        )
    )


@click.command(
    help="Validates that the format of the specified object manifest is correct."
)
@click.argument("file", required=False)
@click.option(
    "--allowed-protocols",
    "allowed_protocols",
    help="""
    space-delimited string list of allowed protocols for url validation. (default: "s3 gs")

    Note that if allowed_protocols is provided, url values will only be
    validated using the provided protocols (e.g. if
    allowed_protocols="http https" an error would be raised when
    validating "s3://valid_bucket/valid_key" url)
    """,
    default="s3 gs",
)
@click.option(
    "--allow-base64-encoded-md5",
    "allow_base64_encoded_md5",
    help="""
    whether or not Base64 encoded md5 values are allowed. (default: False)

    if False, only hexadecimal encoded 128-bit md5 values are considered valid,
    and Base64 encoded values will be logged as errors.

    if arg provided, both hexadecimal and Base64 encoded 128-bit md5 values are considered
    valid
    """,
    is_flag=True,
    default=False,
)
@click.option(
    "--error-on-empty-url",
    "error_on_empty_url",
    help="""
    whether to treat completely empty url values as errors (default: False)

    for the following example manifest, if error_on_empty_url is False,
    a warning would be logged for the completely empty url value on
    line 2. if error_on_empty_url is True, an error would be generated
    instead
    ```
    md5,url,size
    1596f493ba9ec53023fca640fb69bd3b,,42
    ```

    note that regardless of error_on_empty_url, errors will be
    generated no matter what for arrays or quotes from which urls could
    not be extracted. for example, for the following manifest, errors
    would be generated for the url value on lines 2, 3, 4, and 5
    ```
    md5,url,size
    1596f493ba9ec53023fca640fb69bd30,"",42
    1596f493ba9ec53023fca640fb69bd31,'',43
    1596f493ba9ec53023fca640fb69bd32,[],44
    1596f493ba9ec53023fca640fb69bd33,["", ""],45
    ```
    """,
    is_flag=True,
    default=False,
)
@click.option(
    "--line-limit",
    "line_limit",
    help="""
    number of lines in manifest to validate including the header. if
    not provided, every line is validated. (default: None)

    This can be helpful as a way to only attempt validation of a few rows to get
    an idea if there are large format issues without needing to run against every
    row in the manifest.
    """,
    default=None,
    type=int,
)
@click.pass_context
def objects_manifest_validate_format(
    ctx,
    file,
    allowed_protocols,
    allow_base64_encoded_md5,
    error_on_empty_url,
    line_limit,
):
    if not file:
        file = click.prompt("Enter Discovery metadata file path to validate format for")

    is_valid = is_valid_manifest_format(
        manifest_path=file,
        column_names_to_enums=None,
        allowed_protocols=allowed_protocols.split(" "),
        allow_base64_encoded_md5=allow_base64_encoded_md5,
        error_on_empty_url=error_on_empty_url,
        line_limit=line_limit,
    )

    # non-zero exit code
    if not is_valid:
        sys.exit(1)


@click.command(help="Publishes specified object manifest to Gen3 instance.")
@click.argument("file", required=False)
@click.option(
    "--thread-num",
    "thread_num",
    help="number of threads for indexing",
    default=8,
    type=int,
)
@click.option(
    "--append-urls",
    "append_urls",
    help="""
    If supplied, will append urls for existing records. e.g. existing urls will
    still exist and new ones will be added

    By default the newly provided urls will REPLACE existing urls
    """,
    is_flag=True,
    default=False,
)
@click.pass_context
def objects_manifest_publish(ctx, file, thread_num, append_urls):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()

    if not file:
        file = click.prompt("Enter Discovery metadata file path to publish")

    click.echo(
        f"Publishing/writing object data from {file}...\n    to: {auth.endpoint}"
    )

    index_object_manifest(
        commons_url=auth.endpoint,
        manifest_file=file,
        thread_num=thread_num,
        auth=auth,
        replace_urls=not append_urls,
    )


@click.command(help="Deletes specified object manifest to Gen3 instance.")
@click.argument("file", required=True)
@click.pass_context
def objects_manifest_delete_all_guids(ctx, file):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()

    if not file:
        file = click.prompt("Enter Discovery metadata file path to delete")

    click.echo(f"        DELETING ALL GUIDS\n  from: {file}\n    in: {auth.endpoint}")
    click.confirm(
        f"Are you sure you want to DELETE ALL GUIDS in {auth.endpoint} as specified by this file: {file}?",
        abort=True,
    )
    click.confirm(
        f"Please confirm again, this is irreversible. All GUIDs specified specified in {file} will be deleted from {auth.endpoint}. You are sure?",
        abort=True,
    )

    delete_all_guids(auth, file)


manifest.add_command(objects_manifest_read, name="read")
manifest.add_command(objects_manifest_verify, name="verify")
manifest.add_command(objects_manifest_validate_format, name="validate-manifest-format")
manifest.add_command(objects_manifest_publish, name="publish")
manifest.add_command(objects_manifest_delete_all_guids, name="delete-all-guids")
