import asyncio
import click
import logging
import sys
from urllib.parse import urlparse


from gen3.tools import indexing
from gen3.tools.indexing import is_valid_manifest_format, index_object_manifest
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
    "--num-processes",
    "num_processes",
    default=8,
    help="",
)
@click.option(
    "--max-concurrent-requests",
    "max_concurrent_requests",
    help="",
    default=24,
)
@click.pass_context
def objects_manifest_read(ctx, num_processes, max_concurrent_requests):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()
    click.echo(f"Getting minimal object metadata from {auth.endpoint}")
    output_file = loop.run_until_complete(
        indexing.async_download_object_manifest(
            auth.endpoint,
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
    "--default-file",
    "use_default_file",
    is_flag=True,
    default=False,
    help="object-manifest.csv from current directory",
)
@click.pass_context
def objects_manifest_verify(ctx, file, use_default_file):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()

    if not file and not use_default_file:
        file = click.prompt("Enter Discovery metadata file path to publish")

    click.echo(f"Verifying {file}...\n    Against: {auth.endpoint}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        indexing.async_verify_object_manifest(auth.endpoint, manifest_file=file)
    )


@click.command(
    help="Validates that the format of the specified object manifest is correct."
)
@click.argument("file", required=False)
@click.option(
    "--default-file",
    "use_default_file",
    is_flag=True,
    default=False,
    help="object-manifest.csv from current directory",
)
# TODO add arguments for the arguments below
@click.pass_context
def objects_manifest_validate_format(ctx, file, use_default_file):
    if not file and not use_default_file:
        file = click.prompt("Enter Discovery metadata file path to publish")

    is_valid_manifest_format(
        manifest_path=file,
        column_names_to_enums=None,
        allowed_protocols=["s3", "gs"],
        allow_base64_encoded_md5=False,
        error_on_empty_url=False,
        line_limit=None,
    )


@click.command(help="Publishes specified object manifest to Gen3 instance.")
@click.argument("file", required=False)
@click.option(
    "--default-file",
    "use_default_file",
    is_flag=True,
    default=False,
    help="object-manifest.csv from current directory",
)
# TODO add arguments for the arguments below
@click.pass_context
def objects_manifest_publish(ctx, file, use_default_file):
    auth = ctx.obj["auth_factory"].get()
    loop = asyncio.get_event_loop()

    if not file and not use_default_file:
        file = click.prompt("Enter Discovery metadata file path to publish")

    click.echo(
        f"Publishing/writing object data from {file}...\n    to: {auth.endpoint}"
    )

    index_object_manifest(
        commons_url=auth.endpoint,
        manifest_file=file,
        thread_num=8,
        auth=auth,
        replace_urls=False,
    )


manifest.add_command(objects_manifest_read, name="read")
manifest.add_command(objects_manifest_verify, name="verify")
manifest.add_command(objects_manifest_validate_format, name="validate-manifest-format")
manifest.add_command(objects_manifest_publish, name="publish")
