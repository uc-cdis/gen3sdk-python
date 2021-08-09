import click
from cdiserrors import get_logger

from gen3.tools.download.manifest import (
    describe_access_to_files_in_workspace_manifest,
    list_files_in_workspace_manifest,
    download_files_in_workspace_manifest,
)

logger = get_logger("manifest", log_level="warning")


@click.command()
@click.argument("infile", help="input manifest file")
@click.pass_context
def my_access(ctx, infile: str):
    """List access right to commons listed in manifest"""
    describe_access_to_files_in_workspace_manifest(
        ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
    )


@click.command()
@click.argument("infile", help="input manifest file")
def listfiles(infile: str):
    """List files and size in manifest"""
    list_files_in_workspace_manifest(infile)


@click.command()
@click.argument("infile", help="input manifest file")
@click.argument(
    "output_dir", default="", help="optional output directory, create if needed"
)
@click.pass_context
def download(ctx, infile: str, output_dir=""):
    """
    Download all files in manifest where the manifest can optional handle hostname based DRS using the
    commons_url field within the manifest entry.
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint healdata.org manifest download manifest1.json

    """
    download_files_in_workspace_manifest(
        ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile, output_dir
    )


@click.group()
def manifest():
    """Commands for downloading Gen3 manifests"""
    pass


manifest.add_command(my_access, name="access")
manifest.add_command(listfiles, name="list")
manifest.add_command(download, name="download")
