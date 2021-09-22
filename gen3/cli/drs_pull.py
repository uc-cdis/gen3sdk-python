import click
from cdiserrors import get_logger

from gen3.tools.download.drs_download import (
    list_files_in_workspace_manifest,
    download_files_in_workspace_manifest,
    download_drs_object,
)

logger = get_logger("manifest", log_level="warning")


@click.command()
@click.argument("infile")
@click.option(
    "-l",
    "--long",
    is_flag=True,
    help="long listing of file",
)
@click.pass_context
def listfiles(ctx, infile: str, long: bool):
    """List files and size in manifest"""
    list_files_in_workspace_manifest(
        ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile, long
    )


@click.command()
@click.argument("infile")
@click.argument("output_dir", default=".")
@click.pass_context
def download_manifest(ctx, infile: str, output_dir: str):
    """
    pulls all files in manifest where the manifest can contain DRS objects.
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint healdata.org pull_manifest manifest1.json

    """
    download_files_in_workspace_manifest(
        ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile, output_dir
    )


@click.command()
@click.argument("object_id")
@click.argument("output_dir", default=".")
@click.option("--object_hostname", "object_hostname", default=None, required=False)
@click.pass_context
def download_object(ctx, object_id: str, output_dir: str, object_hostname: str):
    """
    Download a DRS object by it's object id
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint healdata.org pull_object dg.6VTS/181af989-5d66-4139-91e7-69f4570ccd41

    """
    download_drs_object(
        ctx.obj["endpoint"],
        ctx.obj["auth_factory"].get(),
        object_id,
        output_dir,
        object_hostname,
    )


@click.group()
def manifest():
    """Commands for downloading Gen3 manifests"""
    pass


manifest.add_command(listfiles, name="list")
manifest.add_command(download_manifest, name="pull_manifest")
manifest.add_command(download_object, name="pull_object")
