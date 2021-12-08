import click
from cdiserrors import get_logger

from gen3.tools.download.drs_download import (
    list_files_in_workspace_manifest,
    list_drs_object,
    download_files_in_workspace_manifest,
    download_drs_object,
    list_access_in_manifest,
)

logger = get_logger("download", log_level="warning")


@click.command()
@click.argument("infile")
@click.option(
    "--access",
    is_flag=True,
    help="list access rights to Gen3 commons hosting DRS objects, instead of files",
)
@click.option(
    "--object", is_flag=True, help="list file or access for object instead of manifest"
)
@click.pass_context
def list_files_or_access(ctx, infile: str, access: bool, object: bool) -> bool:
    """List files and size in manifest or if access is True list instead list user access"""
    if access:
        return list_access_in_manifest(
            ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
        )
    else:
        if object:
            return list_drs_object(
                ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
            )
        else:
            return list_files_in_workspace_manifest(
                ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
            )


@click.command()
@click.argument("infile", help="input manifest file")
@click.argument(
    "output_dir",
    default=".",
    help="directory to write downloads to. Directory will be created if it does not exists.",
)
@click.pass_context
def download_manifest(ctx, infile: str, output_dir: str):
    """
    Pulls all DIR objects in manifest where the manifest can contain DRS objects.
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint healdata.org pull_manifest manifest1.json

    """
    download_files_in_workspace_manifest(
        ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile, output_dir
    )


@click.command()
@click.argument("object_id", help="DRS object to download")
@click.argument(
    "output_dir",
    default=".",
    help="directory to write downloads to. Directory will be created if it does not exists.",
)
@click.pass_context
def download_object(ctx, object_id: str, output_dir: str):
    """
    Download a DRS object by it's object id
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint healdata.org pull_object dg.6VTS/181af989-5d66-4139-91e7-69f4570ccd41

    """
    download_drs_object(
        ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), object_id, output_dir
    )


@click.group()
def drs_pull():
    """Commands for downloading and listing DRS objects and manifests"""
    pass


drs_pull.add_command(download_manifest, name="pull_manifest")
drs_pull.add_command(download_object, name="pull_object")
drs_pull.add_command(list_files_or_access, name="ls")
