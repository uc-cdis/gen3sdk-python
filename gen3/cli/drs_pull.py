import click
import json

from cdislogging import get_logger

from gen3.tools.download.drs_download import (
    list_files_in_drs_manifest,
    list_drs_object,
    download_files_in_drs_manifest,
    download_drs_objects,
    list_access_in_drs_manifest,
)

logger = get_logger("__name__")


@click.command()
@click.argument("infile")
@click.option(
    "--access",
    is_flag=True,
    help="list access rights to Gen3 commons hosting DRS objects, instead of files",
    show_default=True,
)
@click.option(
    "--object",
    is_flag=True,
    help="list file or access for object instead of manifest",
    show_default=True,
)
@click.pass_context
def list_files_or_access(ctx, infile: str, access: bool, object: bool) -> bool:
    """List files and size in manifest or if access is True instead list user access"""
    if access:
        return list_access_in_drs_manifest(
            ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
        )
    else:
        if object:
            return list_drs_object(
                ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
            )
        else:
            return list_files_in_drs_manifest(
                ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile
            )


@click.command()
@click.argument("infile")
@click.argument("output-dir", default=".")
@click.option(
    "--no-progress",
    is_flag=True,
    help="Hide the progress bar when downloading",
    show_default=True,
)
@click.option(
    "--no-unpack-packages",
    is_flag=True,
    help="disable the unpacking of downloaded packages",
    show_default=True,
)
@click.option(
    "--delete-unpacked-packages",
    is_flag=True,
    help="delete package files after unpacking them",
    show_default=True,
)
@click.pass_context
def download_manifest(
    ctx,
    infile: str,
    output_dir: str,
    no_progress: bool,
    no_unpack_packages: bool,
    delete_unpacked_packages: bool,
):
    """
    Pulls all DRS objects in manifest where the manifest can contain DRS objects.
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint mydata.org drs-pull manifest manifest1.json

    """
    download_files_in_drs_manifest(
        ctx.obj["endpoint"],
        ctx.obj["auth_factory"].get(),
        infile,
        output_dir,
        not no_progress,
        not no_unpack_packages,
        delete_unpacked_packages,
    )


@click.command()
@click.argument("object_id")
@click.option(
    "--no-progress",
    is_flag=True,
    help="Hide the progress bar when downloading",
    show_default=True,
)
@click.option(
    "--no-unpack-packages",
    is_flag=True,
    help="disable the unpacking of downloaded packages",
    show_default=True,
)
@click.option(
    "--delete-unpacked-packages",
    is_flag=True,
    help="delete package files after unpacking them",
    show_default=True,
)
@click.option(
    "--output-dir",
    default=".",
    show_default=True,
)
@click.pass_context
def download_object(
    ctx,
    object_id: str,
    output_dir: str,
    no_progress: bool,
    no_unpack_packages: bool,
    delete_unpacked_packages: bool,
):
    """
    Download a DRS object by it's object id
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint mydata.org drs-pull object dg.XXXT/181af989-5d66-4139-91e7-69f4570ccd41

    """
    logger.info(f"GA4GH DRS Object Streaming Starting...")
    res = download_drs_objects(
        ctx.obj["endpoint"],
        ctx.obj["auth_factory"].get(),
        [object_id],
        output_dir,
        no_progress,
        not no_unpack_packages,
        delete_unpacked_packages,
    )

    success = True

    results = {
        "succeeded": [],
        "failed": [],
    }

    for drs_object_id in res:
        if drs_object_id in res and res[drs_object_id].status == "downloaded":
            results["succeeded"].append(drs_object_id)
        else:
            logger.error(f"Object {drs_object_id} download failed.")
            success = False
            results["failed"].append(drs_object_id)

    if success:
        logger.info("All objects downloaded successfully.")
    else:
        logger.error(f"One or more objects have failed to be downloaded. Details:")

    click.echo(json.dumps(results))


@click.command()
@click.argument("object_ids", nargs=-1)
@click.option(
    "--output-dir",
    default=".",
    show_default=True,
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Hide the progress bar when downloading",
    show_default=True,
)
@click.option(
    "--no-unpack-packages",
    is_flag=True,
    help="disable the unpacking of downloaded packages",
    show_default=True,
)
@click.option(
    "--delete-unpacked-packages",
    is_flag=True,
    help="delete package files after unpacking them",
    show_default=True,
)
@click.pass_context
def download_objects(
    ctx,
    output_dir: str,
    object_ids: list,
    no_progress: bool,
    no_unpack_packages: bool,
    delete_unpacked_packages: bool,
):
    """
    Download DRS objects by their object ids
    The user credentials use the Gen3Auth class so the Gen3Auth options are applicable (--auth and --endpoint)
    An example:
        gen3 --endpoint mydata.org drs-pull objects dg.XXXT/181af989-5d66-4139-91e7-69f4570ccd41 dg.YYYT/221af989-2266-2239-22e7-22f4570ccd22

    """
    success = True

    results = {
        "succeeded": [],
        "failed": [],
    }

    logger.info(f"GA4GH DRS Object Streaming Starting...")
    logger.debug(f"object_ids: {object_ids}")
    res = download_drs_objects(
        ctx.obj["endpoint"],
        ctx.obj["auth_factory"].get(),
        object_ids,
        output_dir,
        no_progress,
        not no_unpack_packages,
        delete_unpacked_packages,
    )
    for drs_object_id in res:
        if drs_object_id in res and res[drs_object_id].status == "downloaded":
            results["succeeded"].append(drs_object_id)
        else:
            logger.error(f"Object {drs_object_id} download failed.")
            success = False
            results["failed"].append(drs_object_id)

    if success:
        logger.info("All objects downloaded successfully.")
    else:
        logger.error(f"One or more objects have failed to be downloaded. Details:")

    click.echo(json.dumps(results))


@click.group()
def drs_pull():
    """Commands for downloading and listing DRS objects and manifests"""
    pass


drs_pull.add_command(download_manifest, name="manifest")
drs_pull.add_command(download_object, name="object")
drs_pull.add_command(download_objects, name="objects")
drs_pull.add_command(list_files_or_access, name="ls")
