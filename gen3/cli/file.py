import click
import time

from cdislogging import get_logger

from gen3.file import Gen3File
from gen3.utils import get_or_create_event_loop_for_thread


logger = get_logger("__name__")


@click.group()
def file():
    "Commands for asynchronously downloading files from a server"
    pass


@click.command(
    help="""Asynchronouslt download all entries in the provided manifest.
    The manifest should be a JSON file in the following format:
    [
        { "object_id": "", "file_name"(optional): "" },
        ...
    ]
"""
)
@click.argument("file", required=True)
@click.option("--path", "path", help="Path to store downloaded files at", default=".")
@click.option(
    "--semaphores", "semaphores", help="Number of semaphores (default = 10)", default=10
)
@click.pass_context
def manifest_async_download(ctx, file, path, semaphores):
    auth = ctx.obj["auth_factory"].get()
    file_tool = Gen3File(auth)
    loop = get_or_create_event_loop_for_thread()
    loop.run_until_complete(
        file_tool.download_manifest(
            manifest_file_path=file,
            download_path=path,
            total_sem=semaphores,
        )
    )


@click.command(help="Download a single file using its GUID")
@click.argument("object_id", required=True)
@click.option("--path", "path", help="Path to store downloaded file in", default=".")
@click.pass_context
def single_download(ctx, object_id, path):
    auth = ctx.obj["auth_factory"].get()
    file_tool = Gen3File(auth)

    start_time = time.perf_counter()
    logger.info(f"Start time: {start_time}")

    result = file_tool.download_single(
        object_id=object_id,
        path=path,
    )

    logger.info(f"Download - {'success' if result else 'failure'}")

    duration = time.perf_counter() - start_time
    logger.info(f"\nDuration = {duration}\n")


file.add_command(manifest_async_download, name="download-manifest")
file.add_command(single_download, name="download-single")
