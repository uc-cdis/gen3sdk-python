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


file.add_command(single_download, name="download-single")
