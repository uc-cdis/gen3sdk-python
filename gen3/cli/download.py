import asyncio
import click
import csv
import logging
import os
import sys
from urllib.parse import urlparse

from gen3.tools import download
from gen3.tools.download.download_manifest import async_download
from gen3.utils import get_or_create_event_loop_for_thread

@click.group()
def download_files():
    "Commands for asynchronously downloading files from a server"
    pass


@click.command(help="Downloads all files from manifest asynchronously")
@click.argument("file",required=True)
@click.option("--path","path",help="Path to store downloaded files in",default=".")
@click.option("--cred","cred",help="Path to credentials", required=True)
@click.pass_context
def manifest_async_download(ctx, file, path, cred):
    auth = ctx.obj["auth_factory"].get()
    loop = get_or_create_event_loop_for_thread()
    loop.run_until_complete(
        download.async_download(
            auth,
            manifest_file=file,
            download_path=path,
            cred=cred,
        )
    )

download_files.add_command(manifest_async_download,name="manifest")
