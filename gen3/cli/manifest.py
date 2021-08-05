import datetime
from dataclasses import dataclass
from json import load as json_load, JSONDecodeError
from pathlib import Path
from typing import List, Optional

import click
import requests
import humanfriendly
from cdiserrors import get_logger
from dataclasses_json import dataclass_json, LetterCase
from tqdm import tqdm

import os

from gen3.auth import Gen3Auth, Gen3AuthError
from gen3.auth import _handle_access_token_response

from gen3.tools.download.manifest import ( 
    describe_access_to_files_in_workspace_manifest,
    list_files_in_workspace_manifest,
    download_files_in_workspace_manifest,
)

logger = get_logger("manifest", log_level="warning")

@click.command()
@click.argument("infile")
@click.pass_context
def my_access(ctx, infile: str):
    describe_access_to_files_in_workspace_manifest(ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile)


@click.command()
@click.argument("infile")
def listfiles(infile: str):
    list_files_in_workspace_manifest(infile)


@click.command()
@click.argument("infile")
@click.argument("output_dir")
@click.pass_context
def download(ctx, infile: str, output_dir: str):
    download_files_in_workspace_manifest(ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile, output_dir)

@click.group()
def manifest():
    """Commands for downloading Gen3 manifests"""
    pass

manifest.add_command(my_access, name="access")
manifest.add_command(listfiles, name="list")
manifest.add_command(download, name="download")