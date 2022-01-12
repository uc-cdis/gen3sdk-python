import click
import os.path
import json
import requests
import sys
from gen3.metadata import Gen3Metadata
import gen3.metadata as metadata


@click.command()
@click.pass_context
def is_healthy(ctx):
    """Get the version of indexd """
    print(ctx.obj["metadata_factory"].is_healthy())


@click.group()
def metadata():
    """Gen3 sdk metadata commands"""
    pass


metadata.add_command(is_healthy, name="is_healthy")
