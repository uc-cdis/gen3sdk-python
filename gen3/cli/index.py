import click
import os.path
import json
import requests
import sys
from gen3.index import Gen3Index
import gen3.index as index


@click.command()
@click.option("--limit", "limit", help="limit number of records")
@click.option(
    "--paginate", "paginate", help="boolian of if you want pagniated records or not"
)
@click.option(
    "--start", "start", help="starting page if you choose to paginate records"
)
@click.pass_context
def get_all_records(ctx, limit=None, paginate=False, start=None):
    """Get all the records in index with optional pagination """
    print(ctx.obj["index_factory"].get_all_records(limit, paginate, start))


@click.command()
@click.option(
    "--dist_resolution",
    "dist_resolution",
    help="boolean - optional Specify if we want distributed dist_resolution or not",
)
@click.argument("guid")
@click.pass_context
def get(ctx, guid, dist_resolution=True):
    """Get the indexd record from the GUID input"""
    print(ctx.obj["index_factory"].get(guid, dist_resolution))


@click.command()
@click.pass_context
def get_stats(ctx):
    """Get the stats associated with """
    print(ctx.obj["index_factory"].get_stats())


@click.group()
def index():
    """Gen3 sdk index commands"""
    pass


index.add_command(get, name="get")
index.add_command(get_all_records, name="get_all_records")
index.add_command(get_stats, name="get_stats")
