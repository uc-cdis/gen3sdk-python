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
@click.argument("guid")
@click.pass_context
def get_record(ctx, guid):
    """Get the indexd record from the GUID input"""
    print(ctx.obj["index_factory"].get_record(guid))


@click.command()
@click.argument("guid")
@click.pass_context
def get_record_doc(ctx, guid):
    """Get the indexd record from the GUID input"""
    print(ctx.obj["index_factory"].get_record_doc(guid))


@click.command()
@click.argument("dids")
@click.pass_context
def get_records(ctx, dids):
    """Get the indexd record from the GUID input"""
    print(ctx.obj["index_factory"].get_records(dids))


@click.command()
@click.argument("guid")
@click.pass_context
def delete_record(ctx, guid):
    """Get the indexd record from the GUID input"""
    print(ctx.obj["index_factory"].delete_record(guid))


@click.command()
@click.pass_context
def get_stats(ctx):
    """Get the stats associated with """
    print(ctx.obj["index_factory"].get_stats())


@click.command()
@click.pass_context
def get_version(ctx):
    """Get the version of indexd """
    print(ctx.obj["index_factory"].get_version())


@click.command()
@click.argument("guid")
@click.pass_context
def get_versions(ctx, guid):
    """Get the indexd record from the GUID input"""
    print(ctx.obj["index_factory"].get_versions(guid))


@click.command()
@click.pass_context
def is_healthy(ctx):
    """Get the version of indexd """
    print(ctx.obj["index_factory"].is_healthy())


@click.group()
def index():
    """Gen3 sdk index commands"""
    pass


index.add_command(get, name="get")
index.add_command(get_all_records, name="get_all_records")
index.add_command(get_record, name="get_record")
index.add_command(get_record_doc, name="get_record_doc")
index.add_command(get_records, name="get_records")
index.add_command(delete_record, name="delete_record")
index.add_command(get_stats, name="get_stats")
index.add_command(get_version, name="get_version")
index.add_command(get_versions, name="get_versions")
index.add_command(is_healthy, name="is_healthy")
