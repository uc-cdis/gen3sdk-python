import click
import json
import sys
from gen3.wss import Gen3WsStorage, wsurl_to_tokens


def clean_path(path):
    """
    Add ws:///@user/ prefix if necessary
    """
    if path[0:3] != "ws:":
        while path and path[0] == "/":
            path = path[1:]
        path = "ws:///@user/" + path
    return path


@click.command()
@click.argument("path", default="")
@click.pass_context
def ls(ctx, path=""):
    """List the given workspace key"""
    clean = clean_path(path)
    auth_provider = ctx.obj["auth_factory"].get()
    wss = Gen3WsStorage(auth_provider)
    print(json.dumps(wss.ls_path(clean)))


@click.command()
@click.argument("path")
@click.pass_context
def rm(ctx, path):
    """Remove the given workspace key"""
    clean = clean_path(path)
    auth_provider = ctx.obj["auth_factory"].get()
    wss = Gen3WsStorage(auth_provider)
    print(json.dumps(wss.rm_path(clean)))


@click.command()
@click.argument("path")
@click.pass_context
def download_url(ctx, path):
    """Download url for the given workspace key"""
    tokens = wsurl_to_tokens(clean_path(path))
    auth_provider = ctx.obj["auth_factory"].get()
    wss = Gen3WsStorage(auth_provider)
    print(json.dumps(wss.download_url(tokens[0], tokens[1])))


@click.command()
@click.argument("path")
@click.pass_context
def upload_url(ctx, path):
    """Upload url for the given workspace key"""
    tokens = wsurl_to_tokens(clean_path(path))
    auth_provider = ctx.obj["auth_factory"].get()
    wss = Gen3WsStorage(auth_provider)
    print(json.dumps(wss.upload_url(tokens[0], tokens[1])))


@click.command()
@click.argument("src")
@click.argument("dest")
@click.pass_context
def copy(ctx, src, dest):
    """Upload url for the given workspace key"""
    auth_provider = ctx.obj["auth_factory"].get()
    wss = Gen3WsStorage(auth_provider)
    wss.copy(src, dest)


@click.group()
def wss():
    """[unfinished] Commands for Workspace Storage Service"""
    pass


wss.add_command(ls, name="ls")
wss.add_command(copy, name="cp")
wss.add_command(download_url, name="download-url")
wss.add_command(upload_url, name="upload-url")
wss.add_command(rm, name="rm")
