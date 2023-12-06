import click
import os.path
import json
import requests
import sys
import gen3.auth as auth_tool

from cdislogging import get_logger

logging = get_logger("__name__")


def stderr(*str):
    logging.error(*str, sys.stderr)


@click.command()
@click.option("--request", "request", help="HTTP Method - GET, PUT, POST, DELETE")
@click.option(
    "--data", "data", help="json data to post - read from file if starts with @"
)
@click.argument("path")
@click.pass_context
def curl(ctx, path, request=None, data=None):
    """Curl the endpoint with a token - ex: gen3 curl /user/user"""
    auth_provider = ctx.obj["auth_factory"].get()
    output = auth_provider.curl(path, request, data)
    logging.info(output.text)
    if output.status_code < 200 or output.status_code > 299:
        stderr("err status code %i" % output.status_code)
        sys.exit(1)


@click.command()
@click.pass_context
def endpoint(ctx):
    """Get the endpoint associated with the active authenticator"""
    logging.info(ctx.obj["auth_factory"].get().endpoint)


@click.command()
@click.pass_context
def get_access_token(ctx):
    """Get an access token suitable to pass as an Authorization header bearer"""
    logging.info(ctx.obj["auth_factory"].get().get_access_token())


@click.command()
@click.argument("token_file")
def token_decode(token_file):
    """Decode the given token file - may be "-" to indicate stdin"""
    if token_file == "-":
        tokenStr = sys.stdin.read()
    else:
        with open(token_file) as f:
            tokenStr = f.read()
    token = auth_tool.decode_token(tokenStr)
    logging.info(json.dumps(token, indent=2))


@click.command()
def wts_endpoint():
    """Get the wts endpoint"""
    logging.info(auth_tool.get_wts_endpoint())


@click.command()
def wts_list():
    """list the idp's available from the wts in a Gen3 workspace environment"""
    logging.info(json.dumps(auth_tool.get_wts_idps(), indent=2))


@click.group()
def auth():
    """Commands for authentication and authorization"""
    pass


auth.add_command(wts_endpoint, name="wts-endpoint")
auth.add_command(wts_list, name="wts-list")
auth.add_command(token_decode, name="token-decode")
auth.add_command(get_access_token, name="access-token")
auth.add_command(endpoint, name="endpoint")
auth.add_command(curl, name="curl")
