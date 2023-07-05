import click
import os
import sys

import cdislogging
import pkg_resources
import gen3.cli.auth as auth
import gen3.cli.pfb as pfb
import gen3.cli.wss as wss
import gen3.cli.discovery as discovery
import gen3.cli.configure as configure
import gen3.cli.objects as objects
import gen3.cli.file as file
import gen3.cli.drs_pull as drs_pull
import gen3
from gen3 import logging as sdklogging


class AuthFactory:
    def __init__(self, refresh_file):
        self.refresh_file = refresh_file
        self._cache = None

    def get(self):
        """Lazy factory"""
        if self._cache:
            return self._cache
        self._cache = gen3.auth.Gen3Auth(refresh_file=self.refresh_file)
        return self._cache


@click.group()
@click.option(
    "--auth",
    "auth_config",
    default=os.getenv("GEN3_API_KEY", None),
    help="""authentication source, by default expects an API key in "~/.gen3/credentials.json".
    Has special support for token service: "idp://wts/<idp>", and raw access tokens
    "accesstoken:///<token>",
    otherwise a path to an API key or basename of key under ~/.gen3/ can be used.
    Default value is "credentials" if ~/.gen3/credentials.json exists, otherwise "idp://wts/local"
    """,
)
@click.option(
    "--endpoint",
    "endpoint",
    default=os.getenv("GEN3_ENDPOINT", "default"),
    help="commons hostname - optional if API Key given in `auth`",
)
@click.option(
    "-v",
    "verbose_logs",
    is_flag=True,
    default=False,
    help="verbose logs show INFO, WARNING & ERROR logs",
)
@click.option(
    "-vv",
    "very_verbose_logs",
    is_flag=True,
    default=False,
    help="very verbose logs show DEGUG, INFO, WARNING & ERROR logs",
)
@click.option(
    "--only-error-logs",
    "only_error_logs",
    is_flag=True,
    default=False,
    help="only show ERROR logs",
)
@click.option(
    "--version",
    help="Show Gen3 Version",
)
@click.pass_context
def main(
    ctx,
    auth_config,
    endpoint,
    verbose_logs,
    very_verbose_logs,
    only_error_logs,
    version,
):
    """Gen3 Command Line Interface"""
    ctx.ensure_object(dict)
    ctx.obj["auth_config"] = auth_config
    ctx.obj["endpoint"] = endpoint
    ctx.obj["auth_factory"] = AuthFactory(auth_config)

    if version:
        click.echo(pkg_resources.get_distribution("gen3").version)
    if very_verbose_logs:
        logger = cdislogging.get_logger(
            __name__, format=gen3.LOG_FORMAT, log_level="debug"
        )
        sdklogging.setLevel("DEBUG")
    elif verbose_logs:
        logger = cdislogging.get_logger(
            __name__, format=gen3.LOG_FORMAT, log_level="info"
        )
        sdklogging.setLevel("INFO")
    elif only_error_logs:
        logger = cdislogging.get_logger(
            __name__, format=gen3.LOG_FORMAT, log_level="error"
        )
        sdklogging.setLevel("ERROR")
    else:
        logger = cdislogging.get_logger(
            __name__, format=gen3.LOG_FORMAT, log_level="warning"
        )
        sdklogging.setLevel("WARNING")


main.add_command(auth.auth)
main.add_command(pfb.pfb)
main.add_command(wss.wss)
main.add_command(discovery.discovery)
main.add_command(configure.configure)
main.add_command(objects.objects)
main.add_command(drs_pull.drs_pull)
main.add_command(file.file)
main()
