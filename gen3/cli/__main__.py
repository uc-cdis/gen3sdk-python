import logging
import os
from importlib.metadata import version

import click

import cdislogging
import gen3.cli.auth as auth
import gen3.cli.pfb as pfb
import gen3.cli.wss as wss
import gen3.cli.discovery as discovery
import gen3.cli.configure as configure
import gen3.cli.objects as objects
import gen3.cli.file as file
import gen3.cli.drs_pull as drs_pull
import gen3.cli.users as users
import gen3.cli.wrap as wrap
import gen3
from gen3 import logging as sdklogging
from gen3.cli import nih
import gen3.configure as config_tool
import tempfile
import json


class AuthFactory:
    def __init__(self, refresh_file, profile_name=None):
        self.refresh_file = refresh_file
        self.profile_name = profile_name
        self._cache = None

    def get(self):
        """Lazy factory with profile support"""
        if self._cache:
            return self._cache
            
        if self.profile_name and not self.refresh_file:
            try:
                profile_creds = config_tool.get_profile_credentials(self.profile_name)
                self._cache = gen3.auth.Gen3Auth(
                    endpoint=profile_creds['api_endpoint'],
                    refresh_token=profile_creds.get('access_token')
                )
            except Exception as e:
                raise ValueError(f"Error creating auth from profile '{self.profile_name}': {e}")
        else:
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
    default=os.getenv("GEN3_ENDPOINT", None),
    help="commons hostname - optional if API Key given in `auth`",
)
@click.option(
    "--profile",
    "profile_name",
    default=os.getenv("GEN3_PROFILE", None),
    help="Profile name to use for authentication (compatible with cdis-data-client profiles)",
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
    "--silent",
    "silent",
    is_flag=True,
    default=False,
    help="don't show ANY logs",
)
@click.option(
    "--commons_url",
    "commons_url",
    default=os.getenv("GEN3_COMMONS_URL", None),
    help="commons url for fetching file metadata from if different than endpoint",
)
@click.pass_context
@click.version_option(version=version("gen3"))
def main(
    ctx,
    auth_config,
    endpoint,
    profile_name,
    verbose_logs,
    very_verbose_logs,
    only_error_logs,
    silent,
    commons_url,
):
    """Gen3 Command Line Interface"""
    ctx.ensure_object(dict)
    
    if profile_name and not auth_config:
        try:
            profile_creds = config_tool.get_profile_credentials(profile_name)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(profile_creds, f)
                auth_config = f.name
                ctx.obj["temp_cred_file"] = f.name
            if not endpoint:
                endpoint = profile_creds.get('api_endpoint')
        except Exception as e:
            click.echo(f"Error loading profile '{profile_name}': {e}")
            ctx.exit(1)
    
    ctx.obj["auth_config"] = auth_config
    ctx.obj["endpoint"] = endpoint
    ctx.obj["commons_url"] = commons_url
    ctx.obj["profile_name"] = profile_name
    ctx.obj["auth_factory"] = AuthFactory(auth_config, profile_name)

    if silent:
        # we still need to define the logger, the log_level here doesn't
        # really matter b/c we immediately disable all logging
        logger = cdislogging.get_logger(
            __name__, format=gen3.LOG_FORMAT, log_level="debug"
        )
        # disables all logging
        logging.disable(logging.CRITICAL)
    elif very_verbose_logs:
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
main.add_command(configure.list_all_profiles, name="list-profiles")
main.add_command(configure.show_profile, name="show-profile")
main.add_command(objects.objects)
main.add_command(drs_pull.drs_pull)
main.add_command(file.file)
main.add_command(nih.nih)
main.add_command(users.users)
main.add_command(wrap.run)
main()
