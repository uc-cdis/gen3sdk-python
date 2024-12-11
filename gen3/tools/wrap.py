import os
import subprocess

from cdislogging import get_logger
from gen3.auth import Gen3Auth, Gen3AuthError

logger = get_logger("__name__")


class Gen3Wrap:
    def __init__(self, auth: Gen3Auth, command_args: tuple):
        """
        auth : Gen3Auth instance
        command_args: A tuple consisting of all the commands sent to the `gen3 run` tool
        """
        self.auth = auth
        self.command_args = command_args

    def run_command(self):
        """
        Take the command args and run a subprocess with appropriate access token in the env var
        """
        cmd = list(self.command_args)
        try:
            os.environ["GEN3_TOKEN"] = self.auth.get_access_token()
        except Gen3AuthError as e:
            logger.error(f"ERROR getting Gen3 Access Token:", e)
            raise
        logger.info(
            f"Running the command {self.command_args} with gen3 access token in environment variable"
        )
        try:
            subprocess.run(cmd, stderr=subprocess.STDOUT)
        except Exception as e:
            logger.error(f"ERROR while running '{cmd}':", e)
            raise
