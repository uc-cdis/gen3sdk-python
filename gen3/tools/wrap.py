# WIP: Explanation of the code, will be taken off once reviewed and approved
# We first write a class in Gen3SDK say Gen3Wrap that utilizes the Gen3Auth class to retrieve
# the access token from the user's `~/.gen3/credentials.json` file and set it in the Env Var `GEN3_TOKEN`

#  we then take all the other commands including options sent to the `gen3 run` command and execute them in a subprocess
# Combining STDOUT and STDERR of the subprocess outputs on to the console

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
            logger.info(
                f"Running the command {self.command_args} with gen3 access token in environment variable"
            )
            subprocess.run(cmd, stderr=subprocess.STDOUT)
        except Gen3AuthError as e:
            logger.error(f"ERROR getting Gen3 Access Token:", e)
            raise
        except Exception as e:
            logger.error(f"ERROR while running '{cmd}':", e)
            raise
