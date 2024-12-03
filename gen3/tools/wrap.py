# WIP: Explanation of the code, will be taken off once reviewed and approved
# We first write a class in Gen3SDK say Gen3Wrap that utilizes the Gen3Auth class to retrieve
# the access token from the user's `~/.gen3/credentials.json` file and set it in the Env Var `GEN3_TOKEN`

#  we then take all the other commands including options sent to the gen3 wrap command and execute them in a subprocess
# Combining STDOUT and STDERR of the subprocess outputs on to the console

import os
import subprocess

from gen3.auth import Gen3Auth


class Gen3Wrap:
    def __init__(self, auth: Gen3Auth, command_args: tuple):
        """
        auth : Gen3Auth instance
        command_args: A tuple consisting of all the commands sent to the `gen3 wrap` tool
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
            subprocess.run(cmd, stderr=subprocess.STDOUT)
        except Exception as e:
            print("ERROR running wrap command: ", e)
