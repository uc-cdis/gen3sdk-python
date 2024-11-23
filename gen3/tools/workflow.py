# We first write a class in Gen3SDK say Gen3Workflow that utilizes the Gen3Auth class to retrieve the access token from the user's `~/.gen3/credentials.json` file
# Then, using the access token and other environment variables like the AWS Key ID and secret,\
#  we execute `nextflow run workflow.nf` in a subprocess and return the output of that subprocess as the response.
# -- responding parallelly is a bonus

import os
import subprocess

from gen3.auth import Gen3Auth


class Gen3Workflow:
    def __init__(self, auth: Gen3Auth, workflow_agent: str, workflow_filename: str):
        """
        auth : Gen3Auth instance
        workflow_agent: str -- Name of the workflow agent to run, default to nextflow, can we use gen3Config to pull in more info?
        workflow_filename: String consisting the name of the workflow file
        """
        self.auth = auth
        self.workflow_agent = workflow_agent
        self.workflow_filename = workflow_filename

    def launch_workflow(self):
        """
        Take the workflow file and run a subprocess with appropriate creds
        """
        cmd = [self.workflow_agent, "run", self.workflow_filename]
        try:
            os.environ["GEN3_TOKEN"] = self.auth.get_access_token()
            subprocess.run(cmd, stderr=subprocess.STDOUT)
        except Exception as e:
            print("ERROR running workflow file: ", e)
