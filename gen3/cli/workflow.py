import click
import os
import shutil

from cdislogging import get_logger

from gen3.tools.workflow import Gen3Workflow


logger = get_logger("__name__")


@click.group()
def workflow():
    "Commands for running workflows on Gen3"
    pass


@click.command(help="Run a workflow task, by passing a workflow file")
@click.argument("workflow_filename", required=True)
@click.option(
    "--workflow-agent",
    "workflow_agent",
    default="nextflow",
    help="Path to binary used to run workflows. Defaults to nextflow",
)
@click.pass_context
def run(ctx, workflow_filename, workflow_agent):
    auth = ctx.obj["auth_factory"].get()

    if "AWS_KEY_ID" not in os.environ:
        logger.error("Expects env var `AWS_KEY_ID` to be set to run this workflow")
    elif "AWS_KEY_ID" not in os.environ:
        logger.error("Expects env var `AWS_KEY_SECRET` to be set to run this workflow")
    elif not shutil.which(workflow_agent):
        logger.error(f"Cannot run workflow: {workflow_agent=} does not exist in path")
    else:
        workflow_object = Gen3Workflow(auth, workflow_agent, workflow_filename)
        logger.info(
            f"Launching workflow using {workflow_agent} with {workflow_filename}"
        )
        workflow_object.launch_workflow()


workflow.add_command(run, name="run")
