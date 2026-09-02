import click
import os
import pathlib
from cdislogging import get_logger
from gen3.fhir import *

logging = get_logger(__name__)


@click.group()
def fhir():
    """Commands for FHIR data processing"""
    pass


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="""Tag Bulk FHIR data with Gen3 compatible authorization tags.

    \b 
    input_file (str): Input .ndjson file with Bulk FHIR data, MUST be one resource type per file
    output_file (str): Output file name, also an .ndjson file
    config (str): .yaml file with authorization rules, see docs/howto/fhir.md for more details on formatting
    """,
)
@click.argument(
    "input_file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    metavar="input_file",
)
@click.argument(
    "output_file", type=click.Path(dir_okay=False, writable=True), metavar="output_file"
)
@click.argument(
    "config",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    metavar="config",
)
@click.option(
    "--work_dir",
    type=click.Path(),
    metavar="work_dir",
    help=f"Specify which working directory to clean, if not specified the default ({DEFAULT_WORK_DIR}) will be used. Can also be set as an environment variable: GEN3_FHIR_WORK_DIR",
)
@click.option(
    "-b",
    "--batch_size",
    type=click.IntRange(min=1),
    default=10000,
    show_default=True,
    metavar="batch_size",
    help="batch size for chunking",
)
@click.option(
    "--force",
    is_flag=True,
    help="Remove all intermediate files for this run before exiting even if run crashes",
)
def cli(
    input_file: str | os.PathLike[str],
    output_file: str | os.PathLike[str],
    config: str | os.PathLike[str],
    work_dir: str | os.PathLike[str] | None,
    batch_size: int,
    force: bool,
):
    """
    CLI implementation of tag_fhir_resources_with_authz.

    Args:
        input_file (str): Input .ndjson file
        output_file (str): Output file name
        config (str): .yaml file with authorization rules
        work_dir (str): Working directory to save intermediate files for each run
        batch_size (int): number of lines per chunk
        force (bool): remove all intermediate files for this run before exiting even if it crashes
    """
    tag_fhir_resources_with_authz(
        input_file=input_file,
        output_file=output_file,
        config=config,
        batch_size=batch_size,
        work_dir=work_dir,
        force=force,
    )


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Remove all intermediate files in the tmp folder from previous runs",
)
@click.option(
    "--work_dir",
    type=click.Path(),
    metavar="work_dir",
    help=f"Specify which working directory to clean, if not specified the default ({DEFAULT_WORK_DIR}) will be cleaned. Can also be set as an environment variable: GEN3_FHIR_WORK_DIR",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Report what would be deleted with --cleanup without deleting the files",
)
@click.option(
    "--force",
    is_flag=True,
    help="Remove temporary directory ignoring status of each directory",
)
def cleanup(work_dir, dry_run: bool, force: bool):
    """
    Remove all intermediate files in the tmp folder from previous runs

    Args:
        work_dir (str): Working directory to save intermediate files for each run
        dry_run (bool): If True, list the files that would be removed, but not actually remove them
        force (bool): Delete all intermediate directories disregarding the status
    """

    cleanup_fhir_transform_artifacts(work_dir=work_dir, dry_run=dry_run, force=force)


fhir.add_command(cli, name="transform")
fhir.add_command(cleanup, name="cleanup")
