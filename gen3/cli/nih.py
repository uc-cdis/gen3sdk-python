import json

import click

from gen3.external.nih.dbgap_study_registration import dbgapStudyRegistration
from cdislogging import get_logger

logger = get_logger("__name__")


@click.group()
def nih():
    """Commands for reading from NIH APIs"""
    pass


@nih.group()
def dbgap_study_registration():
    """Commands for interacting with the dbgap study registration api"""
    pass


@dbgap_study_registration.command(name="get-metadata")
@click.argument(
    "studies",
    nargs=-1,
)
def get_child_studies(studies):
    """
    Retrieve the study metadata associated with the provided study names.

    This command fetches the metadata associated with the given study names from the dbGaP Study Registration API.

    NOTE: If no version is provided, then the latest version for a study will be used.

    Args:
        studies (str): A space-separated list of study names in which to get metadata for.
    Example:
        gen3 nih dbgap-study-registration get-metadata phs002793 phs002794 phs002795
        > {
            "phs002793.v2.p1": {
                "@uid": "48490",
                "@whole_study_id": "44467",
                "@phs": "002793",
                "@v": "2",
                "@p": "1",
                "@createDate": "2022-09-14T11:39:44-05:00",
                "@completedByGPADate": "2022-11-07T09:51:24-05:00",
                "@modDate": "2022-12-21T09:03:26-05:00",
                "@maxParentChildStudyModDate": "2022-12-21T09:04:04-05:00",
                "@handle": "SLICE",
                "@num_participants": "500",
                "StudyInfo": {
                    "@accession": "phs002793.v2.p1",
                    "@parentAccession": "phs002793.v2.p1",
                    "childAccession": [
                        "phs002795.v1.p1",
                        "phs002796.v1.p1",
                        "phs002797.v1.p1",
                        "phs002798.v1.p1",
                        "phs002794.v2.p1"
                    ],
                ...truncated for brevity...
            }
        }
    """
    result = dbgapStudyRegistration().get_metadata_for_ids(studies)

    if not result:
        logger.info(f"No study found for {studies}")
    else:
        click.echo(f"{json.dumps(result, indent=4)}")


@dbgap_study_registration.command(name="get-child-studies")
@click.argument(
    "studies",
    nargs=-1,
)
def get_child_studies(studies):
    """
    Retrieve the child studies associated with the provided study names.

    This command fetches the child studies associated with the specified parent study names
    from the dbGaP Study Registration API.

    NOTE: If no version is provided, then the latest version for a study will be used.

    Args:
        studies (str): A space-separated list of parent study names for which to fetch child studies.
    Example:
        gen3 nih dbgap-study-registration get-child-studies phs002793
        > {
              "phs002793.v2.p1": ["phs002795.v1.p1", "phs002796.v1.p1", "phs002797.v1.p1", "phs002798.v1.p1"]
           }
        gen3 nih dbgap-study-registration get-child-studies phs002076 phs002793
         > {
              "phs002076.v2.p1": ["phs002077.v2.p1"],
              "phs002793.v2.p1": ["phs002795.v1.p1", "phs002796.v1.p1", "phs002797.v1.p1", "phs002798.v1.p1"]
            }
    """
    result = dbgapStudyRegistration().get_child_studies_for_ids(studies)

    if not result:
        logger.info(f"No child studies found for {studies}")
    else:
        click.echo(f"{json.dumps(result, indent=4)}")


@dbgap_study_registration.command(name="get-parent-studies")
@click.argument(
    "studies",
    nargs=-1,
)
def get_parent_studies(studies):
    """
    Retrieve the parent study associated with each of the provided study names.

    This command fetches the parent study associated with each of the specified child study names
    from the dbGaP Study Registration API.

    NOTE: If no version is provided, then the latest version for a study will be used.

    Args:
        studies (str): A space-separated list of child study names for which to fetch their parent study.
    Example:
        gen3 nih dbgap-study-registration get-parent-studies phs002795
        > {
              "phs002795.v1.p1": "phs002793.v2.p1"
           }
        gen3 nih dbgap-study-registration get-parent-studies phs002795 phs002796 phs002793
         > {
              "phs002795.v1.p1": "phs002793.v2.p1",
              "phs002796.v1.p1": "phs002793.v2.p1"
              "phs002793.v2.p1": None
            }
    """
    result = dbgapStudyRegistration().get_parent_studies_for_ids(studies)

    if not result:
        logger.info(f"No parent studies found for any {studies}")
    else:
        click.echo(f"{json.dumps(result, indent=4)}")
