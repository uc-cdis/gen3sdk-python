import json

import click

from gen3.external.nih.dbgap_study_registration import dbgapStudyRegistration


@click.group()
def nih():
    """Commands for reading from NIH APIs"""
    pass


@nih.group()
def dbgap_study_registration():
    """Commands for interacting with the dbgap study registration api"""
    pass


@dbgap_study_registration.command(name="get-metadata")
@click.argument("study_name")
def get_child_studies(study_name):
    """
    Retrieve the study metadata associated with the provided study names.

    This command fetches the metadata associated with the given study names from the dbGaP Study Registration API.

    NOTE: If no version is provided, then the latest version for a study will be used.

    Args:
        study_name (str): A space-separated list of study names in which to get metadata for.
    Example:
        gen3 nih dbgap-study-registration get-metadata "phs002793"
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
    result = dbgapStudyRegistration().get_metadata_for_ids(study_name.split(" "))

    if not result:
        click.echo(f"No study found for {study_name}")
    else:
        click.echo(f"{json.dumps(result, indent=4)}")


@dbgap_study_registration.command(name="get-child-studies")
@click.argument("study_name")
def get_child_studies(study_name):
    """
    Retrieve the child studies associated with the provided study names.

    This command fetches the child studies associated with the specified parent study names
    from the dbGaP Study Registration API.

    NOTE: If no version is provided, then the latest version for a study will be used.

    Args:
        study_name (str): A space-separated list of parent study names for which to fetch child studies.
    Example:
        gen3 nih dbgap-study-registration get-child-studies "phs002793"
        > phs002793.v2.p1: phs002795.v1.p1 phs002796.v1.p1 phs002797.v1.p1 phs002798.v1.p1 phs002794.v2.p1

        gen3 nih dbgap-study-registration get-child-studies "phs002076 phs002793"
         > phs002076.v2.p1: phs002077.v2.p1
           phs002793.v2.p1: phs002795.v1.p1 phs002796.v1.p1 phs002797.v1.p1 phs002798.v1.p1 phs002794.v2.p1
    """
    result = dbgapStudyRegistration().get_child_studies_for_ids(study_name.split(" "))

    if not result:
        click.echo(f"No child studies found for {study_name}")
    else:
        for parent, children in result.items():
            click.echo(f"{parent}: {' '.join(children)}")
