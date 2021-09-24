import logging
import click
import gen3.configure as config_tool


@click.command()
@click.option("--profile", help="name of the profile to name for this credentials")
@click.option("--cred", help="path to the credentials.json")
@click.option(
    "--apiendpoint", default="", help="[optional] API endpoint of the commons"
)
@click.option(
    "--use-shepherd",
    default="",
    help="[optional] true/false -- to enable/disable the gen3Sdk to upload using the Gen3 Object Management API (if available)",
)
@click.option(
    "--min-shepherd-version",
    default="",
    help="[optional] Specify the minimum version of the Gen3 Object Management API ",
)
def configure(profile, cred, apiendpoint, use_shepherd, min_shepherd_version):
    """Command to configure multiple profiles with corresponding credentials

    ./gen3 configure --profile=<profile-name> --cred=<path-to-credential/cred.json> --apiendpoint=https://example.com --use-sheperd=<true/false> --min-shepherd-version=<version_number>
    """

    logging.info(f"Configuring profile [ {profile} ] with credentials at {cred}")

    try:
        profile_title, new_lines = config_tool.get_profile_from_creds(
            profile, cred, apiendpoint, use_shepherd, min_shepherd_version
        )
        lines = config_tool.get_current_config_lines()
        config_tool.update_config_lines(lines, profile_title, new_lines)
    except Exception as e:
        logging.warning(str(e))
        raise e
