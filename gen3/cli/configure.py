from cdislogging import get_logger
import click
import gen3.configure as config_tool

logging = get_logger("__name__")


@click.command()
@click.option("--profile", help="name of the profile to name for this credentials")
@click.option("--cred", help="path to the credentials.json")
def configure(profile, cred):
    """[unfinished] Commands to configure multiple profiles with corresponding credentials

    ./gen3 configure --profile=<profile-name> --cred=<path-to-credential/cred.json>
    """

    logging.info(f"Configuring profile [ {profile} ] with credentials at {cred}")

    try:
        profile_title, new_lines = config_tool.get_profile_from_creds(profile, cred)
        lines = config_tool.get_current_config_lines()
        config_tool.update_config_lines(lines, profile_title, new_lines)
    except Exception as e:
        logging.warning(str(e))
        raise e
