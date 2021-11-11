import logging
import click
import gen3.configure as config_tool


@click.command()
@click.option("--profile", help="name of the profile to name for this credentials")
@click.option("--cred", help="path to the credentials.json")
def configure(profile, cred):
    """Command to configure multiple profiles with corresponding credentials

    ./gen3 configure --profile=<profile-name> --cred=<path-to-credential/cred.json>
    """

    logging.info(f"Configuring profile [ {profile} ] with credentials at {cred}")

    try:
        cfg = config_tool.get_profile_from_creds(cred)
        config_tool.update_config_lines(profile, cfg)
    except Exception as e:
        logging.warning(str(e))
        raise e
