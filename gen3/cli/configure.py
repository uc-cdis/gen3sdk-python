from cdislogging import get_logger
import click
import gen3.configure as config_tool

logging = get_logger("__name__")


@click.command()
@click.option("--profile", required=True, help="name of the profile to name for this credentials")
@click.option("--cred", help="path to the credentials.json file")
@click.option("--apiendpoint", help="API endpoint URL (optional if derivable from credentials)")
def configure(profile, cred, apiendpoint):
    """Configure multiple profiles with corresponding credentials
    
    Compatible with cdis-data-client profile format.
    
    Examples:
        ./gen3 configure --profile=<profile-name> --cred=<path-to-credential/cred.json>
        ./gen3 configure --profile=<profile-name> --cred=<cred.json> --apiendpoint=https://data.mycommons.org
    """

    if not cred:
        click.echo("Error: --cred option is required")
        return
        
    logging.info(f"Configuring profile [ {profile} ] with credentials at {cred}")

    try:
        profile_title, new_lines = config_tool.get_profile_from_creds(
            profile, cred, apiendpoint
        )
        lines = config_tool.get_current_config_lines()
        config_tool.update_config_lines(lines, profile_title, new_lines)
        
        click.echo(f"Profile '{profile}' has been configured successfully.")
        
        profiles = config_tool.list_profiles()
        if len(profiles) > 1:
            click.echo(f"Available profiles: {', '.join(profiles)}")
            
    except Exception as e:
        logging.error(str(e))
        click.echo(f"Error configuring profile: {str(e)}")
        raise e


@click.command()
def list_profiles():
    """List all available profiles from both gen3sdk and cdis-data-client configs"""
    try:
        profiles = config_tool.list_profiles()
        if profiles:
            click.echo("Available profiles:")
            for profile in profiles:
                click.echo(f"  - {profile}")
        else:
            click.echo("No profiles found. Use 'gen3 configure' to create one.")
    except Exception as e:
        click.echo(f"Error listing profiles: {str(e)}")
        raise e


@click.command()
@click.option("--profile", required=True, help="Profile name to show details for")
def show_profile(profile):
    """Show details for a specific profile"""
    try:
        profile_data = config_tool.parse_profile_from_config(profile)
        if profile_data:
            click.echo(f"Profile '{profile}' details:")
            for key, value in profile_data.items():
                if key in ['api_key', 'access_key', 'access_token']:
                    masked_value = value[:8] + '...' if len(value) > 8 else '***'
                    click.echo(f"  {key}: {masked_value}")
                else:
                    click.echo(f"  {key}: {value}")
        else:
            click.echo(f"Profile '{profile}' not found")
    except Exception as e:
        click.echo(f"Error showing profile: {str(e)}")
        raise e
