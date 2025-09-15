"""
The format of config file is described as following

[profile1]
key_id=key_id_example_1
api_key=api_key_example_1
access_key=access_key_example_1
api_endpoint=http://localhost:8000
use_shepherd=true
min_shepherd_version=2.0.0

[profile2]
key_id=key_id_example_2
api_key=api_key_example_2
access_key=access_key_example_2
api_endpoint=http://example.com
use_shepherd=false
min_shepherd_version=

"""
import json
import os
from os.path import expanduser
import gen3.auth as auth_tool
import configparser

from cdislogging import get_logger

logging = get_logger("__name__")

CONFIG_FILE_PATH = expanduser("~/.gen3/gen3_client_config.ini")


def get_profile_from_creds(profile, cred, apiendpoint=None):
    """Create profile configuration from credentials file with validation"""
    with open(expanduser(cred)) as f:
        creds_from_json = json.load(f)

        if "key_id" not in creds_from_json:
            raise ValueError("key_id not found in credentials file")
        if "api_key" not in creds_from_json:
            raise ValueError("api_key not found in credentials file")

        credentials = {
            "key_id": creds_from_json["key_id"],
            "api_key": creds_from_json["api_key"]
        }

        if apiendpoint:
            if not apiendpoint.startswith(('http://', 'https://')):
                raise ValueError("API endpoint must start with http:// or https://")
            credentials["api_endpoint"] = apiendpoint.rstrip('/')
        else:
            credentials["api_endpoint"] = auth_tool.endpoint_from_token(
                credentials["api_key"]
            )

        try:
            credentials["access_token"] = auth_tool.get_access_token_with_key(credentials)
        except Exception as e:
            logging.warning(f"Could not validate credentials with endpoint: {e}")
            credentials["access_token"] = ""

    return profile, credentials


def ensure_config_dir():
    """Ensure the ~/.gen3 directory exists"""
    config_dir = os.path.dirname(CONFIG_FILE_PATH)
    os.makedirs(config_dir, exist_ok=True)


def update_config_profile(profile_name, credentials):
    """Update config file with new profile using configparser"""
    ensure_config_dir()

    config = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE_PATH):
        config.read(CONFIG_FILE_PATH)

    if profile_name not in config:
        config.add_section(profile_name)

    for key, value in credentials.items():
        config.set(profile_name, key, str(value))

    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)

    logging.info(f"Profile '{profile_name}' saved to {CONFIG_FILE_PATH}")


def parse_profile_from_config(profile_name):
    """Parse profile configuration from config file using configparser"""
    if not os.path.exists(CONFIG_FILE_PATH):
        return None

    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE_PATH)
        if profile_name in config:
            profile_data = dict(config[profile_name])
            # Normalize access_token vs access_key
            if 'access_token' in profile_data and 'access_key' not in profile_data:
                profile_data['access_key'] = profile_data['access_token']
            return profile_data
    except Exception as e:
        logging.warning(f"Error reading config file: {e}")

    return None


def list_profiles():
    """List all available profiles from config file"""
    if not os.path.exists(CONFIG_FILE_PATH):
        return []

    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE_PATH)
        return sorted(config.sections())
    except Exception as e:
        logging.warning(f"Error reading config file: {e}")
        return []


def get_profile_credentials(profile_name):
    """Get credentials for a specific profile"""
    profile_data = parse_profile_from_config(profile_name)
    if not profile_data:
        raise ValueError(f"Profile '{profile_name}' not found in config file")

    required_fields = ['key_id', 'api_key', 'api_endpoint']
    for field in required_fields:
        if field not in profile_data or not profile_data[field]:
            raise ValueError(f"Profile '{profile_name}' missing required field: {field}")

    credentials = {
        'key_id': profile_data['key_id'],
        'api_key': profile_data['api_key'],
        'api_endpoint': profile_data['api_endpoint']
    }

    if 'access_key' in profile_data:
        credentials['access_token'] = profile_data['access_key']
    elif 'access_token' in profile_data:
        credentials['access_token'] = profile_data['access_token']

    return credentials
