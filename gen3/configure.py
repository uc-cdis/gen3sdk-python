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
from pathlib import Path
from collections import OrderedDict
import gen3.auth as auth_tool
import configparser

from cdislogging import get_logger

logging = get_logger("__name__")

CONFIG_FILE_PATH = expanduser("~/.gen3/config")
GEN3_CLIENT_CONFIG_PATH = expanduser("~/.gen3/gen3_client_config.ini")


def get_profile_from_creds(profile, cred, apiendpoint=None):
    """Create profile configuration from credentials file with validation"""
    with open(expanduser(cred)) as f:
        creds_from_json = json.load(f)
        credentials = OrderedDict()
        
        if "key_id" not in creds_from_json:
            raise ValueError("key_id not found in credentials file")
        if "api_key" not in creds_from_json:
            raise ValueError("api_key not found in credentials file")
            
        credentials["key_id"] = creds_from_json["key_id"]
        credentials["api_key"] = creds_from_json["api_key"]
        
        if apiendpoint:
            if not apiendpoint.startswith(('http://', 'https://')):
                raise ValueError("API endpoint must start with http:// or https://")
            credentials["api_endpoint"] = apiendpoint.rstrip('/')
        else:
            credentials["api_endpoint"] = auth_tool.endpoint_from_token(
                credentials["api_key"]
            )
        
        try:
            credentials["access_key"] = auth_tool.get_access_token_with_key(credentials)
        except Exception as e:
            logging.warning(f"Could not validate credentials with endpoint: {e}")
            credentials["access_key"] = ""
        
    profile_line = "[" + profile + "]\n"
    new_lines = [key + "=" + value + "\n" for key, value in credentials.items()]
    new_lines.append("\n")  # Adds an empty line between two profiles.
    return profile_line, new_lines


def get_current_config_lines():
    """Read lines from the config file if exists in ~/.gen3 folder, else create new config file"""
    try:
        with open(CONFIG_FILE_PATH) as configFile:
            logging.info(f"Reading existing config file at {CONFIG_FILE_PATH}")
            return configFile.readlines()
    except FileNotFoundError:
        Path(CONFIG_FILE_PATH).touch()
        logging.info(f"Config file doesn't exist at {CONFIG_FILE_PATH}, creating one")
        return []


def update_config_lines(lines, profile_title, new_lines):
    """Update config file contents with the new profile values"""

    if profile_title in lines:
        profile_line_index = lines.index(profile_title)
        next_profile_index = len(lines)
        for i in range(profile_line_index, len(lines)):
            if lines[i][0] == "[":
                next_profile_index = i
                break
        del lines[profile_line_index:next_profile_index]

    with open(CONFIG_FILE_PATH, "a+") as configFile:
        configFile.write(profile_title)
        configFile.writelines(new_lines)


def parse_profile_from_config(profile_name):
    """Parse profile configuration from config file, checking both gen3sdk and cdis-data-client formats"""
    
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            lines = f.readlines()
            profile_line = f"[{profile_name}]\n"
            if profile_line in lines:
                profile_idx = lines.index(profile_line)
                profile_data = {}
                for i in range(profile_idx + 1, len(lines)):
                    if lines[i].startswith('[') or lines[i].strip() == '':
                        break
                    if '=' in lines[i]:
                        key, value = lines[i].strip().split('=', 1)
                        profile_data[key] = value
                return profile_data
    except FileNotFoundError:
        pass
    
    try:
        if os.path.exists(GEN3_CLIENT_CONFIG_PATH):
            config = configparser.ConfigParser()
            config.read(GEN3_CLIENT_CONFIG_PATH)
            if profile_name in config:
                profile_data = dict(config[profile_name])
                if 'access_token' in profile_data:
                    profile_data['access_key'] = profile_data['access_token']
                return profile_data
    except Exception as e:
        logging.warning(f"Error reading cdis-data-client config: {e}")
    
    return None


def list_profiles():
    """List all available profiles from both config files"""
    profiles = set()
    
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('[') and line.endswith(']\n'):
                    profile_name = line[1:-2]
                    profiles.add(profile_name)
    except FileNotFoundError:
        pass
    
    try:
        if os.path.exists(GEN3_CLIENT_CONFIG_PATH):
            config = configparser.ConfigParser()
            config.read(GEN3_CLIENT_CONFIG_PATH)
            for section in config.sections():
                profiles.add(section)
    except Exception as e:
        logging.warning(f"Error reading cdis-data-client config: {e}")
    
    return sorted(list(profiles))


def get_profile_credentials(profile_name):
    """Get credentials for a specific profile, compatible with both config formats"""
    profile_data = parse_profile_from_config(profile_name)
    if not profile_data:
        raise ValueError(f"Profile '{profile_name}' not found in any config file")
    
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
