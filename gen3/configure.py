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
from os.path import expanduser
from pathlib import Path
from collections import OrderedDict
import gen3.auth as auth_tool

from cdislogging import get_logger

logging = get_logger("__name__")

CONFIG_FILE_PATH = expanduser("~/.gen3/config")


def get_profile_from_creds(profile, cred):
    with open(expanduser(cred)) as f:
        creds_from_json = json.load(f)
        credentials = OrderedDict()
        credentials["key_id"] = creds_from_json["key_id"]
        credentials["api_key"] = creds_from_json["api_key"]
        credentials["api_endpoint"] = auth_tool.endpoint_from_token(
            credentials["api_key"]
        )
        credentials["access_key"] = auth_tool.get_access_token_with_key(credentials)
        credentials["use_shepherd"] = ""
        credentials["min_shepherd_version"] = ""
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
