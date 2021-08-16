import json
import logging
from os.path import expanduser
from pathlib import Path
from collections import OrderedDict
import gen3.auth as auth_tool


CONFIG_FILE_PATH = expanduser("~/.gen3/config")


def get_profile_from_creds(profile, cred):
    with open(expanduser(cred)) as f:
        creds_from_json = json.load(f)
        credentials = OrderedDict()
        credentials["key_id"] = creds_from_json["key_id"]
        credentials["api_key"] = creds_from_json["api_key"]
        credentials["end_point"] = auth_tool.endpoint_from_token(credentials["api_key"])
        credentials["access_key"] = auth_tool.get_access_token_with_key(credentials)
        credentials["use_shepherd"] = ""
        credentials["min_shepherd_version"] = ""
    profile_line = "[" + profile + "]\n"
    new_lines = [key + "=" + value + "\n" for key, value in credentials.items()]
    return profile_line, new_lines


def get_current_config_lines():
    """ Read lines from the config file if exists, else create new config file """
    try:
        with open(CONFIG_FILE_PATH) as configFile:
            logging.info(f"Reading existing config file at {CONFIG_FILE_PATH}")
            return configFile.readlines()
    except FileNotFoundError:
        Path(CONFIG_FILE_PATH).touch()
        logging.info(f"Config file doesn't exist at {CONFIG_FILE_PATH}, creating one")
        return []


def update_config_lines(lines, profile_title, new_lines):
    """ Update config file contents with the new profile values """
    try:
        replace_line_index = lines.index(profile_title)
        replace_line_index += 1
        lines[replace_line_index : replace_line_index + len(new_lines)] = new_lines
        with open(CONFIG_FILE_PATH, "w+") as configFile:
            configFile.writelines(lines)
    # If no profile exists then append new profile in the end
    except ValueError:
        with open(CONFIG_FILE_PATH, "a+") as configFile:
            configFile.write(profile_title)
            configFile.writelines(new_lines)
