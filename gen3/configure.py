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
from cdislogging import get_logger
import configparser
from os.path import expanduser, abspath
from pathlib import Path
from collections import OrderedDict

import gen3.auth as auth_tool
from gen3.utils import CONFIG_FILE_PATH

logging = get_logger("__name__")


def get_profile_from_creds(cred):
    with open(expanduser(cred)) as f:
        creds_from_json = json.load(f)
        credentials = OrderedDict()
        credentials["key_id"] = creds_from_json["key_id"]
        credentials["api_key"] = creds_from_json["api_key"]
        credentials["api_key_filepath"] = abspath(f.name)
        credentials["api_endpoint"] = auth_tool.endpoint_from_token(
            credentials["api_key"]
        )
        credentials["use_shepherd"] = ""
        credentials["min_shepherd_version"] = ""

    return credentials


def update_config_lines(profile, creds):
    """
    Update config file contents with the new profile values
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_PATH)

    config[profile] = creds

    with open(CONFIG_FILE_PATH, "w") as file:
        config.write(file)
