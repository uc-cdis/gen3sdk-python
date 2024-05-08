"""
Module for checking user yaml configurations for security concerns
"""

import yaml
import requests
from os import environ
from sys import exit
from io import StringIO
from cdislogging import get_logger


logging = get_logger("__name__", log_level="info")


def check_yaml(phsid, path, repo="commons-users", verbose=False):
    """
    Scans a user.yaml file for occurrences of a given phsid to check if it is open access
    Must store a fine-grain github PAT with read access to the commons-user repository
    `export USER_YAML_GITHUB_PAT=<your-token>`
    Args:
        phsid (str): the ID of the project being checked for open access
        path (str): the relative path from the repository the user.yaml file is hosted on
        repo (str): the repository the user.yaml file is hosted on
        verbose (bool): if true scans entire user.yaml file and finds any instance of phsid
    """
    token = environ.get("USER_YAML_GITHUB_PAT")
    owner = "uc-cdis"

    response = requests.get(
        "https://api.github.com/repos/{owner}/{repo}/contents/{path}".format(
            owner=owner, repo=repo, path=path
        ),
        headers={
            "accept": "application/vnd.github.raw",
            "authorization": "token {}".format(token),
        },
    )

    content = response.content
    content = yaml.safe_load(content)

    # Figure Out whether authz or rbac
    auth_meth = ""
    try:
        content["rbac"]
        auth_meth = "rbac"
    except KeyError:
        auth_meth = "authz"

    if verbose:
        f = StringIO(response.content.decode())
        logging.info(f"========= Scanning file for {phsid} occurence =========")
        found = False
        for no, line in enumerate(f):
            if phsid in line:
                logging.info(f"{phsid} found in line number {no + 1}")
                found = True
        if found == False:
            logging.info(f"{phsid} not found in file")

    logging.info("\n")

    logging.info(f"========= Scanning file for {phsid} in open projects =========")
    found = False
    # Track down open_data_reader
    policies = content[auth_meth]["policies"]
    for i in range(len(policies)):
        if policies[i]["id"] == "open_data_reader":
            policy = policies[i]
            break

    # Check if phsid is hardcoded into the open data reader resource path
    if (
        phsid in policy["resource_paths"]
        or "/programs/" + phsid in policy["resource_paths"]
    ):
        logging.info("PHSID Directly in Open Data Reader Resource Path")

    # Check if phsid is in one of the resource paths
    else:
        resources = content[auth_meth]["resources"]
        open_resources = policy["resource_paths"]
        for i in range(len(open_resources)):
            resource = resources
            path = open_resources[i].split("/")
            for j in range(len(path)):
                for k in range(len(resource)):
                    if resource[k]["name"] == path[j]:
                        try:
                            resource = resource[k]["subresources"]
                            break
                        except KeyError:
                            break

            for j in range(len(resource)):
                if resource[j]["name"] == "projects":
                    resource = resource[j]["subresources"]
                    for k in range(len(resource)):
                        if resource[k]["name"] == phsid:
                            found = True
                            location = "resources" + "/".join(path)
                            logging.info(
                                f"PHSID Found in {location} listed as an open program"
                            )

        if found == False:
            logging.info(f"{phsid} not found in open projects")

        continue_input = ""
        while continue_input.lower() not in ["y", "n"]:
            continue_input = input("Would you like to continue? (y/n) ")
            if continue_input.lower() == "n":
                exit(1)
        # If we reach the end of open resources without finding the phsid then return from the function
        return


if __name__ == "__main__":
    from argparse import ArgumentParser

    args = ArgumentParser()

    args.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show all occurrences of phsid in file",
    )

    args.add_argument(
        "-p",
        "--path",
        type=str,
        default="users/ncicrdc/user.yaml",
        help="Relative path from repository home",
    )

    args.add_argument(
        "-r", "--repo", type=str, default="commons-users", help="Repository to search"
    )

    args.add_argument(
        "--phsid",
        type=str,
    )

    args = args.parse_args()

    check_yaml(args.phsid, args.path, args.repo, args.verbose)
