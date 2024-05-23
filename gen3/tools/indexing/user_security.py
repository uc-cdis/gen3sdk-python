"""
Module for checking user yaml configurations for security concerns
"""

import yaml
import requests
from os import environ
from sys import exit, stdin
from select import select
from io import StringIO
from cdislogging import get_logger


logging = get_logger("__name__", log_level="info")


def check_yaml(phsids, path, repo="commons-users", verbose=False):
    """
    Scans a user.yaml file for occurrences of a given phsid to check if it is open access
    Must store a fine-grain github PAT with read access to the commons-user repository
    `export USER_YAML_GITHUB_PAT=<your-token>`
    Args:
        phsids (list): A list of phsids to check for
        path (str): the relative path from the repository the user.yaml file is hosted on
        repo (str): the repository the user.yaml file is hosted on
        verbose (bool): if true scans entire user.yaml file and finds any instance of phsid
    """

    if type(phsids) == str:
        phsids = [phsids]
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

    # Store whether any instance of any phsid has been found
    anything_found = False

    # Iterate through phsids
    for phsid in phsids:
        # Scans for any occurrence of phsid
        if verbose:
            lines = []
            f = StringIO(response.content.decode())
            logging.info(f"========= Scanning file for {phsid} occurrence =========")
            found = False

            # Find every line number containing the phsid
            for no, line in enumerate(f):
                if phsid in line:
                    lines.append(str(no + 1))
                    found = True
                    anything_found = True

            if found == False:
                logging.info(f"{phsid} not found in file \n")
                # If phsid isn't in the file we can continue to the next phsid
                continue

            # Log locations of phsid
            else:
                lines = ", ".join(lines)
                logging.info(f"{phsid} found in lines {lines} \n")

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

                locs = []
                for j in range(len(resource)):
                    if resource[j]["name"] == "projects":
                        resource = resource[j]["subresources"]
                        for k in range(len(resource)):
                            if resource[k]["name"] == phsid:
                                found = True
                                anything_found = True
                                location = "resources" + "/".join(path)
                                locs.append(location)

            if found == True:
                logging.info(f"{phsid} found in {*locs,} listed as open\n")

            if found == False:
                logging.info(f"{phsid} not found in open projects \n")
                if verbose == False:
                    # If verbose is true and no phsid is found we would have already continued
                    continue

    # If we find anything ask for input (eventually times out)
    if anything_found:
        print("Would you like to continue? (y/n): ", end="", flush=True)
        timeout = 90
        i = None
        i, o, e = select([stdin], [], [], timeout)

        if i:
            user_response = stdin.readline().strip()
            logging.info(f"User Response: {user_response}")
            if user_response != "y":
                exit(1)
        else:
            logging.info("No user response")
            exit(1)

    # If we reach the end of the function we can return
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
        nargs="+",
    )

    args = args.parse_args()

    check_yaml(args.phsid, args.path, args.repo, args.verbose)
