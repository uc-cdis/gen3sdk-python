"""
Contains class for interacting with custom user management tool deployed
in a few Gen3 Commons. Eventually this should interact with a more official
Administrative API once that's finalized.
"""
import backoff
import datetime
import logging
import os
import requests
import sys
from email_validator import validate_email, EmailNotValidError

from gen3.utils import DEFAULT_BACKOFF_SETTINGS


class UserManagementTool:
    """For interacting with custom user management tool deployed in a few Gen3 Commons.

    Args:
        endpoint (str): The URL for the user management tool API.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the UserManagementTool class pointed at the API while
        using the credentials.json downloaded from the connected commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... sub = Gen3File(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint.rstrip("/")

    def create(self, datasets):
        datasets = []
        with open(ACCESS_INFO_FILE, "r") as file:
            for line in file:
                phs_consent, authid, full_name = line.strip("\n").split(" ")
                try:
                    program, project = full_name.split("-", 1)
                except ValueError:
                    # nothing to split, assume program
                    program = full_name
                    project = "N/A"

                payload = {
                    "name": f"{full_name}",
                    "phsid": f"{phs_consent}",
                    "authid": f"{authid}",
                    "program": f"{program}",
                    "project": f"{project}",
                }
                datasets.append(payload)

        for dataset in datasets:
            logging.info(f"creating {dataset}...")
            response = requests.post(
                self._endpoint + "/datasets", json=dataset, auth=self._auth_provider
            )
            logging.debug(f"response: {response.text}")

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_datasets(self):
        logging.info(f"getting all datasets...")
        response = requests.get(
            self._endpoint + f"/datasets/", auth=self._auth_provider
        )
        response.raise_for_status()
        logging.debug(f"response: {response.text}")
        return response.json()

    def create_dataset(self):
        pass

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_dataset(self, phsid):
        logging.info(f"deleting {phsid}...")
        response = requests.delete(
            self._endpoint + f"/datasets/{phsid}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_user_under_admin(self, name, admin):
        logging.info(f"deleting {name} under {admin}...")
        response = requests.delete(
            self._endpoint + f"/users/{admin}/{name}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_whoami(self):
        response = requests.get(self._endpoint + f"/whoami", auth=self._auth_provider)
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    # users

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_user(
        self, name, username, organization=None, datasets=None, expiration=None
    ):
        """Create a user

        Args:
            name (str): Full name of the user
            username (str): username (either a Google email or eRA Commons ID)
            organization (None, optional): organization user should be associated with
            datasets (List[str], optional): list of dataset.phsid's user should have access to
            expiration (str, optional): string for expiration in format "YYYY-MM-DD"

        Returns:
            Dict: Response from creating user
        """
        # default expiration to 5 years from now
        now = datetime.datetime.today()
        expiration = expiration or f"{now.year + 5}-{now.month}-{now.day}"
        datasets = datasets or []

        # only set organization for non-DAC users based on person
        # trying to add the user
        if not organization:
            whoami = self.get_whoami()
            if whoami.get("iam") != "DAC":
                organization = whoami.get("organization")
        organization = organization or "N/A"

        body = {
            "name": name,
            "expiration": expiration,
            "datasets": datasets,
            "orcid": "N/A",
            "organization": organization,
            "contact_email": "N/A",
        }

        try:
            validate_email(username, allow_smtputf8=False, check_deliverability=False)
            # username is valid email, so it's a google email
            body["google_email"] = username
            body["contact_email"] = username
            body["eracommons"] = "N/A"
        except EmailNotValidError as e:
            # username is not a valid email, so this must be an era commons id
            body["eracommons"] = username
            body["google_email"] = "N/A"
            body["contact_email"] = "N/A"

        body["username"] = username

        logging.debug(f"body for create user: {body}")

        response = requests.post(
            self._endpoint + f"/users", json=body, auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update_user(self):
        response = requests.put(self._endpoint + f"/users", auth=self._auth_provider)
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_users(self):
        response = requests.get(self._endpoint + f"/users", auth=self._auth_provider)
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_user(self, user):
        response = requests.delete(
            self._endpoint + f"/users/{user}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()

    # for admins

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_users_under_admin(self, admin):
        response = requests.get(
            self._endpoint + f"/users/{admin}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_user_under_admin(self, admin):
        response = requests.post(
            self._endpoint + f"/users/{admin}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update_user_under_admin(self, admin):
        response = requests.put(
            self._endpoint + f"/users/{admin}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_user_under_admin(self, admin, user):
        response = requests.delete(
            self._endpoint + f"/users/{admin}/{user}", auth=self._auth_provider
        )
        logging.debug(f"response: {response.text}")
        response.raise_for_status()
