import json
import requests


class Gen3FileError(Exception):
    pass


class Gen3File:
    """For interacting with Gen3 file management features.

    A class for interacting with the Gen3 file download services.
    Supports getting presigned urls right now.

    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3File class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... sub = Gen3File(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint

    def get_presigned_url(self, guid, protocol="http"):
        """Generates a presigned URL for a file.

        Retrieves a presigned url for a file giving access to a file for a limited time.

        Args:
            guid (str): The GUID for the object to retrieve.
            protocol (:obj:`str`, optional): The protocol to use for picking the available URL for generating the presigned URL.

        Examples:

            >>> Gen3File.get_presigned_url(query)

        """
        api_url = "{}/user/data/download/{}&protocol={}".format(
            self._endpoint, guid, protocol
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        data = json.loads(output)
        return data
