import json
import requests


class Gen3FileError(Exception):
    pass


class Gen3File:
    """
    A class for interacting with the Gen3 file download services.
    Supports getting presigned urls right now.
    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint

    def get_presigned_url(self, guid, protocol="http"):
        api_url = "{}/user/data/download/{}&protocol={}".format(
            self._endpoint, guid, protocol
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        data = json.loads(output)
        return data
