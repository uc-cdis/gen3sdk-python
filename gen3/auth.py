import json
from requests.auth import AuthBase
import requests


class Gen3AuthError(Exception):
    pass


class Gen3Auth(AuthBase):
    """
    Adds a bearer token to the request and fetches it from the refresh token if necessary.
    """

    def __init__(self, endpoint, refresh_file=None, refresh_token=None):

        if not refresh_file and not refresh_token:
            raise ValueError(
                "Either parameter 'refresh_file' or parameter 'refresh_token' must be specified."
            )

        if refresh_file and refresh_token:
            raise ValueError(
                "Only one of 'refresh_file' and 'refresh_token' must be specified."
            )

        self._refresh_file = refresh_file
        self._refresh_token = refresh_token

        if refresh_file:
            try:
                file_data = open(self._refresh_file).read()
                self._refresh_token = json.loads(file_data)
            except Exception as e:
                raise ValueError(
                    "Couldn't load your refresh token file: {}\n{}".format(
                        self._refresh_file, str(e)
                    )
                )

        self._access_token = None
        self._endpoint = endpoint

    def __call__(self, request):
        request.headers["Authorization"] = self._get_auth_value()
        request.register_hook("response", self._handle_401)
        return request

    def _handle_401(self, response, **kwargs):
        """
        Handle cases where the access token may have expired
        """
        if not response.status_code == 401:
            return response

        # Free the original connection
        response.content
        response.close()

        # copy the request to resend
        newreq = response.request.copy()

        self._access_token = None
        newreq.headers["Authorization"] = self._get_auth_value()

        _response = response.connection.send(newreq, **kwargs)
        _response.history.append(response)
        _response.request = newreq

        return _response

    def _get_auth_value(self):
        if not self._access_token:
            auth_url = "{}/user/credentials/cdis/access_token".format(self._endpoint)
            try:
                self._access_token = requests.post(auth_url, json=self._refresh_token).json()[
                    "access_token"
                ]
            except Exception as e:
                raise Gen3AuthError(
                    "Failed to authenticate to {}\n{}".format(auth_url, str(e))
                )

        return b"Bearer " + self._access_token
