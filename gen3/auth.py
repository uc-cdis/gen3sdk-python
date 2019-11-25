import json
from requests.auth import AuthBase
import requests


class Gen3AuthError(Exception):
    pass


class Gen3Auth(AuthBase):
    """Gen3 auth helper class for use with requests auth.

    Implements requests.auth.AuthBase in order to support JWT authentication.
    Generates access tokens from the provided refresh token file or string.
    Automatically refreshes access tokens when they expire.

    Args:
        endpoint (str): The URL of the data commons.
        refresh_file (str): The file containing the downloaded json web token.
        refresh_token (str): The json web token.

    Examples:
        This generates the Gen3Auth class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth("https://nci-crdc-demo.datacommons.io", refresh_file="credentials.json")

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
        """Adds authorization header to the request

        This gets called by the python.requests package on outbound requests
        so that authentication can be added.

        Args:
            request (object): The incoming request object

        """
        request.headers["Authorization"] = self._get_auth_value()
        request.register_hook("response", self._handle_401)
        return request

    def _handle_401(self, response, **kwargs):
        """Handles failed requests when authorization failed.

        This gets called after a failed request when an HTTP 401 error
        occurs. This then tries to refresh the access token in the event
        that it expired.

        Args:
            request (object): The failed request object

        """
        if not response.status_code == 401 and not response.status_code == 403:
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
        """Returns the Authorization header value for the request

        This gets called when added the Authorization header to the request.
        This fetches the access token from the refresh token if the access token is missing.

        """
        if not self._access_token:
            auth_url = "{}/user/credentials/cdis/access_token".format(self._endpoint)
            try:
                self._access_token = requests.post(
                    auth_url, json=self._refresh_token
                ).json()["access_token"]
            except Exception as e:
                raise Gen3AuthError(
                    "Failed to authenticate to {}\n{}".format(auth_url, str(e))
                )

        return "Bearer " + self._access_token
