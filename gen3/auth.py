import json
from requests.auth import AuthBase
import os
import requests


class Gen3AuthError(Exception):
    pass


class Gen3Auth(AuthBase):
    """Gen3 auth helper class for use with requests auth.

    Implements requests.auth.AuthBase in order to support JWT authentication.
    Generates access tokens from the provided refresh token file or string.
    Automatically refreshes access tokens when they expire.

    Args:
        endpoint (str, opt): The URL of the data commons. Optional if working in a Gen3 Workspace.
        refresh_file (str, opt): The file containing the downloaded JSON web token. Optional if working in a Gen3 Workspace.
        refresh_token (str, opt): The JSON web token. Optional if working in a Gen3 Workspace.
        idp (str, opt): If working in a Gen3 Workspace, the IDP to use can be specified.

    Examples:
        This generates the Gen3Auth class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth("https://nci-crdc-demo.datacommons.io", refresh_file="credentials.json")

        If working in a Gen3 Workspace, initialize as follows:

        >>> auth = Gen3Auth()
    """

    def __init__(self, endpoint=None, refresh_file=None, refresh_token=None, idp=None):
        self._endpoint = endpoint
        self._refresh_file = refresh_file
        self._refresh_token = refresh_token
        self._wts_idp = idp
        self._access_token = None

        self._use_wts = False
        self._wts_url = None

        # if working in a Gen3 Workspace, we'll use the WTS
        namespace = os.environ.get("NAMESPACE")
        if namespace:
            # attempt to get a token from the workspace-token-service
            self._wts_url = (
                "http://workspace-token-service.{}.svc.cluster.local".format(namespace)
            )
            resp = requests.get("{}/token/".format(self._wts_url))
            if resp.status_code == 200:
                self._use_wts = True
                return

        # fall back to non-WTS initialization
        if not endpoint:
            raise ValueError(
                "When working outside of the Gen3 Workspace, parameter 'endpoint' must be specified."
            )

        if not refresh_file and not refresh_token:
            raise ValueError(
                "When working outside of the Gen3 Workspace, either parameter 'refresh_file' or parameter 'refresh_token' must be specified."
            )

        if refresh_file and refresh_token:
            raise ValueError(
                "Only one of 'refresh_file' and 'refresh_token' can be specified."
            )

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
            if self._use_wts:
                # attempt to get a token from the workspace-token-service
                auth_url = "{}/token/".format(self._wts_url)
                if self._wts_idp:
                    auth_url += "?idp={}".format(self._wts_idp)
                resp = requests.get(auth_url)
                err_msg = "Failed to get an access token from WTS at {}:\n{}"
                token_key = "token"
            else:
                # attempt to get a token from Fence
                auth_url = "{}/user/credentials/cdis/access_token".format(
                    self._endpoint
                )
                resp = requests.post(auth_url, json=self._refresh_token)
                err_msg = "Failed to get an access token from Fence at {}:\n{}"
                token_key = "access_token"

            assert resp.status_code == 200, err_msg.format(auth_url, resp.text)
            try:
                json_resp = resp.json()
                self._access_token = json_resp[token_key]
            except ValueError:  # cannot parse JSON
                raise Gen3AuthError(err_msg.format(auth_url, resp.text))
            except KeyError:  # no access_token in JSON response
                raise Gen3AuthError(err_msg.format(auth_url, json_resp))

        return "Bearer " + self._access_token
