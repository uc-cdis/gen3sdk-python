import base64
import hashlib
import json
from requests.auth import AuthBase
import os
import random
import requests
import time
from cdislogging import get_logger

from urllib.parse import urlparse
import backoff

from gen3.utils import (
    DEFAULT_BACKOFF_SETTINGS,
    raise_for_status_and_print_error,
    remove_trailing_whitespace_and_slashes_in_url,
)

logging = get_logger("__name__")


class Gen3AuthError(Exception):
    pass


def decode_token(token_str):
    """
    jq -r '.api_key' < ~/.gen3/qa-covid19.planx-pla.net.json | awk -F . '{ print $2 }' | base64 --decode | jq -r .
    """
    tokenParts = token_str.split(".")
    if len(tokenParts) < 3:
        raise Exception("Invalid JWT. Could not split into parts.")
    padding = "===="
    infoStr = tokenParts[1] + padding[0 : len(tokenParts[1]) % 4]
    jsonStr = base64.urlsafe_b64decode(infoStr)
    return json.loads(jsonStr)


def endpoint_from_token(token_str):
    """
    Extract the endpoint from a JWT issue ("iss" property)
    """
    info = decode_token(token_str)
    urlparts = urlparse(info["iss"])
    endpoint = urlparts.scheme + "://" + urlparts.hostname
    if urlparts.port:
        endpoint += ":" + str(urlparts.port)
    return remove_trailing_whitespace_and_slashes_in_url(endpoint)


def _handle_access_token_response(resp, token_key):
    """
    Shared helper for both get_access_token_with_key and get_access_token_from_wts
    """
    err_msg = "Failed to get an access token from {}:\n{}"
    if resp.status_code != 200:
        raise Gen3AuthError(err_msg.format(resp.url, resp.text))
    try:
        json_resp = resp.json()
        return json_resp[token_key]
    except ValueError:  # cannot parse JSON
        raise Gen3AuthError(err_msg.format(resp.url, resp.text))
    except KeyError:  # no access_token in JSON response
        raise Gen3AuthError(err_msg.format(resp.url, json_resp))


def get_access_token_with_key(api_key):
    """
    Try to fetch an access token given the api key
    """
    endpoint = endpoint_from_token(api_key["api_key"])
    # attempt to get a token from Fence
    auth_url = "{}/user/credentials/cdis/access_token".format(endpoint)
    resp = requests.post(auth_url, json=api_key)
    token_key = "access_token"
    return _handle_access_token_response(resp, token_key)


def get_access_token_with_client_credentials(endpoint, client_credentials, scopes):
    """
    Try to get an access token from Fence using client credentials

    Args:
        endpoint (str): URL of the Gen3 instance to get an access token for
        client_credentials ((str, str) tuple): (client ID, client secret) tuple
        scopes (str): space-delimited list of scopes to request
    """
    if not endpoint:
        raise ValueError("'endpoint' must be specified when using client credentials")
    url = f"{endpoint}/user/oauth2/token?grant_type=client_credentials&scope={scopes}"
    resp = requests.post(url, auth=client_credentials)
    return _handle_access_token_response(resp, "access_token")


def get_wts_endpoint(namespace=os.getenv("NAMESPACE", "default")):
    return "http://workspace-token-service.{}.svc.cluster.local".format(namespace)


def get_wts_idps(namespace=os.getenv("NAMESPACE", "default"), external_wts_host=None):
    wts_url = None
    if external_wts_host == None:
        wts_url = get_wts_endpoint(namespace)
    else:
        wts_url = external_wts_host
    url = wts_url.rstrip("/") + "/external_oidc/"
    resp = requests.get(url)
    raise_for_status_and_print_error(resp)
    return resp.json()


def get_token_cache_file_name(key):
    """Compute the path to the access-token cache file"""
    cache_folder = "{}/.cache/gen3/".format(os.path.expanduser("~"))
    if not os.path.isdir(cache_folder):
        os.makedirs(cache_folder)

    cache_prefix = cache_folder + "token_cache_"
    m = hashlib.md5()
    m.update(key.encode("utf-8"))
    return cache_prefix + m.hexdigest()


class Gen3Auth(AuthBase):
    """Gen3 auth helper class for use with requests auth.

    Implements requests.auth.AuthBase in order to support JWT authentication.
    Generates access tokens from the provided refresh token file or string.
    Automatically refreshes access tokens when they expire.

    Args:
        refresh_file (str, opt): The file containing the downloaded JSON web token. Optional if working in a Gen3 Workspace.
                Defaults to (env["GEN3_API_KEY"] || "credentials") if refresh_token and idp not set.
                Includes ~/.gen3/ in search path if value does not include /.
                Interprets "idp://wts/<idp>" as an idp.
                Interprets "accesstoken:///<token>" as an access token
        refresh_token (str, opt): The JSON web token. Optional if working in a Gen3 Workspace.
        idp (str, opt): If working in a Gen3 Workspace, the IDP to use can be specified -
                "local" indicates the local environment fence idp
        client_credentials (tuple, opt): The (client_id, client_secret) credentials for an OIDC client
                that has the 'client_credentials' grant, allowing it to obtain access tokens.
        client_scopes (str, opt): Space-separated list of scopes requested for access tokens obtained from client
                credentials. Default: "user data openid"

    Examples:
        This generates the Gen3Auth class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page
        and installed in ~/.gen3/credentials.json

        >>> auth = Gen3Auth()

        or use ~/.gen3/crdc.json:

        >>> auth = Gen3Auth(refresh_file="crdc")

        or use some arbitrary file:

        >>> auth = Gen3Auth(refresh_file="./key.json")

        or set the GEN3_API_KEY environment variable rather
        than pass the refresh_file argument to the Gen3Auth
        constructor.

        If working with an OIDC client that has the 'client_credentials' grant, allowing it to obtain
        access tokens, provide the client ID and secret:

        Note: client secrets should never be hardcoded!

        >>> auth = Gen3Auth(
            endpoint="https://datacommons.example",
            client_credentials=("client ID", os.environ["GEN3_OIDC_CLIENT_CREDS_SECRET"])
        )

        If working in a Gen3 Workspace, initialize as follows:

        >>> auth = Gen3Auth()
    """

    def __init__(
        self,
        endpoint=None,
        refresh_file=None,
        refresh_token=None,
        idp=None,
        client_credentials=None,
        client_scopes=None,
    ):
        logging.debug("Initializing auth..")
        self.endpoint = remove_trailing_whitespace_and_slashes_in_url(endpoint)
        # note - `_refresh_token` is not actually a JWT refresh token - it's a
        #  gen3 api key with a token as the "api_key" property
        self._refresh_token = refresh_token
        self._access_token = None
        self._access_token_info = None
        self._wts_idp = idp or "local"
        self._wts_namespace = os.environ.get("NAMESPACE", "default")
        self._use_wts = False
        self._external_wts_host = None
        self._refresh_file = refresh_file
        self._client_credentials = client_credentials
        if self._client_credentials:
            self._client_scopes = client_scopes or "user data openid"
        elif client_scopes:
            raise ValueError(
                "'client_scopes' cannot be specified without 'client_credentials'"
            )

        if refresh_file and refresh_token:
            raise ValueError(
                "Only one of 'refresh_file' and 'refresh_token' can be specified."
            )

        if endpoint and idp:
            raise ValueError("Only one of 'endpoint' and 'idp' can be specified.")

        if not refresh_file and not refresh_token and not idp:
            refresh_file = os.getenv("GEN3_API_KEY", "credentials")

        if refresh_file and not idp:
            idp_prefix = "idp://wts/"
            access_token_prefix = "accesstoken:///"
            if refresh_file[0 : len(idp_prefix)] == idp_prefix:
                idp = refresh_file[len(idp_prefix) :]
                refresh_file = None
            elif refresh_file[0 : len(access_token_prefix)] == access_token_prefix:
                self._access_token = refresh_file[len(access_token_prefix) :]
                self._access_token_info = decode_token(self._access_token)
                refresh_file = None
            elif (
                not os.path.isfile(refresh_file)
                and "/" not in refresh_file
                and "\\" not in refresh_file
            ):
                refresh_file = "{}/.gen3/{}".format(
                    os.path.expanduser("~"), refresh_file
                )
                if not os.path.isfile(refresh_file) and refresh_file[-5:] != ".json":
                    refresh_file += ".json"
                if not os.path.isfile(refresh_file):
                    logging.warning("Unable to find refresh_file")
                    refresh_file = None

        if self._client_credentials:
            if not endpoint:
                raise ValueError(
                    "'endpoint' must be specified when '_client_credentials' is specified"
                )
            self._access_token = get_access_token_with_client_credentials(
                endpoint, self._client_credentials, self._client_scopes
            )

        if not self._access_token:
            # at this point - refresh_file either exists or is None
            if not refresh_file and not refresh_token:
                # check if this is a Gen3 workspace environment
                # most production environments are in the "default" namespace
                # attempt to get a token from the workspace-token-service
                self._use_wts = True
                # hate calling a method from the constructor, but avoids copying code
                self.get_access_token()
            elif refresh_file:
                try:
                    with open(refresh_file) as f:
                        file_data = f.read()
                    self._refresh_token = json.loads(file_data)
                except Exception as e:
                    raise ValueError(
                        "Couldn't load your refresh token file: {}\n{}".format(
                            refresh_file, str(e)
                        )
                    )

                assert "api_key" in self._refresh_token
                # if both endpoint and refresh file are provided, compare endpoint with iss in refresh file
                # May need to use network wts endpoint
                if idp or (
                    endpoint
                    and (
                        not endpoint.rstrip("/")
                        == endpoint_from_token(self._refresh_token["api_key"])
                    )
                ):
                    try:
                        logging.debug(
                            "Switch to using WTS and set external WTS host url.."
                        )
                        self._use_wts = True
                        self._external_wts_host = (
                            endpoint_from_token(self._refresh_token["api_key"])
                            + "/wts/"
                        )
                        self.get_access_token()
                    except Gen3AuthError as g:
                        logging.warning(
                            "Could not obtain access token from WTS service."
                        )
                        raise g

        if not self.endpoint:
            if self._access_token:
                self.endpoint = endpoint_from_token(self._access_token)
            else:
                self.endpoint = endpoint_from_token(self._refresh_token["api_key"])

    @property
    def _token_info(self):
        """
        Wrapper to fix intermittent errors when the token is being refreshed
        and `_access_token_info` == None
        """
        if not self._access_token_info:
            self.refresh_access_token()
        return self._access_token_info

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

        newreq.headers["Authorization"] = self._get_auth_value()

        _response = response.connection.send(newreq, **kwargs)
        _response.history.append(response)
        _response.request = newreq

        return _response

    def refresh_access_token(self, endpoint=None):
        """Get a new access token"""
        if self._use_wts:
            self._access_token = self.get_access_token_from_wts(endpoint)
        elif self._client_credentials:
            self._access_token = get_access_token_with_client_credentials(
                endpoint, self._client_credentials, self._client_scopes
            )
        else:
            self._access_token = get_access_token_with_key(self._refresh_token)

        self._access_token_info = decode_token(self._access_token)

        cache_file = None
        if self._use_wts:
            cache_file = get_token_cache_file_name(self._wts_idp)
        elif self._refresh_file:
            cache_file = get_token_cache_file_name(self._refresh_token["api_key"])

        if cache_file:
            try:
                self._write_to_file(cache_file, self._access_token)
            except Exception as e:
                logging.warning(
                    f"Unable to write access token to cache file. Exceeded number of retries. Details: {e}"
                )

        return self._access_token

    @backoff.on_exception(
        wait_gen=backoff.expo, exception=Exception, **DEFAULT_BACKOFF_SETTINGS
    )
    def _write_to_file(self, cache_file, content):
        # write a temp file, then rename - to avoid
        # simultaneous writes to same file race condition
        temp = cache_file + (
            ".tmp_eraseme_%d_%d" % (random.randrange(100000), time.time())
        )
        try:
            with open(temp, "w") as f:
                f.write(content)
            os.rename(temp, cache_file)
            return True
        except Exception as e:
            logging.warning("failed to write token cache file: " + cache_file)
            logging.warning(str(e))
            raise e

    def get_access_token(self):
        """Get the access token - auto refresh if within 5 minutes of expiration"""
        if not self._access_token:
            if self._use_wts == True:
                cache_file = get_token_cache_file_name(self._wts_idp)
            else:
                if self._refresh_token:
                    cache_file = get_token_cache_file_name(
                        self._refresh_token["api_key"]
                    )
            if cache_file and os.path.isfile(cache_file):
                try:  # don't freak out on invalid cache
                    with open(cache_file) as f:
                        self._access_token = f.read()
                        self._access_token_info = decode_token(self._access_token)
                except Exception as e:
                    logging.warning("ignoring invalid token cache: " + cache_file)
                    self._access_token = None
                    self._access_token_info = None
                    logging.warning(str(e))

        need_new_token = (
            not self._access_token
            or not self._access_token_info
            or time.time() + 300 > self._access_token_info["exp"]
        )
        if need_new_token:
            return self.refresh_access_token(
                self.endpoint if hasattr(self, "endpoint") else None
            )
        # use cache
        return self._access_token

    def _get_auth_value(self):
        """Returns the Authorization header value for the request

        This gets called when added the Authorization header to the request.
        This fetches the access token from the refresh token if the access token is missing.

        """
        return "bearer " + self.get_access_token()

    def curl(self, path, request=None, data=None):
        """
        Curl the given endpoint - ex: gen3 curl /user/user.  Return requests.Response

        Args:
            path (str): path under the commons to curl (/user/user, /index/index, /authz/mapping, ...)
            request (str in GET|POST|PUT|DELETE): default to GET if data is not set, else default to POST
            data (str): json string or "@filename" of a json file
        """
        if not request:
            request = "GET"
            if data:
                request = "POST"
        json_data = data
        output = None
        if data and data[0] == "@":
            with open(data[1:]) as f:
                json_data = f.read()
        if request == "GET":
            output = requests.get(self.endpoint + "/" + path, auth=self)
        elif request == "POST":
            output = requests.post(
                self.endpoint + "/" + path, json=json_data, auth=self
            )
        elif request == "PUT":
            output = requests.put(self.endpoint + "/" + path, json=json_data, auth=self)
        elif request == "DELETE":
            output = requests.delete(self.endpoint + "/" + path, auth=self)
        else:
            raise Exception("Invalid request type: " + request)
        return output

    def get_access_token_from_wts(self, endpoint=None):
        """
        Try to fetch an access token for the given idp from the wts
        in the given namespace.  If idp is not set, then default to "local"
        """
        # attempt to get a token from the workspace-token-service
        logging.debug("getting access token from wts..")
        auth_url = get_wts_endpoint(self._wts_namespace) + "/token/"

        # If non "local" idp value exists, append to auth url
        # If user specified endpoint value, then first attempt to determine idp value.
        if self.endpoint or (self._wts_idp and self._wts_idp != "local"):
            # If user supplied endpoint value and not idp, figure out the idp value
            if self.endpoint:
                logging.debug(
                    "First try to use the local WTS to figure out idp name for the supplied endpoint.."
                )
                try:
                    provider_List = get_wts_idps(self._wts_namespace)
                    matchProviders = list(
                        filter(
                            lambda provider: provider["base_url"] == endpoint,
                            provider_List["providers"],
                        )
                    )
                    if len(matchProviders) == 1:
                        logging.debug("Found matching idp from local WTS.")
                        self._wts_idp = matchProviders[0]["idp"]
                    elif len(matchProviders) > 1:
                        raise ValueError(
                            "Multiple idps matched with endpoint value provided."
                        )
                    else:
                        logging.debug("Could not find matching idp from local WTS.")
                except Exception as e:
                    logging.debug(
                        "Exception occured when making network call to local WTS."
                    )
                    if not self._external_wts_host:
                        raise e
                    else:
                        logging.debug("Since external WTS host exists, continuing on..")
                        pass

            if self._wts_idp and self._wts_idp != "local":
                auth_url += "?idp={}".format(self._wts_idp)

        # If endpoint value exists, only get WTS token if idp value has been successfully determined
        # Otherwise skip to querying external WTS
        # This is to prevent local WTS from supplying an incorrect token to user
        if (
            not self._external_wts_host
            or not self.endpoint
            or (self.endpoint and self._wts_idp != "local")
        ):
            try:
                logging.debug("Try to get access token from local WTS..")
                logging.debug(f"{auth_url=}")
                resp = requests.get(auth_url)
                if (resp and resp.status_code == 200) or (not self._external_wts_host):
                    return _handle_access_token_response(resp, "token")
            except Exception as e:
                if not self._external_wts_host:
                    raise e
                else:
                    # Try to obtain token from external wts
                    logging.debug("Could get obtain token from Local WTS.")
                    pass

        # local workspace wts call failed, try using a network call
        # First get access token with WTS host
        logging.debug("Trying to get access token from external WTS Host..")
        wts_token = get_access_token_with_key(self._refresh_token)
        auth_url = self._external_wts_host + "token/"

        provider_List = get_wts_idps(self._wts_namespace, self._external_wts_host)

        # if user already supplied idp, use that
        if self._wts_idp and self._wts_idp != "local":
            matchProviders = list(
                filter(
                    lambda provider: provider["idp"] == self._wts_idp,
                    provider_List["providers"],
                )
            )
        elif endpoint:
            matchProviders = list(
                filter(
                    lambda provider: provider["base_url"] == endpoint,
                    provider_List["providers"],
                )
            )
        else:
            raise Exception(
                "Unable to generate matching identity providers (no IdP or endpoint provided)"
            )

        if len(matchProviders) == 1:
            self._wts_idp = matchProviders[0]["idp"]
            logging.debug("Succesfully determined idp value: {}".format(self._wts_idp))
        else:
            idp_list = "\n "

            if len(matchProviders) > 1:
                for idp in matchProviders:
                    idp_list = (
                        idp_list
                        + "idp name: "
                        + idp["idp"]
                        + " url: "
                        + idp["base_url"]
                        + "\n "
                    )
                raise ValueError(
                    "Multiple idps matched with endpoint value provided."
                    + idp_list
                    + "Query /wts/external_oidc/ for more information."
                )
            else:
                for idp in provider_List["providers"]:
                    idp_list = (
                        idp_list
                        + "idp name: "
                        + idp["idp"]
                        + "  Endpoint url: "
                        + idp["base_url"]
                        + "\n "
                    )
                raise ValueError(
                    "No idp matched with the endpoint or idp value provided.\n"
                    + "Please make sure your endpoint or idp value matches exactly with the output below.\n"
                    + "i.e. check trailing '/' character for the endpoint url\n"
                    + "Available Idps:"
                    + idp_list
                    + "Query /wts/external_oidc/ for more information."
                )
        logging.debug("Finally getting access token..")
        auth_url += "?idp={}".format(self._wts_idp)
        header = {"Authorization": "Bearer " + wts_token}
        resp = requests.get(auth_url, headers=header)
        err_msg = "Please make sure the target commons is connected on your profile page and that connection has not expired."
        if resp.status_code != 200:
            logging.warning(err_msg)
        return _handle_access_token_response(resp, "token")
