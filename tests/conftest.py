"""
Conf Test for Gen3 test suite
"""
from multiprocessing import Process
import multiprocessing
from unittest.mock import patch
import pytest
import requests

from drsclient.client import DrsClient
from cdisutilstest.code.indexd_fixture import (
    setup_database,
    clear_database,
    create_user,
)
from gen3.cli.auth import endpoint
from indexd import get_app
from indexd.default_settings import settings

from gen3.file import Gen3File
from gen3.index import Gen3Index
from gen3.submission import Gen3Submission
from gen3.query import Gen3Query
from gen3.auth import Gen3Auth
from gen3.object import Gen3Object


class MockAuth:
    """
    Mock Auth for Gen3Auth
    """

    def __init__(self):
        self.endpoint = "https://example.commons.com"
        self.refresh_token = {"api_key": "123"}
        self._token_info = {"sub": "42"}

    def _get_auth_value(self):
        return "foobar"

    @property
    def __class__(self):
        """
        So that `isinstance(<MockAuth instance>, Gen3Auth)` returns True
        """
        return Gen3Auth

    def __call__(self, request):
        return request


@pytest.fixture
def sub():
    """
    Mock Gen3Submission with MockAuth
    """
    return Gen3Submission(MockAuth())


@pytest.fixture
def gen3_auth():
    """
    Get MockAuth
    """
    return MockAuth()


@pytest.fixture
def mock_gen3_auth():
    """
    Mock gen3 auth with endpoint and refresh token
    """
    mock_auth = MockAuth()
    # patch as __init__ has method call
    with patch("gen3.auth.endpoint_from_token") as mock_endpoint_from_token:
        mock_endpoint_from_token().return_value = mock_auth.endpoint
        return Gen3Auth(
            endpoint=mock_auth.endpoint, refresh_token=mock_auth.refresh_token
        )


@pytest.fixture
def gen3_file_no_auth():
    """
    Mock Gen3File without auth
    """
    return Gen3File(endpoint=gen3_auth.endpoint, auth_provider=None)


@pytest.fixture
def gen3_file(mock_gen3_auth):
    """
    Mock Gen3File with auth
    """
    return Gen3File(endpoint=mock_gen3_auth.endpoint, auth_provider=mock_gen3_auth)


@pytest.fixture
def gen3_object(gen3_auth):
    """
    Mock Gen3Object with auth
    """
    return Gen3Object(auth_provider=gen3_auth)


@pytest.fixture(scope="function", params=("s3", "http", "ftp", "https", "gs", "az"))
def supported_protocol(request):
    """
    return "s3", "http", "ftp", "https", "gs", "az"

    Note that "az" is an internal mapping for a supported protocol
    """
    return request.param


@pytest.fixture(scope="session")
def indexd_server():
    """
    Fixture copied from cdisutils-test and updated to mock Arborist
    """

    class MockServer(object):
        def __init__(self, port):
            self.port = port
            self.baseurl = "http://localhost:{}".format(port)

    def run_indexd(port):
        app = get_app()
        app.run(host="localhost", port=port, debug=False)

    def wait_for_indexd_alive(port):
        url = "http://localhost:{}".format(port)
        try:
            requests.get(url)
        except requests.ConnectionError:
            return wait_for_indexd_alive(port)
        else:
            return

    def wait_for_indexd_not_alive(port):
        url = "http://localhost:{}".format(port)
        try:
            requests.get(url)
        except requests.ConnectionError:
            return
        else:
            return wait_for_indexd_not_alive(port)

    class MockArboristClient(object):
        def auth_request(*args, **kwargs):
            return True

    port = 8001
    settings["auth"].arborist = MockArboristClient()
    indexd = Process(target=run_indexd, args=[port])
    # Add this line because OS X multiprocessing default is spawn which will cause pickling errors
    # NOTE: fork is unstable and not technically supported on OS X, forking is only supported on Unix
    # However explicitly setting default behavior to fork to pass unit test, only used for tests
    # https://docs.python.org/3/library/multiprocessing.html
    # https://github.com/pytest-dev/pytest-flask/issues/104
    multiprocessing.set_start_method("fork")
    indexd.start()
    wait_for_indexd_alive(port)

    yield MockServer(port=port)

    indexd.terminate()


@pytest.fixture
def index_client(indexd_server):
    """
    Handles getting all the docs from an
    indexing endpoint. Currently this is changing from
    signpost to indexd, so we'll use just indexd_client now.
    I.E. test to a common interface this could be multiply our
    tests:
    https://docs.pytest.org/en/latest/fixture.html#parametrizing-fixtures
    """
    setup_database()

    try:
        user = create_user("admin", "admin")
    except Exception:
        # assume user already exists, try using username and password for admin
        user = ("admin", "admin")

    client = Gen3Index(indexd_server.baseurl, user, service_location="")

    yield client

    clear_database()


@pytest.fixture
def gen3_index(index_client):
    """
    Mock Gen3Index
    """
    return index_client


@pytest.fixture
def gen3_query(gen3_auth):
    """
    Mock Gen3Query
    """
    return Gen3Query(gen3_auth)


@pytest.fixture(scope="function")
def drs_client(indexd_server):
    """
    Returns a DrsClient. This will delete any documents,
    aliases, or users made by this
    client after the test has completed.
    Currently the default user is the admin user
    Runs once per test.
    """
    try:
        user = create_user("user", "user")
    except Exception:
        user = ("user", "user")
    client = DrsClient(baseurl=indexd_server.baseurl, auth=user)
    yield client
    clear_database()


@pytest.fixture(scope="function")
def drsclient(drs_client):
    """
    Mock drsclient
    """
    return drs_client
