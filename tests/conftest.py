"""
Conf Test for Gen3 test suite
"""
from unittest.mock import patch
import pytest

from drsclient.client import DrsClient
from cdisutilstest.code.conftest import indexd_server
from cdisutilstest.code.indexd_fixture import (
    setup_database,
    clear_database,
    create_user,
)
from gen3.file import Gen3File
from gen3.index import Gen3Index
from gen3.submission import Gen3Submission
from gen3.query import Gen3Query
from gen3.auth import Gen3Auth


class MockAuth:
    """
    Mock Auth for Gen3Auth
    """

    def __init__(self):
        self.endpoint = "https://example.commons.com"
        self.refresh_token = {"api_key": "123"}


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


@pytest.fixture(scope="function", params=("s3", "http", "ftp", "https", "gs", "az"))
def supported_protocol(request):
    """
    return "s3", "http", "ftp", "https", "gs", "az"

    Note that "az" is an internal mapping for a supported protocol
    """
    return request.param


# for unittest with mock server
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
