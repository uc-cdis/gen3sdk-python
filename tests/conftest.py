"""
Conf Test for Gen3 test suite
"""

from collections.abc import Generator
from unittest.mock import patch
import os
import pytest

from drsclient.client import DrsClient

from gen3.cli.auth import endpoint

from gen3.file import Gen3File
from gen3.index import Gen3Index
from gen3.submission import Gen3Submission
from gen3.query import Gen3Query
from gen3.auth import Gen3Auth
from gen3.object import Gen3Object

from tests.fake_indexd import FakeIndexd

os.makedirs("tests/outputs", exist_ok=True)


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


@pytest.fixture
def indexd_server() -> Generator[FakeIndexd, None, None]:
    """
    An empty in-memory indexd, with every HTTP client routed to it.

    Each test gets its own store, so there is no database to clear between them.
    """
    fake_indexd = FakeIndexd()
    with fake_indexd.serving_in_process():
        yield fake_indexd


@pytest.fixture
def index_client(indexd_server: FakeIndexd) -> Gen3Index:
    """
    Gen3Index pointed at the fake indexd.
    """
    return Gen3Index(indexd_server.baseurl, ("admin", "admin"), service_location="")


@pytest.fixture
def gen3_index_over_http() -> Generator[Gen3Index, None, None]:
    """
    Gen3Index pointed at a fake indexd listening on a real socket.

    For tools that shell out: a subprocess cannot see the in-process patching that
    `indexd_server` relies on, so it needs a server it can actually connect to.
    """
    fake = FakeIndexd()
    with fake.serving_over_http():
        yield Gen3Index(fake.baseurl, ("admin", "admin"), service_location="")


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
def drs_client(indexd_server: FakeIndexd) -> DrsClient:
    """
    Returns a DrsClient pointed at the fake indexd.
    """
    return DrsClient(baseurl=indexd_server.baseurl, auth=("user", "user"))


@pytest.fixture(scope="function")
def drsclient(drs_client):
    """
    Mock drsclient
    """
    return drs_client
