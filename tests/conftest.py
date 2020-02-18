from cdisutilstest.code.conftest import indexd_server
from cdisutilstest.code.indexd_fixture import (
    setup_database,
    clear_database,
    create_user,
)
from gen3.index import Gen3Index
from gen3.submission import Gen3Submission
import pytest


@pytest.fixture
def sub():
    return Gen3Submission("http://localhost/api", None)


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
    return index_client
