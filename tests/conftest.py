from gen3 import auth, submission, index
import pytest

endpoint = 'https://avantol.planx-pla.net/'
auth = auth.Gen3Auth(endpoint, refresh_file="alexcredentials.json")

@pytest.fixture
def subm():
    return submission.Gen3Submission(endpoint, auth)

@pytest.fixture
def indexd():
    return index.Gen3Index(endpoint, auth)