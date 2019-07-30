from gen3 import auth, submission
import pytest

endpoint = 'https://endpoint.net/'
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

@pytest.fixture
def sub():
    return submission.Gen3Submission(endpoint, auth)

