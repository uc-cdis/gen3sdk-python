import asyncio
import copy
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests.exceptions import HTTPError

from gen3.workflow import Gen3Workflow
from gen3.tools.metadata import async_ingest_metadata_manifest
from gen3.utils import get_or_create_event_loop_for_thread

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

TEST_TASK_ID = "some-test-id"
TEST_USER_ID = "42"
TEST_BODY = {
    "name": "Hello-World",
    "executors": [{"image": "quay.io/nextflow/bash", "command": ["echo hello world!"]}],
}
TEST_TASK = {
    "creation_time": "2026-08-21T21:11:06.198654625Z",
    "description": "",
    "executors": [
        {
            "command": ["echo hello world!"],
        }
    ],
    "id": TEST_TASK_ID,
    "inputs": [],
    "logs": [],
    "name": "Hello-World",
    "outputs": [],
    "state": "COMPLETE",
    "tags": {"_AUTHZ": f"/services/workflow/gen3-workflow/tasks/123/{TEST_TASK_ID}"},
    "volumes": [],
}


@pytest.fixture
def gen3_workflow(gen3_auth):
    """
    Gen3Workflow with MockAuth
    """
    return Gen3Workflow(gen3_auth)


def _task(state):
    task = copy.deepcopy(TEST_TASK)
    task["state"] = state
    return task


def _mock_request_for_states(states):
    """
    Build a requests.get side_effect that returns one task per call, with the
    given states in order. Fails the test if polled more often than expected.
    """
    remaining = list(states)
    responses = []  # returned tasks

    def _mock_request(url, **kwargs):
        assert f"/tasks/{TEST_TASK_ID}" in url
        assert "view=FULL" in url
        assert remaining, "polled more times than expected"

        task = _task(remaining.pop(0))
        responses.append(task)

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = task
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    _mock_request.responses = responses
    return _mock_request


@patch("gen3.workflow.requests.get")
def test_is_healthy(requests_mock):
    """
    Test is healthy response
    """
    workflow = Gen3Workflow("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_status")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "status": "OK",
            "timestamp": "2020-03-13T15:23:53.765568+00:00",
        }
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = workflow.is_healthy()

    assert response


@patch("gen3.workflow.requests.get")
def test_is_not_healthy(requests_mock):
    """
    Test is not healthy response
    """
    workflow = Gen3Workflow("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_status")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.text = "Not Healthy"
        mocked_response.json.return_value = {}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = workflow.is_healthy()

    assert not response


@patch("gen3.workflow.requests.get")
def test_get_version(requests_mock):
    """
    Test getting version
    """
    workflow = Gen3Workflow("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_version")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.text = "1.2.0"
        mocked_response.json.return_value = {}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = workflow.get_version()

    assert response


@patch("gen3.workflow.requests.post")
def test_create_task(requests_mock):
    """
    Test creating a task
    """
    test_id = TEST_TASK_ID
    workflow = Gen3Workflow("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/tasks")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {"id": test_id}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = workflow.create_task(TEST_BODY)

    assert response
    assert response == test_id
    assert requests_mock.call_args.kwargs["json"] == TEST_BODY


@pytest.mark.parametrize(
    "test_user",
    ["main@example.org", None],
)
@patch("gen3.workflow.requests.get")
def test_get_task(requests_mock, test_user):
    """
    Test getting a task
    """
    workflow = Gen3Workflow("https://example.com")

    test_id = TEST_TASK.get("id")

    def _mock_request(url, **kwargs):

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = TEST_TASK
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = workflow.get_tes_task(task_id=test_id, user=test_user)

    assert response
    assert response == TEST_TASK
    assert requests_mock.call_args.kwargs["url"].endswith(f"/tasks/{test_id}?view=FULL")
    if test_user is not None:
        # We have a user so we should have an "auth" arg but no "headers" arg
        assert requests_mock.call_args.kwargs["auth"] is None
        assert "headers" not in requests_mock.call_args.kwargs
    else:
        assert "auth" not in requests_mock.call_args.kwargs
        assert requests_mock.call_args.kwargs["headers"] == {}


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_waits_through_transient_states(requests_mock, mock_sleep, gen3_auth):
    """
    Test polling through transient states sleeps once between each poll
    """
    test_poll_interval = 7
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states(
        ["QUEUED", "INITIALIZING", "RUNNING", "COMPLETE"]
    )

    response = workflow.poll_task_until_expected_state(
        TEST_TASK_ID, user=TEST_USER_ID, poll_interval=test_poll_interval
    )

    assert response["state"] == "COMPLETE"
    assert requests_mock.call_count == 4
    assert mock_sleep.call_count == 3
    mock_sleep.assert_called_with(test_poll_interval)


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_returns_task_dict_when_complete_immediately(
    requests_mock, mock_sleep, gen3_auth
):
    """
    Test an already-complete task returns the full task dict with no waiting
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states(["COMPLETE"])

    response = workflow.poll_task_until_expected_state(TEST_TASK_ID, user=TEST_USER_ID)

    assert response == _task("COMPLETE")
    assert requests_mock.call_count == 1
    mock_sleep.assert_not_called()


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_passes_auth_to_request(requests_mock, mock_sleep, gen3_auth):
    """
    Test the auth provider is passed through to the underlying request
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states(["COMPLETE"])

    workflow.poll_task_until_expected_state(TEST_TASK_ID, user=TEST_USER_ID)

    assert requests_mock.call_args.kwargs["auth"] is gen3_auth


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_task_never_reaches_expected_state(requests_mock, mock_sleep, gen3_auth):
    """
    Test polling a task that does not get to final expected state after max_retries
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    mock_request = _mock_request_for_states(["QUEUED", "RUNNING"])
    requests_mock.side_effect = mock_request

    with pytest.raises(Exception, match="Last known state: RUNNING"):
        workflow.poll_task_until_expected_state(
            TEST_TASK_ID, user=TEST_USER_ID, max_retries=2
        )

    served_states = [task["state"] for task in mock_request.responses]
    assert served_states == ["QUEUED", "RUNNING"]
    assert served_states[-1] != "COMPLETE"


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_single_attempt_does_not_sleep(requests_mock, mock_sleep, gen3_auth):
    """
    Test max_retries=1 makes one request and never sleeps
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states(["RUNNING"])

    with pytest.raises(Exception, match="did not reach a final state in time"):
        workflow.poll_task_until_expected_state(
            TEST_TASK_ID, user=TEST_USER_ID, max_retries=1
        )

    assert requests_mock.call_count == 1
    mock_sleep.assert_not_called()


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_zero_retries_raises_timeout_not_unbound_error(
    requests_mock, mock_sleep, gen3_auth
):
    """
    Regression test: max_retries=0 used to hit an UnboundLocalError in the final
    raise because state and task_info were never assigned
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)

    with pytest.raises(Exception, match="Last known state: None"):
        workflow.poll_task_until_expected_state(
            TEST_TASK_ID, user=TEST_USER_ID, max_retries=0
        )

    requests_mock.assert_not_called()
    mock_sleep.assert_not_called()


@pytest.mark.parametrize("error_state", ["SYSTEM_ERROR", "EXECUTOR_ERROR"])
@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_raises_immediately_on_error_states(
    requests_mock, mock_sleep, error_state, gen3_auth
):
    """
    Test error states raise on the first poll without waiting
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    # the trailing COMPLETE must never be reached
    requests_mock.side_effect = _mock_request_for_states([error_state, "COMPLETE"])

    with pytest.raises(
        Exception, match=f"Task failed with error state '{error_state}'"
    ):
        workflow.poll_task_until_expected_state(TEST_TASK_ID, user=TEST_USER_ID)

    assert requests_mock.call_count == 1
    mock_sleep.assert_not_called()


@pytest.mark.parametrize("final_state", ["FAILED", "CANCELED"])
@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_raises_on_unexpected_final_state(
    requests_mock, mock_sleep, final_state, gen3_auth
):
    """
    Test a final state other than the expected one raises on the first poll
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states([final_state, "COMPLETE"])

    with pytest.raises(Exception, match=f"Final state: {final_state}"):
        workflow.poll_task_until_expected_state(TEST_TASK_ID, user=TEST_USER_ID)

    assert requests_mock.call_count == 1
    mock_sleep.assert_not_called()


@pytest.mark.parametrize(
    "expected_state", ["FAILED", "CANCELED", "SYSTEM_ERROR", "EXECUTOR_ERROR"]
)
@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_returns_when_non_complete_state_is_expected(
    requests_mock, mock_sleep, expected_state, gen3_auth
):
    """
    The expected-state check runs before the error-state checks, so asking for a
    failure state returns normally instead of raising
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states(["RUNNING", expected_state])

    response = workflow.poll_task_until_expected_state(
        TEST_TASK_ID, user=TEST_USER_ID, expected_final_state=expected_state
    )

    assert response["state"] == expected_state
    assert requests_mock.call_count == 2


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_keeps_polling_through_unrecognized_state(
    requests_mock, mock_sleep, gen3_auth
):
    """
    Test an unrecognized state is warned about but doesn't stop polling
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)
    requests_mock.side_effect = _mock_request_for_states(["SOMETHING_NEW", "COMPLETE"])

    response = workflow.poll_task_until_expected_state(TEST_TASK_ID, user=TEST_USER_ID)

    assert response["state"] == "COMPLETE"
    assert requests_mock.call_count == 2
    assert mock_sleep.call_count == 1


@patch("gen3.workflow.time.sleep")
@patch("gen3.workflow.requests.get")
def test_poll_propagates_http_error(requests_mock, mock_sleep, gen3_auth):
    """
    Test an HTTP error from the task endpoint surfaces instead of being retried
    """
    workflow = Gen3Workflow(auth_provider=gen3_auth)

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.text = "Internal Server Error"
        mocked_response.json.return_value = {"error": "some error"}
        mocked_response.raise_for_status.side_effect = HTTPError("got an error")

        return mocked_response

    requests_mock.side_effect = _mock_request

    with pytest.raises(Exception):
        workflow.poll_task_until_expected_state(TEST_TASK_ID, user=TEST_USER_ID)

    assert requests_mock.call_count == 1
    mock_sleep.assert_not_called()
