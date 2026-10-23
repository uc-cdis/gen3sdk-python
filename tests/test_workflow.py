import asyncio
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

TEST_ID = "some-test-id"
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
    "id": TEST_ID,
    "inputs": [],
    "logs": [],
    "name": "Hello-World",
    "outputs": [],
    "state": "COMPLETE",
    "tags": {"_AUTHZ": f"/services/workflow/gen3-workflow/tasks/123/{TEST_ID}"},
    "volumes": [],
}


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
    workflow = Gen3Workflow("https://example.com")

    test_id = TEST_ID

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
    print(f"CALL ARGS {requests_mock.call_args}")
    assert requests_mock.call_args.kwargs["url"].endswith(f"/tasks/{test_id}?view=FULL")
    if test_user is not None:
        # We have a user so we should have an "auth" arg but no "headers" arg
        assert requests_mock.call_args.kwargs["auth"] is None
        assert "headers" not in requests_mock.call_args.kwargs
    else:
        assert "auth" not in requests_mock.call_args.kwargs
        assert requests_mock.call_args.kwargs["headers"] == {}


# TODO: parametrize final state (not error states)
@patch("gen3.workflow.requests.get")
def test_poll_task_until_expected_state(requests_mock):
    """
    Test getting a task
    """
    workflow = Gen3Workflow("https://example.com")

    test_id = TEST_TASK.get("id")
    test_user = "main@example.org"
    expected_state = "COMPLETE"

    def create_mock_response(state):

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {**TEST_TASK, "state": state}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    mock_responses = [
        create_mock_response(state="RUNNING"),
        create_mock_response(state=expected_state),
    ]
    requests_mock.side_effect = mock_responses

    response = workflow.poll_task_until_expected_state(
        task_id=test_id,
        user=test_user,
        expected_final_state=expected_state,
        poll_interval=1,
    )

    assert response
    assert response == TEST_TASK
    assert response.get("state") == expected_state
    assert requests_mock.call_count >= len(mock_responses)
    assert requests_mock.call_args.kwargs["url"].endswith(f"/tasks/{test_id}?view=FULL")
    # TODO: check for "auth" and "headers"


@patch("gen3.workflow.requests.get")
def test_poll_task_never_reaches_expected_state(requests_mock):
    """
    Test polling a task that does not get to final expected state after max_retries
    """
    workflow = Gen3Workflow("https://example.com")

    test_id = TEST_TASK.get("id")
    test_user = "main@example.org"
    expected_state = "COMPLETE"

    def create_mock_response(state):

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {**TEST_TASK, "state": state}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    # test with responses that do not include expected_state
    mock_responses = [
        create_mock_response(state="QUEUED"),
        create_mock_response(state="INITIALIZING"),
        create_mock_response(state="RUNNING"),
        create_mock_response(state="RUNNING"),
    ]
    requests_mock.side_effect = mock_responses

    max_retries = len(mock_responses) - 1
    last_response = mock_responses[-1]
    expected_message = (
        "TES task did not reach a final state in time."
        f' Last known state: {last_response.json().get("state")},'
        f" Response: {last_response.json()}"
    )

    with pytest.raises(Exception) as err:
        workflow.poll_task_until_expected_state(
            task_id=test_id,
            user=test_user,
            expected_final_state=expected_state,
            max_retries=max_retries,
            poll_interval=1,
        )
    assert expected_message in str(err.value)
    assert last_response.json().get("state") != expected_state


# Test failed state or incorrect final state
@pytest.mark.parametrize(
    "test_final_state, expected_state, message",
    [
        ("SYSTEM_ERROR", "COMPLETE", "Task failed with error state 'SYSTEM_ERROR'"),
        ("EXECUTOR_ERROR", "COMPLETE", "Task failed with error state 'EXECUTOR_ERROR'"),
        (
            "FAILED",
            "COMPLETE",
            "TES task reached a final state, that is not 'COMPLETE'.",
        ),
        (
            "CANCELED",
            "COMPLETE",
            "TES task reached a final state, that is not 'COMPLETE'.",
        ),
        ("CANCELED", "FAILED", "TES task reached a final state, that is not 'FAILED'."),
    ],
)
@patch("gen3.workflow.requests.get")
def test_poll_task_with_error_states(
    requests_mock, test_final_state, expected_state, message
):
    """
    Test polling a task that does not get to final expected state after max_retries
    """
    workflow = Gen3Workflow("https://example.com")

    test_id = TEST_TASK.get("id")
    test_user = "main@example.org"

    def create_mock_response(state):

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {**TEST_TASK, "state": state}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    # test with responses that do not include expected_state
    mock_responses = [
        create_mock_response(state="QUEUED"),
        create_mock_response(state="INITIALIZING"),
        create_mock_response(state=test_final_state),
    ]
    requests_mock.side_effect = mock_responses

    last_response = mock_responses[-1]

    with pytest.raises(Exception) as err:
        # Use default max_retries (60)
        workflow.poll_task_until_expected_state(
            task_id=test_id,
            user=test_user,
            expected_final_state=expected_state,
            poll_interval=1,
        )
    assert message in str(err.value)
    assert last_response.json().get("state") != expected_state


# DO a test for unknown state
