"""
Contains class for interacting with Gen3's Workflow Service.
"""
from dataclasses import dataclass
import time

import backoff
import requests
from cdislogging import get_logger

from gen3.auth import Gen3Auth
from gen3.utils import DEFAULT_BACKOFF_SETTINGS, raise_for_status_and_print_error

logging = get_logger(__name__)


class Gen3Workflow:
    """
    A class for interacting with the Gen3 Workflow service.

    Examples:
        This generates the Gen3Workflow class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... workflow = Gen3Workflow(auth)

    Attributes:
        endpoint (str): public endpoint for reading/querying workflow - only necessary if auth_provider not provided
        auth_provider (Gen3Auth): auth manager
    """

    def __init__(
        self,
        endpoint=None,
        auth_provider=None,
        service_location="workflows",
    ):
        """
        Initialization for instance of the class to setup basic endpoint info.

        Args:
            endpoint (str): URL for a Data Commons that has workflow service deployed
            auth_provider (Gen3Auth, optional): Gen3Auth class to handle passing your
                token, required for admin endpoints
            service_location (str, optional): deployment location relative to the
                endpoint provided
        """
        # legacy interface required endpoint as 1st arg
        if endpoint and isinstance(endpoint, Gen3Auth):
            auth_provider = endpoint
            endpoint = None
        if auth_provider and isinstance(auth_provider, Gen3Auth):
            endpoint = auth_provider.endpoint
        endpoint = endpoint.strip("/")
        # if running locally, gen3-workflow is deployed by itself without a location relative
        # to the commons
        if "http://localhost" in endpoint:
            service_location = ""

        if not endpoint.endswith(service_location):
            endpoint += "/" + service_location

        self.endpoint = endpoint.rstrip("/")
        # TODO: need TES URL and S3 URL
        self.ui_url = self.endpoint + "/ui"
        # TODO: include "/task" at the end and replace settings in methods
        # change to tes_task_url
        self.tes_url = self.endpoint + "/ga4gh/tes/v1"
        self._auth_provider = auth_provider

    def is_healthy(self):
        """
        Return if is healthy or not

        Returns:
            bool: True if healthy
        """
        try:
            response = requests.get(
                self.endpoint + "/_status", auth=self._auth_provider
            )
            response.raise_for_status()
        except Exception as exc:
            logging.error(exc)
            return False

        return response.json().get("status") == "OK"

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_version(self):
        """
        Return the version

        Returns:
            str: the version
        """
        response = requests.get(self.endpoint + "/_version", auth=self._auth_provider)
        raise_for_status_and_print_error(response)
        return response.text

    # Create task
    # https://github.com/uc-cdis/gen3-workflow/blob/3cc65d4bb175a17a3e1e391d3082577cabeb3a6c/scripts/performance_test/run_tes_task.py#L19-L28
    def create_task(self, body: dict) -> str:
        """
        Create a TES task.

        Args:
            body (dict): The json of the TES workflow.

        Returns:
            task_id
        """

        print("IN CREATE")
        tes_task_url = f"{self.tes_url}/tasks"
        print(f"POSTING TO URL {tes_task_url}")
        response = requests.post(
            tes_task_url,
            json=body,
            # Do like the get_page
            auth=self._auth_provider,
            timeout=60,
        )
        expected_status = 200
        if response.status_code != expected_status:
            raise_for_status_and_print_error(response)
        data = response.json()
        return data.get("id")

    def get_tes_task(self, task_id: str, user: str, expected_status=200):
        """
        Get task for task_id.

        Args:
            task_id (str): ID of the TES task to poll.
            user (str): User_id that matches credentials.
            expected_status (int): Expected HTTP status_code, default=200.

        Returns:
            Task object
        """

        tes_task_url = f"{self.tes_url}/tasks/{task_id}?view=FULL"

        logging.info(f"Getting task for {task_id}")

        response = requests.get(
            url=tes_task_url,
            **({"auth": self._auth_provider} if user else {"headers": {}}),
            timeout=60,
        )
        if response.status_code != expected_status:
            raise_for_status_and_print_error(response)

        return response.json()

    def poll_task_until_expected_state(
        self,
        task_id: str,
        user: str,
        expected_final_state: str = "COMPLETE",
        max_retries: int = 60,
        poll_interval: int = 5,
    ) -> str:
        """
        Poll the TES task status until it reaches a final state or exceeds max retries.

        Args:
            task_id (str): ID of the TES task to poll.
            user (str): User_id that matches credentials.
            expected_final_state (str): Final state, which this task is expected to return
                (e.g., {"COMPLETE", "FAILED"}).
            max_retries (int): Maximum number of polling attempts before giving up.
            poll_interval (int): Time in seconds between polling attempts.

        Returns:
            Final task state if completed successfully.
        """

        # TODO: maybe move these higher up for accessing from tests, etc.
        transient_states = {"QUEUED", "INITIALIZING", "RUNNING"}
        final_states = {
            "COMPLETE",
            "FAILED",
            "EXECUTOR_ERROR",
            "CANCELED",
            "SYSTEM_ERROR",
        }
        logging.info(f"Polling task {task_id} until {expected_final_state}")
        for attempt in range(1, max_retries + 1):
            # TODO: might wrap in try-except here.
            task_info = self.get_tes_task(
                task_id=task_id,
                user=user,
                expected_status=200,
            )
            state = task_info.get("state")

            if state == expected_final_state:
                logging.info(f"TES task reached final state '{state}'")
                return task_info

            # TODO: can't use raise_for_status - these are all using task dicts.
            if state in ["SYSTEM_ERROR", "EXECUTOR_ERROR"]:
                logging.error("Task failed")
                break
            if state in final_states:
                message = (
                    f"TES task reached a final state, that is not '{expected_final_state}'."
                    f" Final state: {state}, Response: {task_info}"
                )
                logging.warning(message)
                break
            if state not in transient_states:
                message = f"Unexpected TES task state '{state}' encountered. Response: {task_info}"
                logging.warning(message)

            logging.info(
                f"Attempt {attempt} of {max_retries}: Task state is '{state}',"
                f" retrying after {poll_interval} seconds..."
            )
            if attempt <= max_retries:
                time.sleep(poll_interval)

        raise Exception(
            f"TES task did not reach a final state in time. Last known state: {state}, Response: {task_info}"
        )

    # Get a single page of tasks for user
    def get_tasks_for_page(self, user: str, page=None) -> dict:
        """
        Get a list of tasks for page for user

        Args:
            user (str): User_id that matches credentials.
            page (str): Page token, default=None.

        Returns a page of tasks for user
        {
            "next_page_token": "",
            "tasks": [<TASKS>]
        }
        """

        tes_task_url = f"{self.tes_url}/tasks/?view=MINIMAL"
        if page:
            tes_task_url += f"&page_token={page}"

        response = requests.get(
            url=tes_task_url,
            **({"auth": self._auth_provider} if user else {"headers": {}}),
            timeout=60,
        )
        expected_status = 200
        if response.status_code != expected_status:
            raise_for_status_and_print_error(response)

        return response.json()

    # Get all tasks for user
    def get_all_tasks_for_user(self, user: str) -> dict:
        """
        Get all tasks for user.

        Args:
            user (str): User_id that matches credentials.

        Returns JSON with {"tasks": []}
        """

        all_tasks = []
        next_page_token = None

        while True:
            logging.info(
                f"Calling get_tasks_for_page with page_token = {next_page_token}"
            )
            page_data = self.get_tasks_for_page(user=user, page=next_page_token)
            all_tasks.extend(page_data.get("tasks", []))

            next_page_token = page_data.get("next_page_token")
            if not next_page_token:
                break

        return {"tasks": all_tasks}

    def cancel_tes_task(self, task_id: str, user: str, expected_status=200):
        """
        Cancel task with task_id.

        Args:
            task_id (str): ID of the TES task to poll.
            user (str): User_id that matches credentials.
            expected_status (int): Expected HTTP status_code, default=200.

        Returns:
            Task object
        """

        tes_task_url = f"{self.tes_url}/tasks/{task_id}:cancel"

        logging.info(f"Cancelling task for {task_id}")

        response = requests.post(
            url=tes_task_url,
            **({"auth": self._auth_provider} if user else {"headers": {}}),
            timeout=60,
        )
        if response.status_code != expected_status:
            raise_for_status_and_print_error(response)

        return response.json()

    # Perform S3 action
    # https://github.com/uc-cdis/gen3-code-vigil/blob/20bb83b/gen3-integration-tests/services/gen3workflow.py#L152

    # Delete task
