"""
Contains class for interacting with Gen3's Job Dispatching Service(s).
"""
import aiohttp
import asyncio
import backoff
import json
import requests
import urllib.parse
from cdislogging import get_logger

import sys
import time

from gen3.utils import (
    append_query_params,
    DEFAULT_BACKOFF_SETTINGS,
    raise_for_status_and_print_error,
)

# sower's "action" mapping to the relevant job
INGEST_METADATA_JOB = "ingest-metadata-manifest"
DBGAP_METADATA_JOB = "get-dbgap-metadata"
INDEX_MANIFEST_JOB = "index-object-manifest"
DOWNLOAD_MANIFEST_JOB = "download-indexd-manifest"
MERGE_MANIFEST_JOB = "merge-manifests"

logging = get_logger("__name__")


class Gen3Jobs:
    """
    A class for interacting with the Gen3's Job Dispatching Service(s).

    Examples:
        This generates the Gen3Jobs class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... jobs = Gen3Jobs(auth)
    """

    def __init__(self, endpoint=None, auth_provider=None, service_location="job"):
        """
        Initialization for instance of the class to setup basic endpoint info.

        Args:
            auth_provider (Gen3Auth, optional): Gen3Auth class to handle passing your
                token, required for admin endpoints
            service_location (str, optional): deployment location relative to the
                endpoint provided
        """
        # auth_provider legacy interface required endpoint as 1st arg
        auth_provider = auth_provider or endpoint
        endpoint = auth_provider.endpoint.strip("/")
        # if running locally, mds is deployed by itself without a location relative
        # to the commons
        if "http://localhost" in endpoint:
            service_location = ""

        if not endpoint.endswith(service_location):
            endpoint += "/" + service_location

        self.endpoint = endpoint.rstrip("/")
        self._auth_provider = auth_provider

    async def async_run_job_and_wait(self, job_name, job_input, _ssl=None, **kwargs):
        """
        Asynchronous function to create a job, wait for output, and return. Will
        sleep in a linear delay until the job is done, starting with 1 second.

        Args:
            _ssl (None, optional): whether or not to use ssl
            job_name (str): name for the job, can use globals in this file
            job_input (Dict): dictionary of input for the job

        Returns:
            Dict: Response from the endpoint
        """
        job_create_response = await self.async_create_job(job_name, job_input)

        status = {"status": "Running"}
        sleep_time = 3
        while status.get("status") == "Running":
            logging.info(f"job still running, waiting for {sleep_time} seconds...")
            time.sleep(sleep_time)
            sleep_time *= 1.5
            status = await self.async_get_status(job_create_response.get("uid"))
            logging.info(f"{status}")

        logging.info(f"Job is finished!")

        if status.get("status") != "Completed":
            raise Exception(f"Job status not complete: {status.get('status')}.")

        response = await self.async_get_output(job_create_response.get("uid"))
        return response

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
            raise_for_status_and_print_error(response)
        except Exception as exc:
            logging.error(exc)
            return False

        return response.text == "Healthy"

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_version(self):
        """
        Return the version

        Returns:
            str: the version
        """
        response = requests.get(self.endpoint + "/_version", auth=self._auth_provider)
        raise_for_status_and_print_error(response)
        return response.json().get("version")

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def list_jobs(self):
        """
        List all jobs
        """
        response = requests.get(self.endpoint + "/list", auth=self._auth_provider)
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_job(self, job_name, job_input):
        """
        Create a job with given name and input

        Args:
            job_name (str): name for the job, can use globals in this file
            job_input (Dict): dictionary of input for the job

        Returns:
            Dict: Response from the endpoint
        """
        data = {"action": job_name, "input": job_input}
        response = requests.post(
            self.endpoint + "/dispatch", json=data, auth=self._auth_provider
        )
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_create_job(self, job_name, job_input, _ssl=None, **kwargs):
        async with aiohttp.ClientSession() as session:
            url = self.endpoint + f"/dispatch"
            url_with_params = append_query_params(url, **kwargs)

            data = json.dumps({"action": job_name, "input": job_input})

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            async with session.post(
                url_with_params, data=data, headers=headers, ssl=_ssl
            ) as response:
                raise_for_status_and_print_error(response)
                response = await response.json(content_type=None)
                return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_status(self, job_id):
        """
        Get the status of a previously created job
        """
        response = requests.get(
            self.endpoint + f"/status?UID={job_id}", auth=self._auth_provider
        )
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get_status(self, job_id, _ssl=None, **kwargs):
        async with aiohttp.ClientSession() as session:
            url = self.endpoint + f"/status?UID={job_id}"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            async with session.get(
                url_with_params, headers=headers, ssl=_ssl
            ) as response:
                raise_for_status_and_print_error(response)
                response = await response.json(content_type=None)
                return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_output(self, job_id):
        """
        Get the output of a previously completed job
        """
        response = requests.get(
            self.endpoint + f"/output?UID={job_id}", auth=self._auth_provider
        )
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get_output(self, job_id, _ssl=None, **kwargs):
        async with aiohttp.ClientSession() as session:
            url = self.endpoint + f"/output?UID={job_id}"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            async with session.get(
                url_with_params, headers=headers, ssl=_ssl
            ) as response:
                raise_for_status_and_print_error(response)
                response = await response.json(content_type=None)
                return response
