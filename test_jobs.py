import sys
import logging
import asyncio

from gen3.index import Gen3Index
from gen3.auth import Gen3Auth
from gen3.jobs import Gen3Jobs, DBGAP_METADATA_JOB, INGEST_METADATA_JOB

# Gen3 Commons URL
# COMMONS = "https://caninedc.org/"
COMMONS = "https://avantol.planx-pla.net/"

# An API Key downloaded from the above commons' "Profile" page
API_KEY_FILEPATH = "credentials.json"

logging.basicConfig(filename="output.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def metadata_ingest():
    auth = Gen3Auth(COMMONS, refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(COMMONS, auth_provider=auth)

    job_input = {
        "URL": "https://cdistest-public-test-bucket.s3.amazonaws.com/04_28_20_21_55_13_merged_metadata_manifest.tsv",
        "metadata_source": "dbgaptest",
    }

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    job_output = loop.run_until_complete(
        jobs.async_run_job_and_wait(job_name=INGEST_METADATA_JOB, job_input=job_input)
    )
    print(job_output)


def main():
    auth = Gen3Auth(COMMONS, refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(COMMONS, auth_provider=auth)

    job_input = {
        "phsid_list": "phs000920 phs000921 phs000946 phs000951 phs000954 phs000956 phs000964 phs000972 phs000974 phs000988 phs000993 phs000997 phs001024 phs001032 phs001040 phs001062 phs001143 phs001189 phs001207 phs001211 phs001215 phs001217 phs001218 phs001237 phs001293 phs001345 phs001359 phs001368 phs001387 phs001402 phs001412 phs001416",
        "indexing_manifest_url": "https://cdistest-public-test-bucket.s3.amazonaws.com/release_manifest_no_dbgap_no_sample.csv",
        "manifests_mapping_config": {
            "guid_column_name": "guid",
            "row_column_name": "submitted_sample_id",
            "indexing_manifest_column_name": "gcp_uri",
        },
        "partial_match_or_exact_match": "partial_match",
    }

    # job_input = {
    #     "phsid_list": "phs000956 phs000920",
    #     "indexing_manifest_url": "https://gist.githubusercontent.com/Avantol13/3343f914e6f6c639900b76d577737ca3/raw/40bfb20a4ddc3716bd1481ac84aad1d68b05a76f/public_indexing_manifest.csv",
    #     "manifests_mapping_config": {
    #         "guid_column_name": "guid",
    #         "row_column_name": "submitted_sample_id",
    #         "indexing_manifest_column_name": "urls",
    #     },
    #     "partial_match_or_exact_match": "partial_match",
    # }

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    job_output = loop.run_until_complete(
        jobs.async_run_job_and_wait(job_name=DBGAP_METADATA_JOB, job_input=job_input)
    )
    print(job_output)


def example_non_async_run_job():
    auth = Gen3Auth(COMMONS, refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(COMMONS, auth_provider=auth)

    is_healthy = jobs.is_healthy()
    print(is_healthy)

    version = jobs.get_version()
    print(version)

    job_input = {
        "phsid_list": "phs000956 phs000920",
        "indexing_manifest_url": "https://gist.githubusercontent.com/Avantol13/3343f914e6f6c639900b76d577737ca3/raw/40bfb20a4ddc3716bd1481ac84aad1d68b05a76f/public_indexing_manifest.csv",
        "manifests_mapping_config": {
            "guid_column_name": "guid",
            "row_column_name": "submitted_sample_id",
            "indexing_manifest_column_name": "urls",
        },
        "partial_match_or_exact_match": "partial_match",
    }

    create_job = jobs.create_job(job_name=DBGAP_METADATA_JOB, job_input=job_input)
    print(create_job)

    status = "Running"
    while status == "Running":
        status = jobs.get_status(create_job.get("uid")).get("status")
        print(status)

    get_output = jobs.get_output(create_job.get("uid"))
    print(get_output)


if __name__ == "__main__":
    # metadata_ingest()
    example_non_async_run_job()
