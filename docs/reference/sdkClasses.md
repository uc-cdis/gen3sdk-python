## Available Classes

TOC
- [Gen3Auth](#gen3auth)
- [Gen3Index](#gen3index)
- [Gen3Submission](#gen3submission)
- [Gen3Jobs](#gen3jobs)
- Gen3File
- [Gen3Query](#gen3query)
- Gen3Metadata
- Gen3WsStorage

### Gen3Auth

This contains an auth wrapper for supporting JWT based authentication with `requests`. The access token is generated from the refresh token and is regenerated on expiration.

By default - the `Gen3Auth` constructor looks for an api key
in `~/.gen3/credentials.json`.  You may override that path
via the `GEN3_API_KEY` environment varialbe, or by passing a
`refresh_file` parameter.

When working in a Gen3 Workspace, all parameters are optional and the `Gen3Auth` instance should be initialized as follows:

```
auth = Gen3Auth()
```

See [detailed Gen3Auth documentation](https://uc-cdis.github.io/gen3sdk-python/_build/html/auth.html) for more details.

### Gen3Index

This is the client for interacting with the Indexd service for GUID brokering and resolution.

### Gen3Submission

This is the client for interacting with the Gen3 submission service including GraphQL queries.

### Gen3Query

This is the client for interacting with the Gen3 ElasticSearch query service.

### Gen3Jobs

This is client for interacting with Gen3's job dispatching service. A complex example script which calls a job that combines dbGaP data with indexed file objects can be seen below:


```python
import sys
import logging
import asyncio

from gen3.index import Gen3Index
from gen3.auth import Gen3Auth
from gen3.jobs import Gen3Jobs, DBGAP_METADATA_JOB, INGEST_METADATA_JOB
from gen3.utils import get_or_create_event_loop_for_thread

# An API Key downloaded from the above commons' "Profile" page
API_KEY_FILEPATH = "credentials.json"

logging.basicConfig(filename="output.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def metadata_ingest():
    auth = Gen3Auth(refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(auth_provider=auth)

    job_input = {
        "URL": "https://cdistest-public-test-bucket.s3.amazonaws.com/04_28_20_21_55_13_merged_metadata_manifest.tsv",
        "metadata_source": "dbgaptest",
    }

    loop = get_or_create_event_loop_for_thread()

    job_output = loop.run_until_complete(
        jobs.async_run_job_and_wait(job_name=INGEST_METADATA_JOB, job_input=job_input)
    )
    print(job_output)


def main():
    auth = Gen3Auth(refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(auth_provider=auth)

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

    loop = get_or_create_event_loop_for_thread()

    job_output = loop.run_until_complete(
        jobs.async_run_job_and_wait(job_name=DBGAP_METADATA_JOB, job_input=job_input)
    )
    print(job_output)


if __name__ == "__main__":
    metadata_ingest()

```

```python
import sys
import logging
import asyncio

from gen3.auth import Gen3Auth
from gen3.jobs import Gen3Jobs, DBGAP_METADATA_JOB
from gen3.utils import get_or_create_event_loop_for_thread

# An API Key downloaded from the above commons' "Profile" page
API_KEY_FILEPATH = "credentials.json"

logging.basicConfig(filename="output.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# NOTE: The indexing_manifest_url must exist and be publically accessible
JOB_INPUT = {
    "phsid_list": "phs000956 phs000920",
    "indexing_manifest_url": "https://example.com/public_indexing_manifest.csv",
    "manifests_mapping_config": {
        "guid_column_name": "guid",
        "row_column_name": "submitted_sample_id",
        "indexing_manifest_column_name": "urls",
    },
    "partial_match_or_exact_match": "partial_match",
}


def example_async_run_job():
    auth = Gen3Auth(refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(auth_provider=auth)

    loop = get_or_create_event_loop_for_thread()

    job_output = loop.run_until_complete(
        jobs.async_run_job_and_wait(job_name=DBGAP_METADATA_JOB, job_input=JOB_INPUT)
    )
    print(job_output)

def example_non_async_run_job():
    auth = Gen3Auth(refresh_file=API_KEY_FILEPATH)
    jobs = Gen3Jobs(auth_provider=auth)

    is_healthy = jobs.is_healthy()
    print(is_healthy)

    version = jobs.get_version()
    print(version)

    create_job = jobs.create_job(job_name=DBGAP_METADATA_JOB, job_input=JOB_INPUT)
    print(create_job)

    status = "Running"
    while status == "Running":
        status = jobs.get_status(create_job.get("uid")).get("status")
        print(status)

    get_output = jobs.get_output(create_job.get("uid"))
    print(get_output)


if __name__ == "__main__":
    example_async_run_job()
```

