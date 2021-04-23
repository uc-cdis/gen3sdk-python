## Dev-Test

### Set up Python Virtual Environment

You can set up a Python development environment with a virtual environment:

```bash
python3 -m venv py3
```

Make sure that you have the virtual environment activated:

```bash
. py3/bin/activate
```

### Install poetry

To use the latest code in this repo (or to develop new features) you can clone this repo, install `poetry`:

```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```

and then use `poetry` to install this package:

```
poetry install -vv
```


### Setup and run tests

Local development like this:

```
poetry shell
poetry install -vv
python3 -m pytest
```

There are various ways to select a subset of python unit-tests - see: https://stackoverflow.com/questions/36456920/is-there-a-way-to-specify-which-pytest-tests-to-run-from-a-file

### Manual Testing

You can also set up credentials to submit data to the graph in your data commons.  This assumes that you can get API access by downloading your [credentials.json](https://gen3.org/resources/user/using-api/#credentials-to-send-api-requests).

> Make sure that your python virtual environment and dependencies are updated.  Also, check that your credentials have appropriate permissions to make the service calls too.
```python
COMMONS_URL = "https://mycommons.azurefd.net"
PROGRAM_NAME = "MyProgram"
PROJECT_NAME = "MyProject"
CREDENTIALS_FILE_PATH = "credentials.json"
gen3_node_json = {
  "projects": {"code": PROJECT_NAME},
  "type": "core_metadata_collection",
  "submitter_id": "core_metadata_collection_myid123456",
}
auth = Gen3Auth(endpoint=COMMONS_URL, refresh_file=CREDENTIALS_FILE_PATH)
sheepdog_client = Gen3Submission(COMMONS_URL, auth)
json_result = sheepdog_client.submit_record(PROGRAM_NAME, PROJECT_NAME, gen3_node_json)
```

### Smoke test

Most of the SDK functionality requires a backend Gen3 environment
which is not available at unit test time.  The [smoke test](../../tests/smokeTest.sh) can be run as part of a CICD test suite
with access to a gen3 environment for QA and testing.

### CLI

If the `gen3` cli is not in your path, or is overwritten by a shell function, then you can still invoke the cli:

```
python -m gen3.cli --help
or
poetry run gen3 --help
```
