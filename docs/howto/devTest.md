## Dev-Test

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
python -m pytest
```

There are various ways to select a subset of python unit-tests - see: https://stackoverflow.com/questions/36456920/is-there-a-way-to-specify-which-pytest-tests-to-run-from-a-file

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
