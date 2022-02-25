#!/bin/bash
#
# Run tests/smokeTest.sh in a Jenkins environment
#
# TODO These things should probably be migrated to our integration tests suite and/or
#      just converted to unit tests with WSS mocked. This mixture of bash testing in our
#      Python SDK is confusing

source "${GEN3_HOME}/gen3/lib/utils.sh"
gen3_load "gen3/gen3setup"

set -e
set -v

cd "$( dirname "${BASH_SOURCE[0]}" )"
cd ..
export PATH="/var/jenkins_home/.local/bin:$PATH"
export GEN3_API_KEY="accesstoken:///$(gen3 api access-token cdis.autotest@gmail.com)"

poetry config virtualenvs.path "${WORKSPACE}/pysdkvirtenv" --local
pip install virtualenv==20.7.2
poetry env use python3
poetry run python --version
poetry run python -m pip install --upgrade pip
poetry run pip install poetry
poetry install -vvv
poetry run bash tests/smokeTest.sh test-all
