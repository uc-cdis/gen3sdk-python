#!/bin/bash
#
# Run tests/smokeTest.sh in a Jenkins environment
#

source "${GEN3_HOME}/gen3/lib/utils.sh"
gen3_load "gen3/gen3setup"

set -e
set -v

cd "$( dirname "${BASH_SOURCE[0]}" )"
cd ..
export PATH="/var/jenkins_home/.local/bin:$PATH"
export GEN3_API_KEY="accesstoken:///$(gen3 api access-token cdis.autotest@gmail.com)"

poetry self update
poetry config virtualenvs.path "${WORKSPACE}/pysdkvirtenv" --local
poetry env use python3
poetry install -vv
poetry run bash tests/smokeTest.sh test-all
