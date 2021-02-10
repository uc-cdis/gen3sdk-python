#!/bin/bash

XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp}"

#-- lib ------

pipecheck() {
  local it
  for it in "$@"; do
    if [[ "$it" -ne 0 ]]; then
      return 1
    fi
  done
  return 0
}


# 
# Adapted from cloud-automation/gen3/lib/shunit.sh
#
because() {
  local exitCode
  if [[ $# -lt 2 ]]; then
    echo -e "\n\x1B[31mAssertion failed:\x1B[39m because takes 2 arguments: code message"
    exit 1
  fi
  let exitCode=$1
  let SHUNIT_ASSERT_COUNT+=1
  if [[ $exitCode != 0 ]]; then
    let SHUNIT_ASSERT_FAIL+=1
    echo -e "\n\x1B[31mAssertion failed:\x1B[39m $2"
    exit 1
  fi
  echo -e "\x1B[32mAssertion passed:\x1B[39m $2"
  return 0
}

test-wss() {
  gen3 wss ls | jq -e -r .; pipecheck "${PIPESTATUS[@]}"; because $? "wss ls seems to work"
  local testFile="sdkSmokeTest$(date +%Y%m%d).txt"
  (cat - <<EOM
bla bla bla
frickjack frickjack frickjack
EOM
  ) > "$XDG_RUNTIME_DIR/$testFile"
  gen3 wss upload-url "ws:///@user/$testFile" | jq -e -r .Data > /dev/null; pipecheck "${PIPESTATUS[@]}"; because $? "wss upload-url should work"
  gen3 wss download-url "ws:///@user/$testFile" | jq -e -r .Data > /dev/null; pipecheck "${PIPESTATUS[@]}"; because $? "wss download-url should work"
  gen3 wss cp "$XDG_RUNTIME_DIR/$testFile" "ws:///@user/$testFile"; because $? "wss cp $testFile should work"
  gen3 wss cp "ws:///@user/$testFile" "$XDG_RUNTIME_DIR/download_$testFile" && [[ -f "$XDG_RUNTIME_DIR/download_$testFile" ]]; because $? "wss cp $testFile download should work"
  gen3 wss ls | jq --arg key "$testFile" -e -r '.Data.Objects | map(select(.WorkspaceKey==$key)) | .[]'; pipecheck "${PIPESTATUS[@]}"; because $? "wss ls finds $testFile"
  gen3 wss rm "ws:///@user/$testFile" | jq -e -r .; pipecheck "${PIPESTATUS[@]}"; because $? "wss rm should cleanup $testFile"
  gen3 wss ls | (! jq --arg key "$testFile" -e -r '.Data.Objects | map(select(.WorkspaceKey==$key)) | .[]'); because $? "wss ls should not find removed $testFile"
}

test-auth() {
  gen3 auth curl /user/user; because $? "auth curl /user/user works"
  gen3 auth access-token | gen3 auth token-decode - | jq -e -r .; pipecheck "${PIPESTATUS[@]}"; because $? "auth access-token works"
}

test-all() {
  test-auth
  test-wss
}


help() {
    cat - <<EOM
Run a series of non-destructive intractive tests against
the gen3sdk cli.  Assumes either GEN3_API_KEY environment
is set, or running in workspace environment, or
~/.gen3/credentials.json exists.

Use: bash smokeTest.sh run <auth flags>

Ex:
  bash smokeTest.sh run

Commands:
* help
* test-all
* test-auth
* test-wss

EOM
}

#-- main ----

if [[ $# -lt 1 ]]; then
  help
  exit 0
fi

"$@"
