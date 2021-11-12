#!groovy

// See 'Loading libraries dynamically' here: https://jenkins.io/doc/book/pipeline/shared-libraries/
library 'cdis-jenkins-lib@fix/python'

node {
  def AVAILABLE_NAMESPACES = ['jenkins-blood', 'jenkins-brain', 'jenkins-niaid', 'jenkins-dcp', 'jenkins-genomel']
  List<String> namespaces = []
  List<String> listOfSelectedTests = []
  doNotRunTests = false
  kubectlNamespace = null
  kubeLocks = []
  testedEnv = "" // for manifest pipeline
  pipeConfig = pipelineHelper.setupConfig([:])
  pipelineHelper.cancelPreviousRunningBuilds()
  prLabels = githubHelper.fetchLabels()

  try {
    stage('CleanWorkspace') {
      cleanWs()
    }
    stage('FetchCode'){
      gitHelper.fetchAllRepos(pipeConfig['currentRepoName'])
      sh 'ls && test -f ./tmpGitClone/tests/jenkinsSmoke.sh'
    }
    stage('CheckPRLabels') {
      // giving a chance for auto-label gh actions to catch up
      // sleep(10)
      for(label in prLabels) {
        println(label['name']);
        switch(label['name']) {
          case "doc-only":
            println('Skip tests if git diff matches expected criteria')
            doNotRunTests = docOnlyHelper.checkTestSkippingCriteria()
            break
          case "not-ready-for-ci":
            currentBuild.result = 'ABORTED'
            error('This PR is not ready for CI yet, aborting...')
            break
          case AVAILABLE_NAMESPACES:
            println('found this namespace label! ' + label['name']);
            namespaces.add(label['name'])
            break
          case "qaplanetv2":
            println('This PR check will run in a qaplanetv2 environment! ');
            namespaces.add('ci-env-1')
            break
          default:
            println('no-effect label')
            break
        }
      }
      // If none of the jenkins envs. have been selected pick one at random
      if (namespaces.isEmpty()) {
        println('populating namespaces with list of available namespaces...')
        namespaces = AVAILABLE_NAMESPACES
      }
    }
    stage('SelectNamespace') {
     if(!doNotRunTests) {
        (kubectlNamespace, lock) = kubeHelper.selectAndLockNamespace(pipeConfig['UID'], namespaces)
        kubeLocks << lock
      } else {
        Utils.markStageSkippedForConditional(STAGE_NAME)
      }
    }
    stage('K8sReset') {
      if(!doNotRunTests) {
        // adding the reset-lock lock in case reset fails before unlocking
        kubeLocks << kubeHelper.newKubeLock(kubectlNamespace, "gen3-reset", "reset-lock")
        kubeHelper.reset(kubectlNamespace)
      } else {
        Utils.markStageSkippedForConditional(STAGE_NAME)
      }
    }
    stage('VerifyClusterHealth') {
      if(!doNotRunTests) {
        kubeHelper.waitForPods(kubectlNamespace)
        testHelper.checkPodHealth(kubectlNamespace, "")
      } else {
        Utils.markStageSkippedForConditional(STAGE_NAME)
      }
    }
    stage('SmokeTest') {
      if(!doNotRunTests) {
        kubeHelper.kube(kubectlNamespace, {
          sh 'bash ./tmpGitClone/tests/jenkinsSmoke.sh'
        })
      } else {
        Utils.markStageSkippedForConditional(STAGE_NAME)
      }
    }
  } catch (e) {
    pipelineHelper.handleError(e)
  }
  finally {
    stage('Post') {
      kubeHelper.teardown(kubeLocks)
      // tear down network policies deployed by the tests
      kubeHelper.kube(kubectlNamespace, {
          sh(script: 'kubectl --namespace="' + kubectlNamespace + '" delete networkpolicies --all', returnStatus: true);
        });
      pipelineHelper.teardown(currentBuild.result)
    }
  }
}
