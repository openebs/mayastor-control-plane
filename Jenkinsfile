#!/usr/bin/env groovy

// Searches previous builds to find first non aborted one
def getLastNonAbortedBuild(build) {
  if (build == null) {
    return null;
  }

  if(build.result.toString().equals("ABORTED")) {
    return getLastNonAbortedBuild(build.getPreviousBuild());
  } else {
    return build;
  }
}

// Send out a slack message if branch got broken or has recovered
def notifySlackUponStateChange(build) {
  def cur = build.getResult()
  def prev = getLastNonAbortedBuild(build.getPreviousBuild())?.getResult()
  if (cur != prev) {
    if (cur == 'SUCCESS') {
      slackSend(
        channel: '#mayastor-control-plane',
        color: 'normal',
        message: "Branch ${env.BRANCH_NAME} has been fixed :beers: (<${env.BUILD_URL}|Open>)"
      )
    } else if (prev == 'SUCCESS') {
      slackSend(
        channel: '#mayastor-control-plane',
        color: 'danger',
        message: "Branch ${env.BRANCH_NAME} is broken :face_with_raised_eyebrow: (<${env.BUILD_URL}|Open>)"
      )
    }
  }
}

def mainBranches() {
    return BRANCH_NAME == "develop" || BRANCH_NAME.startsWith("release/");
}

// TODO: Use multiple choices
run_linter = true
rust_test = true
bdd_test = true
run_tests = params.run_tests
build_images = params.build_images

// Will skip steps for cases when we don't want to build
if (currentBuild.getBuildCauses('jenkins.branch.BranchIndexingCause') && mainBranches()) {
    print "INFO: Branch Indexing, skip tests and push the new images."
    run_tests = false
    build_images = true
}

// Only schedule regular builds on main branches, so we don't need to guard against it
String cron_schedule = mainBranches() ? "0 2 * * *" : ""

pipeline {
  agent none
  options {
    timeout(time: 1, unit: 'HOURS')
  }
  parameters {
    booleanParam(defaultValue: false, name: 'build_images')
    booleanParam(defaultValue: true, name: 'run_tests')
  }
  triggers {
    cron(cron_schedule)
  }

  stages {
    stage('init') {
      agent { label 'nixos-mayastor' }
      steps {
        step([
          $class: 'GitHubSetCommitStatusBuilder',
          contextSource: [
            $class: 'ManuallyEnteredCommitContextSource',
            context: 'continuous-integration/jenkins/branch'
          ],
          statusMessage: [ content: 'Pipeline started' ]
        ])
      }
    }
    stage('linter') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        not {
          anyOf {
            branch 'master'
            branch 'release/*'
            expression { run_linter == false }
          }
        }
      }
      steps {
        sh 'printenv'
        sh 'nix-shell --run "./scripts/rust/generate-openapi-bindings.sh"'
        sh 'nix-shell --run "cargo fmt --all -- --check"'
        sh 'nix-shell --run "cargo clippy --all-targets -- -D warnings"'
        sh 'nix-shell --run "black tests/bdd"'
      }
    }
    stage('test') {
      when {
        beforeAgent true
        not {
          anyOf {
            branch 'master'
          }
        }
        expression { run_tests == true }
      }
      parallel {
        stage('rust unit tests') {
          when{
            expression { rust_test == true }
          }
          agent { label 'nixos-mayastor' }
          steps {
            withCredentials([usernamePassword(credentialsId: 'OPENEBS_DOCKERHUB', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
              sh 'echo $PASSWORD | docker login -u $USERNAME --password-stdin'
            }
            sh 'printenv'
            sh 'nix-shell --run "./scripts/rust/test.sh"'
          }
          post {
            always {
              // in case of abnormal termination of any nvmf test
              sh 'sudo nvme disconnect-all'
            }
          }
        }
        stage('BDD tests') {
          when{
            expression { bdd_test == true }
          }
          agent { label 'nixos-mayastor' }
          steps {
            withCredentials([usernamePassword(credentialsId: 'OPENEBS_DOCKERHUB', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
              sh 'echo $PASSWORD | docker login -u $USERNAME --password-stdin'
            }
            sh 'printenv'
            sh 'nix-shell --run "cargo test -p deployer-cluster"'
            sh 'nix-shell --run "./scripts/python/test.sh"'
          }
          post {
            always {
              // in case of abnormal termination of any nvmf test
              sh 'sudo nvme disconnect-all'
            }
          }
        }
        stage('image build test') {
          when {
            branch 'staging'
          }
          agent { label 'nixos-mayastor' }
          steps {
            sh './scripts/nix/git-submodule-init.sh --force'
            sh './scripts/release.sh --skip-publish --debug --build-bins'
          }
        }
      }// parallel stages block
    }// end of test stage
    stage('build and push images') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        anyOf {
          expression { build_images == true }
          anyOf {
            branch 'master'
            branch 'release/*'
            branch 'develop'
          }
        }
      }
      steps {
        withCredentials([usernamePassword(credentialsId: 'OPENEBS_DOCKERHUB', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            sh 'echo $PASSWORD | docker login -u $USERNAME --password-stdin'
        }
        sh 'printenv'
        sh './scripts/nix/git-submodule-init.sh --force'
        sh './scripts/release.sh'
      }
      post {
        always {
          sh 'docker image prune --all --force'
        }
      }
    }
  }

  // The main motivation for post block is that if all stages were skipped
  // (which happens when running cron job and branch != develop) then we don't
  // want to set commit status in github (jenkins will implicitly set it to
  // success).
  post {
    always {
      node(null) {
        script {
          // If no tests were run then we should neither be updating commit
          // status in github nor send any slack messages
          if (currentBuild.result != null) {
            step([
              $class            : 'GitHubCommitStatusSetter',
              errorHandlers     : [[$class: "ChangingBuildStatusErrorHandler", result: "UNSTABLE"]],
              contextSource     : [
                $class : 'ManuallyEnteredCommitContextSource',
                context: 'continuous-integration/jenkins/branch'
              ],
              statusResultSource: [
                $class : 'ConditionalStatusResultSource',
                results: [
                  [$class: 'AnyBuildResult', message: 'Pipeline result', state: currentBuild.getResult()]
                ]
              ]
            ])
            if (mainBranches()) {
              notifySlackUponStateChange(currentBuild)
            }
          }
        }
      }
    }
  }
}
