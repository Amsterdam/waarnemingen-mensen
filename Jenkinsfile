#!groovy

def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block();
    }
    catch (Throwable t) {
//         NOTE: This slack message has been disabled for now, until automatic builds are implemented
//         slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#waarnemingen-deployments', color: 'danger'
        throw t;
    }
    finally {
        if (tearDown) {
            tearDown();
        }
    }
}


node {
    stage("Checkout") {
        checkout scm
    }

    stage('Test') {
        tryStep "test", {
            sh "deploy/test/test.sh"
        }
    }

    stage("Build dockers") {
        tryStep "build", {
            docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                def api = docker.build("datapunt/waarnemingen-mensen:${env.BUILD_NUMBER}", "--build-arg AUTHORIZATION_TOKEN=dev --build-arg GET_AUTHORIZATION_TOKEN=get-auth-token --build-arg SECRET_KEY=dev api")
                api.push()
                api.push("acceptance")
            }
        }
    }

    stage("Locust load test") {
        sh("./api/deploy/docker-locust-load-test.sh")
    }
}

String BRANCH = "${env.BRANCH_NAME}"

if (BRANCH == "master") {

    node {
        stage('Push acceptance image') {
            tryStep "image tagging", {
               docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                    def image = docker.image("datapunt/waarnemingen-mensen:${env.BUILD_NUMBER}")
                    image.pull()
                    image.push("acceptance")
                }
            }
        }
    }

    node {
        stage("Deploy to ACC") {
            tryStep "deployment", {
                build job: 'Subtask_Openstack_Playbook',
                parameters: [
                    [$class: 'StringParameterValue', name: 'INVENTORY', value: 'acceptance'],
                    [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy-waarnemingen-mensen.yml'],
                ]
            }
        }
    }

    stage('Waiting for approval') {
        input "Deploy to Production?"
    }

    node {
        stage('Push production image') {
            tryStep "image tagging", {
                docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                    def api = docker.image("datapunt/waarnemingen-mensen:${env.BUILD_NUMBER}")
                    api.push("production")
                    api.push("latest")
                }
            }
        }
    }

    node {
        stage("Deploy") {
            tryStep "deployment", {
                build job: 'Subtask_Openstack_Playbook',
                parameters: [
                        [$class: 'StringParameterValue', name: 'INVENTORY', value: 'production'],
                        [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy-waarnemingen-mensen.yml'],
                ]
            }
        }
    }
}
