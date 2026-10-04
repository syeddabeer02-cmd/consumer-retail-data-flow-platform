pipeline {
    agent {
        docker {
            image 'python:3.12-bookworm'
            args '--user 0:0'
        }
    }
    options {
        timeout(time: 20, unit: 'MINUTES')
        disableConcurrentBuilds()
        skipDefaultCheckout(true)
    }
    environment {
        SPARK_LOCAL_IP = '127.0.0.1'
    }
    stages {
        stage('Checkout') {
            steps { checkout scm }
        }
        stage('Dependencies') {
            steps {
                sh 'apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps'
                sh "python -m pip install -e '.[spark,dashboard,dev]'"
            }
        }
        stage('Lint and tests') {
            steps {
                sh 'python -m ruff check src tests dags databricks scripts'
                sh 'python -m pytest -q --junitxml=test-results.xml'
            }
        }
        stage('Pipeline and replay') {
            steps {
                sh 'python -m retail_flow.cli demo --root data/jenkins --date 2026-10-03 --rows 20'
                sh 'python scripts/verify_scenarios.py --root data/jenkins'
            }
        }
    }
    post {
        always {
            junit allowEmptyResults: true, testResults: 'test-results.xml'
            archiveArtifacts allowEmptyArchive: true, artifacts: 'data/jenkins/published/*.json'
        }
    }
}
