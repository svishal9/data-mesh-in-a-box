#!/bin/bash

set -euo pipefail


function trace() {
    {
        local tracing
        [[ "$-" = *"x"* ]] && tracing=true || tracing=false
        set +x
    } 2>/dev/null
    if [ "$tracing" != true ]; then
        # Bash's own trace mode is off, so explicitely write the message.
        echo "$@" >&2
    else
        # Restore trace
        set -x
    fi
}


function contains () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}


# Parse arguments.
operations=()
subcommand_opts=()
while true; do
    case "${1:-}" in
    setup)
        operations+=( setup )
        shift
        ;;
    lint|linting)
        operations+=( linting )
        shift
        ;;
    tests)
        operations+=( tests )
        shift
        ;;
    spark-k8s-tests)
        operations+=( spark-k8s-tests )
        shift
        ;;
    get-k8s-spark-test-logs)
            operations+=( get-k8s-spark-test-logs )
            shift
            ;;
    build-flyway-docker)
        operations+=( build-flyway-docker )
        shift
        ;;
    start-jenkins)
        operations+=( start-jenkins )
        shift
        ;;
    start-airflow)
      operations+=( start-airflow )
      shift
      ;;
    stop-airflow)
          operations+=( stop-airflow )
          shift
          ;;
    airflow)
              operations+=( airflow )
              shift
              ;;
    start-postgres)
        operations+=( start-postgres )
        shift
        ;;
    stop-postgres)
      operations+=( stop-postgres )
      shift
      ;;
    connect-to-local-postgres)
        operations+=( connect-to-local-postgres )
        shift
        ;;
    run-flyway)
        operations+=( run-flyway )
        shift
        ;;
    run)
        operations+=( run )
        shift
        ;;
    --)
        shift
        break
        ;;
    -h|--help)
        operations+=( usage )
        shift
        ;;
    *)
        break
        ;;
    esac
done
if [ "${#operations[@]}" -eq 0 ]; then
    operations=( usage )
fi
if [ "$#" -gt 0 ]; then
    subcommand_opts=( "$@" )
fi


function usage() {
    trace "$0 <command> [--] [options ...]"
    trace "Commands:"
    trace "    linting   Static analysis, code style, etc."
    trace "    precommit Run sensible checks before committing"
    trace "    run       Run the application"
    trace "    setup     Install dependencies"
    trace "    tests     Run tests"
    trace "    spark-k8s-tests     Run spark k8s tests"
    trace "    get-k8s-spark-test-logs     Get spark k8s tests logs"
    trace "    start-jenkins     Start Jenkins"
    trace "    start-airflow    Start Airflow"
    trace "    stop-airflow     Stop Airflow"
    trace "    airflow     Airflow CLI"
    trace "    start-postgres     Start Postgres"
    trace "    stop-postgres     Stop Postgres"
    trace "    connect-to-local-postgres     Connect to local Postgres"
    trace "    build-flyway-docker     Build Flyway Docker"
    trace "    run       Run flyway migrate"
    trace "Options are passed through to the sub-command."
}


function setup() {
    trace "Setting up"
    ./scripts/setup.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function linting() {
    trace "Linting"
    ./scripts/linting.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function tests() {
    trace "Running tests"
    ./scripts/run_tests.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function spark-k8s-tests() {
    trace "Running tests in Kubernetes container"
    ./scripts/spark-k8s-tests.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}

function get-k8s-spark-test-logs() {
    trace "Get logs for Kubernetes spark test container"
    ./scripts/spark-k8s-tests-logs.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function build-flyway-docker() {
    trace "Build Flyway Docker"
    ./scripts/build-flyway-docker.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}

function start-airflow() {
    trace "Start Airflow docker container"
    ./scripts/start-airflow.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}

function stop-airflow() {
    trace "Start Jenkins docker container"
    ./scripts/stop-airflow.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}

function airflow() {
    trace "Airflow CLI"
    ./scripts/airflow.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}

function start-jenkins() {
    trace "Start Jenkins docker container"
    ./scripts/start-jenkins.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function start-postgres() {
    trace "Start Postgres docker container"
    ./scripts/start-postgres.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function stop-postgres() {
    trace "Start Postgres docker container"
    ./scripts/stop-postgres.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}

function connect-to-local-postgres() {
    trace "Connect to local Postgres"
    ./scripts/connect-to-local-postgres.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function run-flyway() {
    trace "Running flyway migrate"
    ./scripts/run-flyway.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function run() {
    trace "Running app"
    pipenv run python -m \
        generate_monthly_payslip.flask_app "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/"


if contains usage "${operations[@]}"; then
    usage
    exit 1
fi
if contains setup "${operations[@]}"; then
    setup
fi
if contains linting "${operations[@]}"; then
    linting
fi
if contains tests "${operations[@]}"; then
    tests
fi
if contains spark-k8s-tests "${operations[@]}"; then
    spark-k8s-tests
fi
if contains get-k8s-spark-test-logs "${operations[@]}"; then
    get-k8s-spark-test-logs
fi
if contains build-flyway-docker "${operations[@]}"; then
    build-flyway-docker
fi
if contains start-airflow "${operations[@]}"; then
    start-airflow
fi
if contains stop-airflow "${operations[@]}"; then
    stop-airflow
fi
if contains airflow "${operations[@]}"; then
    airflow
fi
if contains start-jenkins "${operations[@]}"; then
    start-jenkins
fi
if contains start-postgres "${operations[@]}"; then
    start-postgres
fi
if contains stop-postgres "${operations[@]}"; then
    stop-postgres
fi
if contains connect-to-local-postgres "${operations[@]}"; then
    connect-to-local-postgres
fi
if contains run-flyway "${operations[@]}"; then
    run-flyway
fi
if contains run "${operations[@]}"; then
    run
fi


trace "Exited cleanly."
