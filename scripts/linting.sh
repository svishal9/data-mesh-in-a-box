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


function usage() {
    trace "$0 default   Run all checks"
    trace "$0 pylint    Python linting with pylint"
    trace "Default target runs all"
}


# Parse arguments.
operations=()

while true; do
    case "${1:-}" in
    pylint)
        operations+=( pylint )
        shift
        ;;
    default)
        operations+=( pylint )
        shift
        ;;
    -h|--help)
        usage
        exit 1
        ;;
    *)
        break
        ;;
    esac
done
if [ "${#operations[@]}" -eq 0 ]; then
    operations=( pylint )
fi

function pylint() {
    trace "Linting with pylint"
    pipenv --python 3.6
    pipenv run pylint --rcfile=setup.cfg generate_monthly_payslip tests
}

script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/.."

if contains pylint "${operations[@]}"; then
    pylint
fi

trace "No issues found."