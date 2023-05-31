#!/bin/bash

set -euo pipefail

script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/.."

if [ "${1:-}" = '--dev' ]; then
    PIPENV_VENV_IN_PROJECT=1 pipenv sync --dev
else
    PIPENV_VENV_IN_PROJECT=1 pipenv sync
fi

echo "Fetched all dependencies" >&2
