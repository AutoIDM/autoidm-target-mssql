#!/bin/sh

unset VIRTUAL_ENV 

# This simple script allows you to test your tap from any directory, while still taking
# advantage of the poetry-managed virtual environment.
# Adapted from: https://github.com/python-poetry/poetry/issues/2179#issuecomment-668815276

STARTDIR=$(pwd)
TOML_DIR=$(dirname "$0")

cd "$TOML_DIR" || exit
poetry install 1>&2
poetry run target-mssql $*
