#!/usr/bin/env bash
# Initialize the project's development environment.
set -e

python3.12 -m venv .venv
.venv/bin/pip3 install -U pip
.venv/bin/pip3 install -r requirements.txt
.venv/bin/pip3 install .