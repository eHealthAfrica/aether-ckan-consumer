#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "Installing project's requirements."
pip install -r requirements.txt
pip install -r dev-requirements.txt

echo "travis-build.bash is done."
