#!/bin/sh -e

nosetests ./tests --with-coverage --cover-tests --cover-inclusive --cover-erase

pycodestyle .
