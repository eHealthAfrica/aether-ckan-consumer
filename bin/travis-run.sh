#!/bin/sh -e

nosetests ./tests

pycodestyle .
