#!/bin/sh -e

docker-compose -f ./docker-compose.test.yml run test
pycodestyle .
