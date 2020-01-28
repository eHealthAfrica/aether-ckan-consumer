#!/bin/bash
set -Eeuo pipefail


# Define help message
show_help() {
    echo """
    Commands
    ----------------------------------------------------------------------------
    bash          : run bash
    build         : build python wheel of library in /dist
    eval          : eval shell command
    pip_freeze    : freeze pip dependencies and write to requirements.txt
    start         : run application
    test_unit     : run tests
    test_lint     : run flake8 tests
    test_coverage : run tests with coverage output

    """
}

PYTEST="pytest --cov-report term-missing --cov=app --cov-append -p no:cacheprovider"

test_flake8() {
    flake8 /code/. --config=/code/conf/extras/flake8.cfg
}

test_unit() {
    $PYTEST -m unit
    cat /code/conf/extras/good_job.txt
    rm -R .pytest_cache || true
    rm -rf tests/__pycache__ || true
}

test_integration() {
    $PYTEST -m integration
    cat /code/conf/extras/good_job.txt
    rm -R .pytest_cache || true
    rm -rf tests/__pycache__ || true
}

case "$1" in
    bash )
        bash
    ;;

    eval )
        eval "${@:2}"
    ;;


    pip_freeze )

        rm -rf /tmp/env
        pip3 install -r ./conf/pip/primary-requirements.txt --upgrade

        cat /code/conf/pip/requirements_header.txt | tee conf/pip/requirements.txt
        pip freeze --local | grep -v appdir | tee -a conf/pip/requirements.txt
    ;;

    start )
        python manage.py "${@:2}"
    ;;

    test_unit)
        test_flake8
        test_unit "${@:2}"
    ;;

    test_lint)
        test_flake8
    ;;

    test_integration)
        test_flake8
        test_integration "${@:2}"
    ;;

    build)
        # remove previous build if needed
        rm -rf dist
        rm -rf build
        rm -rf .eggs
        rm -rf aether-sdk-example.egg-info

        # create the distribution
        python setup.py bdist_wheel --universal

        # remove useless content
        rm -rf build
        rm -rf myconsumer.egg-info
    ;;

    help)
        show_help
    ;;

    *)
        show_help
    ;;
esac
