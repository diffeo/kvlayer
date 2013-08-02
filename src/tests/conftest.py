# content of conftest.py

import pytest


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true",
                     help="run slow tests")
    parser.addoption("--runperf", action="store_true",
                     help="run performance tests")


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")
    if 'performance' in item.keywords and not item.config.getoption("--runperf"):
        pytest.skip("need --runperf option to run")
