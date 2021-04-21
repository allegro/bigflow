# pytest configuration

import pytest
import pathlib
import re

@pytest.hookimpl(hookwrapper=True)
def pytest_ignore_collect(path: pathlib.Path, config):

    outcome = yield
    if outcome.get_result():
        # path is already ignored
        return

    # Ignore any "tests" from "bigflow project proto" directories
    pathstr = str(path)
    if any(re.search(x, pathstr) for x in [
        r"/bf-projects/",
        r"/example_project/.*",
    ]):
        outcome.force_result(True)