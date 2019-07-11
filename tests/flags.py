import pytest

private_access = pytest.mark.skipif(
    pytest.config.getoption("--skip-private"),
    reason="set to not run tests that require private s3 access")
