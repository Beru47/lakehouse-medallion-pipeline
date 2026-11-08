import pytest

from lakehouse.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("lakehouse-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
