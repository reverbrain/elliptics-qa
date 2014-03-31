import pytest

@pytest.fixture(scope="module", params=pytest.testrunner.tests.keys())
def test_name(request):
    return request.param

def test_elliptics(test_name):
    pytest.testrunner.run(test_name)

