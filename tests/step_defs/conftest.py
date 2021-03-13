import pytest

from pytest_bdd import given



# Constants




# Hooks

def pytest_bdd_step_error(request, feature, scenario, step, step_func, step_func_args, exception):
    print(f'Step failed: {step}')


# Fixtures

@pytest.fixture




# Shared Given Steps

@given('')
def ddg_home(browser):
    browser.get(DUCKDUCKGO_HOME)
