import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "new_behavior(reason): проверяет поведение новой сборки PinkRabbitMQ "
        "(на старой сборке ожидаемо падает)",
    )


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def broker(request):
    """MockBroker на свободном порту. Параметры: @pytest.mark.parametrize("broker", [{...}],
    indirect=True). В teardown снимаются отказы, освобождаются объекты компоненты, брокер
    останавливается; при падении теста печатается журнал брокера."""
    from mock_broker import MockBroker
    import prmq_helpers

    kwargs = dict(getattr(request, "param", None) or {})
    b = MockBroker(**kwargs).start()
    yield b
    prmq_helpers.finalize_broker(request, b)
