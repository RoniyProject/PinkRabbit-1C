"""
TLS-тесты PinkRabbitMQ на mock-брокере (MockBroker(tls_cert=..., tls_key=...)).

Сертификаты: самоподписанные (CN=localhost, SAN DNS:localhost, IP:127.0.0.1), генерируются
openssl CLI (PATH или Git for Windows). Компонента проверяет сертификат строго: хранилище Windows
ROOT/CA плюс PEM-файл из переменной окружения PINKRABBITMQ_TLS_CA_FILE, которая читается при
каждом Connect.

Запуск из каталога сборки:

    cd windows\\x64\\Release
    python -m pytest ../../../test/test_tls.py -v
"""


import time

import pytest

from mock_broker import MockBroker, find_openssl, make_self_signed_cert
from prmq_helpers import (
    MB, ack, connect, consume, consume_message, declare_queue, expect_error, finalize_broker,
    is_connected, publish, sleep_native,
)

CA_ENV = "PINKRABBITMQ_TLS_CA_FILE"

pytestmark = [
    pytest.mark.new_behavior("TLS: строгая проверка сертификата, CA из PINKRABBITMQ_TLS_CA_FILE"),
    pytest.mark.skipif(find_openssl() is None, reason="openssl CLI не найден (PATH, Git for Windows)"),
]


@pytest.fixture(scope="session")
def tls_certs(tmp_path_factory):
    d = str(tmp_path_factory.mktemp("prmq_tls"))
    return {
        "trusted": make_self_signed_cert(d, name="trusted"),
        "untrusted": make_self_signed_cert(d, name="untrusted"),
    }


@pytest.fixture
def tls_broker(request, tls_certs, monkeypatch):
    """TLS-брокер. param: cert ("trusted"|"untrusted"), ca ("trusted"|None - без переменной),
    остальные ключи - параметры MockBroker."""
    opts = dict(getattr(request, "param", None) or {})
    cert_name = opts.pop("cert", "trusted")
    ca_name = opts.pop("ca", "trusted")
    cert, key = tls_certs[cert_name]
    if ca_name:
        monkeypatch.setenv(CA_ENV, tls_certs[ca_name][0])
    else:
        monkeypatch.delenv(CA_ENV, raising=False)
    b = MockBroker(tls_cert=cert, tls_key=key, **opts).start()
    yield b
    finalize_broker(request, b)


def _roundtrip(broker, com, queue, bodies):
    declare_queue(com, queue)
    for body in bodies:
        publish(com, "", queue, body)
    assert broker.wait_until(lambda b: b.queue_depth(queue) == len(bodies), 15), \
        f"до очереди дошло {broker.queue_depth(queue)} из {len(bodies)}"
    ctag = consume(com, queue, select_size=10)
    for expected in bodies:
        got, body, tag = consume_message(com, ctag, 15000)
        assert got, "сообщение не получено по TLS"
        same = body == expected
        assert same, f"тело искажено: длина {len(body or '')} вместо {len(expected)}"
        ack(com, tag)
    assert broker.wait_until(lambda b: b.unacked_count(queue) == 0, 5)


def test_tls_a_localhost_roundtrip_5mb(tls_broker):
    """(а) secure=Истина к "localhost" с доверенным CA: publish/consume/ack, включая 5 МБ."""
    com = connect(tls_broker, host="localhost", secure=True)
    assert tls_broker.has_event("TLS handshake completed"), tls_broker.events()
    big = "TLS|" + "Юникод и ASCII 0123456789" * (5 * MB // 40)
    _roundtrip(tls_broker, com, "q.tls", ["короткое сообщение по TLS", big])
    assert is_connected(com)


def test_tls_b_connect_by_ip(tls_broker):
    """(б) TLS к "127.0.0.1": IP есть в subjectAltName, проверка имени проходит."""
    com = connect(tls_broker, host="127.0.0.1", secure=True)
    _roundtrip(tls_broker, com, "q.tls_ip", ["по IP-адресу"])


@pytest.mark.parametrize("tls_broker", [
    {"cert": "untrusted", "ca": "trusted"},
    {"cert": "trusted", "ca": None},
], indirect=True, ids=["other-cert", "no-ca-env"])
def test_tls_c_untrusted_certificate_rejected(tls_broker):
    """(в) Сертификат сервера не доверен: Connect завершается ошибкой проверки, а не зависает."""
    timeout = 5
    msg, el = expect_error(connect, tls_broker, host="localhost", secure=True, timeout=timeout)
    print(f"Connect к недоверенному серверу: {el:.2f} с: {msg}")
    assert el < timeout + 3, f"Connect завис на {el:.1f} с: {msg}"
    low = msg.lower()
    assert any(w in low for w in ("certificate", "verify", "ssl", "tls", "handshake")), \
        f"текст ошибки не указывает на проверку сертификата: {msg}"
    assert tls_broker.stats()["tls_handshakes"] == 0, "TLS-рукопожатие завершилось успешно"
    assert tls_broker.stats()["connection.open"] == 0, "AMQP-соединение открылось"


@pytest.mark.parametrize("tls_broker", [{"heartbeat": 2}], indirect=True, ids=["hb2"])
def test_tls_d_heartbeat_idle(tls_broker):
    """(г) TLS + простой 7 с при heartbeat=2: соединение живо, publish проходит."""
    com = connect(tls_broker, host="localhost", secure=True)
    conns = tls_broker.connections()
    assert len(conns) == 1 and conns[0]["heartbeat"] == 2, conns
    conn_id = conns[0]["id"]
    declare_queue(com, "q.tls_hb")
    hb_before = tls_broker.stats()["heartbeat_from_client"]
    sleep_native(com, 7000)
    assert is_connected(com), "после 7 с простоя по TLS IsConnected() = Ложь"
    publish(com, "", "q.tls_hb", "после простоя по TLS")
    assert tls_broker.wait_until(lambda b: b.queue_depth("q.tls_hb") == 1, 3)
    hb = tls_broker.stats()["heartbeat_from_client"] - hb_before
    assert hb >= 3, f"heartbeat от клиента за 7 с: {hb}"
    assert not tls_broker.has_event("missed heartbeats")
    assert [c["id"] for c in tls_broker.connections()] == [conn_id], "клиент переподключался"


# --- Регрессия после ревью ---------------------------------------------------------------

@pytest.mark.new_behavior("TLS без 100 мс задержки Poco на каждую операцию")
def test_tls_review_1_latency(tls_broker):
    """(ревью 1) 50 BasicPublish(waitConfirm) по TLS < 1.5 с; 30 сообщений при prefetch=1
    получены и подтверждены < 1.5 с. Раньше каждая операция ждала ~100 мс внутри Poco."""
    com = connect(tls_broker, host="localhost", secure=True)
    declare_queue(com, "q.tls_lat_pub")
    t0 = time.perf_counter()
    for i in range(50):
        publish(com, "", "q.tls_lat_pub", f"tls {i}", wait_confirm=True)
    t_pub = time.perf_counter() - t0
    print(f"50 BasicPublish(waitConfirm) по TLS: {t_pub:.3f} с")
    assert tls_broker.queue_depth("q.tls_lat_pub") == 50
    assert t_pub < 1.5, f"50 публикаций по TLS заняли {t_pub:.2f} с (>= 1.5 с)"

    declare_queue(com, "q.tls_lat_con")
    for i in range(30):
        tls_broker.inject_message("q.tls_lat_con", f"c {i}")
    ctag = consume(com, "q.tls_lat_con", select_size=1)
    t0 = time.perf_counter()
    got_bodies = []
    for _ in range(30):
        got, body, tag = consume_message(com, ctag, 3000)
        assert got, f"получено {len(got_bodies)} из 30"
        got_bodies.append(body)
        ack(com, tag)
    t_con = time.perf_counter() - t0
    print(f"30 сообщений при prefetch=1 по TLS: {t_con:.3f} с")
    assert got_bodies == [f"c {i}" for i in range(30)]
    assert tls_broker.wait_until(lambda b: b.unacked_count("q.tls_lat_con") == 0
                                 and b.queue_depth("q.tls_lat_con") == 0, 3)
    assert t_con < 1.5, f"30 сообщений при prefetch=1 заняли {t_con:.2f} с (>= 1.5 с)"


@pytest.fixture
def weak_tls_broker(request, tmp_path_factory, monkeypatch):
    """TLS-брокер с самоподписанным RSA-<param> (по умолчанию 1024), сервер с @SECLEVEL=0.
    CA-файл лежит по отдельному пути: компонента кеширует TLS-контекст по значению
    PINKRABBITMQ_TLS_CA_FILE."""
    bits = int(getattr(request, "param", 1024))
    d = str(tmp_path_factory.mktemp(f"prmq_tls_rsa{bits}"))
    try:
        cert, key = make_self_signed_cert(d, name=f"rsa{bits}", bits=bits)
        b = MockBroker(tls_cert=cert, tls_key=key, tls_security_level=0).start()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"брокер на python не может работать с RSA-1024: {e}")
    monkeypatch.setenv(CA_ENV, cert)
    yield b
    finalize_broker(request, b)


@pytest.mark.new_behavior("TLS-контекст компоненты требует уровень безопасности 2")
def test_tls_review_3_weak_rsa1024_rejected(weak_tls_broker):
    """(ревью 3) Сертификат RSA-1024 указан как CA: Connect завершается ошибкой, а не подключается."""
    msg, el = expect_error(connect, weak_tls_broker, host="localhost", secure=True, timeout=5)
    print(f"Connect с RSA-1024: {el:.2f} с: {msg}")
    assert el < 8, f"Connect висел {el:.1f} с"
    assert weak_tls_broker.stats()["connection.open"] == 0, "AMQP-соединение открылось со слабым ключом"
    assert weak_tls_broker.stats()["tls_handshakes"] == 0, "TLS-рукопожатие со слабым ключом завершилось"


@pytest.mark.parametrize("weak_tls_broker", [2048], indirect=True, ids=["rsa2048"])
def test_tls_review_3_control_rsa2048_accepted(weak_tls_broker):
    """(ревью 3, контроль) Тот же стенд (сервер @SECLEVEL=0, отдельный путь CA), но RSA-2048:
    Connect проходит. Значит отказ в тесте с RSA-1024 вызван длиной ключа, а не стендом."""
    com = connect(weak_tls_broker, host="localhost", secure=True)
    declare_queue(com, "q.rsa2048")
    publish(com, "", "q.rsa2048", "rsa2048")
    assert weak_tls_broker.wait_until(lambda b: b.queue_depth("q.rsa2048") == 1, 3)
