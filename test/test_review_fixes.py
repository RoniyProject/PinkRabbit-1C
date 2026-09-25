"""
Регрессионные тесты исправлений после ревью PinkRabbitMQ (mock-брокер test/mock_broker.py).
TLS-часть (ревью 1 и 3) - в test_tls.py.

Запуск из каталога сборки:

    cd windows\\x64\\Release
    python -m pytest ../../../test/test_review_fixes.py -v -p no:cacheprovider
"""

import time

import pytest

from prmq_helpers import (
    ack, call_proc, closed_by_server_events, connect, consume, consume_message, consumer_info,
    declare_queue, expect_error, get_headers, publish, reject,
)

NB = pytest.mark.new_behavior


@NB("пробуждение потока IO без залипшего флага")
def test_review_2_io_wakeup_race(broker):
    """3000 последовательных BasicPublish(waitConfirm) по TCP: максимум одного вызова < 300 мс
    (при залипшем флаге пробуждения было бы ~1 с на вызов)."""
    n = 3000
    com = connect(broker)
    declare_queue(com, "q.wakeup")
    times = []
    t_all = time.perf_counter()
    for i in range(n):
        t0 = time.perf_counter()
        publish(com, "", "q.wakeup", f"w{i}", wait_confirm=True)
        times.append(time.perf_counter() - t0)
    total = time.perf_counter() - t_all
    worst = max(times)
    slow = sum(1 for t in times if t >= 0.1)
    print(f"{n} BasicPublish(waitConfirm): всего {total:.2f} с, среднее {total / n * 1000:.2f} мс, "
          f"максимум {worst * 1000:.1f} мс (вызов #{times.index(worst)}), >=100 мс: {slow}")
    assert broker.queue_depth("q.wakeup") == n
    assert worst < 0.3, f"самый долгий BasicPublish {worst * 1000:.0f} мс (>= 300 мс)"
    assert total < 30, f"{n} публикаций заняли {total:.1f} с"


@NB("целые в заголовках кодируются как 'l' (знаковое 64)")
def test_review_4_integer_headers_int64(broker):
    com = connect(broker)
    declare_queue(com, "q.int")
    publish(com, "", "q.int", "целые", headers={"neg": -5, "big": 9000000000})
    assert broker.wait_until(lambda b: b.queue_depth("q.int") == 1, 3)
    ht = broker.published_messages()[-1]["headers_typed"]
    print(f"заголовки на брокере: {ht}")
    assert ht.get("neg") == ("l", -5), f"neg: {ht.get('neg')}, ожидалось ('l', -5)"
    assert ht.get("big") == ("l", 9000000000), f"big: {ht.get('big')}, ожидалось ('l', 9000000000)"
    ctag = consume(com, "q.int", select_size=10)
    got, body, tag = consume_message(com, ctag, 5000)
    assert got and body == "целые"
    r = get_headers(com)
    print(f"GetHeaders: {r}")
    assert r.get("neg") == -5, f"GetHeaders: {r}"
    assert r.get("big") == 9000000000, f"GetHeaders: {r}"
    ack(com, tag)


@NB("BasicCancel возвращает в очередь невыданные сообщения")
def test_review_5_cancel_requeues_undelivered(broker):
    com = connect(broker)
    declare_queue(com, "q.cancel")
    ctag = consume(com, "q.cancel", select_size=10)
    for i in range(5):
        publish(com, "", "q.cancel", f"m{i}")
    got, body, tag = consume_message(com, ctag, 5000)
    assert got and body == "m0", (got, body)
    assert broker.wait_until(lambda b: b.unacked_count("q.cancel") == 5, 3), \
        f"предвыборка: unacked={broker.unacked_count('q.cancel')}"

    call_proc(com, "BasicCancel", ctag)

    ok = broker.wait_until(
        lambda b: b.queue_depth("q.cancel") == 4 and b.unacked_count("q.cancel") == 1, 3)
    assert ok, (f"после BasicCancel: готовых {broker.queue_depth('q.cancel')} (ожидалось 4), "
                f"неподтвержденных {broker.unacked_count('q.cancel')} (ожидалось 1)")
    assert not consumer_info(broker, "q.cancel"), "потребитель остался активным на брокере"
    msg, _ = expect_error(consume_message, com, ctag, 1000)
    assert "no active consumers" in msg.lower(), f"BasicConsumeMessage после BasicCancel: {msg}"
    st = broker.stats()
    print(f"basic.reject={st['basic.reject']} basic.nack={st['basic.nack']} "
          f"requeued={st['messages_requeued']}")


@NB("проверка очереди перед подпиской не закрывает канал потребителей")
def test_review_6_consume_missing_queue_keeps_consumer_channel(broker):
    com = connect(broker)
    declare_queue(com, "q.exists")
    ctag = consume(com, "q.exists", select_size=10)
    broker.inject_message("q.exists", "до ошибки")
    got, body, tag_a = consume_message(com, ctag, 5000)
    assert got and body == "до ошибки"
    before = consumer_info(broker, "q.exists")

    msg, _ = expect_error(consume, com, "q.nonexistent")
    print(f"BasicConsume на несуществующую очередь: {msg}")
    assert "404" in msg or "not_found" in msg.lower() or "no queue" in msg.lower(), msg

    ack(com, tag_a)
    assert broker.wait_until(lambda b: consumer_info(b, "q.exists").get("acked") == 1, 3), \
        "ack первого потребителя не дошел"
    after = consumer_info(broker, "q.exists")
    assert after and after["tag"] == before["tag"] and after["channel"] == before["channel"], \
        f"потребитель на брокере изменился/исчез: {before} -> {after}"
    broker.inject_message("q.exists", "после ошибки")
    got, body, tag_b = consume_message(com, ctag, 5000)
    assert got and body == "после ошибки", "первый потребитель перестал получать сообщения"
    ack(com, tag_b)
    closes = [e for e in closed_by_server_events(broker) if f"channel {before['channel']} " in e]
    assert not closes, f"канал потребителей закрыт брокером: {closes}"


@NB("BasicReject по тегу noConfirm=Истина -> ошибка, BasicAck -> no-op")
def test_review_7_reject_noconfirm_tag(broker):
    com = connect(broker)
    declare_queue(com, "q.noconf")
    for i in range(2):
        broker.inject_message("q.noconf", f"n{i}")
    ctag = consume(com, "q.noconf", no_confirm=True, select_size=10)
    got, _, tag1 = consume_message(com, ctag, 5000)
    assert got
    msg, _ = expect_error(reject, com, tag1, False)
    assert "cannot be rejected" in msg.lower(), f"BasicReject: {msg}"
    got, _, tag2 = consume_message(com, ctag, 5000)
    assert got
    ack(com, tag2)
    assert broker.wait_until(lambda b: consumer_info(b, "q.noconf").get("acked") == 2, 3), \
        consumer_info(broker, "q.noconf")
    assert broker.stats()["messages_rejected"] == 0, "BasicReject отправил reject брокеру"
    assert not closed_by_server_events(broker), closed_by_server_events(broker)


@NB('arguments = "null" и "[]" допустимы')
@pytest.mark.parametrize("args", ["null", "[]"])
def test_review_8_null_and_empty_array_arguments(broker, args):
    com = connect(broker)
    q = f"q.args_{'null' if args == 'null' else 'arr'}"
    assert declare_queue(com, q, args=args) == q
    assert broker.queue_info(q)["arguments"] == {}, broker.queue_info(q)
    publish(com, "", q, "без заголовков", headers=args)
    assert broker.wait_until(lambda b: b.queue_depth(q) == 1, 3)
    assert broker.published_messages()[-1]["headers"] == {}, broker.published_messages()[-1]


@NB("потребитель, чей consume-ok пришел после таймаута, отменяется компонентой")
def test_review_9_consumer_after_timeout(broker):
    com = connect(broker, timeout=1)
    declare_queue(com, "q.late")
    for i in range(3):
        broker.inject_message("q.late", f"l{i}")
    broker.delay_method("basic.consume", 3)
    msg, el = expect_error(consume, com, "q.late", select_size=10)
    print(f"BasicConsume с задержкой consume-ok: {el:.2f} с: {msg}")
    assert el < 2.5, f"таймаут 1 с сработал через {el:.2f} с"
    broker.delay_method("basic.consume", 0)
    time.sleep(4.0)
    active = broker.consumers(queue="q.late")
    history = broker.consumers(queue="q.late", include_inactive=True)
    print(f"потребители очереди: {history}")
    assert not active, f"на брокере остался активный потребитель: {active}"
    assert broker.queue_depth("q.late") == 3 and broker.unacked_count("q.late") == 0, \
        (f"готовых {broker.queue_depth('q.late')} (ожидалось 3), "
         f"неподтвержденных {broker.unacked_count('q.late')}")
    try:
        got, body, _ = consume_message(com, "", 500)
    except RuntimeError as e:
        print(f"BasicConsumeMessage без потребителя: {e}")
        got, body = False, None
    assert not got, f"в 1С выдано сообщение потребителя, о котором 1С не знает: {body!r}"
    ctag = consume(com, "q.late", select_size=10)
    bodies = []
    for _ in range(3):
        got, body, tag = consume_message(com, ctag, 5000)
        assert got, f"после новой подписки получено {len(bodies)} из 3"
        bodies.append(body)
        ack(com, tag)
    assert sorted(bodies) == ["l0", "l1", "l2"], bodies


@NB("BasicAck(multiple) с тегом больше выданного -> ошибка unknown")
def test_review_10_ack_multiple_unknown_tag(broker):
    com = connect(broker)
    declare_queue(com, "q.unknown")
    for i in range(3):
        broker.inject_message("q.unknown", f"u{i}")
    ctag = consume(com, "q.unknown", select_size=10)
    tags = []
    for _ in range(3):
        got, _, tag = consume_message(com, ctag, 5000)
        assert got
        tags.append(tag)
    assert broker.wait_until(lambda b: b.unacked_count("q.unknown") == 3, 3)
    msg, _ = expect_error(ack, com, max(tags) + 100, True)
    assert "unknown" in msg.lower(), f"BasicAck(multiple, неизвестный тег): {msg}"
    time.sleep(0.3)
    assert broker.unacked_count("q.unknown") == 3, "что-то подтверждено несмотря на ошибку"
    assert consumer_info(broker, "q.unknown").get("acked") == 0
    assert not closed_by_server_events(broker), closed_by_server_events(broker)
    ack(com, max(tags), True)
    assert broker.wait_until(lambda b: b.unacked_count("q.unknown") == 0, 3), \
        "канал непригоден после ошибки: корректный ack(multiple) не дошел"


@NB("Connect к несуществующему DNS-имени укладывается в таймаут")
def test_review_11_connect_nonexistent_dns(broker):
    msg, el = expect_error(connect, broker, host="nonexistent.invalid", timeout=2)
    print(f"Connect к nonexistent.invalid: {el:.2f} с: {msg}")
    assert el <= 3.0, f"Connect к несуществующему имени занял {el:.2f} с (> 3 с)"
