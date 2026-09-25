"""
Тесты стабильности PinkRabbitMQ на mock-брокере AMQP 0-9-1 (test/mock_broker.py).

Запуск из каталога сборки (там лежат python_test.dll и PinkRabbitMQ.dll):

    cd windows\\x64\\Release
    python -m pytest ../../../test/test_stability.py -v

Тесты с маркером new_behavior проверяют поведение, которого нет в старой сборке; на ней они
ожидаемо падают. Отбор: -m new_behavior или -m "not new_behavior".
Все параметры методов передаются явно (работает и со стендом, требующим полного списка).
"""

import decimal
import gc
import struct
import threading
import time

import pytest

from prmq_helpers import (
    MB, ack, bind_queue, closed_by_server_events, connect, consume, consume_message,
    consumer_info, cpu_seconds, declare_exchange, declare_queue, dispose, expect_error,
    get_headers, is_connected, private_bytes, publish, reject, sleep_native, thread_count,
    wait_for_confirms, wait_until,
)

NB = pytest.mark.new_behavior


def assert_lost_text(err):
    """Текст ошибки потерянного соединения: один из префиксов контракта BasicConsumeMessage."""
    low = err.lower()
    assert "connection lost" in low or "consumer channel closed" in low,         f"неожиданный текст ошибки: {err}"
HB2 = pytest.mark.parametrize("broker", [{"heartbeat": 2}], indirect=True, ids=["hb2"])


# 1. Базовый круг ----------------------------------------------------------------

def test_01_basic_roundtrip_cyrillic(broker):
    """declare exchange/queue/bind, publish, consume, BasicConsumeMessage, ack; тело с кириллицей."""
    com = connect(broker)
    declare_exchange(com, "ex.basic", "direct")
    assert declare_queue(com, "q.basic") == "q.basic"
    bind_queue(com, "q.basic", "ex.basic", "rk.basic")
    text = "Привет, мир! Проверка кириллицы: ЪъЭэЮюЯя 123"
    publish(com, "ex.basic", "rk.basic", text)
    assert broker.wait_until(lambda b: b.queue_depth("q.basic") == 1, 3), \
        f"сообщение не дошло до очереди, depth={broker.queue_depth('q.basic')}"
    pub = broker.published_messages()[-1]
    assert pub["body"] == text.encode("utf-8"), f"брокер получил другое тело: {pub['body']!r}"

    ctag = consume(com, "q.basic", select_size=10)
    assert ctag, "BasicConsume вернул пустой тег потребителя"
    got, body, tag = consume_message(com, ctag, 5000)
    assert got, "BasicConsumeMessage не получил сообщение за 5 с"
    assert body == text, f"тело искажено: {body!r}"
    assert tag > 0, f"тег доставки {tag}"
    ack(com, tag)
    assert broker.wait_until(
        lambda b: b.unacked_count("q.basic") == 0 and b.queue_depth("q.basic") == 0, 3), \
        f"ack не дошел: unacked={broker.unacked_count('q.basic')}"
    assert broker.stats()["basic.ack"] >= 1


def test_01b_large_body_5mb(broker):
    """Тело 5 МБ: многокадровая публикация и доставка (frame_max 128 КБ)."""
    com = connect(broker)
    declare_queue(com, "q.big")
    body = "НАЧАЛО|" + "0123456789abcdef" * (5 * MB // 16) + "|КОНЕЦ"
    publish(com, "", "q.big", body)
    assert broker.wait_until(lambda b: b.queue_depth("q.big") == 1, 10), "5 МБ не дошли до очереди"
    pub = broker.published_messages()[-1]
    assert len(pub["body"]) == len(body.encode("utf-8")), \
        f"брокер получил {len(pub['body'])} байт вместо {len(body.encode('utf-8'))}"
    frames = broker.stats()["body_frames_in"]
    assert frames >= 5 * MB // 131072, f"тело пришло {frames} кадрами, ожидалось многокадровое"

    ctag = consume(com, "q.big", select_size=1)
    got, rbody, tag = consume_message(com, ctag, 15000)
    assert got, "5 МБ не получены за 15 с"
    same = rbody == body
    assert same, f"тело 5 МБ искажено: длина {len(rbody or '')} вместо {len(body)}"
    ack(com, tag)
    assert broker.wait_until(lambda b: b.unacked_count("q.big") == 0, 3)


# 2-4. Heartbeat, полуоткрытое соединение, обрыв ------------------------------------------

@NB("клиент сам шлет heartbeat; метод IsConnected")
@HB2
def test_02_heartbeat_idle(broker):
    """heartbeat=2, простой 7 с: соединение живо, publish проходит, брокер видел heartbeat клиента."""
    com = connect(broker, ping_rate=0)
    conns = broker.connections()
    assert len(conns) == 1 and conns[0]["heartbeat"] == 2, f"согласованный heartbeat: {conns}"
    conn_id = conns[0]["id"]
    declare_queue(com, "q.hb")
    hb_before = broker.stats()["heartbeat_from_client"]

    sleep_native(com, 7000)

    assert is_connected(com), "после 7 с простоя IsConnected() = Ложь"
    publish(com, "", "q.hb", "после простоя")
    assert broker.wait_until(lambda b: b.queue_depth("q.hb") == 1, 3), "publish после простоя не дошел"
    hb = broker.stats()["heartbeat_from_client"] - hb_before
    assert hb >= 3, f"за 7 с простоя от клиента пришло heartbeat-кадров: {hb}"
    assert not broker.has_event("missed heartbeats"), \
        [e for e in broker.events() if "missed heartbeats" in e]
    assert [c["id"] for c in broker.connections()] == [conn_id], \
        "клиент переподключался во время простоя"


@NB("обнаружение полуоткрытого соединения: ошибка вместо вечного Ложь")
@HB2
def test_03_half_open_connection_detected(broker):
    """broker.freeze(): в течение 2*hb+3 с BasicConsumeMessage бросает ошибку, IsConnected()=Ложь."""
    hb = 2
    com = connect(broker)
    declare_queue(com, "q.freeze")
    ctag = consume(com, "q.freeze")
    got, _, _ = consume_message(com, ctag, 300)
    assert not got

    broker.freeze(True)
    t0 = time.monotonic()
    limit = 2 * hb + 3
    err = None
    falses = 0
    while time.monotonic() - t0 < limit:
        try:
            got, _, _ = consume_message(com, ctag, 1000)
            if not got:
                falses += 1
        except RuntimeError as e:
            err = str(e)
            break
    elapsed = time.monotonic() - t0
    assert err is not None, (f"за {elapsed:.1f} с после freeze BasicConsumeMessage {falses} раз "
                             f"вернул Ложь и не бросил ошибку")
    print(f"обрыв обнаружен через {elapsed:.1f} с: {err}")
    assert_lost_text(err)
    assert not is_connected(com), "после обнаружения обрыва IsConnected() = Истина"


@NB("ошибка вместо вечного Ложь при потере соединения")
@pytest.mark.parametrize("rst", [False, True], ids=["fin", "rst"])
def test_04_broker_drops_connection(broker, rst):
    """drop_connections() во время ожидания BasicConsumeMessage -> ошибка в течение ~1-2 с."""
    com = connect(broker)
    declare_queue(com, "q.drop")
    ctag = consume(com, "q.drop")
    state = {}

    def do_drop():
        state["t"] = time.monotonic()
        state["n"] = broker.drop_connections(rst=rst)

    timer = threading.Timer(1.0, do_drop)
    timer.start()
    err = None
    t_err = None
    try:
        t0 = time.monotonic()
        while time.monotonic() - t0 < 10:
            try:
                consume_message(com, ctag, 1000)
            except RuntimeError as e:
                err = str(e)
                t_err = time.monotonic()
                break
    finally:
        timer.join()
    assert state.get("n") == 1, f"брокер закрыл соединений: {state.get('n')}"
    assert err is not None, "за 9 с после обрыва BasicConsumeMessage не бросил ошибку"
    latency = t_err - state["t"]
    assert latency < 2.5, f"ошибка через {latency:.2f} с после обрыва (ожидалось ~1-2 с): {err}"
    print(f"ошибка через {latency:.2f} с: {err}")
    assert_lost_text(err)
    assert not is_connected(com)


# 5. Ошибка канала ------------------------------------------------------------------

def test_05_channel_error_then_recovery(broker):
    """Passive DeclareQueue несуществующей очереди -> ошибка; дальше канал восстанавливается."""
    com = connect(broker)
    msg, _ = expect_error(declare_queue, com, "q.missing", passive=True)
    assert "NOT_FOUND" in msg or "no queue" in msg, f"неожиданный текст ошибки: {msg}"
    assert broker.has_event("closed by server: 404"), "брокер не закрывал канал с 404"
    assert declare_queue(com, "q.after_error") == "q.after_error"
    publish(com, "", "q.after_error", "после ошибки канала")
    assert broker.wait_until(lambda b: b.queue_depth("q.after_error") == 1, 3), \
        "publish после ошибки канала не дошел"
    assert is_connected(com)


# 6. Пример README: noConfirm=Истина + BasicAck ----------------------------------------------

@NB("noConfirm=Истина: ручное подтверждение внутри компоненты, BasicAck по такому тегу - no-op")
def test_06_readme_noconfirm_with_basic_ack(broker):
    N, SELECT = 50, 10
    com = connect(broker)
    declare_queue(com, "q.readme")
    for i in range(N):
        broker.inject_message("q.readme", f"сообщение {i}")
    ctag = consume(com, "q.readme", "", no_confirm=True, exclusive=False, select_size=SELECT)
    assert ctag
    received = []
    while len(received) < N:
        got, body, tag = consume_message(com, "", 5000)  # как в README: пустой consumerId
        if not got:
            break
        received.append(body)
        ack(com, tag)
    assert len(received) == N, f"прочитано {len(received)} из {N}; " \
                               f"закрытия каналов: {closed_by_server_events(broker)}"
    assert received == [f"сообщение {i}" for i in range(N)], "нарушен порядок или содержимое"
    c = consumer_info(broker, "q.readme")
    assert c, "потребитель исчез на брокере (канал закрыт?)"
    assert not c["no_ack"], "компонента подписалась с no_ack=true (prefetch не действует)"
    assert broker.wait_until(lambda b: consumer_info(b, "q.readme").get("acked") == N, 3), \
        f"брокер подтвердил {consumer_info(broker, 'q.readme').get('acked')} из {N}"
    c = consumer_info(broker, "q.readme")
    assert c["max_unacked"] <= SELECT, \
        f"одновременно неподтвержденных {c['max_unacked']} > selectSize {SELECT}"
    assert broker.unacked_count("q.readme") == 0
    assert not closed_by_server_events(broker), closed_by_server_events(broker)
    assert is_connected(com)


# 7. Устаревший тег --------------------------------------------------------------------------

@NB("устаревшие теги ('stale'), ошибка 'Consumer channel closed', монотонные теги")
def test_07_stale_tag_after_channel_close(broker):
    com = connect(broker)
    declare_queue(com, "q.stale")
    for i in range(3):
        broker.inject_message("q.stale", f"m{i}")
    ctag = consume(com, "q.stale", select_size=10)
    got, body, tag1 = consume_message(com, ctag, 5000)
    assert got and body == "m0", (got, body)
    assert broker.wait_until(lambda b: b.unacked_count("q.stale") == 3, 3), \
        f"предвыборка: unacked={broker.unacked_count('q.stale')}"

    assert broker.close_consumer_channels(queue="q.stale") == 1
    assert broker.wait_for_event("close-ok received", 3), "клиент не ответил channel.close-ok"
    assert broker.wait_until(lambda b: b.queue_depth("q.stale") == 3, 3), "сообщения не вернулись в очередь"
    time.sleep(0.2)
    st0 = broker.stats()

    msg, _ = expect_error(ack, com, tag1)
    assert "stale" in msg.lower(), f"BasicAck(старый тег): {msg}"
    msg2, _ = expect_error(consume_message, com, ctag, 1000)
    assert "consumer channel closed" in msg2.lower(), f"BasicConsumeMessage: {msg2}"
    time.sleep(0.3)
    st1 = broker.stats()
    assert st1["basic.ack"] == st0["basic.ack"], "BasicAck по устаревшему тегу отправил кадр брокеру"
    assert st1["channel_errors"] == st0["channel_errors"], closed_by_server_events(broker)

    ctag2 = consume(com, "q.stale", select_size=10)
    tags, bodies = [], []
    for _ in range(3):
        got, body, tag = consume_message(com, ctag2, 5000)
        assert got, f"после повторного BasicConsume получено {len(bodies)} из 3"
        tags.append(tag)
        bodies.append(body)
        ack(com, tag)
    assert sorted(bodies) == ["m0", "m1", "m2"], bodies
    assert min(tags) > tag1, f"новые теги {tags} не больше старого {tag1}"
    got, body, _ = consume_message(com, ctag2, 500)
    assert not got, f"лишнее сообщение после повторной подписки: {body!r}"
    assert broker.wait_until(
        lambda b: b.unacked_count("q.stale") == 0 and b.queue_depth("q.stale") == 0, 3)


# 8. Publisher confirms ------------------------------------------------------------------------

@NB("publisher confirms вместо tx; WaitForConfirms")
@pytest.mark.parametrize("broker", [{}, {"confirm_multiple": True}], indirect=True,
                         ids=["ack-single", "ack-multiple"])
def test_08_publisher_confirms(broker):
    com = connect(broker)
    declare_queue(com, "q.confirm")
    declare_queue(com, "q.confirm_async")
    for i in range(50):
        publish(com, "", "q.confirm", f"sync {i}")
    st = broker.stats()
    assert st["confirm.select"] >= 1, f"confirm.select не было: {dict(st)}"
    assert st["tx.select"] == 0, f"компонента использует транзакции: tx.select={st['tx.select']}"
    assert broker.queue_depth("q.confirm") == 50

    t0 = time.monotonic()
    for i in range(1000):
        publish(com, "", "q.confirm_async", f"async {i}", wait_confirm=False)
    t_pub = time.monotonic() - t0
    assert wait_for_confirms(com, 10000) is True, "WaitForConfirms(10000) = Ложь"
    t_all = time.monotonic() - t0
    print(f"1000 асинхронных публикаций: {t_pub:.2f} с, с подтверждением {t_all:.2f} с")
    assert broker.queue_depth("q.confirm_async") == 1000
    msgs = broker.queue_messages("q.confirm_async")
    assert msgs[0]["text"] == "async 0" and msgs[-1]["text"] == "async 999", "нарушен порядок"
    assert broker.stats()["tx.select"] == 0


@NB("WaitForConfirms и BasicPublish сообщают о basic.nack")
def test_08b_confirms_report_nack(broker):
    com = connect(broker)
    declare_queue(com, "q.nack")
    publish(com, "", "q.nack", "warmup")
    broker.nack_publishes(1)
    for i in range(10):
        publish(com, "", "q.nack", f"n{i}", wait_confirm=False)
    msg, _ = expect_error(wait_for_confirms, com, 10000)
    print(f"WaitForConfirms после nack: {msg}")
    assert broker.queue_depth("q.nack") == 1 + 9
    broker.nack_publishes(1)
    msg2, _ = expect_error(publish, com, "", "q.nack", "sync nack")
    print(f"BasicPublish с nack: {msg2}")
    publish(com, "", "q.nack", "after nack")
    assert broker.queue_depth("q.nack") == 11


# 9. Таймаут и чужой результат ---------------------------------------------------------------

@NB("таймаут операции не подменяет результат следующей (чужой ответ отбрасывается)")
def test_09_timeout_and_late_reply(broker):
    com = connect(broker, timeout=1)
    broker.delay_method("queue.declare", 3)
    msg, el = expect_error(declare_queue, com, "q1")
    assert el < 2.5, f"таймаут 1 с сработал через {el:.2f} с: {msg}"
    print(f"таймаут DeclareQueue(q1) через {el:.2f} с: {msg}")
    broker.delay_method("queue.declare", 0)
    time.sleep(3.0)  # за это время брокер отвечает на q1 (поздний ответ)
    assert broker.queue_exists("q1"), "брокер так и не обработал отложенный declare q1"

    # q2 задерживаем на 0.5 с (< таймаута): корректный клиент ждет свой ответ,
    # некорректный возвращается сразу, приняв поздний ответ на q1 за свой.
    broker.delay_method("queue.declare", 0.5)
    t0 = time.monotonic()
    name = declare_queue(com, "q2")
    el2 = time.monotonic() - t0
    exists = broker.queue_exists("q2")
    broker.delay_method("queue.declare", 0)
    assert name == "q2", f"DeclareQueue(q2) вернул {name!r}"
    assert exists, "DeclareQueue(q2) вернул управление до ответа брокера (чужой результат)"
    assert el2 >= 0.4, f"DeclareQueue(q2) вернулся за {el2:.2f} с, раньше ответа брокера"
    assert declare_queue(com, "q2b") == "q2b"

    # Еще один таймаут, затем объект удаляется сразу: поздний ответ приходит в закрытое соединение.
    broker.delay_method("queue.declare", 3)
    expect_error(declare_queue, com, "q3")
    t0 = time.monotonic()
    del com
    gc.collect()
    t_del = time.monotonic() - t0
    broker.delay_method("queue.declare", 0)
    print(f"удаление объекта после таймаута: {t_del:.2f} с")
    time.sleep(3.5)
    com2 = connect(broker)
    assert declare_queue(com2, "q4") == "q4", "после удаления объекта компонента не работает"


# 10. connection.blocked ------------------------------------------------------------------------

@NB("быстрая ошибка BasicPublish при connection.blocked")
def test_10_connection_blocked(broker):
    com = connect(broker)
    declare_queue(com, "q.blocked")
    publish(com, "", "q.blocked", "до блокировки")
    caps = broker.connections()[0]["capabilities"]
    broker.block("low memory")
    assert broker.wait_until(lambda b: b.stats()["sent:connection.blocked"] >= 1, 2), \
        f"брокер не отправил connection.blocked; capabilities клиента: {caps}"
    time.sleep(0.2)
    msg, el = expect_error(publish, com, "", "q.blocked", "во время блокировки")
    print(f"BasicPublish при blocked: {el:.2f} с: {msg}")
    assert el < 2.0, f"ошибка через {el:.2f} с (ожидалось < 2 с): {msg}"
    assert "blocked" in msg.lower(), f"в тексте ошибки нет 'blocked': {msg}"
    broker.unblock()
    time.sleep(0.2)
    publish(com, "", "q.blocked", "после разблокировки")
    assert broker.wait_until(
        lambda b: "после разблокировки" in [m["text"] for m in b.queue_messages("q.blocked")], 3), \
        [m["text"] for m in broker.queue_messages("q.blocked")]


# 11. Скорость освобождения -----------------------------------------------------------------------

def test_11_fast_release_with_active_consumer(broker):
    com = connect(broker)
    declare_queue(com, "q.release")
    for i in range(20):
        broker.inject_message("q.release", f"r{i}")
    ctag = consume(com, "q.release", select_size=5)
    got, _, _ = consume_message(com, ctag, 5000)
    assert got
    assert broker.wait_until(lambda b: b.unacked_count("q.release") == 5, 3)
    t0 = time.monotonic()
    del com
    gc.collect()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.5, f"объект с активным потребителем удалялся {elapsed:.2f} с"
    assert broker.wait_until(lambda b: b.connection_count() == 0, 3), "соединение не закрыто"
    assert broker.wait_until(lambda b: b.queue_depth("q.release") == 20, 3), \
        "неподтвержденные сообщения не вернулись в очередь"


# 12. Повторный Connect -------------------------------------------------------------------------------

def test_12_reconnect_same_object(broker):
    com = connect(broker)
    declare_queue(com, "q.old")
    for i in range(10):
        broker.inject_message("q.old", f"old {i}")
    ctag_old = consume(com, "q.old", select_size=10)
    got, body, _ = consume_message(com, ctag_old, 5000)
    assert got and body == "old 0"
    assert broker.wait_until(lambda b: b.unacked_count("q.old") == 10, 3)
    old_ids = [c["id"] for c in broker.connections()]

    connect(broker, com=com)

    assert broker.wait_until(
        lambda b: b.connection_count() == 1 and [c["id"] for c in b.connections()] != old_ids, 3), \
        f"старое соединение не закрыто: {broker.connections()}"
    assert broker.wait_until(lambda b: b.queue_depth("q.old") == 10, 3), "old-сообщения не вернулись"
    declare_queue(com, "q.new")
    for i in range(5):
        broker.inject_message("q.new", f"new {i}")
    ctag_new = consume(com, "q.new", select_size=10)
    bodies = []
    while True:
        got, body, tag = consume_message(com, ctag_new, 2000)
        if not got:
            break
        bodies.append(body)
        ack(com, tag)
    assert bodies == [f"new {i}" for i in range(5)], f"получено: {bodies}"
    assert is_connected(com)
    declare_queue(com, "q.after_reconnect")
    publish(com, "", "q.after_reconnect", "publish после переподключения")
    assert broker.wait_until(lambda b: b.queue_depth("q.after_reconnect") == 1, 3)


# 13. Невалидный UTF-8 ------------------------------------------------------------------------------------

@NB("невалидный UTF-8 в теле заменяется, прием не ломается")
def test_13_invalid_utf8_body(broker):
    com = connect(broker)
    declare_queue(com, "q.utf8")
    broker.inject_message("q.utf8", b"\xff\xfe abc", headers={"kind": "broken"})
    ctag = consume(com, "q.utf8", select_size=10)
    got, body, tag = consume_message(com, ctag, 5000)
    assert got, "сообщение с невалидным UTF-8 не выдано"
    assert tag > 0, f"тег {tag}"
    assert body is not None and body.endswith(" abc"), repr(body)
    assert "�" in body or "?" in body, f"нет символа замены: {body!r}"
    ack(com, tag)
    assert broker.wait_until(lambda b: b.unacked_count("q.utf8") == 0, 3), "ack не дошел"
    broker.inject_message("q.utf8", "корректное сообщение")
    got, body, tag = consume_message(com, ctag, 5000)
    assert got and body == "корректное сообщение", (got, body)
    ack(com, tag)


# 14-15. Reject / ack multiple ----------------------------------------------------------------------------

@NB("BasicReject(tag, requeue)")
def test_14_reject_requeue(broker):
    com = connect(broker)
    declare_queue(com, "q.reject")
    broker.inject_message("q.reject", "на повтор")
    ctag = consume(com, "q.reject", select_size=10)
    got, body, tag = consume_message(com, ctag, 5000)
    assert got and body == "на повтор"
    reject(com, tag, True)
    got2, body2, tag2 = consume_message(com, ctag, 5000)
    assert got2 and body2 == "на повтор", f"после reject(requeue) не пришло повторно: {got2} {body2!r}"
    assert tag2 > tag, (tag, tag2)
    assert broker.stats()["messages_requeued"] == 1
    reject(com, tag2, False)
    got3, body3, _ = consume_message(com, ctag, 1000)
    assert not got3, f"после reject без requeue сообщение пришло снова: {body3!r}"
    assert broker.wait_until(
        lambda b: b.queue_depth("q.reject") == 0 and b.unacked_count("q.reject") == 0, 3)
    assert broker.stats()["messages_rejected"] == 2


@NB("BasicAck(tag, multiple)")
def test_15_ack_multiple(broker):
    com = connect(broker)
    declare_queue(com, "q.multi")
    for i in range(5):
        broker.inject_message("q.multi", f"a{i}")
    ctag = consume(com, "q.multi", select_size=10)
    tags = []
    for _ in range(5):
        got, _, tag = consume_message(com, ctag, 5000)
        assert got
        tags.append(tag)
    assert broker.wait_until(lambda b: b.unacked_count("q.multi") == 5, 3)
    ack(com, tags[-1], True)
    assert broker.wait_until(lambda b: b.unacked_count("q.multi") == 0, 3), \
        f"после ack(multiple) неподтвержденных: {broker.unacked_count('q.multi')}"
    assert consumer_info(broker, "q.multi")["acked"] == 5
    assert not closed_by_server_events(broker), closed_by_server_events(broker)


# 16. Заголовки -----------------------------------------------------------------------------------------

@NB("дробные числа в заголовках: decimal со знаком или double")
def test_16_headers_decimal_and_double(broker):
    """Брокер декодирует заголовки по спецификации (сетевой порядок байт, decimal = scale + int32
    со знаком). 'D' обязан быть точным; если значение не помещается в int32 без потери знаков,
    ожидается 'd'. Затем GetHeaders на приеме возвращает те же значения."""
    com = connect(broker)
    declare_queue(com, "q.hdr")
    hdrs = {"neg": -1.5, "big": 12345.678901, "huge": 1e12, "i": 5, "s": "строка", "b": True}
    publish(com, "", "q.hdr", "заголовки", headers=hdrs)
    assert broker.wait_until(lambda b: b.queue_depth("q.hdr") == 1, 3)
    pub = broker.published_messages()[-1]
    h, ht = pub["headers"], pub["headers_typed"]
    print(f"заголовки на брокере: {ht}")
    assert set(h) == set(hdrs), f"набор заголовков: {ht}"

    problems = []
    for key in ("neg", "big", "huge"):
        t, v = ht[key]
        expected = hdrs[key]
        if t == "D":
            if v != decimal.Decimal(repr(expected)):
                problems.append(f"{key}: decimal {v} (масштаб {-v.as_tuple().exponent}) неточен, "
                                f"ожидалось {expected!r} - при нехватке int32 нужен double 'd'")
        elif t == "d":
            if abs(v - expected) > abs(expected) * 1e-12:
                swapped = struct.unpack("<d", struct.pack(">d", v))[0]
                note = (" - double записан в порядке байт хоста, а не в сетевом "
                        "(AMQP-CPP OutBuffer::add(double))" if swapped == expected else "")
                problems.append(f"{key}: double {v!r}, ожидалось {expected!r}{note}")
        else:
            problems.append(f"{key}: тип поля {t!r}, ожидался 'D' или 'd'")
    if not (h["i"] == 5 and ht["i"][0] in ("b", "B", "s", "U", "u", "I", "i", "l", "L")):
        problems.append(f"i: {ht['i']}")
    if h["s"] != "строка":
        problems.append(f"s: {ht['s']}")
    if h["b"] is not True:
        problems.append(f"b: {ht['b']}")

    ctag = consume(com, "q.hdr", select_size=10)
    got, body, tag = consume_message(com, ctag, 5000)
    assert got and body == "заголовки"
    r = get_headers(com)
    print(f"GetHeaders: {r}")
    for key, expected in hdrs.items():
        v = r.get(key)
        if isinstance(expected, float):
            ok = isinstance(v, (int, float)) and abs(v - expected) <= abs(expected) * 1e-12
        else:
            ok = v == expected and type(v) is type(expected)
        if not ok:
            problems.append(f"GetHeaders[{key}] = {v!r}, ожидалось {expected!r}")
    ack(com, tag)
    assert not problems, "\n".join(problems)


# 17. Ресурсы ------------------------------------------------------------------------------------------

def test_17_resources_30_connections(broker):
    """30 объектов с Connect: прирост private bytes < 200 МБ, CPU за 5 с простоя < 0.5 с."""
    limit = 200 * MB
    warm = connect(broker)
    declare_queue(warm, "q.res")
    dispose(warm)
    del warm
    gc.collect()
    time.sleep(0.5)
    base = private_bytes()
    coms = []
    for i in range(30):
        c = connect(broker)
        declare_queue(c, f"q.res{i}")
        coms.append(c)
        grown = private_bytes() - base
        if grown > limit:
            pytest.fail(f"после {i + 1} объектов прирост private bytes {grown / MB:.0f} МБ > 200 МБ")
    assert broker.wait_until(lambda b: b.connection_count() >= 30, 5)
    time.sleep(1.0)
    grown = private_bytes() - base
    print(f"30 соединений: прирост private bytes {grown / MB:.1f} МБ")
    assert grown < limit, f"прирост private bytes {grown / MB:.0f} МБ"
    cpu0 = cpu_seconds()
    time.sleep(5.0)
    cpu = cpu_seconds() - cpu0
    print(f"CPU процесса за 5 с простоя 30 соединений: {cpu:.3f} с")
    assert cpu < 0.5, f"30 простаивающих соединений заняли {cpu:.2f} с CPU за 5 с"
    for c in coms:
        dispose(c)
    coms.clear()
    assert broker.wait_until(lambda b: b.connection_count() == 0, 10)


# 18. Имя очереди от сервера --------------------------------------------------------------------------

@NB("DeclareQueue('') возвращает имя, сгенерированное сервером")
def test_18_server_named_queue(broker):
    com = connect(broker)
    name = declare_queue(com, "")
    assert name and name.startswith("amq.gen-"), f"DeclareQueue('') вернул {name!r}"
    assert broker.queue_exists(name)
    publish(com, "", name, "в серверную очередь")
    assert broker.wait_until(lambda b: b.queue_depth(name) == 1, 3)


# 19. Утечка потоков ------------------------------------------------------------------------------------

def test_19_no_thread_leak(broker):
    warm = connect(broker)
    declare_queue(warm, "q.leak")
    dispose(warm)
    del warm
    gc.collect()
    time.sleep(0.5)
    base = thread_count()
    for i in range(20):
        c = connect(broker)
        declare_queue(c, "q.leak")
        del c
        gc.collect()
    settled = wait_until(lambda: abs(thread_count() - base) <= 2, 5)
    assert settled, f"потоков было {base}, стало {thread_count()} после 20 циклов Connect/удаление"
    assert broker.wait_until(lambda b: b.connection_count() == 0, 5), \
        f"незакрытых соединений: {broker.connection_count()}"

    one = connect(broker)
    declare_queue(one, "q.leak")
    with_one = thread_count()
    for _ in range(10):
        connect(broker, com=one)
        declare_queue(one, "q.leak")
    settled = wait_until(lambda: abs(thread_count() - with_one) <= 2, 5)
    assert settled, f"10 повторных Connect на одном объекте: потоков {thread_count()}, было {with_one}"
    assert broker.wait_until(lambda b: b.connection_count() == 1, 5), \
        f"после повторных Connect открыто соединений: {broker.connection_count()}"
