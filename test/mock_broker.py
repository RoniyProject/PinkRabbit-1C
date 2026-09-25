#!/usr/bin/env python3
"""
Тестовый mock-брокер AMQP 0-9-1 с внедрением отказов для тестов PinkRabbitMQ.

Только стандартная библиотека. Сервер работает в отдельном потоке (selectors, один поток
ввода-вывода), держит много соединений одновременно и реализует подмножество AMQP 0-9-1,
которое использует клиент AMQP-CPP: connection/channel/exchange/queue/basic/confirm/tx.

Быстрый старт:

    from mock_broker import MockBroker
    broker = MockBroker(heartbeat=2).start()
    ...  # Connect("127.0.0.1", broker.port, "guest", "guest", "/")
    broker.freeze(True)            # полуоткрытое TCP: брокер перестает читать и писать
    broker.drop_connections()      # закрыть все сокеты клиентов
    broker.stop()

Все методы управления и инспекции безопасно вызывать из потока теста: изменения состояния
выполняются в потоке ввода-вывода (через очередь команд), чтение снимков - под общей блокировкой.

Автономный запуск (например, чтобы прогнать старые тесты test_amqp.py против брокера):

    python mock_broker.py --port 5672 --vhost guest -v

Реализованные особенности RabbitMQ (коды ошибок и тексты - как у RabbitMQ):
- мягкие ошибки (403/404/405/406) закрывают канал, жесткие (5xx, 530) - соединение;
- неизвестный delivery tag в ack/reject/nack -> channel.close 406 PRECONDITION_FAILED;
- publisher confirms (basic.ack/basic.nack по порядку, опционально multiple=true);
- connection.blocked: уведомление клиентам с capability connection.blocked, чтение соединения
  останавливается на первом basic.publish (как делает RabbitMQ);
- heartbeat: брокер шлет heartbeat, если ничего не отправлял hb/2 секунд, и закрывает сокет,
  если от клиента не было никакого трафика hb * client_timeout_factor секунд
  (событие "missed heartbeats from client").
"""

from __future__ import annotations

import argparse
import base64
import collections
import datetime as _dt
import decimal
import functools
import logging
import os
import selectors
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time

__all__ = [
    "MockBroker", "Typed", "AMQPError", "ProtocolError",
    "decode_table", "encode_table", "untype_table", "decode_properties", "encode_properties",
    "encode_method", "decode_method", "topic_match", "METHOD_IDS",
    "find_openssl", "make_self_signed_cert",
]

log = logging.getLogger("mock_broker")

PROTOCOL_HEADER = b"AMQP\x00\x00\x09\x01"

FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_END = 0xCE
FRAME_MIN_SIZE = 4096

REPLY_NAMES = {
    200: "REPLY_SUCCESS", 311: "CONTENT_TOO_LARGE", 312: "NO_ROUTE", 313: "NO_CONSUMERS",
    320: "CONNECTION_FORCED", 402: "INVALID_PATH", 403: "ACCESS_REFUSED", 404: "NOT_FOUND",
    405: "RESOURCE_LOCKED", 406: "PRECONDITION_FAILED", 501: "FRAME_ERROR", 502: "SYNTAX_ERROR",
    503: "COMMAND_INVALID", 504: "CHANNEL_ERROR", 505: "UNEXPECTED_FRAME", 506: "RESOURCE_ERROR",
    530: "NOT_ALLOWED", 540: "NOT_IMPLEMENTED", 541: "INTERNAL_ERROR",
}
# Коды, при которых RabbitMQ закрывает только канал; остальные закрывают соединение.
SOFT_ERRORS = frozenset({311, 312, 313, 403, 404, 405, 406})

EXCHANGE_TYPES = ("direct", "fanout", "topic", "headers")

# Аргументы очереди, которые RabbitMQ сверяет при повторном объявлении.
EQUIVALENT_QUEUE_ARGS = (
    "x-max-priority", "x-message-ttl", "x-expires", "x-max-length", "x-max-length-bytes",
    "x-dead-letter-exchange", "x-dead-letter-routing-key", "x-queue-type", "x-overflow",
    "x-queue-mode", "x-single-active-consumer",
)

if os.name == "nt":
    _LINGER_RST = struct.pack("HH", 1, 0)
else:
    _LINGER_RST = struct.pack("ii", 1, 0)


class AMQPError(Exception):
    """Ошибка протокола уровня AMQP: code + текст (без префикса имени кода)."""

    def __init__(self, code, text, class_id=0, method_id=0):
        super().__init__(f"{code} {text}")
        self.code = code
        self.text = text
        self.class_id = class_id
        self.method_id = method_id

    @property
    def reply_text(self):
        return f"{REPLY_NAMES.get(self.code, str(self.code))} - {self.text}"

    @property
    def soft(self):
        return self.code in SOFT_ERRORS


class ProtocolError(Exception):
    """Некорректный кадр или поле (обрабатывается как ошибка соединения)."""


# ---------------------------------------------------------------------------
# Кодек полей и таблиц
# ---------------------------------------------------------------------------

class Typed:
    """Значение с явным типом поля AMQP для encode_table, например Typed('D', (1, -15))."""

    __slots__ = ("type", "value")

    def __init__(self, type_, value):
        self.type = type_
        self.value = value

    def __repr__(self):
        return f"Typed({self.type!r}, {self.value!r})"


# Числовые типы полей. 's' - short int по RabbitMQ errata, 'U' - int16 в AMQP-CPP,
# 'l' - int64 (RabbitMQ; AMQP-CPP кодирует так uint64), 'L' - int64 в AMQP-CPP.
_NUM_FORMATS = {
    "b": ">b", "B": ">B", "s": ">h", "U": ">h", "u": ">H", "I": ">i", "i": ">I",
    "l": ">q", "L": ">q", "f": ">f", "d": ">d", "T": ">Q",
}
_NUM_SIZES = {k: struct.calcsize(v) for k, v in _NUM_FORMATS.items()}


def _need(buf, off, n):
    if n < 0 or off + n > len(buf):
        raise ProtocolError(f"buffer underflow: need {n} bytes at offset {off}, have {len(buf) - off}")


def decode_field_value(buf, off):
    """Декодирует одно поле таблицы. Возвращает (тип, значение, новое смещение)."""
    _need(buf, off, 1)
    t = chr(buf[off])
    off += 1
    if t == "t":
        _need(buf, off, 1)
        return t, buf[off] != 0, off + 1
    fmt = _NUM_FORMATS.get(t)
    if fmt is not None:
        size = _NUM_SIZES[t]
        _need(buf, off, size)
        return t, struct.unpack_from(fmt, buf, off)[0], off + size
    if t == "D":
        _need(buf, off, 5)
        scale = buf[off]
        raw = struct.unpack_from(">i", buf, off + 1)[0]  # значение со знаком (int32)
        return t, decimal.Decimal(raw).scaleb(-scale), off + 5
    if t in ("S", "x"):
        _need(buf, off, 4)
        n = struct.unpack_from(">I", buf, off)[0]
        off += 4
        _need(buf, off, n)
        raw = bytes(buf[off:off + n])
        off += n
        if t == "S":
            try:
                return t, raw.decode("utf-8"), off
            except UnicodeDecodeError:
                return t, raw, off
        return t, raw, off
    if t == "A":
        _need(buf, off, 4)
        n = struct.unpack_from(">I", buf, off)[0]
        off += 4
        _need(buf, off, n)
        end = off + n
        items = []
        while off < end:
            vt, vv, off = decode_field_value(buf, off)
            items.append((vt, vv))
        if off != end:
            raise ProtocolError("field array length mismatch")
        return t, items, off
    if t == "F":
        tbl, off = decode_table(buf, off)
        return t, tbl, off
    if t == "V":
        return t, None, off
    raise ProtocolError(f"unknown field type {t!r}")


def decode_table(buf, off=0):
    """Декодирует таблицу полей. Возвращает ({ключ: (тип, значение)}, новое смещение)."""
    _need(buf, off, 4)
    n = struct.unpack_from(">I", buf, off)[0]
    off += 4
    _need(buf, off, n)
    end = off + n
    result = {}
    while off < end:
        klen = buf[off]
        off += 1
        _need(buf, off, klen)
        key = bytes(buf[off:off + klen]).decode("utf-8", "replace")
        off += klen
        t, v, off = decode_field_value(buf, off)
        result[key] = (t, v)
    if off != end:
        raise ProtocolError("field table length mismatch")
    return result, end


def untype_value(t, v):
    if t == "F":
        return untype_table(v)
    if t == "A":
        return [untype_value(a, b) for a, b in v]
    return v


def untype_table(tbl):
    """Типизированная таблица {k: (t, v)} -> обычный dict (Decimal для 'D', bytes для 'x')."""
    if not tbl:
        return {}
    return {k: untype_value(t, v) for k, (t, v) in tbl.items()}


def _to_bytes(s):
    if isinstance(s, str):
        return s.encode("utf-8", "surrogateescape")
    return bytes(s)


def encode_shortstr(s):
    b = _to_bytes(s)
    if len(b) > 255:
        raise ValueError(f"shortstr is too long ({len(b)} bytes)")
    return bytes((len(b),)) + b


def encode_longstr(s):
    b = _to_bytes(s)
    return struct.pack(">I", len(b)) + b


def _decimal_parts(d):
    d = decimal.Decimal(d)
    sign, digits, exp = d.as_tuple()
    if not isinstance(exp, int):
        raise ValueError("NaN/Infinity cannot be encoded as AMQP decimal")
    value = int("".join(map(str, digits)) or "0")
    if sign:
        value = -value
    if exp > 0:
        value *= 10 ** exp
        scale = 0
    else:
        scale = -exp
    if scale > 255 or not -2 ** 31 <= value < 2 ** 31:
        raise ValueError(f"decimal {d} does not fit AMQP decimal (scale octet + int32)")
    return scale, value


def _encode_typed(t, v):
    if t == "t":
        return b"t" + (b"\x01" if v else b"\x00")
    fmt = _NUM_FORMATS.get(t)
    if fmt is not None:
        return t.encode() + struct.pack(fmt, v)
    if t == "D":
        scale, value = v if isinstance(v, tuple) else _decimal_parts(v)
        return b"D" + struct.pack(">Bi", scale, value)
    if t in ("S", "x"):
        return t.encode() + encode_longstr(v)
    if t == "A":
        body = b"".join(encode_field_value(x) for x in v)
        return b"A" + struct.pack(">I", len(body)) + body
    if t == "F":
        return b"F" + encode_table(v)
    if t == "V":
        return b"V"
    raise ValueError(f"unknown field type {t!r}")


def encode_field_value(v):
    if isinstance(v, Typed):
        return _encode_typed(v.type, v.value)
    if isinstance(v, bool):
        return b"t" + (b"\x01" if v else b"\x00")
    if isinstance(v, int):
        if -2 ** 31 <= v < 2 ** 31:
            return b"I" + struct.pack(">i", v)
        return b"l" + struct.pack(">q", v)
    if isinstance(v, float):
        return b"d" + struct.pack(">d", v)
    if isinstance(v, decimal.Decimal):
        return _encode_typed("D", v)
    if isinstance(v, str):
        return b"S" + encode_longstr(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return b"S" + encode_longstr(bytes(v))
    if isinstance(v, dict):
        return b"F" + encode_table(v)
    if isinstance(v, (list, tuple)):
        body = b"".join(encode_field_value(x) for x in v)
        return b"A" + struct.pack(">I", len(body)) + body
    if v is None:
        return b"V"
    if isinstance(v, _dt.datetime):
        return b"T" + struct.pack(">Q", int(v.timestamp()))
    raise TypeError(f"cannot encode {type(v).__name__} as AMQP field")


def encode_table(d):
    """Обычный dict (значения - python-типы или Typed) -> байты таблицы AMQP."""
    if not d:
        return b"\x00\x00\x00\x00"
    body = b"".join(encode_shortstr(k) + encode_field_value(v) for k, v in d.items())
    return struct.pack(">I", len(body)) + body


# ---------------------------------------------------------------------------
# Методы AMQP 0-9-1
# ---------------------------------------------------------------------------

_METHOD_DEFS = """
10 10 connection.start version_major:octet version_minor:octet server_properties:table mechanisms:longstr locales:longstr
10 11 connection.start-ok client_properties:table mechanism:shortstr response:longstr locale:shortstr
10 20 connection.secure challenge:longstr
10 21 connection.secure-ok response:longstr
10 30 connection.tune channel_max:short frame_max:long heartbeat:short
10 31 connection.tune-ok channel_max:short frame_max:long heartbeat:short
10 40 connection.open virtual_host:shortstr reserved_1:shortstr reserved_2:bit
10 41 connection.open-ok reserved_1:shortstr
10 50 connection.close reply_code:short reply_text:shortstr class_id:short method_id:short
10 51 connection.close-ok
10 60 connection.blocked reason:shortstr
10 61 connection.unblocked
10 70 connection.update-secret new_secret:longstr reason:shortstr
10 71 connection.update-secret-ok
20 10 channel.open reserved_1:shortstr
20 11 channel.open-ok reserved_1:longstr
20 20 channel.flow active:bit
20 21 channel.flow-ok active:bit
20 40 channel.close reply_code:short reply_text:shortstr class_id:short method_id:short
20 41 channel.close-ok
40 10 exchange.declare reserved_1:short exchange:shortstr type:shortstr passive:bit durable:bit auto_delete:bit internal:bit no_wait:bit arguments:table
40 11 exchange.declare-ok
40 20 exchange.delete reserved_1:short exchange:shortstr if_unused:bit no_wait:bit
40 21 exchange.delete-ok
40 30 exchange.bind reserved_1:short destination:shortstr source:shortstr routing_key:shortstr no_wait:bit arguments:table
40 31 exchange.bind-ok
40 40 exchange.unbind reserved_1:short destination:shortstr source:shortstr routing_key:shortstr no_wait:bit arguments:table
40 51 exchange.unbind-ok
50 10 queue.declare reserved_1:short queue:shortstr passive:bit durable:bit exclusive:bit auto_delete:bit no_wait:bit arguments:table
50 11 queue.declare-ok queue:shortstr message_count:long consumer_count:long
50 20 queue.bind reserved_1:short queue:shortstr exchange:shortstr routing_key:shortstr no_wait:bit arguments:table
50 21 queue.bind-ok
50 30 queue.purge reserved_1:short queue:shortstr no_wait:bit
50 31 queue.purge-ok message_count:long
50 40 queue.delete reserved_1:short queue:shortstr if_unused:bit if_empty:bit no_wait:bit
50 41 queue.delete-ok message_count:long
50 50 queue.unbind reserved_1:short queue:shortstr exchange:shortstr routing_key:shortstr arguments:table
50 51 queue.unbind-ok
60 10 basic.qos prefetch_size:long prefetch_count:short global:bit
60 11 basic.qos-ok
60 20 basic.consume reserved_1:short queue:shortstr consumer_tag:shortstr no_local:bit no_ack:bit exclusive:bit no_wait:bit arguments:table
60 21 basic.consume-ok consumer_tag:shortstr
60 30 basic.cancel consumer_tag:shortstr no_wait:bit
60 31 basic.cancel-ok consumer_tag:shortstr
60 40 basic.publish reserved_1:short exchange:shortstr routing_key:shortstr mandatory:bit immediate:bit
60 50 basic.return reply_code:short reply_text:shortstr exchange:shortstr routing_key:shortstr
60 60 basic.deliver consumer_tag:shortstr delivery_tag:longlong redelivered:bit exchange:shortstr routing_key:shortstr
60 70 basic.get reserved_1:short queue:shortstr no_ack:bit
60 71 basic.get-ok delivery_tag:longlong redelivered:bit exchange:shortstr routing_key:shortstr message_count:long
60 72 basic.get-empty reserved_1:shortstr
60 80 basic.ack delivery_tag:longlong multiple:bit
60 90 basic.reject delivery_tag:longlong requeue:bit
60 100 basic.recover-async requeue:bit
60 110 basic.recover requeue:bit
60 111 basic.recover-ok
60 120 basic.nack delivery_tag:longlong multiple:bit requeue:bit
85 10 confirm.select nowait:bit
85 11 confirm.select-ok
90 10 tx.select
90 11 tx.select-ok
90 20 tx.commit
90 21 tx.commit-ok
90 30 tx.rollback
90 31 tx.rollback-ok
"""

METHODS = {}      # (class_id, method_id) -> (name, fields)
METHOD_IDS = {}   # name -> (class_id, method_id)
for _line in _METHOD_DEFS.strip().splitlines():
    _parts = _line.split()
    _key = (int(_parts[0]), int(_parts[1]))
    _fields = tuple(tuple(p.split(":")) for p in _parts[3:])
    METHODS[_key] = (_parts[2], _fields)
    METHOD_IDS[_parts[2]] = _key
del _line, _parts, _key, _fields

_DEFAULTS = {"octet": 0, "short": 0, "long": 0, "longlong": 0, "timestamp": 0,
             "shortstr": "", "longstr": b"", "table": None, "bit": False}


def _decode_simple(typ, buf, off):
    if typ == "octet":
        _need(buf, off, 1)
        return buf[off], off + 1
    if typ == "short":
        _need(buf, off, 2)
        return struct.unpack_from(">H", buf, off)[0], off + 2
    if typ == "long":
        _need(buf, off, 4)
        return struct.unpack_from(">I", buf, off)[0], off + 4
    if typ in ("longlong", "timestamp"):
        _need(buf, off, 8)
        return struct.unpack_from(">Q", buf, off)[0], off + 8
    if typ == "shortstr":
        _need(buf, off, 1)
        n = buf[off]
        off += 1
        _need(buf, off, n)
        return bytes(buf[off:off + n]).decode("utf-8", "surrogateescape"), off + n
    if typ == "longstr":
        _need(buf, off, 4)
        n = struct.unpack_from(">I", buf, off)[0]
        off += 4
        _need(buf, off, n)
        return bytes(buf[off:off + n]), off + n
    if typ == "table":
        return decode_table(buf, off)
    raise ValueError(f"unknown argument type {typ}")


def _encode_simple(typ, v):
    if typ == "octet":
        return struct.pack(">B", v)
    if typ == "short":
        return struct.pack(">H", v)
    if typ == "long":
        return struct.pack(">I", v)
    if typ in ("longlong", "timestamp"):
        if isinstance(v, _dt.datetime):
            v = int(v.timestamp())
        return struct.pack(">Q", v)
    if typ == "shortstr":
        return encode_shortstr(v)
    if typ == "longstr":
        return encode_longstr(v)
    if typ == "table":
        return encode_table(v or {})
    raise ValueError(f"unknown argument type {typ}")


def decode_method_args(fields, buf, off):
    args = {}
    bit_byte = 0
    bit_pos = 8
    for name, typ in fields:
        if typ == "bit":
            if bit_pos >= 8:
                _need(buf, off, 1)
                bit_byte = buf[off]
                off += 1
                bit_pos = 0
            args[name] = bool(bit_byte & (1 << bit_pos))
            bit_pos += 1
            continue
        bit_pos = 8
        args[name], off = _decode_simple(typ, buf, off)
    return args


def decode_method(payload):
    """payload кадра METHOD -> (имя, аргументы). Таблицы - типизированные {k: (t, v)}."""
    if len(payload) < 4:
        raise ProtocolError("method frame is too short")
    cls, mid = struct.unpack_from(">HH", payload, 0)
    spec = METHODS.get((cls, mid))
    if spec is None:
        raise ProtocolError(f"unknown method {cls}.{mid}")
    return spec[0], decode_method_args(spec[1], payload, 4)


def encode_method(name, args=None):
    args = args or {}
    cls, mid = METHOD_IDS[name]
    fields = METHODS[(cls, mid)][1]
    out = bytearray(struct.pack(">HH", cls, mid))
    bits = []

    def flush_bits():
        if bits:
            v = 0
            for i, b in enumerate(bits):
                if b:
                    v |= 1 << i
            out.append(v)
            bits.clear()

    for fname, typ in fields:
        val = args.get(fname, _DEFAULTS[typ])
        if typ == "bit":
            bits.append(bool(val))
            if len(bits) == 8:
                flush_bits()
            continue
        flush_bits()
        out += _encode_simple(typ, val)
    flush_bits()
    return bytes(out)


# Свойства basic (бит 15 - content_type ... бит 2 - cluster_id).
BASIC_PROPERTIES = (
    ("content_type", "shortstr"), ("content_encoding", "shortstr"), ("headers", "table"),
    ("delivery_mode", "octet"), ("priority", "octet"), ("correlation_id", "shortstr"),
    ("reply_to", "shortstr"), ("expiration", "shortstr"), ("message_id", "shortstr"),
    ("timestamp", "timestamp"), ("type", "shortstr"), ("user_id", "shortstr"),
    ("app_id", "shortstr"), ("cluster_id", "shortstr"),
)


def decode_properties(buf, off=0):
    """Список свойств content header (после class/weight/body size). headers - типизированная таблица."""
    words = []
    while True:
        _need(buf, off, 2)
        w = struct.unpack_from(">H", buf, off)[0]
        off += 2
        words.append(w)
        if not w & 1:
            break
    flags = words[0]
    props = {}
    for i, (name, typ) in enumerate(BASIC_PROPERTIES):
        if flags & (1 << (15 - i)):
            props[name], off = _decode_simple(typ, buf, off)
    return props


def encode_properties(props):
    """dict свойств (headers - обычный dict или Typed) -> флаги + список свойств."""
    flags = 0
    parts = []
    for i, (name, typ) in enumerate(BASIC_PROPERTIES):
        v = props.get(name)
        if v is None:
            continue
        flags |= 1 << (15 - i)
        parts.append(_encode_simple(typ, v))
    return struct.pack(">H", flags) + b"".join(parts)


def topic_match(pattern, key):
    """Сопоставление ключа маршрутизации с шаблоном topic ('*' - одно слово, '#' - ноль и более)."""
    p = pattern.split(".")
    k = key.split(".")

    @functools.lru_cache(maxsize=None)
    def m(i, j):
        if i == len(p):
            return j == len(k)
        if p[i] == "#":
            return m(i + 1, j) or (j < len(k) and m(i, j + 1))
        if j == len(k):
            return False
        if p[i] == "*" or p[i] == k[j]:
            return m(i + 1, j + 1)
        return False

    return m(0, 0)


def _headers_match(binding_args, headers):
    x_match = binding_args.get("x-match", "all")
    pairs = {k: v for k, v in binding_args.items() if not k.startswith("x-")}
    headers = headers or {}

    def ok(k, v):
        if k not in headers:
            return False
        return v is None or headers[k] == v

    if x_match in ("any", "any-with-x"):
        return any(ok(k, v) for k, v in pairs.items())
    return all(ok(k, v) for k, v in pairs.items())


def _bool_text(v):
    return "true" if v else "false"


def _gen_name(prefix):
    return prefix + base64.urlsafe_b64encode(os.urandom(16)).rstrip(b"=").decode("ascii")


def _trunc_shortstr(text):
    b = text.encode("utf-8", "surrogateescape")
    if len(b) <= 255:
        return text
    return b[:255].decode("utf-8", "ignore")


# ---------------------------------------------------------------------------
# Сертификаты для TLS-режима
# ---------------------------------------------------------------------------

_OPENSSL_CANDIDATES = (
    r"C:\Program Files\Git\usr\bin\openssl.exe",
    r"C:\Program Files\Git\mingw64\bin\openssl.exe",
    "/usr/bin/openssl",
)


def find_openssl():
    """Путь к openssl CLI (PATH, затем openssl из Git for Windows) или None."""
    env = os.environ.get("OPENSSL")
    if env and os.path.exists(env):
        return env
    found = shutil.which("openssl")
    if found:
        return found
    for p in _OPENSSL_CANDIDATES:
        if os.path.exists(p):
            return p
    return None


def make_self_signed_cert(directory, name="server", cn="localhost",
                          san="DNS:localhost,IP:127.0.0.1", days=3, openssl=None, bits=2048):
    """Создает самоподписанный сертификат (CA:TRUE, serverAuth) через openssl CLI.

    Возвращает (путь к cert.pem, путь к key.pem). Сертификат одновременно служит
    доверенным корнем: его PEM можно передать клиенту как CA-файл."""
    openssl = openssl or find_openssl()
    if not openssl:
        raise FileNotFoundError("openssl CLI not found (PATH, Git for Windows)")
    os.makedirs(directory, exist_ok=True)
    cfg = os.path.join(directory, f"{name}.cnf")
    cert = os.path.join(directory, f"{name}.pem")
    key = os.path.join(directory, f"{name}.key")
    san_line = ", ".join(s.strip() for s in san.split(","))
    with open(cfg, "w", encoding="ascii") as f:
        f.write(
            "[req]\n"
            "distinguished_name = dn\n"
            "x509_extensions = v3\n"
            "prompt = no\n"
            "[dn]\n"
            f"CN = {cn}\n"
            "O = PinkRabbitMQ mock broker\n"
            "[v3]\n"
            "basicConstraints = critical, CA:TRUE\n"
            "keyUsage = critical, digitalSignature, keyEncipherment, keyCertSign\n"
            "extendedKeyUsage = serverAuth\n"
            f"subjectAltName = {san_line}\n"
            "subjectKeyIdentifier = hash\n"
        )
    cmd = [openssl, "req", "-x509", "-newkey", f"rsa:{int(bits)}", "-nodes", "-sha256",
           "-days", str(days), "-keyout", key, "-out", cert, "-config", cfg]
    env = dict(os.environ)
    env["MSYS_NO_PATHCONV"] = "1"
    env["MSYS2_ARG_CONV_EXCL"] = "*"
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
                          timeout=60)
    if proc.returncode != 0 or not os.path.exists(cert):
        raise RuntimeError(f"openssl failed ({proc.returncode}): "
                           f"{proc.stdout.decode('utf-8', 'replace')}")
    return cert, key


# ---------------------------------------------------------------------------
# Состояние брокера
# ---------------------------------------------------------------------------

class _Message:
    __slots__ = ("body", "props_raw", "props", "exchange", "routing_key", "redelivered",
                 "expires_at", "expiration_ms", "priority")

    def __init__(self, body, props_raw, props, exchange, routing_key):
        self.body = body
        self.props_raw = props_raw
        self.props = props
        self.exchange = exchange
        self.routing_key = routing_key
        self.redelivered = False
        self.expires_at = None
        self.expiration_ms = None
        pr = props.get("priority")
        self.priority = pr if isinstance(pr, int) else 0

    def clone(self):
        m = _Message.__new__(_Message)
        for s in _Message.__slots__:
            setattr(m, s, getattr(self, s))
        return m


class _Queue:
    def __init__(self, name, durable, exclusive_owner, auto_delete, args):
        self.name = name
        self.durable = bool(durable)
        self.exclusive_owner = exclusive_owner
        self.auto_delete = bool(auto_delete)
        self.args = dict(args)
        mp = self.args.get("x-max-priority")
        self.max_priority = max(0, min(255, int(mp))) if mp else 0
        ttl = self.args.get("x-message-ttl")
        self.message_ttl = int(ttl) if ttl is not None else None
        self.consumers = []
        self.rr = 0
        self.had_consumer = False
        self.expired = 0
        self._buckets = [collections.deque() for _ in range(self.max_priority + 1)]
        self._count = 0

    def _bucket(self, msg):
        if not self.max_priority:
            return self._buckets[0]
        return self._buckets[max(0, min(self.max_priority, msg.priority))]

    def push(self, msg, now):
        ttls = [t for t in (self.message_ttl, msg.expiration_ms) if t is not None]
        msg.expires_at = now + min(ttls) / 1000.0 if ttls else None
        self._bucket(msg).append(msg)
        self._count += 1

    def push_front(self, msg):
        self._bucket(msg).appendleft(msg)
        self._count += 1

    def pop(self, now):
        for b in reversed(self._buckets):
            while b:
                m = b.popleft()
                self._count -= 1
                if m.expires_at is not None and m.expires_at < now:
                    self.expired += 1
                    continue
                return m
        return None

    def drop_expired(self, now):
        for b in self._buckets:
            keep = [m for m in b if m.expires_at is None or m.expires_at >= now]
            dropped = len(b) - len(keep)
            if dropped:
                self.expired += dropped
                self._count -= dropped
                b.clear()
                b.extend(keep)

    def snapshot(self):
        out = []
        for b in reversed(self._buckets):
            out.extend(b)
        return out

    def purge(self):
        n = self._count
        for b in self._buckets:
            b.clear()
        self._count = 0
        return n

    def __len__(self):
        return self._count


class _Exchange:
    def __init__(self, name, type_, durable=True, auto_delete=False, internal=False, args=None):
        self.name = name
        self.type = type_
        self.durable = bool(durable)
        self.auto_delete = bool(auto_delete)
        self.internal = bool(internal)
        self.args = dict(args or {})
        self.bindings = []  # (kind "queue"|"exchange", destination, routing_key, args)


class _Consumer:
    def __init__(self, tag, queue, channel, no_ack, exclusive, prefetch, args):
        self.tag = tag
        self.queue = queue
        self.channel = channel
        self.no_ack = bool(no_ack)
        self.exclusive = bool(exclusive)
        self.prefetch = prefetch
        self.args = args
        self.unacked = 0
        self.max_unacked = 0
        self.delivered = 0
        self.acked = 0
        self.rejected = 0
        self.active = True
        self.cancel_reason = None


_Unacked = collections.namedtuple("_Unacked", "msg queue consumer")


class _Content:
    __slots__ = ("exchange", "routing_key", "mandatory", "seq", "header", "body_size",
                 "props_raw", "props", "body")

    def __init__(self, exchange, routing_key, mandatory, seq):
        self.exchange = exchange
        self.routing_key = routing_key
        self.mandatory = mandatory
        self.seq = seq
        self.header = False
        self.body_size = 0
        self.props_raw = b"\x00\x00"
        self.props = {}
        self.body = bytearray()


class _Channel:
    def __init__(self, conn, cid):
        self.conn = conn
        self.id = cid
        self.state = "open"            # open | closing (сервер отправил channel.close)
        self.consumers = {}            # tag -> _Consumer
        self.unacked = {}              # delivery tag -> _Unacked (упорядочено по тегу)
        self.next_tag = 1
        self.prefetch_consumer = 0     # basic.qos global=false: для новых потребителей
        self.prefetch_global = 0       # basic.qos global=true: на канал
        self.confirm = False
        self.publish_seq = 0
        self.pending_confirms = []
        self.tx = False
        self.tx_publishes = []
        self.content = None
        self.last_queue = None
        self.flow_active = True


class _Conn:
    def __init__(self, cid, sock, peer, offered_heartbeat):
        self.id = cid
        self.sock = sock
        self.peer = f"{peer[0]}:{peer[1]}"
        # header -> start_sent -> tune_sent -> open_wait -> open ; closing (сервер отправил close);
        # closed_by_client (отправлен close-ok) ; closed
        self.state = "header"
        self.closed = False
        self.cleaned = False
        self.inbuf = bytearray()
        self.outbuf = bytearray()
        self.channels = {}
        self.offered_heartbeat = offered_heartbeat
        self.heartbeat = 0
        self.frame_max = 0
        self.channel_max = 0
        now = time.monotonic()
        self.created = now
        self.last_recv = now
        self.last_send = now
        self.user = None
        self.vhost = None
        self.client_properties = {}
        self.capabilities = {}
        self.stats = collections.Counter()
        self.read_paused = False
        self.blocked_notified = False
        self.close_deadline = None
        self.close_after_flush = False
        self.close_reason = None
        self.paused_channels = {}      # channel -> время возобновления (delay_method)
        self.backlog = {}              # channel -> deque[(type, payload, skip_delay)]
        self.reg_ev = 0
        # TLS: SSLObject поверх MemoryBIO; outbuf - открытый текст, rawout - зашифрованные байты
        self.tls = None
        self.tls_in = None
        self.tls_out = None
        self.tls_done = False
        self.rawout = bytearray()


class _Listener:
    def __init__(self, sock):
        self.sock = sock
        self.reg_ev = 0


# ---------------------------------------------------------------------------
# Брокер
# ---------------------------------------------------------------------------

class MockBroker:
    """
    Mock-брокер AMQP 0-9-1.

    Параметры:
        host, port            - адрес прослушивания (port=0 - свободный порт, см. .port)
        user, password        - единственная учетная запись (PLAIN/AMQPLAIN)
        vhost                 - разрешенный vhost (None - любой)
        heartbeat             - интервал heartbeat, предлагаемый в connection.tune
        frame_max, channel_max
        client_timeout_factor - сокет закрывается, если от клиента нет трафика
                                heartbeat * factor секунд (RabbitMQ: примерно 2..3)
        handshake_timeout     - таймаут рукопожатия AMQP, с
        confirm_multiple      - подтверждать публикации пачкой (basic.ack multiple=true),
                                как это часто делает RabbitMQ
        close_timeout         - сколько ждать connection.close-ok после close от сервера
        tls_cert, tls_key     - PEM-файлы сертификата и ключа: принятые сокеты оборачиваются
                                серверным ssl.SSLContext (AMQPS). См. make_self_signed_cert().
        listen_ipv6_loopback  - при host="127.0.0.1" слушать и [::1] на том же порту
                                ("localhost" на Windows разрешается в ::1 первым)
        tls_security_level    - уровень безопасности OpenSSL серверного контекста (@SECLEVEL);
                                0 позволяет загрузить слабый ключ, например RSA-1024
    """

    def __init__(self, host="127.0.0.1", port=0, user="guest", password="guest", vhost="/",
                 heartbeat=60, frame_max=131072, channel_max=2047, *,
                 client_timeout_factor=1.0, handshake_timeout=10.0, confirm_multiple=False,
                 close_timeout=2.0, tls_cert=None, tls_key=None, listen_ipv6_loopback=True,
                 tls_security_level=None):
        self.host = host
        self._req_port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.heartbeat = int(heartbeat)
        self.frame_max = int(frame_max)
        self.channel_max = int(channel_max)
        self.client_timeout_factor = float(client_timeout_factor)
        self.handshake_timeout = float(handshake_timeout)
        self.confirm_multiple = bool(confirm_multiple)
        self.close_timeout = float(close_timeout)
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self._ssl_ctx = None
        if tls_cert:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            if tls_security_level is not None:
                # например 0 - чтобы сервер мог загрузить слабый ключ (RSA-1024) для тестов отказа
                ctx.set_ciphers(f"DEFAULT:@SECLEVEL={int(tls_security_level)}")
            ctx.load_cert_chain(tls_cert, tls_key)
            self._ssl_ctx = ctx

        self._lock = threading.RLock()
        self._commands = collections.deque()
        self._thread = None
        self._running = False
        self._sel = None

        self._listeners = []
        self.listen_ipv6_loopback = bool(listen_ipv6_loopback)
        self._wake_r = self._wake_w = None
        self._port = None

        self._conns = {}
        self._next_conn_id = 1
        self._queues = {}
        self._exchanges = {}
        self._all_consumers = collections.deque(maxlen=10000)
        self._install_default_exchanges()

        self._t0 = time.monotonic()
        self._events = []
        self._stats = collections.Counter()
        self._published = collections.deque(maxlen=5000)

        self._frozen = False
        self._blocked = False
        self._block_reason = ""
        self._block_mode = "on_publish"
        self._block_notify = None
        self._delays = {}
        self._nack_budget = 0

    # -- жизненный цикл ------------------------------------------------------

    @property
    def port(self):
        return self._port

    @staticmethod
    def _listen_socket(family, host, port):
        s = socket.socket(family, socket.SOCK_STREAM)
        try:
            if os.name != "nt":
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if family == socket.AF_INET6:
                s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            s.bind((host, port))
            s.listen(128)
            s.setblocking(False)
            return s
        except OSError:
            s.close()
            raise

    def start(self):
        family = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        want_v6 = self.listen_ipv6_loopback and self.host == "127.0.0.1" and socket.has_ipv6
        for attempt in range(10):
            lsock = self._listen_socket(family, self.host, self._req_port)
            port = lsock.getsockname()[1]
            socks = [lsock]
            if want_v6:
                # "localhost" может разрешиться в ::1 первым: слушаем и его на том же порту,
                # как RabbitMQ по умолчанию слушает оба стека.
                try:
                    socks.append(self._listen_socket(socket.AF_INET6, "::1", port))
                except OSError as e:
                    if self._req_port == 0 and attempt < 9:
                        lsock.close()
                        continue
                    self._event(f"cannot listen on [::1]:{port}: {e!r}")
            break

        self._listeners = [_Listener(s) for s in socks]
        self._port = port
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self._sel = selectors.DefaultSelector()
        self._sel.register(self._wake_r, selectors.EVENT_READ, "wake")
        self._running = True
        where = ", ".join(f"[{l.sock.getsockname()[0]}]" if l.sock.family == socket.AF_INET6
                          else l.sock.getsockname()[0] for l in self._listeners)
        self._event(f"broker listening on {where}:{self._port} (heartbeat {self.heartbeat}s, "
                    f"frame_max {self.frame_max}{', TLS' if self._ssl_ctx else ''})")
        self._thread = threading.Thread(target=self._run, name=f"MockBroker:{self._port}", daemon=True)
        self._thread.start()
        return self

    def stop(self, timeout=5.0):
        th = self._thread
        if th is None:
            return
        with self._lock:
            self._running = False
        self._wakeup()
        th.join(timeout)
        self._thread = None

    def __enter__(self):
        if self._thread is None:
            self.start()
        return self

    def __exit__(self, *exc):
        self.stop()

    # -- инспекция (снимки под блокировкой) -----------------------------------

    def events(self, with_time=False):
        """Журнал событий брокера (строки; with_time=True - кортежи (секунды от старта, текст))."""
        with self._lock:
            if with_time:
                return list(self._events)
            return [e for _, e in self._events]

    def clear_events(self):
        with self._lock:
            self._events.clear()

    def has_event(self, substring):
        with self._lock:
            return any(substring in e for _, e in self._events)

    def wait_for_event(self, substring, timeout=5.0):
        return self.wait_until(lambda b: b.has_event(substring), timeout)

    def wait_until(self, predicate, timeout=5.0, interval=0.02):
        """Ждет, пока predicate(broker) не станет истинным. Возвращает True/False."""
        deadline = time.monotonic() + timeout
        while True:
            if predicate(self):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(interval)

    def stats(self, conn_index=None):
        """Счетчики: суммарные по всем соединениям (в т.ч. закрытым) или одного открытого соединения.

        Ключи: имена полученных методов ("basic.ack", "confirm.select", "tx.select", ...),
        "sent:<метод>" для отправленных брокером, "heartbeat_from_client", "heartbeat_to_client",
        "messages_published", "messages_delivered", "messages_acked", "messages_rejected",
        "messages_requeued", "body_frames_in", "frames_in", "channel_errors", "missed_heartbeats" и др.
        """
        with self._lock:
            if conn_index is None:
                return collections.Counter(self._stats)
            return collections.Counter(self._open_conns()[conn_index].stats)

    def reset_stats(self):
        with self._lock:
            self._stats.clear()
            for c in self._conns.values():
                c.stats.clear()

    def connection_count(self):
        """Число открытых TCP-соединений."""
        with self._lock:
            return len(self._open_conns())

    def connections(self):
        """Снимок открытых соединений (index - позиция для close_channel/close_connection)."""
        with self._lock:
            result = []
            for i, c in enumerate(self._open_conns()):
                result.append({
                    "index": i, "id": c.id, "peer": c.peer, "state": c.state,
                    "user": c.user, "vhost": c.vhost, "heartbeat": c.heartbeat,
                    "frame_max": c.frame_max, "channel_max": c.channel_max,
                    "client_properties": dict(c.client_properties),
                    "capabilities": dict(c.capabilities),
                    "blocked_notified": c.blocked_notified, "read_paused": c.read_paused,
                    "stats": collections.Counter(c.stats),
                    "channels": {
                        ch.id: {
                            "state": ch.state, "confirm": ch.confirm, "tx": ch.tx,
                            "consumers": list(ch.consumers), "unacked": len(ch.unacked),
                            "prefetch": ch.prefetch_consumer, "prefetch_global": ch.prefetch_global,
                            "next_delivery_tag": ch.next_tag,
                        } for ch in c.channels.values()
                    },
                })
            return result

    def queue_names(self):
        with self._lock:
            return list(self._queues)

    def queue_exists(self, name):
        with self._lock:
            return name in self._queues

    def exchange_exists(self, name):
        with self._lock:
            return name in self._exchanges

    def queue_depth(self, name):
        """Число готовых к выдаче сообщений (без неподтвержденных); None, если очереди нет."""
        with self._lock:
            q = self._queues.get(name)
            if q is None:
                return None
            q.drop_expired(time.monotonic())
            return len(q)

    def queue_info(self, name):
        with self._lock:
            q = self._queues.get(name)
            if q is None:
                return None
            return {
                "name": q.name, "ready": len(q), "unacked": self._unacked_count(name),
                "consumers": len(q.consumers), "durable": q.durable,
                "exclusive": q.exclusive_owner is not None, "auto_delete": q.auto_delete,
                "arguments": dict(q.args), "max_priority": q.max_priority, "expired": q.expired,
            }

    def queue_messages(self, name):
        """Готовые сообщения очереди в порядке выдачи (dict: body, text, headers, properties...)."""
        with self._lock:
            q = self._queues.get(name)
            if q is None:
                raise KeyError(f"queue '{name}' not found")
            return [self._msg_info(m) for m in q.snapshot()]

    def unacked_count(self, queue=None):
        with self._lock:
            return self._unacked_count(queue)

    def consumers(self, queue=None, include_inactive=False):
        """Потребители: tag, queue, no_ack, prefetch, unacked, max_unacked, delivered, acked..."""
        with self._lock:
            src = list(self._all_consumers)
            out = []
            for c in src:
                if not include_inactive and not c.active:
                    continue
                if queue is not None and c.queue != queue:
                    continue
                out.append({
                    "tag": c.tag, "queue": c.queue, "conn": c.channel.conn.id,
                    "channel": c.channel.id, "no_ack": c.no_ack, "exclusive": c.exclusive,
                    "prefetch": c.prefetch, "unacked": c.unacked, "max_unacked": c.max_unacked,
                    "delivered": c.delivered, "acked": c.acked, "rejected": c.rejected,
                    "active": c.active, "cancel_reason": c.cancel_reason,
                })
            return out

    def published_messages(self, clear=False):
        """Последние опубликованные клиентами сообщения (как их декодировал брокер)."""
        with self._lock:
            out = list(self._published)
            if clear:
                self._published.clear()
            return out

    # -- управление и внедрение отказов ----------------------------------------

    def freeze(self, frozen=True):
        """Эмуляция полуоткрытого TCP: не читать, не писать, не слать heartbeat, не принимать
        новые соединения (сокеты остаются открытыми, ядро само завершает TCP handshake)."""
        def fn():
            frozen_ = bool(frozen)
            if self._frozen == frozen_:
                return
            self._frozen = frozen_
            if not frozen_:
                now = time.monotonic()
                for c in self._conns.values():
                    c.last_recv = c.last_send = now
            self._event("broker frozen" if frozen_ else "broker unfrozen")
        self._call(fn)

    def drop_connections(self, rst=False):
        """Закрыть сокеты всех клиентов: rst=True - RST (SO_LINGER 0), иначе FIN. Возвращает число."""
        def fn():
            conns = self._open_conns()
            for c in conns:
                self._close_socket(c, f"dropped by test ({'RST' if rst else 'FIN'})", rst=rst)
            return len(conns)
        return self._call(fn)

    def close_connection(self, conn_index=None, code=320,
                         text="CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"):
        """Штатно закрыть соединение(я) со стороны сервера (connection.close)."""
        def fn():
            conns = self._select_conns(conn_index)
            for c in conns:
                self._server_close_connection(c, code, text)
            return len(conns)
        return self._call(fn)

    def close_channel(self, conn_index=None, channel_id=None, code=406,
                      text="PRECONDITION_FAILED - channel closed by test"):
        """Прислать channel.close. conn_index=None - все соединения, channel_id=None - все каналы."""
        def fn():
            n = 0
            for c in self._select_conns(conn_index):
                ids = [channel_id] if channel_id is not None else list(c.channels)
                for cid in ids:
                    if self._server_close_channel(c, cid, code, text, 0, 0):
                        n += 1
            return n
        return self._call(fn)

    def close_consumer_channels(self, queue=None, code=406,
                                text="PRECONDITION_FAILED - consumer channel closed by test"):
        """Прислать channel.close всем каналам, на которых есть потребители (опционально - очереди)."""
        def fn():
            n = 0
            for c in self._open_conns():
                for ch in list(c.channels.values()):
                    if ch.state != "open":
                        continue
                    if any(queue is None or cons.queue == queue for cons in ch.consumers.values()) \
                            and ch.consumers:
                        if self._server_close_channel(c, ch.id, code, text, 0, 0):
                            n += 1
            return n
        return self._call(fn)

    def cancel_consumers(self, queue=None):
        """Серверная отмена потребителей (basic.cancel от брокера)."""
        def fn():
            n = 0
            for c in list(self._all_consumers):
                if not c.active or (queue is not None and c.queue != queue):
                    continue
                ch = c.channel
                if ch.state == "open" and ch.conn.state == "open":
                    self._send_method(ch.conn, ch.id, "basic.cancel", consumer_tag=c.tag, no_wait=True)
                self._remove_consumer(c, "cancelled by broker (test)")
                n += 1
            return n
        return self._call(fn)

    def block(self, reason="low memory", mode="on_publish", notify=None):
        """Эмуляция resource alarm: connection.blocked всем клиентам с capability connection.blocked
        (notify=True/False - принудительно). mode="on_publish" - как RabbitMQ: чтение соединения
        останавливается на первом basic.publish; mode="all" - сразу перестать читать все соединения."""
        if mode not in ("on_publish", "all"):
            raise ValueError("mode must be 'on_publish' or 'all'")

        def fn():
            self._blocked = True
            self._block_reason = reason
            self._block_mode = mode
            self._block_notify = notify
            self._event(f"broker blocked: {reason} (mode {mode})")
            for c in self._open_conns():
                if c.state == "open":
                    self._notify_blocked(c)
                    if mode == "all":
                        c.read_paused = True
        self._call(fn)

    def unblock(self):
        def fn():
            if not self._blocked:
                return
            self._blocked = False
            self._event("broker unblocked")
            now = time.monotonic()
            for c in self._open_conns():
                if c.blocked_notified:
                    self._send_method(c, 0, "connection.unblocked")
                    c.blocked_notified = False
                if c.read_paused:
                    c.read_paused = False
                    c.last_recv = now
                    self._process_input(c)
        self._call(fn)

    def delay_method(self, method_name, seconds):
        """Задерживать обработку (и ответ) указанного метода клиента, например "queue.declare".
        Последующие кадры того же канала ждут (порядок ответов сохраняется), другие каналы
        работают. seconds <= 0 снимает задержку для новых вызовов."""
        if method_name not in METHOD_IDS:
            raise ValueError(f"unknown AMQP method '{method_name}'")
        with self._lock:
            if seconds and seconds > 0:
                self._delays[method_name] = float(seconds)
                self._event(f"delay for {method_name}: {seconds:g}s")
            elif self._delays.pop(method_name, None) is not None:
                self._event(f"delay for {method_name} removed")

    def clear_delays(self):
        with self._lock:
            self._delays.clear()

    def set_heartbeat(self, seconds):
        """Интервал heartbeat для новых соединений."""
        with self._lock:
            self.heartbeat = int(seconds)

    def nack_publishes(self, count=1):
        """Следующие count публикаций в режиме confirm получат basic.nack (и не попадут в очередь)."""
        with self._lock:
            self._nack_budget = int(count)

    def inject_message(self, queue, body, headers=None, properties=None, redelivered=False,
                       exchange="", routing_key=None):
        """Положить сообщение прямо в очередь (тело - bytes как есть или str в UTF-8)."""
        if isinstance(body, str):
            body = body.encode("utf-8")
        props = dict(properties or {})
        if headers is not None:
            props["headers"] = headers
        raw = encode_properties(props)
        decoded = decode_properties(raw, 0)

        def fn():
            q = self._queues.get(queue)
            if q is None:
                raise KeyError(f"queue '{queue}' not found")
            msg = _Message(bytes(body), raw, decoded, exchange,
                           queue if routing_key is None else routing_key)
            msg.redelivered = bool(redelivered)
            q.push(msg, time.monotonic())
            self._stats["messages_injected"] += 1
            self._dispatch(q)
        self._call(fn)

    def publish(self, exchange, routing_key, body, headers=None, properties=None):
        """Серверная публикация через обменник (маршрутизация как для клиента). Возвращает очереди."""
        if isinstance(body, str):
            body = body.encode("utf-8")
        props = dict(properties or {})
        if headers is not None:
            props["headers"] = headers
        raw = encode_properties(props)
        decoded = decode_properties(raw, 0)

        def fn():
            msg = _Message(bytes(body), raw, decoded, exchange, routing_key)
            queues = self._route(exchange, routing_key, msg)
            now = time.monotonic()
            for qn in queues:
                self._queues[qn].push(msg.clone(), now)
            for qn in queues:
                self._dispatch(self._queues[qn])
            return queues
        return self._call(fn)

    def declare_queue(self, name, durable=False, auto_delete=False, arguments=None):
        """Создать очередь со стороны сервера (без клиента)."""
        def fn():
            if name not in self._queues:
                self._queues[name] = _Queue(name, durable, None, auto_delete, arguments or {})
            return name
        return self._call(fn)

    def delete_queue(self, name):
        def fn():
            q = self._queues.get(name)
            return self._delete_queue(q, "deleted by test") if q is not None else 0
        return self._call(fn)

    def purge_queue(self, name):
        def fn():
            q = self._queues.get(name)
            return q.purge() if q is not None else 0
        return self._call(fn)

    # -- внутреннее: поток ввода-вывода ---------------------------------------

    def _event(self, text):
        self._events.append((time.monotonic() - self._t0, text))
        log.info("%s", text)

    def _wakeup(self):
        try:
            self._wake_w.send(b"\x00")
        except (BlockingIOError, OSError, AttributeError):
            pass

    def _call(self, fn, timeout=15.0):
        th = self._thread
        if th is None or not th.is_alive() or threading.current_thread() is th:
            with self._lock:
                return fn()
        done = threading.Event()
        box = {}

        def wrapper():
            try:
                box["r"] = fn()
            except BaseException as e:  # noqa: BLE001 - пробрасывается в поток теста
                box["e"] = e
            finally:
                done.set()

        with self._lock:
            self._commands.append(wrapper)
        self._wakeup()
        if not done.wait(timeout):
            raise RuntimeError("MockBroker: IO thread did not execute the command in time")
        if "e" in box:
            raise box["e"]
        return box.get("r")

    def _run(self):
        try:
            while True:
                with self._lock:
                    if not self._running:
                        break
                    self._sync_registrations()
                    timeout = self._compute_timeout()
                try:
                    ready = self._sel.select(timeout)
                except (OSError, ValueError) as e:
                    with self._lock:
                        self._event(f"selector error: {e!r}; rebuilding registrations")
                        self._rebuild_selector()
                    continue
                with self._lock:
                    if not self._running:
                        break
                    try:
                        self._iteration(ready)
                    except Exception as e:  # noqa: BLE001 - брокер не должен падать
                        log.exception("mock broker loop error")
                        self._event(f"broker internal loop error: {e!r}")
        finally:
            with self._lock:
                self._shutdown_sockets()

    def _iteration(self, ready):
        for key, _mask in ready:
            if key.data == "wake":
                self._drain_wake()
        while self._commands:
            self._commands.popleft()()
        if self._frozen:
            return
        for key, mask in ready:
            data = key.data
            if data == "wake":
                continue
            if isinstance(data, _Listener):
                self._accept(data.sock)
                continue
            conn = data
            if conn.closed:
                continue
            if mask & selectors.EVENT_READ and not conn.read_paused:
                self._on_readable(conn)
            if not conn.closed and mask & selectors.EVENT_WRITE:
                self._flush(conn)
        self._tick(time.monotonic())
        for conn in list(self._conns.values()):
            if conn.outbuf or conn.rawout or conn.close_after_flush:
                self._flush(conn)

    def _drain_wake(self):
        try:
            while self._wake_r.recv(4096):
                pass
        except (BlockingIOError, OSError):
            pass

    def _set_interest(self, holder, sock, ev, data):
        cur = holder.reg_ev
        if ev == cur:
            return
        try:
            if cur == 0:
                self._sel.register(sock, ev, data)
            elif ev == 0:
                self._sel.unregister(sock)
            else:
                self._sel.modify(sock, ev, data)
            holder.reg_ev = ev
        except (KeyError, ValueError, OSError) as e:
            self._event(f"selector registration error: {e!r}")
            holder.reg_ev = 0
            try:
                self._sel.unregister(sock)
            except (KeyError, ValueError, OSError):
                pass

    def _sync_registrations(self):
        for lst in self._listeners:
            self._set_interest(lst, lst.sock, 0 if self._frozen else selectors.EVENT_READ, lst)
        for conn in self._conns.values():
            if conn.closed:
                continue
            ev = 0
            if not self._frozen:
                if not conn.read_paused and not conn.close_after_flush:
                    ev |= selectors.EVENT_READ
                if conn.rawout or (conn.outbuf and (conn.tls is None or conn.tls_done)):
                    ev |= selectors.EVENT_WRITE
            self._set_interest(conn, conn.sock, ev, conn)

    def _rebuild_selector(self):
        try:
            self._sel.close()
        except Exception:  # noqa: BLE001
            pass
        self._sel = selectors.DefaultSelector()
        self._sel.register(self._wake_r, selectors.EVENT_READ, "wake")
        for lst in self._listeners:
            lst.reg_ev = 0
        for conn in list(self._conns.values()):
            conn.reg_ev = 0
            if conn.sock.fileno() < 0:
                self._close_socket(conn, "socket became invalid")

    def _compute_timeout(self):
        if self._frozen:
            return 1.0
        now = time.monotonic()
        t = now + 1.0
        for c in self._conns.values():
            if c.closed:
                continue
            if c.state in ("header", "start_sent", "tune_sent", "open_wait"):
                t = min(t, c.created + self.handshake_timeout)
            if c.close_deadline is not None:
                t = min(t, c.close_deadline)
            if c.heartbeat > 0 and c.state in ("open_wait", "open", "closing"):
                t = min(t, c.last_send + c.heartbeat / 2.0)
                if not c.read_paused:
                    t = min(t, c.last_recv + c.heartbeat * self.client_timeout_factor)
            for resume in c.paused_channels.values():
                t = min(t, resume)
        return max(0.0, t - now)

    def _shutdown_sockets(self):
        for conn in list(self._conns.values()):
            self._close_socket(conn, "broker stopped")
        for s in [lst.sock for lst in self._listeners] + [self._wake_r, self._wake_w]:
            try:
                if s is not None:
                    s.close()
            except OSError:
                pass
        try:
            self._sel.close()
        except Exception:  # noqa: BLE001
            pass
        self._event("broker stopped")

    def _accept(self, lsock):
        while True:
            try:
                sock, addr = lsock.accept()
            except (BlockingIOError, InterruptedError):
                return
            except OSError as e:
                self._event(f"accept error: {e!r}")
                return
            sock.setblocking(False)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            conn = _Conn(self._next_conn_id, sock, addr, self.heartbeat)
            self._next_conn_id += 1
            if self._ssl_ctx is not None:
                conn.tls_in = ssl.MemoryBIO()
                conn.tls_out = ssl.MemoryBIO()
                conn.tls = self._ssl_ctx.wrap_bio(conn.tls_in, conn.tls_out, server_side=True)
            self._conns[conn.id] = conn
            self._stats["connections_accepted"] += 1
            self._event(f"conn#{conn.id} accepted from {conn.peer}{' (TLS)' if conn.tls else ''}")

    def _on_readable(self, conn):
        try:
            data = conn.sock.recv(262144)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as e:
            self._close_socket(conn, f"socket error on read: {e!r}")
            return
        if not data:
            if conn.state in ("closed_by_client",):
                self._close_socket(conn, "closed by client after close handshake")
            elif conn.tls is not None and not conn.tls_done:
                self._stats["tls_handshake_failures"] += 1
                self._event(f"conn#{conn.id} TLS handshake failed: client closed the connection "
                            f"during handshake (certificate rejected?)")
                self._close_socket(conn, "TLS handshake aborted by client")
            else:
                self._close_socket(conn, f"client unexpectedly closed TCP connection (state {conn.state})")
            return
        conn.last_recv = time.monotonic()
        conn.stats["bytes_in"] += len(data)
        if conn.tls is not None:
            self._tls_feed(conn, data)
            return
        conn.inbuf += data
        self._process_input(conn)

    def _tls_collect(self, conn):
        data = conn.tls_out.read()
        if data:
            conn.rawout += data

    def _tls_feed(self, conn, raw):
        conn.tls_in.write(raw)
        if not conn.tls_done:
            try:
                conn.tls.do_handshake()
            except ssl.SSLWantReadError:
                self._tls_collect(conn)
                return
            except ssl.SSLError as e:
                self._stats["tls_handshake_failures"] += 1
                self._event(f"conn#{conn.id} TLS handshake failed: {e}")
                self._tls_collect(conn)
                conn.close_after_flush = True
                conn.close_reason = f"TLS handshake failed: {e}"
                return
            conn.tls_done = True
            self._stats["tls_handshakes"] += 1
            cipher = conn.tls.cipher()
            self._event(f"conn#{conn.id} TLS handshake completed: {conn.tls.version()} "
                        f"{cipher[0] if cipher else ''}")
        while True:
            try:
                data = conn.tls.read(262144)
            except ssl.SSLWantReadError:
                break
            except ssl.SSLZeroReturnError:
                self._tls_collect(conn)
                self._close_socket(conn, "TLS close_notify from client")
                return
            except ssl.SSLError as e:
                self._close_socket(conn, f"TLS error on read: {e}")
                return
            if not data:
                break
            conn.inbuf += data
        self._tls_collect(conn)
        self._process_input(conn)

    def _process_input(self, conn):
        buf = conn.inbuf
        while not conn.closed and not conn.close_after_flush:
            if conn.state == "header":
                if len(buf) < 8:
                    break
                hdr = bytes(buf[:8])
                del buf[:8]
                if hdr != PROTOCOL_HEADER:
                    self._event(f"conn#{conn.id} bad protocol header {hdr!r}")
                    conn.outbuf += PROTOCOL_HEADER
                    conn.close_after_flush = True
                    conn.close_reason = "bad protocol header"
                    break
                conn.state = "start_sent"
                self._send_method(conn, 0, "connection.start", version_major=0, version_minor=9,
                                  server_properties=self._server_properties(),
                                  mechanisms=b"PLAIN AMQPLAIN", locales=b"en_US")
                continue
            if conn.read_paused or len(buf) < 7:
                break
            ftype, ch, size = struct.unpack_from(">BHI", buf, 0)
            limit = (conn.frame_max or self.frame_max) - 8
            if size > limit:
                buf.clear()
                self._server_close_connection(
                    conn, 501, f"FRAME_ERROR - frame size {size + 8} exceeds frame_max {limit + 8}")
                break
            if len(buf) < size + 8:
                break
            if buf[7 + size] != FRAME_END:
                buf.clear()
                self._server_close_connection(conn, 501, "FRAME_ERROR - invalid frame end marker")
                break
            if (self._blocked and ftype == FRAME_METHOD and conn.state == "open" and size >= 4
                    and buf[7:11] == b"\x00\x3c\x00\x28"):
                conn.read_paused = True
                self._stats["blocked_publish_stalls"] += 1
                self._event(f"conn#{conn.id} blocked: stopped reading at basic.publish "
                            f"(reason: {self._block_reason})")
                break
            payload = bytes(buf[7:7 + size])
            del buf[:size + 8]
            self._on_frame(conn, ftype, ch, payload)
        self._flush_confirms(conn)

    def _on_frame(self, conn, ftype, ch, payload):
        conn.stats["frames_in"] += 1
        self._stats["frames_in"] += 1
        if ftype == FRAME_HEARTBEAT:
            conn.stats["heartbeat_from_client"] += 1
            self._stats["heartbeat_from_client"] += 1
            if ch != 0:
                self._server_close_connection(conn, 501, "FRAME_ERROR - heartbeat frame on non-zero channel")
            return
        if conn.state == "closing":
            # Сервер отправил connection.close: ждем только close-ok (или встречный close).
            if ftype == FRAME_METHOD and ch == 0 and len(payload) >= 4:
                key = struct.unpack_from(">HH", payload, 0)
                if key == (10, 51):
                    self._count(conn, "connection.close-ok")
                    self._close_socket(conn, "close handshake completed (server initiated)")
                elif key == (10, 50):
                    self._count(conn, "connection.close")
                    self._send_method(conn, 0, "connection.close-ok")
                    conn.close_after_flush = True
                    conn.close_reason = "crossed connection.close"
            return
        if ch in conn.paused_channels or ch in conn.backlog:
            conn.backlog.setdefault(ch, collections.deque()).append((ftype, payload, False))
            return
        self._process_frame(conn, ftype, ch, payload, False)

    def _process_frame(self, conn, ftype, ch, payload, skip_delay):
        cls = mid = 0
        try:
            if ftype == FRAME_METHOD:
                if len(payload) < 4:
                    raise ProtocolError("method frame is too short")
                cls, mid = struct.unpack_from(">HH", payload, 0)
                spec = METHODS.get((cls, mid))
                if spec is None:
                    raise AMQPError(540, f"unknown method class {cls} method {mid}", cls, mid)
                name, fields = spec
                if not skip_delay:
                    delay = self._delays.get(name)
                    if delay:
                        conn.paused_channels[ch] = time.monotonic() + delay
                        conn.backlog.setdefault(ch, collections.deque()).appendleft((ftype, payload, True))
                        self._stats["delayed_methods"] += 1
                        self._event(f"conn#{conn.id} ch{ch} {name} delayed by {delay:g}s")
                        return
                args = decode_method_args(fields, payload, 4)
                self._count(conn, name)
                if ch == 0:
                    self._handle_connection_method(conn, name, args)
                else:
                    self._handle_channel_method(conn, ch, name, args, cls, mid)
            elif ftype == FRAME_HEADER:
                self._handle_header(conn, ch, payload)
            elif ftype == FRAME_BODY:
                self._handle_body(conn, ch, payload)
            else:
                raise AMQPError(501, f"unknown frame type {ftype}")
        except AMQPError as e:
            cid = e.class_id or cls
            mid2 = e.method_id or mid
            chan = conn.channels.get(ch)
            if e.soft and ch != 0 and chan is not None and chan.state == "open":
                self._server_close_channel(conn, ch, e.code, e.reply_text, cid, mid2)
            else:
                self._server_close_connection(conn, e.code, e.reply_text, cid, mid2)
        except ProtocolError as e:
            self._server_close_connection(conn, 502, f"SYNTAX_ERROR - {e}", cls, mid)
        except Exception as e:  # noqa: BLE001 - ошибка брокера не должна его ронять
            log.exception("mock broker: error processing frame")
            self._event(f"conn#{conn.id} internal error: {e!r}")
            self._server_close_connection(conn, 541, f"INTERNAL_ERROR - {e!r}", cls, mid)

    def _count(self, conn, key, n=1):
        conn.stats[key] += n
        self._stats[key] += n

    # -- отправка ----------------------------------------------------------------

    def _send_frame(self, conn, ftype, ch, payload):
        if conn.closed:
            return
        out = conn.outbuf
        out += struct.pack(">BHI", ftype, ch, len(payload))
        out += payload
        out.append(FRAME_END)
        conn.last_send = time.monotonic()

    def _send_method(self, conn, ch, name, **args):
        self._send_frame(conn, FRAME_METHOD, ch, encode_method(name, args))
        self._count(conn, "sent:" + name)

    def _send_content(self, conn, ch, method, args, msg):
        self._send_method(conn, ch, method, **args)
        body = msg.body
        self._send_frame(conn, FRAME_HEADER, ch, struct.pack(">HHQ", 60, 0, len(body)) + msg.props_raw)
        chunk = (conn.frame_max or self.frame_max) - 8
        for i in range(0, len(body), chunk):
            self._send_frame(conn, FRAME_BODY, ch, body[i:i + chunk])

    def _flush(self, conn):
        if conn.tls is not None:
            if conn.tls_done and conn.outbuf:
                try:
                    n = conn.tls.write(conn.outbuf)
                except ssl.SSLError as e:
                    self._close_socket(conn, f"TLS error on write: {e}")
                    return
                del conn.outbuf[:n]
            self._tls_collect(conn)
            buf = conn.rawout
        else:
            buf = conn.outbuf
        while buf and not conn.closed:
            n = 0
            err = None
            mv = memoryview(buf)
            try:
                chunk = mv[:1 << 20]
                try:
                    n = conn.sock.send(chunk)
                except (BlockingIOError, InterruptedError):
                    n = 0
                except OSError as e:
                    err = e
                finally:
                    chunk.release()
            finally:
                mv.release()
            if err is not None:
                self._close_socket(conn, f"socket error on write: {err!r}")
                return
            if n <= 0:
                break
            del buf[:n]
            conn.stats["bytes_out"] += n
        if not conn.closed and not conn.outbuf and not conn.rawout and conn.close_after_flush:
            self._close_socket(conn, conn.close_reason or "closed after close handshake")

    # -- закрытие ------------------------------------------------------------------

    def _open_conns(self):
        return [c for c in self._conns.values() if not c.closed]

    def _select_conns(self, conn_index):
        conns = self._open_conns()
        if conn_index is None:
            return conns
        return [conns[conn_index]]

    def _server_close_channel(self, conn, ch_id, code, text, class_id=0, method_id=0):
        chan = conn.channels.get(ch_id)
        if chan is None or chan.state != "open" or conn.closed or conn.state != "open":
            return False
        chan.state = "closing"
        self._count(conn, "channel_errors")
        self._event(f"conn#{conn.id} channel {ch_id} closed by server: {code} {text}")
        self._send_method(conn, ch_id, "channel.close", reply_code=code,
                          reply_text=_trunc_shortstr(text), class_id=class_id, method_id=method_id)
        self._cleanup_channel(conn, chan, f"channel {ch_id} closed by server")
        return True

    def _server_close_connection(self, conn, code, text, class_id=0, method_id=0):
        if conn.closed or conn.state in ("closing", "closed_by_client"):
            return
        if conn.state == "header":
            self._close_socket(conn, text)
            return
        conn.state = "closing"
        self._count(conn, "connection_errors")
        self._event(f"conn#{conn.id} connection closed by server: {code} {text}")
        self._send_method(conn, 0, "connection.close", reply_code=code,
                          reply_text=_trunc_shortstr(text), class_id=class_id, method_id=method_id)
        conn.close_deadline = time.monotonic() + self.close_timeout
        self._cleanup_connection(conn)

    def _close_socket(self, conn, reason, rst=False):
        if conn.closed:
            return
        conn.closed = True
        conn.state = "closed"
        self._cleanup_connection(conn)
        if conn.reg_ev:
            try:
                self._sel.unregister(conn.sock)
            except (KeyError, ValueError, OSError):
                pass
            conn.reg_ev = 0
        try:
            if rst:
                conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _LINGER_RST)
            else:
                try:
                    conn.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            conn.sock.close()
        except OSError:
            pass
        conn.outbuf.clear()
        conn.rawout.clear()
        conn.inbuf.clear()
        self._conns.pop(conn.id, None)
        self._event(f"conn#{conn.id} socket closed: {reason}")

    def _cleanup_channel(self, conn, chan, reason):
        for c in list(chan.consumers.values()):
            self._remove_consumer(c, reason)
        chan.consumers.clear()
        affected = set()
        for tag in reversed(list(chan.unacked)):
            u = chan.unacked[tag]
            if u.consumer is not None:
                u.consumer.unacked -= 1
            q = self._queues.get(u.queue)
            if q is not None:
                u.msg.redelivered = True
                q.push_front(u.msg)
                affected.add(q.name)
                self._count(conn, "messages_requeued")
        chan.unacked.clear()
        chan.content = None
        chan.tx_publishes.clear()
        chan.pending_confirms.clear()
        for name in affected:
            q = self._queues.get(name)
            if q is not None:
                self._dispatch(q)

    def _cleanup_connection(self, conn):
        if conn.cleaned:
            return
        conn.cleaned = True
        chans = list(conn.channels.values())
        for chan in chans:
            chan.state = "closing"
        for chan in chans:
            self._cleanup_channel(conn, chan, f"connection conn#{conn.id} closed")
        conn.channels.clear()
        conn.backlog.clear()
        conn.paused_channels.clear()
        for q in list(self._queues.values()):
            if q.exclusive_owner == conn.id:
                self._delete_queue(q, f"exclusive owner conn#{conn.id} closed")

    # -- таймеры -------------------------------------------------------------------

    def _tick(self, now):
        for conn in list(self._conns.values()):
            if conn.closed:
                continue
            if conn.state in ("header", "start_sent", "tune_sent", "open_wait") and \
                    now - conn.created > self.handshake_timeout:
                self._event(f"conn#{conn.id} handshake timeout ({self.handshake_timeout:g}s) "
                            f"in state {conn.state}")
                self._close_socket(conn, "handshake timeout")
                continue
            if conn.close_deadline is not None and now >= conn.close_deadline:
                self._close_socket(conn, "no connection.close-ok from client in time")
                continue
            hb = conn.heartbeat
            if hb > 0 and conn.state in ("open_wait", "open", "closing"):
                if not conn.read_paused and now - conn.last_recv >= hb * self.client_timeout_factor:
                    self._stats["missed_heartbeats"] += 1
                    self._event(f"conn#{conn.id} missed heartbeats from client, timeout: {hb}s")
                    self._close_socket(conn, "missed heartbeats from client")
                    continue
                if now - conn.last_send >= hb / 2.0:
                    self._send_frame(conn, FRAME_HEARTBEAT, 0, b"")
                    self._count(conn, "heartbeat_to_client")
            if conn.paused_channels:
                for ch, t in list(conn.paused_channels.items()):
                    if now >= t:
                        del conn.paused_channels[ch]
                        self._drain_backlog(conn, ch)
                self._flush_confirms(conn)

    def _drain_backlog(self, conn, ch):
        q = conn.backlog.get(ch)
        while q and ch not in conn.paused_channels and not conn.closed \
                and conn.state not in ("closing", "closed_by_client"):
            ftype, payload, skip = q.popleft()
            self._process_frame(conn, ftype, ch, payload, skip)
        if q is not None and not q:
            conn.backlog.pop(ch, None)

    # -- обработка методов: connection -----------------------------------------------

    def _server_properties(self):
        return {
            "capabilities": {
                "publisher_confirms": True, "exchange_exchange_bindings": True, "basic.nack": True,
                "consumer_cancel_notify": True, "connection.blocked": True,
                "consumer_priorities": True, "authentication_failure_close": True,
                "per_consumer_qos": True, "direct_reply_to": False,
            },
            "cluster_name": f"mock@{self.host}",
            "copyright": "PinkRabbitMQ tests",
            "information": "PinkRabbitMQ test mock broker (Python)",
            "platform": f"Python {sys.version.split()[0]}",
            "product": "RabbitMQ",
            "version": "3.13.0-mock",
        }

    def _handle_connection_method(self, conn, name, a):
        handshake = ("connection.start-ok", "connection.tune-ok", "connection.open",
                     "connection.close", "connection.close-ok", "connection.secure-ok")
        if not name.startswith("connection."):
            raise AMQPError(503, f"method {name} is not allowed on channel 0")
        if conn.state != "open" and name not in handshake:
            raise AMQPError(503, f"unexpected {name} in state {conn.state}")
        handler = getattr(self, "_h_" + name.replace(".", "_").replace("-", "_"), None)
        if handler is None:
            raise AMQPError(540, f"{name} is not implemented")
        handler(conn, a)

    def _h_connection_start_ok(self, conn, a):
        if conn.state != "start_sent":
            raise AMQPError(503, "unexpected connection.start-ok")
        props = untype_table(a["client_properties"])
        conn.client_properties = props
        caps = props.get("capabilities")
        conn.capabilities = caps if isinstance(caps, dict) else {}
        mech = a["mechanism"]
        resp = a["response"]
        user = password = None
        if mech == "PLAIN":
            parts = resp.split(b"\x00")
            if len(parts) >= 3:
                user = parts[-2].decode("utf-8", "replace")
                password = parts[-1].decode("utf-8", "replace")
        elif mech == "AMQPLAIN":
            try:
                tbl, _ = decode_table(struct.pack(">I", len(resp)) + resp, 0)
                plain = untype_table(tbl)
                user, password = str(plain.get("LOGIN")), str(plain.get("PASSWORD"))
            except ProtocolError:
                pass
        else:
            raise AMQPError(503, f"unknown authentication mechanism '{mech}'", 10, 11)
        conn.user = user
        if user != self.user or password != self.password:
            self._stats["login_refused"] += 1
            self._event(f"conn#{conn.id} login refused for user '{user}' (mechanism {mech})")
            text = (f"ACCESS_REFUSED - Login was refused using authentication mechanism {mech}. "
                    f"For details see the broker logfile.")
            if conn.capabilities.get("authentication_failure_close"):
                self._server_close_connection(conn, 403, text, 10, 11)
            else:
                self._close_socket(conn, "login refused")
            return
        conn.state = "tune_sent"
        self._send_method(conn, 0, "connection.tune", channel_max=self.channel_max,
                          frame_max=self.frame_max, heartbeat=conn.offered_heartbeat)

    def _h_connection_secure_ok(self, conn, a):
        raise AMQPError(540, "connection.secure-ok is not supported")

    def _h_connection_tune_ok(self, conn, a):
        if conn.state != "tune_sent":
            raise AMQPError(503, "unexpected connection.tune-ok")
        cm = a["channel_max"] or self.channel_max
        conn.channel_max = min(cm, self.channel_max)
        fm = a["frame_max"] or self.frame_max
        if fm < FRAME_MIN_SIZE:
            raise AMQPError(530, f"frame_max={fm} < {FRAME_MIN_SIZE} min size", 10, 31)
        if fm > self.frame_max:
            raise AMQPError(530, f"frame_max={fm} > {self.frame_max} max size", 10, 31)
        conn.frame_max = fm
        conn.heartbeat = a["heartbeat"]
        now = time.monotonic()
        conn.last_recv = conn.last_send = now
        conn.state = "open_wait"

    def _h_connection_open(self, conn, a):
        if conn.state != "open_wait":
            raise AMQPError(503, "unexpected connection.open")
        vh = a["virtual_host"]
        if self.vhost is not None and vh != self.vhost:
            raise AMQPError(530, f"vhost {vh} not found", 10, 40)
        conn.vhost = vh
        conn.state = "open"
        self._send_method(conn, 0, "connection.open-ok")
        self._event(f"conn#{conn.id} opened: user '{conn.user}', vhost '{vh}', heartbeat "
                    f"{conn.heartbeat}s, frame_max {conn.frame_max}, "
                    f"product '{conn.client_properties.get('product')}'")
        if self._blocked:
            self._notify_blocked(conn)
            if self._block_mode == "all":
                conn.read_paused = True

    def _h_connection_close(self, conn, a):
        self._event(f"conn#{conn.id} closed by client: {a['reply_code']} {a['reply_text']}")
        conn.state = "closed_by_client"
        self._cleanup_connection(conn)
        self._send_method(conn, 0, "connection.close-ok")
        conn.close_after_flush = True
        conn.close_reason = "closed by client (connection.close)"

    def _h_connection_close_ok(self, conn, a):
        pass

    def _h_connection_blocked(self, conn, a):
        pass

    def _h_connection_unblocked(self, conn, a):
        pass

    def _h_connection_update_secret(self, conn, a):
        self._send_method(conn, 0, "connection.update-secret-ok")

    def _notify_blocked(self, conn):
        if conn.blocked_notified or conn.closed:
            return
        want = self._block_notify
        if want is None:
            want = bool(conn.capabilities.get("connection.blocked"))
        if want:
            self._send_method(conn, 0, "connection.blocked", reason=self._block_reason)
            conn.blocked_notified = True
        else:
            self._event(f"conn#{conn.id} not notified about block: client has no "
                        f"'connection.blocked' capability")

    # -- обработка методов: каналы -----------------------------------------------------

    def _handle_channel_method(self, conn, ch_id, name, a, cls, mid):
        if name.startswith("connection."):
            raise AMQPError(503, f"{name} is only allowed on channel 0", cls, mid)
        if conn.state != "open":
            raise AMQPError(504, f"{name} before connection.open-ok", cls, mid)
        chan = conn.channels.get(ch_id)
        if name == "channel.open":
            if chan is not None:
                raise AMQPError(504, "second 'channel.open' seen", cls, mid)
            if ch_id > conn.channel_max:
                raise AMQPError(530, f"channel number {ch_id} exceeds limit {conn.channel_max}", cls, mid)
            conn.channels[ch_id] = _Channel(conn, ch_id)
            self._send_method(conn, ch_id, "channel.open-ok")
            return
        if chan is None:
            raise AMQPError(504, f"expected 'channel.open' on channel {ch_id}", cls, mid)
        if chan.state == "closing":
            if name == "channel.close-ok":
                conn.channels.pop(ch_id, None)
                self._event(f"conn#{conn.id} channel {ch_id} close-ok received")
            elif name == "channel.close":
                self._send_method(conn, ch_id, "channel.close-ok")
                conn.channels.pop(ch_id, None)
            return
        if name == "channel.close":
            chan.state = "closing"
            self._cleanup_channel(conn, chan, f"channel {ch_id} closed by client")
            conn.channels.pop(ch_id, None)
            self._send_method(conn, ch_id, "channel.close-ok")
            if a["reply_code"] not in (0, 200):
                self._event(f"conn#{conn.id} channel {ch_id} closed by client: "
                            f"{a['reply_code']} {a['reply_text']}")
            return
        if name == "channel.close-ok":
            return
        if chan.content is not None:
            raise AMQPError(505, "expected content header for class 60, got non content header "
                                 "frame instead", cls, mid)
        handler = getattr(self, "_h_" + name.replace(".", "_").replace("-", "_"), None)
        if handler is None:
            raise AMQPError(540, f"{name} is not implemented", cls, mid)
        handler(conn, chan, a)

    def _h_channel_flow(self, conn, chan, a):
        if not a["active"]:
            raise AMQPError(540, "active=false")
        chan.flow_active = True
        self._send_method(conn, chan.id, "channel.flow-ok", active=True)

    def _h_channel_flow_ok(self, conn, chan, a):
        pass

    # exchange

    def _get_exchange(self, conn, name):
        ex = self._exchanges.get(name)
        if ex is None:
            raise AMQPError(404, f"no exchange '{name}' in vhost '{conn.vhost}'")
        return ex

    def _h_exchange_declare(self, conn, chan, a):
        name = a["exchange"]
        etype = a["type"]
        ex = self._exchanges.get(name)
        if a["passive"]:
            if ex is None:
                raise AMQPError(404, f"no exchange '{name}' in vhost '{conn.vhost}'")
        else:
            if name == "":
                raise AMQPError(403, "operation not permitted on the default exchange")
            if ex is None:
                if name.startswith("amq."):
                    raise AMQPError(403, f"exchange name '{name}' contains reserved prefix 'amq.*'")
                if etype not in EXCHANGE_TYPES:
                    raise AMQPError(503, f"invalid exchange type '{etype}'")
                self._exchanges[name] = _Exchange(name, etype, a["durable"], a["auto_delete"],
                                                  a["internal"], untype_table(a["arguments"]))
            else:
                where = f"for exchange '{name}' in vhost '{conn.vhost}'"
                if ex.type != etype:
                    raise AMQPError(406, f"inequivalent arg 'type' {where}: received '{etype}' "
                                         f"but current is '{ex.type}'")
                for arg, cur in (("durable", ex.durable), ("auto_delete", ex.auto_delete),
                                 ("internal", ex.internal)):
                    if bool(a[arg]) != cur:
                        raise AMQPError(406, f"inequivalent arg '{arg}' {where}: received "
                                             f"'{_bool_text(a[arg])}' but current is '{_bool_text(cur)}'")
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "exchange.declare-ok")

    def _h_exchange_delete(self, conn, chan, a):
        name = a["exchange"]
        if name == "":
            raise AMQPError(403, "operation not permitted on the default exchange")
        if name.startswith("amq."):
            raise AMQPError(403, f"deletion of system exchange '{name}' in vhost '{conn.vhost}' not allowed")
        ex = self._exchanges.get(name)
        if ex is not None:
            if a["if_unused"] and ex.bindings:
                raise AMQPError(406, f"exchange '{name}' in vhost '{conn.vhost}' in use")
            self._delete_exchange(ex)
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "exchange.delete-ok")

    def _delete_exchange(self, ex):
        self._exchanges.pop(ex.name, None)
        for other in self._exchanges.values():
            other.bindings = [b for b in other.bindings if not (b[0] == "exchange" and b[1] == ex.name)]

    def _h_exchange_bind(self, conn, chan, a):
        if a["source"] == "" or a["destination"] == "":
            raise AMQPError(403, "operation not permitted on the default exchange")
        dst = self._get_exchange(conn, a["destination"])
        src = self._get_exchange(conn, a["source"])
        b = ("exchange", dst.name, a["routing_key"], untype_table(a["arguments"]))
        if b not in src.bindings:
            src.bindings.append(b)
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "exchange.bind-ok")

    def _h_exchange_unbind(self, conn, chan, a):
        dst = self._get_exchange(conn, a["destination"])
        src = self._get_exchange(conn, a["source"])
        b = ("exchange", dst.name, a["routing_key"], untype_table(a["arguments"]))
        if b in src.bindings:
            src.bindings.remove(b)
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "exchange.unbind-ok")

    # queue

    def _queue_name(self, chan, name):
        if name == "":
            if not chan.last_queue:
                raise AMQPError(404, "no previously declared queue")
            return chan.last_queue
        return name

    def _locked_text(self, conn, name):
        return (f"cannot obtain exclusive access to locked queue '{name}' in vhost '{conn.vhost}'. "
                f"It could be originally declared on another connection or the exclusive property "
                f"value does not match that of the original declaration.")

    def _get_queue(self, conn, name):
        q = self._queues.get(name)
        if q is None:
            raise AMQPError(404, f"no queue '{name}' in vhost '{conn.vhost}'")
        if q.exclusive_owner is not None and q.exclusive_owner != conn.id:
            raise AMQPError(405, self._locked_text(conn, name))
        return q

    def _h_queue_declare(self, conn, chan, a):
        name = a["queue"]
        args = untype_table(a["arguments"])
        if a["passive"]:
            q = self._get_queue(conn, self._queue_name(chan, name))
        else:
            if name == "":
                name = _gen_name("amq.gen-")
            elif name.startswith("amq."):
                raise AMQPError(403, f"queue name '{name}' contains reserved prefix 'amq.*'")
            q = self._queues.get(name)
            where = f"for queue '{name}' in vhost '{conn.vhost}'"
            if q is None:
                for key in ("x-max-priority", "x-message-ttl"):
                    if key in args:
                        v = args[key]
                        if isinstance(v, bool) or not isinstance(v, int) or v < 0 or \
                                (key == "x-max-priority" and v > 255):
                            raise AMQPError(406, f"invalid arg '{key}' {where}: {v!r}")
                q = _Queue(name, a["durable"], conn.id if a["exclusive"] else None,
                           a["auto_delete"], args)
                self._queues[name] = q
            else:
                if q.exclusive_owner is not None and q.exclusive_owner != conn.id:
                    raise AMQPError(405, self._locked_text(conn, name))
                if bool(a["exclusive"]) != (q.exclusive_owner is not None):
                    raise AMQPError(405, self._locked_text(conn, name))
                for arg, cur in (("durable", q.durable), ("auto_delete", q.auto_delete)):
                    if bool(a[arg]) != cur:
                        raise AMQPError(406, f"inequivalent arg '{arg}' {where}: received "
                                             f"'{_bool_text(a[arg])}' but current is '{_bool_text(cur)}'")
                for key in EQUIVALENT_QUEUE_ARGS:
                    if args.get(key) != q.args.get(key):
                        raise AMQPError(406, f"inequivalent arg '{key}' {where}: received "
                                             f"{args.get(key)!r} but current is {q.args.get(key)!r}")
        chan.last_queue = q.name
        q.drop_expired(time.monotonic())
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "queue.declare-ok", queue=q.name,
                              message_count=len(q), consumer_count=len(q.consumers))

    def _h_queue_bind(self, conn, chan, a):
        qn = self._queue_name(chan, a["queue"])
        if a["exchange"] == "":
            raise AMQPError(403, "operation not permitted on the default exchange")
        q = self._get_queue(conn, qn)
        ex = self._get_exchange(conn, a["exchange"])
        b = ("queue", q.name, a["routing_key"], untype_table(a["arguments"]))
        if b not in ex.bindings:
            ex.bindings.append(b)
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "queue.bind-ok")

    def _h_queue_unbind(self, conn, chan, a):
        qn = self._queue_name(chan, a["queue"])
        if a["exchange"] == "":
            raise AMQPError(403, "operation not permitted on the default exchange")
        q = self._get_queue(conn, qn)
        ex = self._get_exchange(conn, a["exchange"])
        b = ("queue", q.name, a["routing_key"], untype_table(a["arguments"]))
        if b in ex.bindings:
            ex.bindings.remove(b)
            if ex.auto_delete and not ex.bindings:
                self._delete_exchange(ex)
        self._send_method(conn, chan.id, "queue.unbind-ok")

    def _h_queue_purge(self, conn, chan, a):
        q = self._get_queue(conn, self._queue_name(chan, a["queue"]))
        n = q.purge()
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "queue.purge-ok", message_count=n)

    def _h_queue_delete(self, conn, chan, a):
        qn = self._queue_name(chan, a["queue"])
        q = self._queues.get(qn)
        n = 0
        if q is not None:
            if q.exclusive_owner is not None and q.exclusive_owner != conn.id:
                raise AMQPError(405, self._locked_text(conn, qn))
            if a["if_unused"] and q.consumers:
                raise AMQPError(406, f"queue '{qn}' in vhost '{conn.vhost}' in use")
            if a["if_empty"] and len(q):
                raise AMQPError(406, f"queue '{qn}' in vhost '{conn.vhost}' is not empty")
            n = self._delete_queue(q, f"deleted by conn#{conn.id}")
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "queue.delete-ok", message_count=n)

    def _delete_queue(self, q, reason):
        self._queues.pop(q.name, None)
        for ex in list(self._exchanges.values()):
            before = len(ex.bindings)
            ex.bindings = [b for b in ex.bindings if not (b[0] == "queue" and b[1] == q.name)]
            if ex.auto_delete and before and not ex.bindings and ex.name:
                self._delete_exchange(ex)
        for c in list(q.consumers):
            ch = c.channel
            conn = ch.conn
            if ch.state == "open" and conn.state == "open" and not conn.closed and \
                    conn.capabilities.get("consumer_cancel_notify"):
                self._send_method(conn, ch.id, "basic.cancel", consumer_tag=c.tag, no_wait=True)
            ch.consumers.pop(c.tag, None)
            c.active = False
            c.cancel_reason = f"queue deleted ({reason})"
        q.consumers.clear()
        n = len(q)
        self._event(f"queue '{q.name}' deleted ({reason}), {n} ready messages dropped")
        return n

    # basic

    def _h_basic_qos(self, conn, chan, a):
        if a["prefetch_size"]:
            raise AMQPError(540, f"prefetch_size!=0 ({a['prefetch_size']})")
        if a["global"]:
            chan.prefetch_global = a["prefetch_count"]
        else:
            chan.prefetch_consumer = a["prefetch_count"]
        self._send_method(conn, chan.id, "basic.qos-ok")
        self._after_release(chan, {c.queue for c in chan.consumers.values()})

    def _h_basic_consume(self, conn, chan, a):
        qn = self._queue_name(chan, a["queue"])
        q = self._get_queue(conn, qn)
        tag = a["consumer_tag"] or _gen_name("amq.ctag-")
        if tag in chan.consumers:
            raise AMQPError(530, f"attempt to reuse consumer tag '{tag}'")
        if any(c.exclusive for c in q.consumers) or (a["exclusive"] and q.consumers):
            raise AMQPError(403, f"queue '{qn}' in vhost '{conn.vhost}' in exclusive use")
        c = _Consumer(tag, q.name, chan, a["no_ack"], a["exclusive"], chan.prefetch_consumer,
                      untype_table(a["arguments"]))
        chan.consumers[tag] = c
        q.consumers.append(c)
        q.had_consumer = True
        self._all_consumers.append(c)
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "basic.consume-ok", consumer_tag=tag)
        self._event(f"conn#{conn.id} ch{chan.id} consumer '{tag}' on queue '{qn}' "
                    f"(no_ack={c.no_ack}, prefetch={c.prefetch}, prefetch_global={chan.prefetch_global})")
        self._dispatch(q)

    def _h_basic_cancel(self, conn, chan, a):
        tag = a["consumer_tag"]
        c = chan.consumers.get(tag)
        if c is not None:
            self._remove_consumer(c, "cancelled by client")
        if not a["no_wait"]:
            self._send_method(conn, chan.id, "basic.cancel-ok", consumer_tag=tag)

    def _h_basic_cancel_ok(self, conn, chan, a):
        pass

    def _remove_consumer(self, c, reason):
        c.channel.consumers.pop(c.tag, None)
        if not c.active:
            return
        c.active = False
        c.cancel_reason = reason
        q = self._queues.get(c.queue)
        if q is not None and c in q.consumers:
            q.consumers.remove(c)
            if q.auto_delete and q.had_consumer and not q.consumers:
                self._delete_queue(q, "auto-delete, last consumer gone")

    def _h_basic_publish(self, conn, chan, a):
        exn = a["exchange"]
        if exn != "":
            ex = self._get_exchange(conn, exn)
            if ex.internal:
                raise AMQPError(403, f"cannot publish to internal exchange '{exn}' in vhost '{conn.vhost}'")
        if a["immediate"]:
            raise AMQPError(540, "immediate=true")
        seq = None
        if chan.confirm:
            chan.publish_seq += 1
            seq = chan.publish_seq
        chan.content = _Content(exn, a["routing_key"], a["mandatory"], seq)

    def _handle_header(self, conn, ch_id, payload):
        chan = conn.channels.get(ch_id)
        if chan is None:
            raise AMQPError(504, f"content header on unknown channel {ch_id}")
        if chan.state != "open":
            return
        c = chan.content
        if c is None or c.header:
            raise AMQPError(505, "expected method frame, got non method frame instead")
        if len(payload) < 14:
            raise ProtocolError("content header frame is too short")
        cls, _weight, size = struct.unpack_from(">HHQ", payload, 0)
        if cls != 60:
            raise AMQPError(505, f"content header for class {cls}, expected 60")
        c.props_raw = bytes(payload[12:])
        c.props = decode_properties(payload, 12)
        c.header = True
        c.body_size = size
        self._stats["header_frames_in"] += 1
        if size == 0:
            self._complete_publish(conn, chan)

    def _handle_body(self, conn, ch_id, payload):
        chan = conn.channels.get(ch_id)
        if chan is None:
            raise AMQPError(504, f"content body on unknown channel {ch_id}")
        if chan.state != "open":
            return
        c = chan.content
        if c is None or not c.header:
            raise AMQPError(505, "expected content header, got content body instead")
        c.body += payload
        self._stats["body_frames_in"] += 1
        if len(c.body) > c.body_size:
            raise AMQPError(501, f"content body exceeds declared size {c.body_size}")
        if len(c.body) == c.body_size:
            self._complete_publish(conn, chan)

    def _complete_publish(self, conn, chan):
        c = chan.content
        chan.content = None
        props = c.props
        uid = props.get("user_id")
        if uid is not None and uid != conn.user:
            raise AMQPError(406, f"user_id property set to '{uid}' but authenticated user was "
                                 f"'{conn.user}'", 60, 40)
        exp_ms = None
        exp = props.get("expiration")
        if exp is not None:
            if not str(exp).isdigit():
                raise AMQPError(406, f"invalid expiration '{exp}': no_integer", 60, 40)
            exp_ms = int(exp)
        msg = _Message(bytes(c.body), c.props_raw, props, c.exchange, c.routing_key)
        msg.expiration_ms = exp_ms
        self._count(conn, "messages_published")
        self._record_published(conn, chan, msg, c.seq)
        if chan.tx:
            chan.tx_publishes.append((c, msg))
            return
        nack = c.seq is not None and self._nack_budget > 0
        if nack:
            self._nack_budget -= 1
            self._event(f"conn#{conn.id} ch{chan.id} publish seq {c.seq} nacked (injected)")
        else:
            self._route_and_store(conn, chan, c.exchange, c.routing_key, c.mandatory, msg)
        if c.seq is not None:
            self._confirm(conn, chan, c.seq, not nack)

    def _record_published(self, conn, chan, msg, seq):
        typed = msg.props.get("headers") or {}
        props = dict(msg.props)
        props["headers"] = untype_table(typed) if typed else None
        self._published.append({
            "conn": conn.id, "channel": chan.id, "exchange": msg.exchange,
            "routing_key": msg.routing_key, "body": msg.body, "properties": props,
            "headers": untype_table(typed), "headers_typed": typed, "seq": seq,
            "time": time.monotonic() - self._t0,
        })

    def _confirm(self, conn, chan, seq, ok):
        self._count(conn, "confirms_ack" if ok else "confirms_nack")
        if self.confirm_multiple:
            chan.pending_confirms.append((seq, ok))
        elif ok:
            self._send_method(conn, chan.id, "basic.ack", delivery_tag=seq, multiple=False)
        else:
            self._send_method(conn, chan.id, "basic.nack", delivery_tag=seq, multiple=False, requeue=False)

    def _flush_confirms(self, conn):
        if not self.confirm_multiple or conn.closed:
            return
        for chan in conn.channels.values():
            pend = chan.pending_confirms
            if not pend or chan.state != "open":
                continue
            i = 0
            while i < len(pend):
                seq, ok = pend[i]
                if not ok:
                    self._send_method(conn, chan.id, "basic.nack", delivery_tag=seq, multiple=False,
                                      requeue=False)
                    i += 1
                    continue
                j = i
                while j + 1 < len(pend) and pend[j + 1][1]:
                    j += 1
                self._send_method(conn, chan.id, "basic.ack", delivery_tag=pend[j][0], multiple=j > i)
                i = j + 1
            pend.clear()

    def _route(self, exchange, routing_key, msg):
        if exchange == "":
            return [routing_key] if routing_key in self._queues else []
        ex = self._exchanges.get(exchange)
        if ex is None:
            return []
        headers = None
        result = []
        seen = set()
        visited = set()
        stack = [ex]
        while stack:
            e = stack.pop()
            if e.name in visited:
                continue
            visited.add(e.name)
            for kind, dest, key, bargs in e.bindings:
                if e.type == "direct":
                    ok = key == routing_key
                elif e.type == "fanout":
                    ok = True
                elif e.type == "topic":
                    ok = topic_match(key, routing_key)
                else:
                    if headers is None:
                        headers = untype_table(msg.props.get("headers") or {})
                    ok = _headers_match(bargs, headers)
                if not ok:
                    continue
                if kind == "queue":
                    if dest not in seen and dest in self._queues:
                        seen.add(dest)
                        result.append(dest)
                else:
                    de = self._exchanges.get(dest)
                    if de is not None:
                        stack.append(de)
        return result

    def _route_and_store(self, conn, chan, exchange, routing_key, mandatory, msg):
        queues = self._route(exchange, routing_key, msg)
        now = time.monotonic()
        for qn in queues:
            self._queues[qn].push(msg.clone(), now)
        if queues:
            self._count(conn, "messages_routed")
        else:
            self._count(conn, "messages_unroutable")
            if mandatory:
                self._send_content(conn, chan.id, "basic.return",
                                   dict(reply_code=312, reply_text="NO_ROUTE", exchange=exchange,
                                        routing_key=routing_key), msg)
        for qn in queues:
            q = self._queues.get(qn)
            if q is not None:
                self._dispatch(q)
        return queues

    def _can_deliver(self, c):
        ch = c.channel
        conn = ch.conn
        if not c.active or ch.state != "open" or conn.closed or conn.state != "open" or not ch.flow_active:
            return False
        if c.no_ack:
            return True
        if c.prefetch and c.unacked >= c.prefetch:
            return False
        if ch.prefetch_global and len(ch.unacked) >= ch.prefetch_global:
            return False
        return True

    def _next_consumer(self, q):
        n = len(q.consumers)
        for i in range(n):
            idx = (q.rr + i) % n
            c = q.consumers[idx]
            if self._can_deliver(c):
                q.rr = (idx + 1) % n
                return c
        return None

    def _dispatch(self, q):
        if not q.consumers or not len(q):
            return
        now = time.monotonic()
        while True:
            c = self._next_consumer(q)
            if c is None:
                return
            m = q.pop(now)
            if m is None:
                return
            self._deliver(c, m)

    def _deliver(self, c, msg):
        ch = c.channel
        conn = ch.conn
        tag = ch.next_tag
        ch.next_tag += 1
        if not c.no_ack:
            ch.unacked[tag] = _Unacked(msg, c.queue, c)
            c.unacked += 1
            if c.unacked > c.max_unacked:
                c.max_unacked = c.unacked
        c.delivered += 1
        self._count(conn, "messages_delivered")
        self._send_content(conn, ch.id, "basic.deliver",
                           dict(consumer_tag=c.tag, delivery_tag=tag, redelivered=msg.redelivered,
                                exchange=msg.exchange, routing_key=msg.routing_key), msg)

    def _after_release(self, chan, queue_names):
        names = set(queue_names)
        if chan.prefetch_global:
            names.update(c.queue for c in chan.consumers.values())
        for n in names:
            q = self._queues.get(n)
            if q is not None:
                self._dispatch(q)

    def _collect_tags(self, chan, tag, multiple):
        if multiple and tag == 0:
            return list(chan.unacked)
        if tag not in chan.unacked:
            raise AMQPError(406, f"unknown delivery tag {tag}")
        if multiple:
            return [t for t in chan.unacked if t <= tag]
        return [tag]

    def _settle(self, conn, chan, tag, multiple, kind, requeue):
        tags = self._collect_tags(chan, tag, multiple)
        affected = set()
        back = []
        for t in tags:
            u = chan.unacked.pop(t)
            if u.consumer is not None:
                u.consumer.unacked -= 1
                if kind == "ack":
                    u.consumer.acked += 1
                else:
                    u.consumer.rejected += 1
            affected.add(u.queue)
            if kind != "ack" and requeue:
                back.append(u)
        self._count(conn, "messages_acked" if kind == "ack" else "messages_rejected", len(tags))
        for u in reversed(back):
            q = self._queues.get(u.queue)
            if q is not None:
                u.msg.redelivered = True
                q.push_front(u.msg)
                self._count(conn, "messages_requeued")
        self._after_release(chan, affected)

    def _h_basic_ack(self, conn, chan, a):
        self._settle(conn, chan, a["delivery_tag"], a["multiple"], "ack", False)

    def _h_basic_reject(self, conn, chan, a):
        self._settle(conn, chan, a["delivery_tag"], False, "reject", a["requeue"])

    def _h_basic_nack(self, conn, chan, a):
        self._settle(conn, chan, a["delivery_tag"], a["multiple"], "nack", a["requeue"])

    def _h_basic_get(self, conn, chan, a):
        q = self._get_queue(conn, self._queue_name(chan, a["queue"]))
        m = q.pop(time.monotonic())
        if m is None:
            self._send_method(conn, chan.id, "basic.get-empty")
            return
        tag = chan.next_tag
        chan.next_tag += 1
        if not a["no_ack"]:
            chan.unacked[tag] = _Unacked(m, q.name, None)
        self._count(conn, "messages_delivered")
        self._send_content(conn, chan.id, "basic.get-ok",
                           dict(delivery_tag=tag, redelivered=m.redelivered, exchange=m.exchange,
                                routing_key=m.routing_key, message_count=len(q)), m)

    def _recover(self, conn, chan):
        affected = set()
        for tag in reversed(list(chan.unacked)):
            u = chan.unacked.pop(tag)
            if u.consumer is not None:
                u.consumer.unacked -= 1
            q = self._queues.get(u.queue)
            if q is not None:
                u.msg.redelivered = True
                q.push_front(u.msg)
                affected.add(q.name)
        self._after_release(chan, affected)

    def _h_basic_recover_async(self, conn, chan, a):
        self._recover(conn, chan)

    def _h_basic_recover(self, conn, chan, a):
        self._recover(conn, chan)
        self._send_method(conn, chan.id, "basic.recover-ok")

    def _h_confirm_select(self, conn, chan, a):
        if chan.tx:
            raise AMQPError(406, "cannot switch from tx to confirm mode")
        chan.confirm = True
        if not a["nowait"]:
            self._send_method(conn, chan.id, "confirm.select-ok")

    def _h_tx_select(self, conn, chan, a):
        if chan.confirm:
            raise AMQPError(406, "cannot switch from confirm to tx mode")
        chan.tx = True
        self._send_method(conn, chan.id, "tx.select-ok")

    def _h_tx_commit(self, conn, chan, a):
        if not chan.tx:
            raise AMQPError(406, "channel is not transactional")
        pending = chan.tx_publishes
        chan.tx_publishes = []
        for c, msg in pending:
            self._route_and_store(conn, chan, c.exchange, c.routing_key, c.mandatory, msg)
        self._send_method(conn, chan.id, "tx.commit-ok")

    def _h_tx_rollback(self, conn, chan, a):
        if not chan.tx:
            raise AMQPError(406, "channel is not transactional")
        chan.tx_publishes = []
        self._send_method(conn, chan.id, "tx.rollback-ok")

    # -- прочее --------------------------------------------------------------------

    def _install_default_exchanges(self):
        self._exchanges[""] = _Exchange("", "direct")
        for name, t in (("amq.direct", "direct"), ("amq.fanout", "fanout"), ("amq.topic", "topic"),
                        ("amq.headers", "headers"), ("amq.match", "headers")):
            self._exchanges[name] = _Exchange(name, t)

    def _unacked_count(self, queue):
        n = 0
        for c in self._conns.values():
            for ch in c.channels.values():
                if queue is None:
                    n += len(ch.unacked)
                else:
                    n += sum(1 for u in ch.unacked.values() if u.queue == queue)
        return n

    def _msg_info(self, m):
        typed = m.props.get("headers") or {}
        props = dict(m.props)
        props["headers"] = untype_table(typed) if typed else None
        return {
            "body": m.body, "text": m.body.decode("utf-8", "replace"),
            "headers": untype_table(typed), "headers_typed": typed, "properties": props,
            "redelivered": m.redelivered, "exchange": m.exchange, "routing_key": m.routing_key,
        }


def main(argv=None):
    p = argparse.ArgumentParser(description="Mock AMQP 0-9-1 broker for PinkRabbitMQ tests")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5672)
    p.add_argument("--user", default="guest")
    p.add_argument("--password", default="guest")
    p.add_argument("--vhost", default="/")
    p.add_argument("--any-vhost", action="store_true", help="accept any virtual host")
    p.add_argument("--heartbeat", type=int, default=60)
    p.add_argument("--frame-max", type=int, default=131072)
    p.add_argument("--tls-cert", help="PEM certificate: enable TLS (AMQPS)")
    p.add_argument("--tls-key", help="PEM private key for --tls-cert")
    p.add_argument("--tls-self-signed", metavar="DIR",
                   help="generate a self-signed localhost certificate into DIR and enable TLS")
    p.add_argument("-v", "--verbose", action="store_true", help="print broker events")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(message)s")
    cert, key = args.tls_cert, args.tls_key
    if args.tls_self_signed:
        cert, key = make_self_signed_cert(args.tls_self_signed)
        print(f"self-signed certificate (use as CA file): {cert}", flush=True)
    broker = MockBroker(args.host, args.port, args.user, args.password,
                        None if args.any_vhost else args.vhost, args.heartbeat, args.frame_max,
                        tls_cert=cert, tls_key=key).start()
    print(f"mock broker listening on {broker.host}:{broker.port}", flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        broker.stop()
        st = broker.stats()
        print("stats:", dict(sorted(st.items())), flush=True)


if __name__ == "__main__":
    main()
