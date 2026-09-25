"""
Общие хелперы тестов PinkRabbitMQ на mock-брокере (test_stability.py, test_tls.py).

Особенности стенда (test/addin1c.py + python_test.dll):
- стенд эмулирует исключение 1С, бросая TestError из IAddInDefBase::AddError при
  set_raise(True). Новая сборка вызывает AddError внутри reportException() с catch(...), поэтому
  это исключение глушится и метод просто возвращает False. Хелперы здесь не зависят от этого:
  объекты создаются с set_raise(False), результат каждого вызова проверяется, и при False
  поднимается RuntimeError с текстом последней ошибки (из списка AddError стенда, иначе
  GetLastError);
- выходные параметры передаются списком из одного элемента: [value];
- get_result_int/get_result_uint в DLL возвращают int64/uint64, а ctypes по умолчанию читает
  результат как int32 - здесь restype исправляется, иначе теги доставки > 2^31 усекаются;
- объект компоненты освобождается через DLL.delete_connection; dispose() делает это явно
  и обнуляет com.con, чтобы __del__ не удалил чужой объект с переиспользованным номером.
"""

import ctypes
import gc
import json
import os
import sys
import time
import weakref

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import addin1c  # noqa: E402  (грузит ./python_test.dll из текущего каталога)
from addin1c import Component  # noqa: E402

addin1c.DLL.get_result_int.restype = ctypes.c_longlong
addin1c.DLL.get_result_uint.restype = ctypes.c_ulonglong

MB = 1024 * 1024

# ---------------------------------------------------------------------------
# Жизненный цикл объектов компоненты
# ---------------------------------------------------------------------------

_live = []


def new_component():
    com = Component("PinkRabbitMQ")
    com.set_raise(False)
    _live.append(weakref.ref(com))
    return com


def _error_text(com, n0, name):
    n = addin1c.DLL.errornum(com.con)
    texts = [com._read_string(addin1c.DLL.get_error, [com.con, i]) for i in range(n0, n)]
    if texts:
        return texts[-1]
    try:
        text = com.get_last_error()
    except RuntimeError:
        text = None
    return text or f"{name}: компонента вернула Ложь без текста ошибки"


def call_proc(com, name, *args, **kwargs):
    """call_proc с проверкой результата: False -> RuntimeError(текст ошибки компоненты)."""
    n0 = addin1c.DLL.errornum(com.con)
    ok = com.call_proc(name, *args, **kwargs)
    if not ok:
        raise RuntimeError(_error_text(com, n0, name))


def call_func(com, name, *args, **kwargs):
    """call_func с проверкой результата: возвращает значение функции."""
    n0 = addin1c.DLL.errornum(com.con)
    ok, res = com.call_func(name, *args, **kwargs)
    if not ok:
        raise RuntimeError(_error_text(com, n0, name))
    return res


def dispose(com):
    """Немедленно уничтожить объект компоненты (Done + DestroyObject)."""
    con = getattr(com, "con", 0)
    com.con = 0
    if con and con > 0:
        addin1c.DLL.delete_connection(con)


def dispose_all():
    refs = list(_live)
    _live.clear()
    for r in refs:
        c = r()
        if c is not None:
            dispose(c)
    gc.collect()


def finalize_broker(request, broker):
    """Teardown фикстуры брокера: снять отказы, освободить компоненты, остановить брокер.
    При падении теста печатает хвост журнала брокера."""
    try:
        broker.freeze(False)
        broker.unblock()
        broker.clear_delays()
    except Exception:  # noqa: BLE001
        pass
    try:
        dispose_all()
    finally:
        rep = getattr(request.node, "rep_call", None)
        if rep is not None and rep.failed:
            print("\n--- mock broker events (last 80) ---")
            print("\n".join(broker.events()[-80:]))
            print("--- mock broker stats ---")
            print(dict(sorted(broker.stats().items())))
        broker.stop()


# ---------------------------------------------------------------------------
# Обертки над методами компоненты (НОВЫЙ API, все параметры передаются явно)
# ---------------------------------------------------------------------------

def _j(obj):
    if obj is None or isinstance(obj, str):
        return obj
    return json.dumps(obj, ensure_ascii=False)


def _long_kw(value, index):
    if -2 ** 31 <= value < 2 ** 31:
        return {}
    return {"longs": [index]}


def connect(broker, ping_rate=0, timeout=5, com=None, host="127.0.0.1", login=None,
            password=None, vhost=None, secure=False):
    """Connect(host, port, login, pwd, vhost, pingRate, secure, timeoutSec) к mock-брокеру."""
    if com is None:
        com = new_component()
    call_proc(com, "Connect", host, broker.port,
                  broker.user if login is None else login,
                  broker.password if password is None else password,
                  (broker.vhost or "/") if vhost is None else vhost,
                  ping_rate, secure, timeout)
    return com


def declare_exchange(com, name, type_="direct", passive=False, durable=False, auto_delete=False,
                     args=None):
    call_proc(com, "DeclareExchange", name, type_, passive, durable, auto_delete, _j(args))


def delete_exchange(com, name, if_unused=False):
    call_proc(com, "DeleteExchange", name, if_unused)


def declare_queue(com, name, passive=False, durable=False, exclusive=False, auto_delete=False,
                  max_priority=0, args=None):
    res = call_func(com, "DeclareQueue", name, passive, durable, exclusive, auto_delete,
                           max_priority, _j(args))
    return res


def delete_queue(com, name, if_unused=False, if_empty=False):
    call_proc(com, "DeleteQueue", name, if_unused, if_empty)


def bind_queue(com, queue, exchange, routing_key, args=None):
    call_proc(com, "BindQueue", queue, exchange, routing_key, _j(args))


def unbind_queue(com, queue, exchange, routing_key):
    call_proc(com, "UnbindQueue", queue, exchange, routing_key)


def publish(com, exchange, routing_key, body, living_time=0, persist=False, headers=None,
            wait_confirm=True):
    call_proc(com, "BasicPublish", exchange, routing_key, body, living_time, persist, _j(headers),
                  wait_confirm)


def wait_for_confirms(com, timeout_ms=10000):
    res = call_func(com, "WaitForConfirms", timeout_ms)
    return res


def consume(com, queue, consumer_id="", no_confirm=False, exclusive=False, select_size=200,
            args=None):
    tag = call_func(com, "BasicConsume", queue, consumer_id, no_confirm, exclusive, select_size,
                           _j(args))
    return tag


def consume_message(com, consumer="", timeout_ms=1000):
    """BasicConsumeMessage -> (получено, тело, тег)."""
    body = [""]
    tag = [0]
    got = call_func(com, "BasicConsumeMessage", consumer, body, tag, timeout_ms)
    return bool(got), body[0], tag[0]


def ack(com, tag, multiple=False):
    call_proc(com, "BasicAck", tag, multiple, **_long_kw(tag, 0))


def reject(com, tag, requeue=False):
    call_proc(com, "BasicReject", tag, requeue, **_long_kw(tag, 0))


def cancel(com, consumer_tag=""):
    call_proc(com, "BasicCancel", consumer_tag)


def is_connected(com):
    res = call_func(com, "IsConnected")
    return res


def get_headers(com):
    res = call_func(com, "GetHeaders")
    return json.loads(res) if res else {}


def sleep_native(com, ms):
    call_proc(com, "SleepNative", ms)


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def expect_error(fn, *args, **kwargs):
    """Вызов, который должен завершиться ошибкой компоненты. Возвращает (текст, секунды).
    Исключение не сохраняется (его traceback держал бы ссылки на объект компоненты)."""
    t0 = time.monotonic()
    try:
        res = fn(*args, **kwargs)
    except RuntimeError as e:
        return str(e), time.monotonic() - t0
    pytest.fail(f"ожидалась ошибка компоненты, но {getattr(fn, '__name__', fn)} вернул {res!r} "
                f"за {time.monotonic() - t0:.2f} с")


def wait_until(predicate, timeout=5.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while True:
        if predicate():
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)


def consumer_info(broker, queue):
    cons = broker.consumers(queue=queue)
    return cons[0] if cons else {}


def closed_by_server_events(broker):
    return [e for e in broker.events() if "closed by server" in e]


# ---------------------------------------------------------------------------
# Метрики процесса
# ---------------------------------------------------------------------------

if os.name == "nt":
    from ctypes import wintypes

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _k32.GetCurrentProcess.restype = wintypes.HANDLE
    _k32.GetCurrentProcessId.restype = wintypes.DWORD
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]

    class _PMC_EX(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    class _THREADENTRY32(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
            ("th32ThreadID", wintypes.DWORD), ("th32OwnerProcessID", wintypes.DWORD),
            ("tpBasePri", wintypes.LONG), ("tpDeltaPri", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
        ]

    _k32.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(_PMC_EX), wintypes.DWORD]
    _k32.K32GetProcessMemoryInfo.restype = wintypes.BOOL
    _k32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    _k32.GetProcessTimes.restype = wintypes.BOOL
    _k32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    _k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    _k32.Thread32First.argtypes = [wintypes.HANDLE, ctypes.POINTER(_THREADENTRY32)]
    _k32.Thread32First.restype = wintypes.BOOL
    _k32.Thread32Next.argtypes = [wintypes.HANDLE, ctypes.POINTER(_THREADENTRY32)]
    _k32.Thread32Next.restype = wintypes.BOOL

    def private_bytes():
        """Private bytes (PrivateUsage) текущего процесса."""
        pmc = _PMC_EX()
        pmc.cb = ctypes.sizeof(pmc)
        if not _k32.K32GetProcessMemoryInfo(_k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
            raise ctypes.WinError(ctypes.get_last_error())
        return pmc.PrivateUsage

    def cpu_seconds():
        """Суммарное время CPU процесса (user + kernel), с."""
        c, e, k, u = (wintypes.FILETIME() for _ in range(4))
        if not _k32.GetProcessTimes(_k32.GetCurrentProcess(), ctypes.byref(c), ctypes.byref(e),
                                    ctypes.byref(k), ctypes.byref(u)):
            raise ctypes.WinError(ctypes.get_last_error())

        def ft(x):
            return ((x.dwHighDateTime << 32) | x.dwLowDateTime) / 1e7

        return ft(k) + ft(u)

    def thread_count():
        """Число потоков текущего процесса (Toolhelp32)."""
        snap = _k32.CreateToolhelp32Snapshot(0x00000004, 0)  # TH32CS_SNAPTHREAD
        if snap in (None, wintypes.HANDLE(-1).value):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            pid = _k32.GetCurrentProcessId()
            te = _THREADENTRY32()
            te.dwSize = ctypes.sizeof(te)
            n = 0
            ok = _k32.Thread32First(snap, ctypes.byref(te))
            while ok:
                if te.th32OwnerProcessID == pid:
                    n += 1
                ok = _k32.Thread32Next(snap, ctypes.byref(te))
            return n
        finally:
            _k32.CloseHandle(snap)
else:
    import psutil  # noqa: E402

    def private_bytes():
        mi = psutil.Process().memory_full_info()
        return getattr(mi, "uss", mi.rss)

    def cpu_seconds():
        t = psutil.Process().cpu_times()
        return t.user + t.system

    def thread_count():
        return psutil.Process().num_threads()
