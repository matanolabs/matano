from collections import UserDict
from contextlib import contextmanager
import time
import json
import datetime

class Timers(UserDict):
    def get_timer(self, name: str) -> "Timer":
        return self.data.setdefault(name, Timer(name))

def time_micros():
    return time.time_ns() / 1000

class Timer:
    def __init__(self, name) -> None:
        self.name = name
        self._elapsed = 0.0
        self._st = None

    def start(self):
        self._st = time.perf_counter()

    def stop(self):
        assert self._st is not None
        elapsed = time.perf_counter() - self._st
        self._st = None
        self._elapsed += elapsed

    @property
    def elapsed(self):
        if self._st is not None:
            self.stop()
        return self._elapsed

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @contextmanager
    def pause(self):
        try:
            self.stop()
            yield
        finally:
            self.start()

@contextmanager
def timing(timers: dict):
    try: 
        yield
    finally:
        timers.clear()

ALERT_ECS_FIELDS = [
    "message"
    "tags",
    "labels",
    "agent",
    "client",
    "cloud",
    "container",
    "data_stream",
    "destination",
    "dll",
    "dns",
    "error",
    "event",
    "file",
    "group",
    "host",
    "http",
    "log",
    "network",
    "observer",
    "orchestrator",
    "organization",
    "package",
    "process",
    "registry",
    "related",
    "rule",
    "server",
    "service",
    "source",
    "span",
    "threat",
    "tls",
    "trace",
    "transaction",
    "url",
    "user",
]

def unix_time_micros(dt: datetime.datetime):
    return dt.timestamp() * 1e6

def _json_dumps_default(obj):
    if isinstance(obj, datetime.datetime):
        return unix_time_micros(obj)
    raise TypeError('Cannot serialize %s' % (obj,))

def json_dumps_dt(obj, **kwargs):
    return json.dumps(obj, default=_json_dumps_default, **kwargs)
