from collections import UserDict
from contextlib import contextmanager
import time

class Timers(UserDict):
    def get_timer(self, name: str) -> "Timer":
        return self.data.setdefault(name, Timer(name))

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
