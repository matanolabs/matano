import datetime
import json
import time
from collections import UserDict
from contextlib import contextmanager


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


ALERT_ECS_FIELDS = [
    "message",
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


def _json_dumps_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.timestamp()
    raise TypeError("Cannot serialize %s" % (obj,))


def json_dumps_dt(obj, **kwargs):
    return json.dumps(obj, default=_json_dumps_default, **kwargs)


class DeepDict(dict):
    __slots__ = ("_dict",)

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], dict):
            self._dict = args[0]
            super(DeepDict, self).__init__(self._dict)
            return
        self._dict = None
        super(DeepDict, self).__init__(*args, **kwargs)

    def deepget(self, path, default=None):
        """
        Get a value from deep in a nested Mapping returning the default if any of the path in the path are invalid
        """
        o = self._dict
        keys = path.split(".") if type(path) == str else path
        for key in keys:
            try:
                o = o[key]
            except KeyError:
                return default
        # TODO(shaeq): performance? also we should differentiate missing keys vs. nulls, but this info is lost in avro for now so we do this to make them feel the same:
        return o if o is not None else default

    # Pretened to be a dict

    def __bool__(self):
        return super(DeepDict, self).__bool__()

    def __contains__(self, key):
        return super(DeepDict, self).__contains__(key)

    def __deepcopy__(self, memo):
        return super(DeepDict, self).__deepcopy__(memo)

    def __delitem__(self, key):
        return super(DeepDict, self).__delitem__(key)

    def __eq__(self, other):
        return super(DeepDict, self).__eq__(other)

    def __getitem__(self, key):
        return super(DeepDict, self).__getitem__(key)

    def __iter__(self):
        return super(DeepDict, self).__iter__()

    def __len__(self):
        return super(DeepDict, self).__len__()

    def __repr__(self):
        return super(DeepDict, self).__repr__()

    def __setitem__(self, key, value):
        return super(DeepDict, self).__setitem__(key, value)

    def __str__(self):
        return super(DeepDict, self).__str__()

    def clear(self):
        return super(DeepDict, self).clear()

    def copy(self):
        return super(DeepDict, self).copy()

    def get(self, key, default=None):
        return super(DeepDict, self).get(key, default)

    def items(self):
        return super(DeepDict, self).items()

    def keys(self):
        return super(DeepDict, self).keys()

    def pop(self, key, *args):
        return super(DeepDict, self).pop(key, *args)

    def setdefault(self, key, default=None):
        return super(DeepDict, self).setdefault(key, default)

    def update(self, other):
        return super(DeepDict, self).update(other)

    def values(self):
        return super(DeepDict, self).values()
