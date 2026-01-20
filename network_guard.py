import http.client
import os
import socket
import urllib.request

import requests


_ENABLED = False


def _blocked(*_args, **_kwargs) -> None:
    raise RuntimeError("Network disabled in tests (NO_NETWORK=1)")


def apply_guard() -> None:
    global _ENABLED
    if _ENABLED:
        return
    _ENABLED = True
    socket.socket.connect = _blocked
    socket.create_connection = _blocked
    http.client.HTTPConnection.connect = _blocked
    http.client.HTTPSConnection.connect = _blocked
    urllib.request.urlopen = _blocked
    requests.sessions.Session.request = _blocked


def enable_guard_from_env() -> None:
    if os.getenv("NO_NETWORK") == "1":
        apply_guard()
