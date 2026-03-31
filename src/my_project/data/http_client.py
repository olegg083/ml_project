# -*- coding: utf-8 -*-
from __future__ import annotations

import random
import threading
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import HEADERS, REQUEST_TIMEOUT

_thread_local = threading.local()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = build_session()
    return _thread_local.session


def fetch_html(
    url: str,
    params: Optional[dict] = None,
    sleep_sec: float = 1.5,
    session: Optional[requests.Session] = None,
) -> str:
    if sleep_sec > 0:
        time.sleep(sleep_sec + random.uniform(0.0, 0.4))

    sess = session or get_thread_session()
    response = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text
