# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

CITY_NAME = "Москва"
BASE_DOMAIN = "https://www.mirkvartir.ru"
BASE_LIST_URL = f"{BASE_DOMAIN}/{CITY_NAME}/Трехкомнатные/"

REQUEST_TIMEOUT = (5, 20)
LIST_SLEEP_SEC = 1.2
DETAIL_SLEEP_SEC = 0.5
MAX_EMPTY_PAGES = 3

MAX_IMAGES = 6
MAX_WORKERS = 8
MAX_IMAGE_UPLOAD_WORKERS = 3
FIRST_PAGE = 300
MAX_PAGES = 400
MIN_ITEMS = 1000

PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

FILE_NAME = "mirkvartir_moscow_flats_10000.csv"
PROCESSED_PARQUET_NAME = "mirkvartir_moscow_flats_10000.parquet"

LIST_RSC_PARAM = "140g3"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    )
}


@dataclass(frozen=True)
class S3Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str


def load_s3_config() -> S3Config | None:
    endpoint = os.getenv("S3_ENDPOINT_URL")
    key = os.getenv("S3_ACCESS_KEY")
    secret = os.getenv("S3_SECRET_KEY")
    bucket = os.getenv("S3_BUCKET_NAME")
    if not all([endpoint, key, secret, bucket]):
        return None
    return S3Config(
        endpoint_url=endpoint,
        access_key=key,
        secret_key=secret,
        bucket_name=bucket,
    )
