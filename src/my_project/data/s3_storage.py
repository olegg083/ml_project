# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Optional

import boto3
import requests

from .config import S3Config


def create_s3_client(cfg: S3Config) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )


def upload_image_to_s3(
    session: requests.Session,
    s3_client,
    bucket_name: str,
    city_name: str,
    image_url: str,
    flat_id: str,
    image_index: int,
) -> Optional[str]:
    try:
        response = session.get(image_url, timeout=10)
        response.raise_for_status()

        file_name = f"{city_name}/{flat_id}/{image_index + 1}.jpg"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=response.content,
            ContentType=response.headers.get("Content-Type", "image/jpeg"),
        )
        return f"s3://{bucket_name}/{file_name}"
    except Exception:
        return None
