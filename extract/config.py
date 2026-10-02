import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_S3_PREFIX = "bronze/daily_bars"


def load_local_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def s3_bucket() -> str:
    load_local_env()
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not bucket:
        raise SystemExit("S3_BUCKET is not set")
    return bucket


def s3_prefix() -> str:
    load_local_env()
    prefix = os.environ.get("S3_PREFIX", DEFAULT_S3_PREFIX).strip().strip("/")
    if not prefix:
        return DEFAULT_S3_PREFIX
    return prefix


def aws_region() -> str | None:
    load_local_env()
    region = os.environ.get("AWS_REGION", "").strip()
    return region or None


def massive_api_key() -> str:
    load_local_env()
    key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not key:
        raise SystemExit("MASSIVE_API_KEY is not set")
    return key
