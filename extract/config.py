import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_local_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    # A blank value makes boto3 build an invalid endpoint. Unset means
    # "use the default region chain."
    if not os.environ.get("AWS_DEFAULT_REGION", "").strip():
        os.environ.pop("AWS_DEFAULT_REGION", None)


def s3_bucket() -> str:
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not bucket:
        raise RuntimeError("S3_BUCKET is not set")
    return bucket


def massive_api_key() -> str:
    key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not key:
        raise RuntimeError("MASSIVE_API_KEY is not set")
    return key
