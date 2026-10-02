import argparse
import io
import json
import time
from datetime import date, datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from .config import aws_region, massive_api_key, s3_bucket, s3_prefix

# Grouped daily JSON keys, in API order. Types are storage only.
# A closed session writes this schema with zero rows. A live payload
# keeps whatever keys it actually sent, including ones not listed here.
EMPTY_BARS_SCHEMA = pa.schema(
    [
        pa.field("T", pa.string()),
        pa.field("o", pa.float64()),
        pa.field("h", pa.float64()),
        pa.field("l", pa.float64()),
        pa.field("c", pa.float64()),
        pa.field("v", pa.float64()),
        pa.field("vw", pa.float64()),
        pa.field("t", pa.int64()),
        pa.field("n", pa.int64()),
    ]
)
RAW_FIELD_TYPES = {field.name: field.type for field in EMPTY_BARS_SCHEMA}

# Same values the request path and the manifest share.
LOCALE = "us"
MARKET_TYPE = "stocks"
# One try plus three retries. 429 and 5xx get a short backoff.
MAX_ATTEMPTS = 4


def parse_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(f"trade_date must be YYYY-MM-DD, got {value!r}") from None


def fetch_grouped_daily_bars(trade_date: date) -> list:
    # adjusted is forced false. Massive adjusts for splits unless this is set,
    # and a later split would change history if those values were the raw file.
    url = (
        "https://api.massive.com/v2/aggs/grouped/locale/"
        f"{LOCALE}/market/{MARKET_TYPE}/{trade_date.isoformat()}"
    )
    headers = {
        "Authorization": f"Bearer {massive_api_key()}",
        "Accept-Encoding": "gzip",
    }
    params = {"adjusted": "false", "include_otc": "false"}

    for attempt in range(MAX_ATTEMPTS):
        response = requests.get(url, params=params, headers=headers, timeout=(10, 10))
        if response.status_code == 200:
            break
        retryable = response.status_code == 429 or response.status_code >= 500
        if retryable and attempt < MAX_ATTEMPTS - 1:
            time.sleep(0.5 * (2**attempt))
            continue
        raise RuntimeError(
            f"GET {url} failed with HTTP {response.status_code} {response.reason}"
        )

    # No results key, or an empty list, is a closed market.
    results = response.json().get("results")
    if not results:
        return []
    return results


def build_bars_table(bars: list) -> pa.Table:
    # A closed session still gets a zero-row file with the standard raw keys,
    # so a weekend or holiday is different from a day the job never ran.
    if not bars:
        return pa.Table.from_pylist([], schema=EMPTY_BARS_SCHEMA)

    names: list[str] = []
    seen: set[str] = set()
    for name in EMPTY_BARS_SCHEMA.names:
        if any(name in bar for bar in bars):
            names.append(name)
            seen.add(name)
    for bar in bars:
        for name in bar:
            if name not in seen:
                names.append(name)
                seen.add(name)

    columns = []
    for name in names:
        values = [bar.get(name) for bar in bars]
        if name in RAW_FIELD_TYPES:
            columns.append(pa.array(values, type=RAW_FIELD_TYPES[name]))
        else:
            columns.append(pa.array(values))
    return pa.Table.from_arrays(columns, names=names)


def upload_partition(
    bucket: str,
    prefix: str,
    trade_date: date,
    table: pa.Table,
    extracted_at: datetime,
) -> tuple[str, str]:
    partition = f"{prefix}/trade_date={trade_date.isoformat()}"
    parquet_key = f"{partition}/bars.parquet"
    manifest_key = f"{partition}/manifest.json"

    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    manifest = {
        "trade_date": trade_date.isoformat(),
        "adjusted": False,
        "locale": LOCALE,
        "market_type": MARKET_TYPE,
        "row_count": table.num_rows,
        "extracted_at": extracted_at.isoformat(),
    }

    client_kwargs = {}
    region = aws_region()
    if region:
        client_kwargs["region_name"] = region
    s3 = boto3.client("s3", **client_kwargs)

    # One PUT of the finished bytes to the final key. Nothing is deleted
    # first, so a failed request (which never gets here) leaves a previous
    # good object in place. Re-running the same date replaces only this partition.
    s3.put_object(Bucket=bucket, Key=parquet_key, Body=buffer.getvalue())
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=(json.dumps(manifest, indent=2) + "\n").encode("utf-8"),
        ContentType="application/json",
    )
    return parquet_key, manifest_key


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Land one US stock-market date of unadjusted daily bars on S3."
    )
    parser.add_argument("trade_date", help="Market date to extract, YYYY-MM-DD")
    args = parser.parse_args(argv)

    trade_date = parse_trade_date(args.trade_date)
    bucket = s3_bucket()
    prefix = s3_prefix()

    bars = fetch_grouped_daily_bars(trade_date)
    extracted_at = datetime.now(timezone.utc)
    table = build_bars_table(bars)
    parquet_key, manifest_key = upload_partition(
        bucket, prefix, trade_date, table, extracted_at
    )
    print(f"s3://{bucket}/{parquet_key} rows={table.num_rows}")
    print(f"s3://{bucket}/{manifest_key}")


if __name__ == "__main__":
    main()
