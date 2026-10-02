import argparse
import io
import json
from datetime import date, datetime, timezone

import boto3
import exchange_calendars as xcals
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import aws_region, massive_api_key, s3_bucket, s3_prefix

# Standard grouped-daily columns. Types are storage only.
# A closed session writes this schema with zero rows. A live payload
# always includes these columns, and keeps any other key the API sent.
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

# adjusted is forced false. Massive adjusts for splits unless this is set,
# and a later split would change history if those values were the raw file.
# The same dict is the HTTP query string and the manifest request_params.
REQUEST_PARAMS = {"adjusted": "false", "include_otc": "false"}
# The default window is about twenty years, and a date outside it raises.
XNYS = xcals.get_calendar("XNYS", start="2000-01-01")

SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
    ),
)


def parse_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(f"trade_date must be YYYY-MM-DD, got {value!r}") from None


def fetch_grouped_daily_bars(trade_date: date) -> dict:
    url = (
        "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/"
        f"{trade_date.isoformat()}"
    )
    headers = {
        "Authorization": f"Bearer {massive_api_key()}",
        "Accept-Encoding": "gzip",
    }
    response = SESSION.get(
        url, params=REQUEST_PARAMS, headers=headers, timeout=(10, 30)
    )
    response.raise_for_status()
    payload = response.json()

    status = payload.get("status")
    if status not in {"OK", "DELAYED"}:
        raise RuntimeError(f"grouped daily bars status {status!r} is not OK or DELAYED")

    results = payload.get("results")
    if results is None:
        results = []
    results_count = payload.get("resultsCount")
    if results_count is None:
        results_count = 0
    if len(results) != results_count:
        raise RuntimeError(
            f"results length {len(results)} != resultsCount {results_count}"
        )

    # A closed day still lands an empty file. Zero rows on an XNYS session
    # means the vendor has not published yet, so fail and upload nothing.
    if len(results) == 0 and XNYS.is_session(trade_date):
        raise RuntimeError(f"no bars for trading session {trade_date.isoformat()}")

    payload["results"] = results
    return payload


def build_bars_table(bars: list[dict]) -> pa.Table:
    # A closed session still gets a zero-row file with the standard raw keys,
    # so a weekend or holiday is different from a day the job never ran.
    if not bars:
        return EMPTY_BARS_SCHEMA.empty_table()
    table = pa.Table.from_struct_array(pa.array(bars))
    for field in EMPTY_BARS_SCHEMA:
        if field.name in table.column_names:
            index = table.schema.get_field_index(field.name)
            table = table.set_column(index, field, table[field.name].cast(field.type))
        else:
            table = table.append_column(field, pa.nulls(table.num_rows, field.type))
    return table


def upload_partition(
    bucket: str,
    partition: str,
    parquet_bytes: bytes,
    manifest: dict,
) -> tuple[str, str]:
    parquet_key = f"{partition}/bars.parquet"
    manifest_key = f"{partition}/manifest.json"

    client_kwargs = {}
    region = aws_region()
    if region:
        client_kwargs["region_name"] = region
    s3 = boto3.client("s3", **client_kwargs)

    # One PUT of the finished bytes to the final key. Nothing is deleted
    # first, so a failed request (which never gets here) leaves a previous
    # good object in place. The manifest is written last as the commit marker.
    s3.put_object(Bucket=bucket, Key=parquet_key, Body=parquet_bytes)
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

    payload = fetch_grouped_daily_bars(trade_date)
    extracted_at = datetime.now(timezone.utc)
    table = build_bars_table(payload["results"])
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    partition = f"{prefix}/trade_date={trade_date.isoformat()}"
    manifest = {
        "trade_date": trade_date.isoformat(),
        "row_count": table.num_rows,
        "extracted_at": extracted_at.isoformat(),
        "request_id": payload.get("request_id"),
        "api_status": payload["status"],
        "request_params": REQUEST_PARAMS,
    }
    parquet_key, manifest_key = upload_partition(
        bucket, partition, buffer.getvalue(), manifest
    )
    print(f"s3://{bucket}/{parquet_key} rows={table.num_rows}")
    print(f"s3://{bucket}/{manifest_key}")


if __name__ == "__main__":
    main()
