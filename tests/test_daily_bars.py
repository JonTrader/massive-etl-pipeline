import io
import json
from datetime import date, datetime
from urllib.parse import parse_qs, urlsplit

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
import responses
from moto import mock_aws
from responses.matchers import query_param_matcher

from extract.daily_bars import (
    EMPTY_BARS_SCHEMA,
    build_bars_table,
    fetch_grouped_daily_bars,
    main,
)

# Checked against the module XNYS calendar: Friday is a session, Saturday is not.
FRIDAY = date(2024, 3, 15)
SATURDAY = date(2024, 3, 16)
BUCKET = "test-daily-bars-bucket"
QUERY = {"adjusted": "false", "include_otc": "false"}


def _url(trade_date: date) -> str:
    return (
        "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/"
        f"{trade_date.isoformat()}"
    )


def _add(trade_date: date, payload: dict, status: int = 200) -> responses.BaseResponse:
    return responses.add(
        responses.GET,
        _url(trade_date),
        json=payload,
        status=status,
        match=[query_param_matcher(QUERY)],
    )


def _assert_request(request, trade_date: date) -> None:
    parts = urlsplit(request.url)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == _url(trade_date)
    assert parse_qs(parts.query) == {
        "adjusted": ["false"],
        "include_otc": ["false"],
    }


def _assert_get(trade_date: date) -> None:
    assert responses.calls
    for call in responses.calls:
        _assert_request(call.request, trade_date)


def _bar() -> dict:
    return {
        "T": "AAPL",
        "o": 1.5,
        "h": 2.0,
        "l": 1.0,
        "c": 1.75,
        "v": 100.0,
        "vw": 1.6,
        "t": 1710460800000,
        "n": 8,
    }


def _ok(results: list[dict]) -> dict:
    return {
        "status": "OK",
        "request_id": "req-1",
        "resultsCount": len(results),
        "results": results,
    }


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    # load_local_env() does not override variables that are already set.
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setenv("S3_BUCKET", BUCKET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


def test_build_bars_table_empty_day_uses_standard_schema() -> None:
    table = build_bars_table([])
    assert table.num_rows == 0
    assert table.column_names == ["T", "o", "h", "l", "c", "v", "vw", "t", "n"]
    assert table.schema.equals(EMPTY_BARS_SCHEMA)


def test_build_bars_table_keeps_extra_key() -> None:
    table = build_bars_table([{**_bar(), "otc": True}])
    assert table.column("otc").to_pylist() == [True]


def test_build_bars_table_missing_standard_key_is_null() -> None:
    bar = _bar()
    del bar["vw"]
    table = build_bars_table([bar])
    column = table.column("vw")
    assert column.type == pa.float64()
    assert column.to_pylist() == [None]


def test_build_bars_table_casts_int_values_to_float64() -> None:
    table = build_bars_table(
        [
            {
                "T": "AAPL",
                "o": 10,
                "h": 12,
                "l": 9,
                "c": 11,
                "v": 100,
                "vw": 11,
                "t": 1,
                "n": 4,
            }
        ]
    )
    for name in ("o", "h", "l", "c", "v", "vw"):
        assert table.schema.field(name).type == pa.float64()
    assert table.column("o").to_pylist() == [10.0]
    assert table.column("vw").to_pylist() == [11.0]


@responses.activate
def test_fetch_retries_503_then_succeeds() -> None:
    payload = _ok([_bar()])
    _add(FRIDAY, {"status": "ERROR"}, status=503)
    _add(FRIDAY, payload)
    result = fetch_grouped_daily_bars(FRIDAY)
    assert isinstance(result["results"], list)
    assert result["results"] == payload["results"]
    assert len(responses.calls) == 2
    _assert_get(FRIDAY)


@responses.activate
def test_fetch_403_raises() -> None:
    _add(FRIDAY, {"status": "ERROR"}, status=403)
    with pytest.raises(requests.HTTPError):
        fetch_grouped_daily_bars(FRIDAY)
    assert len(responses.calls) == 1
    _assert_get(FRIDAY)


@responses.activate
def test_fetch_status_error_raises() -> None:
    _add(FRIDAY, {"status": "ERROR", "resultsCount": 0, "results": []})
    with pytest.raises(RuntimeError, match="not OK or DELAYED"):
        fetch_grouped_daily_bars(FRIDAY)


@responses.activate
def test_fetch_count_mismatch_raises() -> None:
    _add(FRIDAY, {"status": "OK", "resultsCount": 2, "results": [_bar()]})
    with pytest.raises(RuntimeError, match="results length"):
        fetch_grouped_daily_bars(FRIDAY)


@responses.activate
def test_fetch_empty_weekend_returns_empty_results() -> None:
    _add(SATURDAY, {"status": "OK", "resultsCount": 0, "results": []})
    result = fetch_grouped_daily_bars(SATURDAY)
    assert isinstance(result["results"], list)
    assert result["results"] == []


@responses.activate
def test_fetch_empty_trading_day_raises() -> None:
    _add(FRIDAY, {"status": "OK", "resultsCount": 0, "results": []})
    with pytest.raises(RuntimeError, match="^no bars for trading session"):
        fetch_grouped_daily_bars(FRIDAY)


@responses.activate
def test_main_writes_parquet_and_manifest() -> None:
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        added = _add(FRIDAY, _ok([_bar()]))
        main([FRIDAY.isoformat()])
        # moto owns the active responses mock, so the call is on this response.
        assert added.call_count == 1
        _assert_request(added.calls[0].request, FRIDAY)

        prefix = f"bronze/daily_bars/trade_date={FRIDAY.isoformat()}"
        parquet_body = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/bars.parquet")[
            "Body"
        ].read()
        manifest_body = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/manifest.json")[
            "Body"
        ].read()

    table = pq.read_table(io.BytesIO(parquet_body))
    assert table.num_rows == 1
    manifest = json.loads(manifest_body)
    assert manifest["trade_date"] == FRIDAY.isoformat()
    assert manifest["row_count"] == 1
    assert datetime.fromisoformat(manifest["extracted_at"]).tzinfo is not None
    assert manifest["request_id"] == "req-1"
    assert manifest["api_status"] == "OK"
    assert manifest["request_params"] == QUERY


@responses.activate
def test_existing_object_survives_failed_fetch() -> None:
    key = f"bronze/daily_bars/trade_date={FRIDAY.isoformat()}/bars.parquet"
    original = b"previous-good-object"
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        s3.put_object(Bucket=BUCKET, Key=key, Body=original)
        added = _add(FRIDAY, {"status": "ERROR"}, status=403)
        with pytest.raises(requests.HTTPError):
            main([FRIDAY.isoformat()])
        assert added.call_count == 1
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert body == original
