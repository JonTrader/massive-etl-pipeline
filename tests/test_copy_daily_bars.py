import json
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import load.copy_daily_bars as load_mod
from extract.daily_bars import REQUEST_PARAMS
from load.copy_daily_bars import copy_daily_bars, main

FRIDAY = date(2024, 3, 15)
SATURDAY = date(2024, 3, 16)
BUCKET = "test-daily-bars-bucket"
PREFIX = f"bronze/daily_bars/trade_date={FRIDAY.isoformat()}"


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_BUCKET", BUCKET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
    monkeypatch.setenv("SNOWFLAKE_USER", "test-user")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "rsa_key.p8")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test-wh")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "MARKET_LOADER")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", raising=False)


def _manifest(
    *,
    trade_date: date = FRIDAY,
    row_count: int = 1,
    request_params: dict | None = None,
    api_status: str = "OK",
    extracted_at: str = "2024-03-16T10:00:00+00:00",
    request_id: str | None = "req-1",
) -> dict:
    return {
        "trade_date": trade_date.isoformat(),
        "row_count": row_count,
        "extracted_at": extracted_at,
        "request_id": request_id,
        "api_status": api_status,
        "request_params": REQUEST_PARAMS if request_params is None else request_params,
    }


def _put_landing(
    *,
    trade_date: date = FRIDAY,
    row_count: int = 1,
    manifest: dict | None = None,
) -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    prefix = f"bronze/daily_bars/trade_date={trade_date.isoformat()}"
    body = (
        manifest
        if manifest is not None
        else _manifest(trade_date=trade_date, row_count=row_count)
    )
    # Parquet first, then manifest, matching the extract commit order.
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/bars.parquet", Body=b"parquet")
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{prefix}/manifest.json",
        Body=(json.dumps(body) + "\n").encode("utf-8"),
    )


class FakeCursor:
    def __init__(
        self,
        *,
        copy_rows: list[dict] | None = None,
        updated: int = 1,
        dated: int = 1,
        fail_on: str | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict | None]] = []
        self.copy_rows = (
            copy_rows
            if copy_rows is not None
            else [{"status": "LOADED", "rows_loaded": 1}]
        )
        self.updated = updated
        self.dated = dated
        self.fail_on = fail_on
        self._last_kind: str | None = None

    def execute(self, sql: str, params: dict | None = None):
        text = " ".join(sql.split())
        self.calls.append((text, params))
        upper = text.upper()
        if upper.startswith("DELETE"):
            self._last_kind = "delete"
        elif upper.startswith("COPY"):
            self._last_kind = "copy"
        elif upper.startswith("UPDATE"):
            self._last_kind = "update"
        elif upper.startswith("SELECT"):
            self._last_kind = "count"
        elif upper.startswith("MERGE"):
            self._last_kind = "merge"
        else:
            self._last_kind = "other"
        if self.fail_on == self._last_kind:
            raise RuntimeError(f"{self._last_kind} failed")
        return self

    def fetchall(self):
        assert self._last_kind == "copy"
        return list(self.copy_rows)

    def fetchone(self):
        if self._last_kind == "update":
            return {"number of rows updated": self.updated}
        if self._last_kind == "count":
            return {"dated": self.dated}
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self, *args, **kwargs):
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def _kinds(cursor: FakeCursor) -> list[str]:
    kinds = []
    for sql, _ in cursor.calls:
        upper = sql.upper()
        if upper.startswith("DELETE"):
            kinds.append("delete")
        elif upper.startswith("COPY"):
            kinds.append("copy")
        elif upper.startswith("UPDATE"):
            kinds.append("update")
        elif upper.startswith("SELECT"):
            kinds.append("count")
        elif upper.startswith("MERGE"):
            kinds.append("merge")
        else:
            kinds.append("other")
    return kinds


@mock_aws
def test_good_load_runs_delete_copy_update_count_merge_and_commits() -> None:
    _put_landing(row_count=1)
    cursor = FakeCursor(updated=1, dated=1)
    connection = FakeConnection(cursor)
    connect_kwargs: dict = {}

    def fake_connect(**kwargs):
        connect_kwargs.update(kwargs)
        return connection

    with patch.object(
        load_mod.snowflake.connector, "connect", side_effect=fake_connect
    ):
        copy_daily_bars(FRIDAY)

    assert _kinds(cursor) == ["delete", "copy", "update", "count", "merge"]
    assert connection.committed is True
    assert connection.rolled_back is False
    assert connection.closed is True

    assert connect_kwargs["authenticator"] == "SNOWFLAKE_JWT"
    assert connect_kwargs["autocommit"] is False
    assert connect_kwargs["session_parameters"] == {
        "QUERY_TAG": "daily_bars_load",
        "STATEMENT_TIMEOUT_IN_SECONDS": "600",
    }
    assert "private_key_file_pwd" not in connect_kwargs

    copy_sql = cursor.calls[1][0]
    assert "MATCH_BY_COLUMN_NAME = CASE_SENSITIVE" in copy_sql
    assert "INCLUDE_METADATA = (extracted_at = METADATA$FILE_LAST_MODIFIED)" in copy_sql
    assert "FORCE = TRUE" in copy_sql
    assert f"FILES = ('trade_date={FRIDAY.isoformat()}/bars.parquet')" in copy_sql
    assert "PURGE" not in copy_sql.upper()

    update_sql, update_params = cursor.calls[2]
    assert "WHERE trade_date IS NULL" in update_sql
    assert 'TO_DATE(TO_TIMESTAMP_NTZ("t", 3)) = %(trade_date)s' in update_sql
    assert update_params == {"trade_date": FRIDAY}

    merge_params = cursor.calls[4][1]
    assert merge_params is not None
    assert merge_params["row_count"] == 1
    assert merge_params["api_status"] == "OK"
    assert merge_params["request_id"] == "req-1"
    assert merge_params["extracted_at"] == datetime(2024, 3, 16, 10, 0, 0)


@mock_aws
def test_zero_row_load_commits_and_merges_audit_row() -> None:
    _put_landing(trade_date=SATURDAY, row_count=0)
    cursor = FakeCursor(
        copy_rows=[{"status": "LOADED", "rows_loaded": 0}],
        updated=0,
        dated=0,
    )
    connection = FakeConnection(cursor)

    with patch.object(
        load_mod.snowflake.connector,
        "connect",
        return_value=connection,
    ):
        copy_daily_bars(SATURDAY)

    assert connection.committed is True
    assert connection.rolled_back is False
    assert _kinds(cursor) == ["delete", "copy", "update", "count", "merge"]
    assert cursor.calls[4][1] is not None
    assert cursor.calls[4][1]["row_count"] == 0


@mock_aws
@pytest.mark.parametrize(
    "setup",
    [
        "missing_manifest",
        "missing_parquet",
        "wrong_date",
        "adjusted_true",
        "bad_row_count",
        "blank_api_status",
        "parquet_newer",
    ],
)
def test_manifest_failures_issue_no_sql(setup: str) -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    if setup == "missing_manifest":
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/bars.parquet", Body=b"parquet")
    elif setup == "missing_parquet":
        s3.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}/manifest.json",
            Body=(json.dumps(_manifest()) + "\n").encode("utf-8"),
        )
    elif setup == "wrong_date":
        _put_landing(manifest=_manifest(trade_date=SATURDAY, row_count=1))
    elif setup == "adjusted_true":
        _put_landing(
            manifest=_manifest(
                request_params={"adjusted": "true", "include_otc": "false"}
            )
        )
    elif setup == "bad_row_count":
        _put_landing(manifest=_manifest(row_count=-1))
    elif setup == "blank_api_status":
        body = _manifest()
        body["api_status"] = "  "
        _put_landing(manifest=body)
    elif setup == "parquet_newer":
        _put_landing(row_count=1)
        # moto can keep the same LastModified on a quick overwrite, so skew
        # the timestamps on the client the loader builds.
        earlier = datetime(2024, 3, 16, 10, 0, 0, tzinfo=UTC)
        later = earlier + timedelta(seconds=5)
        real_client = boto3.client("s3", region_name="us-east-1")
        real_get = real_client.get_object
        real_head = real_client.head_object

        def skewed_client(*args, **kwargs):
            def get_object(**kw):
                response = real_get(**kw)
                response["LastModified"] = earlier
                return response

            def head_object(**kw):
                response = real_head(**kw)
                response["LastModified"] = later
                return response

            real_client.get_object = get_object  # type: ignore[method-assign]
            real_client.head_object = head_object  # type: ignore[method-assign]
            return real_client

        with (
            patch.object(load_mod.boto3, "client", side_effect=skewed_client),
            patch.object(load_mod.snowflake.connector, "connect") as mocked_connect,
        ):
            with pytest.raises(RuntimeError, match="manifest mismatch"):
                copy_daily_bars(FRIDAY)
            mocked_connect.assert_not_called()
        return
    else:
        raise AssertionError(setup)

    with patch.object(load_mod.snowflake.connector, "connect") as mocked_connect:
        with pytest.raises(RuntimeError, match="manifest mismatch|missing landing"):
            copy_daily_bars(FRIDAY)
        mocked_connect.assert_not_called()


@mock_aws
@pytest.mark.parametrize(
    ("cursor_kwargs", "match"),
    [
        ({"fail_on": "copy"}, "copy failed|snowflake load failed"),
        (
            {
                "copy_rows": [
                    {
                        "status": "LOAD_SKIPPED",
                        "rows_loaded": 0,
                        "first_error": "already loaded",
                    }
                ]
            },
            r"copy status 'LOAD_SKIPPED'.*first_error='already loaded'",
        ),
        (
            {
                "copy_rows": [{"status": "LOADED", "rows_loaded": 2}],
                "updated": 1,
                "dated": 1,
            },
            "row count mismatch",
        ),
        (
            {
                "copy_rows": [{"status": "LOADED", "rows_loaded": 1}],
                "updated": 0,
                "dated": 1,
            },
            "row count mismatch",
        ),
        (
            {
                "copy_rows": [{"status": "LOADED", "rows_loaded": 1}],
                "updated": 1,
                "dated": 0,
            },
            "trade_date check failed",
        ),
    ],
)
def test_snowflake_failures_roll_back_without_commit(
    cursor_kwargs: dict, match: str
) -> None:
    _put_landing(row_count=1)
    cursor = FakeCursor(**cursor_kwargs)
    connection = FakeConnection(cursor)

    with patch.object(
        load_mod.snowflake.connector,
        "connect",
        return_value=connection,
    ):
        with pytest.raises(RuntimeError, match=match):
            copy_daily_bars(FRIDAY)

    assert connection.committed is False
    assert connection.rolled_back is True
    assert connection.closed is True
    assert "merge" not in _kinds(cursor)


@mock_aws
@pytest.mark.parametrize(
    ("copy_rows", "match"),
    [
        ([], r"copy status returned 0 rows"),
        (
            [{"status": "LOAD_FAILED", "rows_loaded": 0, "first_error": "bad parquet"}],
            r"copy status 'LOAD_FAILED'.*first_error='bad parquet'",
        ),
        (
            [{"status": "LOADED", "rows_loaded": 1}],
            "row count mismatch",
        ),
    ],
)
def test_zero_row_copy_failures_roll_back_without_merge(
    copy_rows: list[dict], match: str
) -> None:
    _put_landing(trade_date=SATURDAY, row_count=0)
    cursor = FakeCursor(copy_rows=copy_rows, updated=0, dated=0)
    connection = FakeConnection(cursor)

    with patch.object(
        load_mod.snowflake.connector,
        "connect",
        return_value=connection,
    ):
        with pytest.raises(RuntimeError, match=match):
            copy_daily_bars(SATURDAY)

    assert connection.committed is False
    assert connection.rolled_back is True
    assert connection.closed is True
    assert "merge" not in _kinds(cursor)


@mock_aws
def test_non_loaded_copy_includes_missing_first_error() -> None:
    _put_landing(row_count=1)
    cursor = FakeCursor(
        copy_rows=[{"status": "PARTIALLY_LOADED", "rows_loaded": 0}],
        updated=1,
        dated=1,
    )
    connection = FakeConnection(cursor)

    with patch.object(
        load_mod.snowflake.connector,
        "connect",
        return_value=connection,
    ):
        with pytest.raises(
            RuntimeError,
            match=r"copy status 'PARTIALLY_LOADED'.*first_error=None",
        ):
            copy_daily_bars(FRIDAY)

    assert connection.committed is False
    assert connection.rolled_back is True
    assert "merge" not in _kinds(cursor)


def test_missing_snowflake_setting_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    with mock_aws():
        _put_landing(row_count=1)
        with patch.object(load_mod.snowflake.connector, "connect") as connect:
            with pytest.raises(RuntimeError, match="SNOWFLAKE_ACCOUNT is not set"):
                copy_daily_bars(FRIDAY)
            connect.assert_not_called()


def test_missing_bucket_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with patch.object(load_mod.snowflake.connector, "connect") as connect:
        with pytest.raises(RuntimeError, match="S3_BUCKET is not set"):
            copy_daily_bars(FRIDAY)
        connect.assert_not_called()


@mock_aws
def test_main_accepts_iso_date() -> None:
    _put_landing(row_count=1)
    cursor = FakeCursor(updated=1, dated=1)
    connection = FakeConnection(cursor)

    with (
        patch.object(load_mod, "load_local_env"),
        patch.object(
            load_mod.snowflake.connector,
            "connect",
            return_value=connection,
        ),
    ):
        main([FRIDAY.isoformat()])

    assert connection.committed is True
    assert connection.closed is True


@mock_aws
def test_passphrase_passed_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "secret")
    _put_landing(row_count=1)
    cursor = FakeCursor(updated=1, dated=1)
    connection = FakeConnection(cursor)
    connect_kwargs: dict = {}

    def fake_connect(**kwargs):
        connect_kwargs.update(kwargs)
        return connection

    with patch.object(
        load_mod.snowflake.connector, "connect", side_effect=fake_connect
    ):
        copy_daily_bars(FRIDAY)

    assert connect_kwargs["private_key_file_pwd"] == "secret"
