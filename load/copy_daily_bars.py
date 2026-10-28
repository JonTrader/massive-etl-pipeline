import argparse
import json
import os
import warnings
from datetime import UTC, date, datetime

# exchange_calendars installs pandas. The connector then warns that the
# pinned pyarrow is outside the range for its pandas extra. This loader
# does not use that extra, and the extract stays on its own pyarrow pin.
warnings.filterwarnings(
    "ignore",
    message="You have an incompatible version of 'pyarrow' installed",
    category=UserWarning,
    module="snowflake.connector.options",
)

import boto3  # noqa: E402
import snowflake.connector  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from snowflake.connector import DictCursor  # noqa: E402

from extract.config import load_local_env, s3_bucket  # noqa: E402
from extract.daily_bars import REQUEST_PARAMS  # noqa: E402

_MISSING_S3_CODES = {"404", "NoSuchKey", "NotFound"}
_FAILED_COPY_STATUSES = {"LOAD_FAILED", "LOAD_SKIPPED", "PARTIALLY_LOADED"}
_SNOWFLAKE_SETTINGS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
)


def _s3_object(operation, bucket: str, key: str) -> dict:
    try:
        return operation(Bucket=bucket, Key=key)
    except ClientError as err:
        code = str(err.response.get("Error", {}).get("Code", ""))
        location = f"s3://{bucket}/{key}"
        if code in _MISSING_S3_CODES:
            raise RuntimeError(f"missing landing: {location}") from err
        raise RuntimeError(f"failed to read landing {location}: {code}") from err


def _column_value(row: dict, name: str):
    # DictCursor names may come back uppercase.
    folded = name.lower()
    for key, value in row.items():
        if str(key).lower() == folded:
            return value
    return None


def _required_setting(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _manifest_timestamp(value) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(
            "manifest mismatch: extracted_at is missing or not a parseable timestamp"
        )
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError as err:
        raise RuntimeError(
            "manifest mismatch: extracted_at is missing or not a parseable timestamp"
        ) from err
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    # Store the manifest clock as naive UTC. A value with no offset is UTC,
    # matching the extract clock. An aware value is sent with an offset, and
    # a session timezone could move that instant into TIMESTAMP_NTZ.
    return parsed.astimezone(UTC).replace(tzinfo=None)


def copy_daily_bars(trade_date: date) -> None:
    bucket = s3_bucket()
    prefix = f"bronze/daily_bars/trade_date={trade_date.isoformat()}"
    manifest_key = f"{prefix}/manifest.json"
    parquet_key = f"{prefix}/bars.parquet"
    s3 = boto3.client("s3")
    manifest_response = _s3_object(s3.get_object, bucket, manifest_key)
    parquet_head = _s3_object(s3.head_object, bucket, parquet_key)

    try:
        manifest = json.loads(manifest_response["Body"].read())
    except (json.JSONDecodeError, UnicodeDecodeError) as err:
        raise RuntimeError(
            "manifest mismatch: manifest.json is not valid JSON"
        ) from err
    if not isinstance(manifest, dict):
        raise RuntimeError("manifest mismatch: manifest.json is not an object")

    if manifest.get("trade_date") != trade_date.isoformat():
        raise RuntimeError(
            "manifest mismatch: trade_date "
            f"{manifest.get('trade_date')!r} is not {trade_date.isoformat()}"
        )
    # adjusted must stay false. Massive adjusts for splits unless this is set,
    # and a later split would rewrite history if those values were loaded as raw.
    if manifest.get("request_params") != REQUEST_PARAMS:
        raise RuntimeError(
            "manifest mismatch: request_params is not the unadjusted daily landing"
        )

    row_count = manifest.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise RuntimeError("manifest mismatch: row_count is missing or not an int >= 0")

    api_status = manifest.get("api_status")
    if not isinstance(api_status, str) or not api_status.strip():
        raise RuntimeError("manifest mismatch: api_status is missing or blank")
    api_status = api_status.strip()

    extracted_at = _manifest_timestamp(manifest.get("extracted_at"))
    request_id = manifest.get("request_id")
    if request_id is not None and not isinstance(request_id, str):
        raise RuntimeError("manifest mismatch: request_id is not a string")

    # The extract writes the parquet first and the manifest last. A parquet
    # LastModified later than the manifest means a re-extract is still in
    # progress. Equal timestamps are a finished landing. The manifest
    # extracted_at field is set before either upload, so the parquet object
    # is newer than that field even on a good landing.
    try:
        parquet_modified = parquet_head["LastModified"].astimezone(UTC)
        manifest_modified = manifest_response["LastModified"].astimezone(UTC)
    except (AttributeError, KeyError) as err:
        raise RuntimeError(
            "manifest mismatch: landing is missing LastModified"
        ) from err
    if parquet_modified > manifest_modified:
        raise RuntimeError(
            "manifest mismatch: parquet LastModified is later than the manifest"
        )

    settings = {name: _required_setting(name) for name in _SNOWFLAKE_SETTINGS}
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()
    connect_args = {
        "account": settings["SNOWFLAKE_ACCOUNT"],
        "user": settings["SNOWFLAKE_USER"],
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": settings["SNOWFLAKE_PRIVATE_KEY_PATH"],
        "warehouse": settings["SNOWFLAKE_WAREHOUSE"],
        "role": settings["SNOWFLAKE_ROLE"],
        "database": "MARKET",
        "schema": "RAW",
        "autocommit": False,
        "session_parameters": {
            "QUERY_TAG": "daily_bars_load",
            "STATEMENT_TIMEOUT_IN_SECONDS": "600",
        },
    }
    if passphrase:
        connect_args["private_key_file_pwd"] = passphrase

    connection = None
    try:
        connection = snowflake.connector.connect(**connect_args)
        try:
            cursor = connection.cursor(DictCursor)
            cursor.execute(
                "DELETE FROM MARKET.RAW.DAILY_BARS WHERE trade_date = %(trade_date)s",
                {"trade_date": trade_date},
            )
            bars_file = f"trade_date={trade_date.isoformat()}/bars.parquet"
            # Without FORCE = TRUE, COPY skips a file it has already loaded and
            # the DELETE above would wipe the day.
            cursor.execute(
                f"""
                COPY INTO MARKET.RAW.DAILY_BARS
                FROM @MARKET.RAW.BRONZE_DAILY_BARS
                FILES = ('{bars_file}')
                FILE_FORMAT = (FORMAT_NAME = 'MARKET.RAW.DAILY_BARS_PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
                INCLUDE_METADATA = (extracted_at = METADATA$FILE_LAST_MODIFIED)
                FORCE = TRUE
                ON_ERROR = ABORT_STATEMENT
                """
            )
            copy_rows = cursor.fetchall()
            if row_count > 0:
                if len(copy_rows) != 1:
                    raise RuntimeError(
                        f"copy status returned {len(copy_rows)} rows for {bars_file}"
                    )
                status = _column_value(copy_rows[0], "status")
                if status != "LOADED":
                    raise RuntimeError(f"copy status {status!r} for {bars_file}")
                try:
                    loaded = int(_column_value(copy_rows[0], "rows_loaded"))
                except (TypeError, ValueError) as err:
                    raise RuntimeError(
                        f"row count mismatch: COPY rows_loaded is not an int "
                        f"for {bars_file}"
                    ) from err
                if loaded != row_count:
                    raise RuntimeError(
                        f"row count mismatch: COPY loaded {loaded} rows, "
                        f"manifest row_count is {row_count}"
                    )
            else:
                # A zero-row Parquet file's COPY result has not been confirmed
                # on a real account, so the result shape is not asserted. The
                # table count below decides whether this closed day loaded.
                for copy_row in copy_rows:
                    status = _column_value(copy_row, "status")
                    if status in _FAILED_COPY_STATUSES:
                        raise RuntimeError(f"copy status {status!r} for {bars_file}")

            # MATCH_BY_COLUMN_NAME cannot take a SELECT, so COPY inserts
            # trade_date as null and this statement sets it. Limit the UPDATE
            # to null rows whose derived date is this trade_date so a stray
            # null on another day cannot rewrite foreign rows or fail a good
            # load. t is epoch milliseconds at the 16:00 New York close; NTZ
            # and scale 3 yield that close's UTC calendar date, which is the
            # session date.
            cursor.execute(
                """
                UPDATE MARKET.RAW.DAILY_BARS
                SET trade_date = TO_DATE(TO_TIMESTAMP_NTZ("t", 3))
                WHERE trade_date IS NULL
                  AND TO_DATE(TO_TIMESTAMP_NTZ("t", 3)) = %(trade_date)s
                """,
                {"trade_date": trade_date},
            )
            # Read the result column. cursor.rowcount stays empty when the
            # connector returns this statement as Arrow instead of a JSON
            # rowset. A null whose t maps to this same date still raises
            # updated above row_count and rolls back.
            updated_row = cursor.fetchone()
            if updated_row is None:
                raise RuntimeError("row count mismatch: UPDATE returned no row")
            try:
                updated = int(_column_value(updated_row, "number of rows updated"))
            except (TypeError, ValueError) as err:
                raise RuntimeError(
                    "row count mismatch: UPDATE did not report how many rows changed"
                ) from err
            if updated != row_count:
                raise RuntimeError(
                    "row count mismatch: UPDATE set trade_date on "
                    f"{updated} rows, manifest row_count is {row_count}"
                )

            cursor.execute(
                """
                SELECT COUNT_IF(trade_date = %(trade_date)s) AS dated
                FROM MARKET.RAW.DAILY_BARS
                """,
                {"trade_date": trade_date},
            )
            count_row = cursor.fetchone()
            if count_row is None:
                raise RuntimeError("trade_date check failed: count returned no row")
            try:
                dated = int(_column_value(count_row, "dated"))
            except (TypeError, ValueError) as err:
                raise RuntimeError(
                    "trade_date check failed: count result is missing"
                ) from err
            if dated != row_count:
                raise RuntimeError(
                    f"trade_date check failed: dated={dated} row_count={row_count}"
                )

            cursor.execute(
                """
                MERGE INTO MARKET.RAW.DAILY_BARS_LOADS AS loads
                USING (
                    SELECT
                        %(trade_date)s AS trade_date,
                        %(row_count)s AS row_count,
                        %(extracted_at)s AS extracted_at,
                        %(request_id)s AS request_id,
                        %(api_status)s AS api_status
                ) AS incoming
                ON loads.trade_date = incoming.trade_date
                WHEN MATCHED THEN UPDATE SET
                    row_count = incoming.row_count,
                    extracted_at = incoming.extracted_at,
                    request_id = incoming.request_id,
                    api_status = incoming.api_status,
                    loaded_at = SYSDATE()
                WHEN NOT MATCHED THEN INSERT (
                    trade_date,
                    row_count,
                    extracted_at,
                    request_id,
                    api_status,
                    loaded_at
                ) VALUES (
                    incoming.trade_date,
                    incoming.row_count,
                    incoming.extracted_at,
                    incoming.request_id,
                    incoming.api_status,
                    SYSDATE()
                )
                """,
                {
                    "trade_date": trade_date,
                    "row_count": row_count,
                    "extracted_at": extracted_at,
                    "request_id": request_id,
                    "api_status": api_status,
                },
            )
            connection.commit()
        except Exception as err:
            try:
                connection.rollback()
            except Exception as rollback_err:
                raise RuntimeError(
                    f"rollback failed after load error: {err}; "
                    f"rollback error: {rollback_err}"
                ) from err
            if isinstance(err, RuntimeError):
                raise
            raise RuntimeError(f"snowflake load failed: {err}") from err
    finally:
        if connection is not None:
            connection.close()


def main(argv: list[str] | None = None) -> None:
    load_local_env()
    parser = argparse.ArgumentParser(
        description="Reload one landed daily-bars date into Snowflake RAW."
    )
    parser.add_argument(
        "trade_date",
        type=date.fromisoformat,
        help="Market date to load, YYYY-MM-DD",
    )
    args = parser.parse_args(argv)
    copy_daily_bars(args.trade_date)


if __name__ == "__main__":
    main()
