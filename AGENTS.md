# Agent notes

Rules for agents changing this repository. `README.md` is the system description. Phase notes and the decision log live under `plans/`. Follow the contracts in this file when a plan and this file disagree; then update both.

## Current work

Follow `plans/02-raw-load.md`. Open fixes are in `plans/improvements.md`. Phase status and the decision log are in `plans/00-roadmap.md`.

Phase 1 extract is committed on `main`. `load/copy_daily_bars.py` and `snowflake/bootstrap.sql` exist only in the working tree on `snowflake-raw`. They are not committed and not covered by tests. A live trading-day load succeeded. A closed-day load and the zero-row Parquet `COPY` result shape have not been confirmed. Do not describe Phase 2 as done. dbt and Airflow are not started.

`.env.example` lists `AWS_DEFAULT_REGION` and the Snowflake names. It does not list access keys. The code reads `AWS_DEFAULT_REGION`. boto3 credentials come from the normal chain (process environment, then `~/.aws/credentials`, and so on). This developer's machine has no shared AWS credentials file; local runs load `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the private gitignored `.env`. Deployed runs are expected to set those in the environment the same way. Do not put access keys in `.env.example`.

## Layout

- `extract/` lands one date on S3. `load/` reloads that date into Snowflake. `snowflake/` is bootstrap SQL. `tests/` is pytest. `plans/` is the roadmap and phase notes.
- There is no `src/`, `lib/`, or `utils/`. Search `extract/`, `load/`, `snowflake/`, and `tests/` before adding a function.
- Modules run as `python -m extract.daily_bars` and `python -m load.copy_daily_bars`. `extract/__init__.py` is empty; keep it empty. Do not add `load/__init__.py` or any other package init.

## Setup and installs

Use the existing `.venv` only. Do not create a second venv or use system Python. Do not add `uv`, a lock file, a `[project]` table, or a `[build-system]`. Do not `pip install .` or `-e .`.

From the repository root, with the venv Python (`.venv\Scripts\python.exe` on Windows, `.venv/bin/python` on macOS or Linux):

```text
python -m pip install -r requirements.txt
python -m pip install --group dev
```

The group install does not include runtime, so run both.

- `requirements.txt` is the runtime source of truth (exact `==` pins). Dev tools (pytest, responses, moto[s3], Ruff) are the `dev` group in `[dependency-groups]` in `pyproject.toml`. There is no `requirements-dev.txt`.
- Bump a runtime pin in `requirements.txt` and a dev pin in `pyproject.toml`.
- The interpreter is Python 3.14 (`pyproject.toml` `target-version`).

## Verify

From the repository root, with the venv Python: `ruff check`, `ruff format --check`, and `pytest`. Fix lint and test failures you find.

- An extract landing change needs a test in `tests/test_daily_bars.py`.
- A load change needs a test in `tests/test_copy_daily_bars.py`. That file is not written yet. Mock S3 with moto and mock `snowflake.connector.connect`. Do not open a live bucket, API, or Snowflake account from pytest.

## Standards

Production behavior matters more than speed of implementation. Prefer the simplest code that fully meets the current requirement, and prefer code a reader can follow in one pass.

- Do not over-engineer. No extra config knobs, wrapper helpers, classes, or plugin hooks. A function that is called once can stay inline.
- Comments explain why, not what the next line does. Worth keeping: why `adjusted` is false, why a closed day still gets a file, why an empty trading session must fail, why the manifest is written last, why `FORCE = TRUE` is set, why `MATCH_BY_COLUMN_NAME` is `CASE_SENSITIVE`, why `trade_date` is nullable and set after `COPY`, why a zero-row `COPY` result shape is not asserted, and why parquet `LastModified` is compared to the manifest object.
- Use the libraries already pinned. Check the installed package before writing a replacement or adding a dependency.
- Do not add the `massive` package. Do not call the API once per ticker.

## Docs

- Update this file when a project decision changes.
- Update the relevant `plans/` file and the decision log in `plans/00-roadmap.md` in the same change.
- Keep `README.md` status honest: Built, In progress, Planned. Lead with the system, the grain, and the design decisions. Do not turn it into a tutorial or a tool glossary.
- Do not read `.env`. It holds secrets.

## Extract

`python -m extract.daily_bars YYYY-MM-DD` lands one US stock-market date as Parquet on S3. Cleansing belongs in SQL. Phase note: `plans/01-extract.md`.

- One grouped GET with `requests`: `https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{date}`, query `adjusted=false` and `include_otc=false`, headers `Authorization: Bearer {MASSIVE_API_KEY}` and `Accept-Encoding: gzip`. Do not loop tickers. Call `load_local_env()` once at the start of `main`, before any getter.
- `adjusted` is forced false. Drop no rows, rename no fields, and filter nothing.
- The nine columns `T`, `o`, `h`, `l`, `c`, `v`, `vw`, `t`, and `n` are always present. Types: `T` string, `o`/`h`/`l`/`c`/`v`/`vw` float, `t`/`n` int. Keep extra keys. Do not add `trade_date`, `adjusted`, or `extracted_at` to the parquet.
- Manifest fields: `trade_date`, `row_count`, `extracted_at`, `request_id`, `api_status`, and `request_params` (`adjusted`, `include_otc`). A zero-row file uses the same columns. Keys: `bronze/daily_bars/trade_date=YYYY-MM-DD/bars.parquet` and `manifest.json`. The prefix `bronze/daily_bars` is hard-coded.
- Region is `AWS_DEFAULT_REGION`. boto3 reads that name. A blank value is unset. Do not add `AWS_REGION` or an `S3_PREFIX`.
- `status` must be `OK` or `DELAYED`, and `len(results)` must equal `resultsCount`. A missing `resultsCount` counts as 0. A missing or null `results` counts as an empty list. Otherwise raise `RuntimeError` and do not upload.
- A non-session day with no rows still writes a zero-row file and a manifest with `row_count` 0. An empty XNYS session raises `RuntimeError` starting with `no bars for trading session` and does not upload. Build the XNYS calendar once at import with start `2000-01-01`.
- One PUT of the parquet to the final key, then the manifest, which is the commit marker. A non-200 response raises and does not upload, so a previous good object stays in place.
- Retries: `requests.Session` mounted with `HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]))`, then `response.raise_for_status()`. Timeout is `(10, 30)`.

## Load

`python -m load.copy_daily_bars YYYY-MM-DD` reloads one landed date. The procedure is `plans/02-raw-load.md`. Do not weaken the rules below. Call `load_local_env()` once at the start of `main`, before any getter.

- Reuse `s3_bucket()` and `REQUEST_PARAMS`. Required settings: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`. Pass `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` only when it is non-blank. Missing required values raise `RuntimeError`. Database, schema, tables, stage, and file format stay hard-coded.
- Authenticator is `SNOWFLAKE_JWT` with `private_key_file`. `autocommit` is false. Cursor is `DictCursor`. Database is `MARKET`, schema is `RAW`. Session parameters `QUERY_TAG = daily_bars_load` and `STATEMENT_TIMEOUT_IN_SECONDS = 600` are hard-coded.
- Read `manifest.json` and head `bars.parquet` before any SQL. Raise `RuntimeError` and run no SQL when either object is missing, `trade_date` or `request_params` disagree with the request, `row_count` is not an int >= 0, `api_status` is blank, `extracted_at` is not a timestamp, `request_id` is present and not a string, `LastModified` is missing, or the parquet `LastModified` is later than the manifest object's. Equal timestamps are a finished landing. A later parquet means a re-extract is in progress. The manifest `extracted_at` field is set before either upload, so do not compare it to the parquet `LastModified`.
- One transaction: `DELETE` that `trade_date`, then `COPY INTO MARKET.RAW.DAILY_BARS` from `@MARKET.RAW.BRONZE_DAILY_BARS`. `FILES` is that date's `bars.parquet` only. Clauses: `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, `INCLUDE_METADATA = (extracted_at = METADATA$FILE_LAST_MODIFIED)`, `FORCE = TRUE`, `ON_ERROR = ABORT_STATEMENT`. Do not `PURGE`. Do not use a pattern that can read `manifest.json`.
- `CASE_SENSITIVE` is required because the file has both `T` and `t`. Unquoted Snowflake names fold to uppercase, so those columns are created quoted.
- `FORCE = TRUE` is required. Without it, `COPY` skips a file it has already loaded and the `DELETE` wipes the day.
- `INCLUDE_METADATA` supplies bar-row `extracted_at` from the object's last-modified time. It is not a Parquet column. `DAILY_BARS_LOADS.extracted_at` is the manifest clock. They are different values.
- `MATCH_BY_COLUMN_NAME` cannot take a `SELECT`, so `COPY` inserts `trade_date` as null. The column stays nullable. A `NOT NULL` constraint would reject the load. The next statement sets `trade_date` with `TO_DATE(TO_TIMESTAMP_NTZ("t", 3))`. `t` is epoch milliseconds at the 16:00 New York close (20:00 or 21:00 UTC), so its UTC calendar date is the session date. Do not shift it by a day.
- For a positive `row_count`, require one `COPY` result with status `LOADED` and `rows_loaded` equal to the manifest. For `row_count` 0, do not assert the result shape. Fail a closed day only when status is `LOAD_FAILED`, `LOAD_SKIPPED`, or `PARTIALLY_LOADED`. The table count decides the commit.
- The `UPDATE` count must equal the manifest `row_count`. Read the result column. `cursor.rowcount` stays empty when the connector returns Arrow. Then `COUNT_IF(trade_date = date)` must equal `row_count` and `COUNT_IF(trade_date IS NULL)` must be 0.
- `MERGE` one audit row into `MARKET.RAW.DAILY_BARS_LOADS` for the date, including a closed day with `row_count` 0, then `COMMIT`. Any failure `ROLLBACK`s and raises `RuntimeError`. Close the connection in `finally`. The previous rows for that date stay.
- Do not dedupe by latest `extracted_at`. Delete-then-copy keeps one landing per date. Do not rename vendor columns. Extra Parquet keys stay in bronze and are not loaded. Do not enable schema evolution.
- Do not bump the `pyarrow` pin to satisfy the connector's pandas extra. This loader does not use that extra. The extract keeps its own pin.
- The loader does not lock. Overlapping runs for the same date are forbidden. Airflow enforces one active run per date when that phase exists.
- Bootstrap creates `MARKET.RAW.DAILY_BARS`, `MARKET.RAW.DAILY_BARS_LOADS`, file format `MARKET.RAW.DAILY_BARS_PARQUET` (no `USE_LOGICAL_TYPE`), storage integration `MARKET_LAKE`, and stage `MARKET.RAW.BRONZE_DAILY_BARS`. The stage name must not collide with the table stage `@%DAILY_BARS`. The integration location is `s3://<bucket>/bronze/daily_bars/`. Its IAM policy is read-only. The extract writer is a different identity. Do not create the warehouse. Replace placeholders before the account-specific statements. `IF NOT EXISTS` will not repair an integration or stage created while a placeholder was still in it.

## Schedule

The daily DAG is not built. Phase note: `plans/04-airflow.md`.

The daily DAG runs at 06:00 America/New_York, Tuesday through Saturday (`0 6 * * 2-6`), and lands the previous day. `trade_date` is an explicit DAG parameter that defaults to the run's New York date minus one day. Do not derive it from Airflow's `logical_date`.

dbt source freshness reads `DAILY_BARS_LOADS.extracted_at` and warns after about 3 days. A closed day writes an audit row and no bar rows, so freshness does not read `DAILY_BARS`. A Monday holiday can stretch the gap from Saturday 06:00 to Wednesday 06:00, which is past that warning. That edge is known and not changed.

## Later phases

Do not implement these while `plans/02-raw-load.md` is the current step.

dbt (`plans/03-dbt-transform.md`): `RAW` is not a dbt model. Silver is `STAGING`, gold is `MARTS`. Override `generate_schema_name` so those names are not concatenated onto the target schema. Quarantine null ticker, null close, high below low, and negative volume. Full refresh. Do not add incremental models or surrogate keys yet. Do not dedupe staging by `extracted_at`. Freshness is the schedule rule above.

Airflow (`plans/04-airflow.md`): the DAG stays thin and calls the same extract and load functions, then `dbt build`. Backfill walks dates one at a time. Secrets stay in the environment. The dbt profile reads them with `env_var`. Snowflake auth is key-pair. Before building the Airflow image, check its supported Python and constraints file against this repo's 3.14 venv.

The backfill DAG is not implemented.
