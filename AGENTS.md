# Agent notes

Rules for agents changing this repository. `README.md` is the system description. Phase status, checklists, open fixes, and the decision log live under `plans/` only. Do not update `README.md` or this file on every plan step. Update them when a durable project decision, layout, command, style rule, test rule, or security rule changes - or when finishing a phase needs a short README status pass.

When a plan and this file disagree on an engineering contract, follow this file, then update both. When only phase progress changes, update `plans/` alone.

## Project overview

Daily US equity bars: one Massive grouped request per market date lands as Parquet plus a manifest on S3; Snowflake loads that date into `MARKET.RAW`; dbt builds silver (`STAGING`) and gold (`MARTS`); Airflow is planned to schedule extract, load, then `dbt build`. Cleansing belongs in SQL, not in the extract.

| Path | Role |
| --- | --- |
| `extract/` | Land one date on S3 (`python -m extract.daily_bars`) |
| `load/` | Reload one date into Snowflake (`python -m load.copy_daily_bars`) |
| `transform/` | dbt project (`market_bars`); run from here with `--profiles-dir .` |
| `snowflake/` | Bootstrap SQL |
| `tests/` | pytest |
| `plans/` | Roadmap, phase notes, improvements (current step lives here) |

There is no `src/`, `lib/`, or `utils/`. Search the packages above before adding a function. `extract/__init__.py` is empty; keep it empty. Do not add `load/__init__.py` or other package inits.

Follow `plans/00-roadmap.md` and the active phase note for what to implement next. Do not implement later phases while that note is the current step. Procedure detail for extract, load, and dbt stays in `plans/01-extract.md`, `plans/02-raw-load.md`, and `plans/03-dbt-transform.md` - do not weaken those contracts in code.

## Build and test commands

Use the existing `.venv` only. Do not create a second venv, use system Python, add `uv`, a lock file, a `[project]` table, or a `[build-system]`. Do not `pip install .` or `-e .`.

Interpreter is Python 3.14 (`pyproject.toml` `target-version`). From the repository root, with the venv Python (`.venv\Scripts\python.exe` on Windows, `.venv/bin/python` on macOS or Linux):

```text
python -m pip install -r requirements.txt
python -m pip install --group dev
```

The group install does not include runtime, so run both. `requirements.txt` is the runtime source of truth (exact `==` pins), including `dbt-snowflake` and `dbt-core` (pinned on 1.12.x so pip does not pull a 1.13 beta). Dev tools (pytest, responses, moto[s3], Ruff) are the `dev` group in `[dependency-groups]` in `pyproject.toml`. There is no `requirements-dev.txt`. Bump a runtime pin in `requirements.txt` and a dev pin in `pyproject.toml`. `dbt_utils` is pinned in `transform/packages.yml` and installed with `dbt deps` from `transform/`, not pip.

Verify from the repository root with the venv Python:

```text
ruff check
ruff format --check
pytest
```

From `transform/`: `dbt deps --profiles-dir .` and `dbt parse --profiles-dir .` (or `dbt build` when models are ready). Fix lint and test failures you find, including ones you did not cause.

## Code style guidelines

Prefer the simplest code that fully meets the current requirement and that a reader can follow in one pass. Production behavior matters more than speed of implementation.

- No extra config knobs, wrapper helpers, classes, or plugin hooks. A function called once can stay inline.
- Comments explain why, not what the next line does.
- Use libraries already pinned. Check the installed package before writing a replacement or adding a dependency.
- Do not add the `massive` package. Do not call the API once per ticker.
- Call `load_local_env()` once at the start of `main`, before any getter.
- Database, schema, tables, stage, file format, and the S3 prefix `bronze/daily_bars` stay hard-coded where the code already hard-codes them.
- Vendor column names stay as landed until staging. Do not rename in the extract or loader.
- Do not dedupe silver by `extracted_at`; delete-then-copy leaves one landing per date. Catch broken loads with uniqueness tests.
- dbt `generate_schema_name` must return the custom schema as-is so `STAGING` / `MARTS` are not concatenated onto the target schema.
- Gold (`MARTS`) star: `dim_date`, `dim_ticker`, `fact_daily_bar`, and `fact_daily_bar_performance`. Performance is a second fact at the same grain, linked only through the shared dims - not a dim and not a bridge table. `dollar_volume` is `close * volume`.
- Keep `README.md` a system description (grain, architecture, design decisions), not a changelog or tutorial. Do not turn it into a tool glossary.

## Testing instructions

- Extract landing changes: add coverage in `tests/test_daily_bars.py`.
- Load changes: add coverage in `tests/test_copy_daily_bars.py`. Mock S3 with moto and mock `snowflake.connector.connect`.
- Do not open a live bucket, Massive API, or Snowflake account from pytest.
- Prefer tests that lock the contracts in `plans/` (manifest checks, delete-then-copy shape, zero-row `COPY`, session parameters, dbt source/staging contracts) over brittle internal structure.

## Security considerations

- Do not read `.env`. It holds secrets. Do not commit it, keys (`*.p8`, `*.pem`), or other credentials.
- `.env.example` lists names such as `AWS_DEFAULT_REGION` and the Snowflake settings. It must not list access keys or other secret values.
- Runtime secrets come from the environment: `MASSIVE_API_KEY`, AWS credentials via the normal boto3 chain, Snowflake key-pair path and optional passphrase. This machine has no shared AWS credentials file; local runs load AWS keys from the private gitignored `.env`. Deployed runs set the same names in the environment.
- Region is `AWS_DEFAULT_REGION` only. Do not add `AWS_REGION`.
- Snowflake auth is key-pair (`SNOWFLAKE_JWT`). The storage integration IAM policy is read-only; the extract writer is a different identity.
- dbt `transform/profiles.yml` uses the same Snowflake env names via `env_var` only. No secrets in that file. dbt loads `.env` from its working directory (`transform/`), not the repo-root `.env` that Python reads via `load_local_env()`. Use a `transform/.env` link to the root `.env` (gitignored); do not commit secrets.
- Do not print or log secret values.
