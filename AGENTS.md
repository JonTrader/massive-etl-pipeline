# Agent notes

Rules for agents changing this repository. `README.md` is the system description. Phase notes, open fixes, and the decision log live under `plans/`. When a plan and this file disagree, follow this file, then update both.

## Project overview

Daily US equity bars: one Massive grouped request per market date lands as Parquet plus a manifest on S3; Snowflake loads that date into `MARKET.RAW`. dbt transform Phase 3 Step 2 sources are in place under `transform/` (`sources.yml` for `RAW.DAILY_BARS` and freshness on `RAW.DAILY_BARS_LOADS`; staging/mart models not written yet; next is Step 3). Airflow is planned and not started. Cleansing belongs in SQL, not in the extract.

| Path | Role |
| --- | --- |
| `extract/` | Land one date on S3 (`python -m extract.daily_bars`) |
| `load/` | Reload one date into Snowflake (`python -m load.copy_daily_bars`) |
| `transform/` | dbt project (`market_bars`); sources declared; staging/mart models not written yet |
| `snowflake/` | Bootstrap SQL |
| `tests/` | pytest |
| `plans/` | Roadmap, phase notes, improvements |

There is no `src/`, `lib/`, or `utils/`. Search those packages above before adding a function. `extract/__init__.py` is empty; keep it empty. Do not add `load/__init__.py` or other package inits.

Current phase and status: follow `plans/00-roadmap.md` and the active phase note it points to. Do not implement later phases while that note is the current step. Procedure detail for extract and load stays in `plans/01-extract.md` and `plans/02-raw-load.md` - do not weaken those contracts in code. Phase 3 procedure is `plans/03-dbt-transform.md`; stay on the checklist step it marks current. Do not implement Airflow.

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

Fix lint and test failures you find, including ones you did not cause.

## Code style guidelines

Prefer the simplest code that fully meets the current requirement and that a reader can follow in one pass. Production behavior matters more than speed of implementation.

- No extra config knobs, wrapper helpers, classes, or plugin hooks. A function called once can stay inline.
- Comments explain why, not what the next line does.
- Use libraries already pinned. Check the installed package before writing a replacement or adding a dependency.
- Do not add the `massive` package. Do not call the API once per ticker.
- Call `load_local_env()` once at the start of `main`, before any getter.
- Database, schema, tables, stage, file format, and the S3 prefix `bronze/daily_bars` stay hard-coded where the code already hard-codes them.
- Vendor column names stay as landed until staging. Do not rename in the extract or loader.
- Update this file when a project decision changes. Update the relevant `plans/` file and `plans/00-roadmap.md` in the same change.
- Keep `README.md` status honest (Built, In progress, Planned). Lead with the system, the grain, and the design decisions. Do not turn it into a tutorial or a tool glossary.

## Testing instructions

- Extract landing changes: add coverage in `tests/test_daily_bars.py`.
- Load changes: add coverage in `tests/test_copy_daily_bars.py`. Mock S3 with moto and mock `snowflake.connector.connect`.
- Do not open a live bucket, Massive API, or Snowflake account from pytest.
- Prefer tests that lock the contracts in `plans/` (manifest checks, delete-then-copy shape, zero-row `COPY`, session parameters) over brittle internal structure.

## Security considerations

- Do not read `.env`. It holds secrets. Do not commit it, keys (`*.p8`, `*.pem`), or other credentials.
- `.env.example` lists names such as `AWS_DEFAULT_REGION` and the Snowflake settings. It must not list access keys or other secret values.
- Runtime secrets come from the environment: `MASSIVE_API_KEY`, AWS credentials via the normal boto3 chain, Snowflake key-pair path and optional passphrase. This machine has no shared AWS credentials file; local runs load AWS keys from the private gitignored `.env`. Deployed runs set the same names in the environment.
- Region is `AWS_DEFAULT_REGION` only. Do not add `AWS_REGION`.
- Snowflake auth is key-pair (`SNOWFLAKE_JWT`). The storage integration IAM policy is read-only; the extract writer is a different identity.
- dbt `transform/profiles.yml` uses the same Snowflake env names via `env_var` only. No secrets in that file.
- Do not print or log secret values.
