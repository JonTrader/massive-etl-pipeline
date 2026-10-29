USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS MARKET;

CREATE SCHEMA IF NOT EXISTS MARKET.RAW;

-- dbt silver and gold (generate_schema_name returns these names as-is).
CREATE SCHEMA IF NOT EXISTS MARKET.STAGING;
CREATE SCHEMA IF NOT EXISTS MARKET.MARTS;

-- "T" and "t" collide when quoted identifiers ignore case.
ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;

-- No USE_LOGICAL_TYPE: the landed Parquet file has no logical types.
CREATE FILE FORMAT IF NOT EXISTS MARKET.RAW.DAILY_BARS_PARQUET
  TYPE = PARQUET;

CREATE TABLE IF NOT EXISTS MARKET.RAW.DAILY_BARS (
  "T" VARCHAR,
  "o" FLOAT,
  "h" FLOAT,
  "l" FLOAT,
  "c" FLOAT,
  "v" FLOAT,
  "vw" FLOAT,
  "t" NUMBER,
  "n" NUMBER,
  trade_date DATE COMMENT 'Nullable because COPY with MATCH_BY_COLUMN_NAME inserts it as null, and the loader sets it in the next statement of the same transaction. A NOT NULL constraint would reject the COPY.',
  extracted_at TIMESTAMP_NTZ NOT NULL COMMENT 'The Parquet object''s S3 last-modified time in UTC (METADATA$FILE_LAST_MODIFIED), not the manifest clock.'
)
COMMENT = 'Vendor names are quoted for a case-sensitive match of T vs t. Unquoted Snowflake names fold to uppercase.';

CREATE TABLE IF NOT EXISTS MARKET.RAW.DAILY_BARS_LOADS (
  trade_date DATE NOT NULL COMMENT 'Informational primary key. Snowflake does not enforce it.',
  row_count NUMBER NOT NULL,
  extracted_at TIMESTAMP_NTZ NOT NULL COMMENT 'The manifest clock, UTC, which is a different value from DAILY_BARS.extracted_at.',
  request_id VARCHAR COMMENT 'Nullable because the API sometimes omits it.',
  api_status VARCHAR NOT NULL,
  loaded_at TIMESTAMP_NTZ NOT NULL,
  PRIMARY KEY (trade_date)
)
COMMENT = 'One row per loaded date, including closed days with row_count 0, so a closed session is distinct from a date that was never loaded.';

-- Replace every <placeholder> before running the statements below.
-- IF NOT EXISTS will not repair an integration or stage created while a
-- placeholder was still in it. Drop that object and run it again.
--
-- In AWS, create the IAM role first and attach the read-only policy below.
-- The trust policy cannot be finished until the integration exists: CREATE
-- needs the role ARN, then DESC INTEGRATION returns the Snowflake user and
-- external ID that the trust policy must allow.

/*
Read-only policy for the IAM role named in STORAGE_AWS_ROLE_ARN.
The extract writer is a different identity, so this policy has no write actions.
GetBucketLocation is its own statement: that call does not send s3:prefix, so
the ListBucket condition would deny it.
ListBucket is allowed on the prefix itself and on the keys under it.

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::<bucket>/bronze/daily_bars/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::<bucket>",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "bronze/daily_bars/",
            "bronze/daily_bars/*"
          ]
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::<bucket>"
    }
  ]
}
*/

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS MARKET_LAKE
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<iam-role-arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/bronze/daily_bars/');

/*
Run DESC INTEGRATION MARKET_LAKE. Put STORAGE_AWS_IAM_USER_ARN and
STORAGE_AWS_EXTERNAL_ID into the role trust policy, or Snowflake cannot
assume the role.

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::081452989562:user/fhyd2000-s"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "RN57925_SFCRole=3_yx5xUAu802bJJvAmkhM6WGe9xQI="
        }
      }
    }
  ]
}
*/

-- SYSADMIN cannot create a stage that uses an integration it cannot use.
GRANT USAGE ON INTEGRATION MARKET_LAKE TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE STAGE IF NOT EXISTS MARKET.RAW.BRONZE_DAILY_BARS
  URL = 's3://<bucket>/bronze/daily_bars/'
  STORAGE_INTEGRATION = MARKET_LAKE
  FILE_FORMAT = MARKET.RAW.DAILY_BARS_PARQUET
  COMMENT = 'Not named DAILY_BARS, because that name would be confused with the table stage @%DAILY_BARS.';

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS LOADER;

-- The warehouse already exists and should suspend when idle.
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE LOADER;

GRANT USAGE ON DATABASE MARKET TO ROLE LOADER;
GRANT USAGE ON SCHEMA MARKET.RAW TO ROLE LOADER;
GRANT USAGE ON SCHEMA MARKET.STAGING TO ROLE LOADER;
GRANT USAGE ON SCHEMA MARKET.MARTS TO ROLE LOADER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA MARKET.STAGING TO ROLE LOADER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA MARKET.MARTS TO ROLE LOADER;
GRANT USAGE ON STAGE MARKET.RAW.BRONZE_DAILY_BARS TO ROLE LOADER;
GRANT USAGE ON FILE FORMAT MARKET.RAW.DAILY_BARS_PARQUET TO ROLE LOADER;

-- UPDATE is required because the loader sets trade_date after COPY.
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE MARKET.RAW.DAILY_BARS TO ROLE LOADER;

-- MERGE updates the existing date row.
GRANT SELECT, INSERT, UPDATE ON TABLE MARKET.RAW.DAILY_BARS_LOADS TO ROLE LOADER;

-- The loader user already exists in the account. This file does not create it.
GRANT ROLE LOADER TO USER <loader-user>;

-- The public key is the base64 body only, without the PEM header or footer.
ALTER USER <loader-user> SET RSA_PUBLIC_KEY = '<public-key>';
