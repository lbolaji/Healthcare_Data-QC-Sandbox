-- One-time setup: run in Athena Query Editor (or via AWS CLI).
-- Replace <your-analytics-bucket> and <prefix> with values from config.yaml publish block.
-- After running, Power BI connects via ODBC: Server=athena.<region>.amazonaws.com
-- Database: healthcare_qc   Table: qc_issues

CREATE DATABASE IF NOT EXISTS healthcare_qc;

CREATE EXTERNAL TABLE IF NOT EXISTS healthcare_qc.qc_issues (
  run_date  STRING,
  client    STRING,
  domain    STRING,
  row_id    STRING,
  column    STRING,
  rule      STRING,
  severity  STRING,
  detail    STRING
)
PARTITIONED BY (
  client_part STRING,
  domain_part STRING,
  date_part   STRING
)
STORED AS PARQUET
LOCATION 's3://<your-analytics-bucket>/<prefix>/qc_issues/'
TBLPROPERTIES ('projection.enabled' = 'false');

-- After the table is created, run this once per new date partition
-- (or enable partition projection to avoid running it at all):
--
-- MSCK REPAIR TABLE healthcare_qc.qc_issues;
--
-- To enable automatic partition projection instead (recommended for daily loads):
--
-- ALTER TABLE healthcare_qc.qc_issues
-- SET TBLPROPERTIES (
--   'projection.enabled'       = 'true',
--   'projection.date_part.type'  = 'date',
--   'projection.date_part.range' = '2026-01-01,NOW',
--   'projection.date_part.format'= 'yyyy-MM-dd',
--   'projection.date_part.interval' = '1',
--   'projection.date_part.interval.unit' = 'DAYS',
--   'storage.location.template' = 's3://<your-analytics-bucket>/<prefix>/qc_issues/client=${client_part}/domain=${domain_part}/date=${date_part}/'
-- );
