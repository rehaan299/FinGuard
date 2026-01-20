-- Creates core FinGuard tables if they do not exist.
-- Note: Dataset must already exist in BigQuery.

CREATE TABLE IF NOT EXISTS {{RAW_TABLE}} (
  transaction_id STRING,
  posting_date STRING,
  vendor_id STRING,
  amount FLOAT64,
  currency STRING,
  cost_center STRING,
  gl_account STRING,
  invoice_id STRING,
  approval_status STRING,
  source_system STRING,
  created_at STRING,
  ingested_date STRING
);

CREATE TABLE IF NOT EXISTS {{VALIDATED_TABLE}} (
  transaction_id STRING,
  posting_date DATE,
  vendor_id STRING,
  amount NUMERIC,
  currency STRING,
  cost_center STRING,
  gl_account STRING,
  invoice_id STRING,
  approval_status STRING,
  source_system STRING,
  created_at TIMESTAMP,
  ingested_date DATE
);

CREATE TABLE IF NOT EXISTS {{EXCEPTIONS_TABLE}} (
  run_date DATE,
  transaction_id STRING,
  rule_name STRING,
  severity STRING,
  description STRING,
  amount NUMERIC,
  vendor_id STRING,
  invoice_id STRING,
  detected_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{SUMMARY_TABLE}} (
  run_date DATE,
  total_transactions INT64,
  flagged_transactions INT64,
  high_severity_count INT64,
  medium_severity_count INT64,
  low_severity_count INT64,
  flagged_percent FLOAT64,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{VENDOR_MASTER}} (
  vendor_id STRING,
  vendor_name STRING,
  status STRING
);

CREATE TABLE IF NOT EXISTS {{COST_CENTER_MASTER}} (
  cost_center STRING,
  department STRING,
  status STRING
);

-- Seed small reference data (idempotent using MERGE)
MERGE {{VENDOR_MASTER}} T
USING (
  SELECT "V00001" AS vendor_id, "Vendor One" AS vendor_name, "ACTIVE" AS status UNION ALL
  SELECT "V00002", "Vendor Two", "ACTIVE" UNION ALL
  SELECT "V00003", "Vendor Three", "ACTIVE" UNION ALL
  SELECT "V00004", "Vendor Four", "ACTIVE"
) S
ON T.vendor_id = S.vendor_id
WHEN NOT MATCHED THEN INSERT (vendor_id, vendor_name, status) VALUES (S.vendor_id, S.vendor_name, S.status);

MERGE {{COST_CENTER_MASTER}} T
USING (
  SELECT "CC0100" AS cost_center, "Operations" AS department, "ACTIVE" AS status UNION ALL
  SELECT "CC0101", "Procurement", "ACTIVE" UNION ALL
  SELECT "CC0102", "Finance", "ACTIVE" UNION ALL
  SELECT "CC0103", "IT", "ACTIVE"
) S
ON T.cost_center = S.cost_center
WHEN NOT MATCHED THEN INSERT (cost_center, department, status) VALUES (S.cost_center, S.department, S.status);
