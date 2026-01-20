-- Normalizes raw transactions into validated_transactions for a single run_date.

DELETE FROM {{VALIDATED_TABLE}}
WHERE ingested_date = DATE("{{RUN_DATE}}");

INSERT INTO {{VALIDATED_TABLE}}
SELECT
  transaction_id,
  SAFE.PARSE_DATE("%Y-%m-%d", posting_date) AS posting_date,
  vendor_id,
  SAFE_CAST(amount AS NUMERIC) AS amount,
  currency,
  cost_center,
  gl_account,
  invoice_id,
  approval_status,
  source_system,
  SAFE.TIMESTAMP(created_at) AS created_at,
  DATE("{{RUN_DATE}}") AS ingested_date
FROM {{RAW_TABLE}}
WHERE ingested_date = "{{RUN_DATE}}";
