-- Rule: required fields must not be NULL
INSERT INTO {{EXCEPTIONS_TABLE}}
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  transaction_id,
  "REQUIRED_FIELDS" AS rule_name,
  "MEDIUM" AS severity,
  "Missing one or more required fields" AS description,
  amount,
  vendor_id,
  invoice_id,
  CURRENT_TIMESTAMP() AS detected_at
FROM {{VALIDATED_TABLE}}
WHERE ingested_date = DATE("{{RUN_DATE}}")
  AND (
    transaction_id IS NULL OR
    vendor_id IS NULL OR
    amount IS NULL OR
    invoice_id IS NULL OR
    cost_center IS NULL
  );
