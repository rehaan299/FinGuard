-- Rule: vendor_id must exist in vendor_master
INSERT INTO {{EXCEPTIONS_TABLE}}
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  t.transaction_id,
  "VALID_VENDOR" AS rule_name,
  "MEDIUM" AS severity,
  "Vendor does not exist in vendor master list" AS description,
  t.amount,
  t.vendor_id,
  t.invoice_id,
  CURRENT_TIMESTAMP() AS detected_at
FROM {{VALIDATED_TABLE}} t
LEFT JOIN {{VENDOR_MASTER}} v
ON t.vendor_id = v.vendor_id
WHERE t.ingested_date = DATE("{{RUN_DATE}}")
  AND t.vendor_id IS NOT NULL
  AND v.vendor_id IS NULL;
