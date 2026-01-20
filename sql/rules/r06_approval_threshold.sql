-- Rule: high-value transactions must be approved
INSERT INTO {{EXCEPTIONS_TABLE}}
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  transaction_id,
  "APPROVAL_REQUIRED" AS rule_name,
  "HIGH" AS severity,
  "High-value transaction is not approved" AS description,
  amount,
  vendor_id,
  invoice_id,
  CURRENT_TIMESTAMP() AS detected_at
FROM {{VALIDATED_TABLE}}
WHERE ingested_date = DATE("{{RUN_DATE}}")
  AND amount >= CAST({{APPROVAL_THRESHOLD}} AS NUMERIC)
  AND approval_status != "APPROVED";
