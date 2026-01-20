-- Rule: amount must be > 0
INSERT INTO {{EXCEPTIONS_TABLE}}
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  transaction_id,
  "POSITIVE_AMOUNT" AS rule_name,
  "HIGH" AS severity,
  "Amount must be positive" AS description,
  amount,
  vendor_id,
  invoice_id,
  CURRENT_TIMESTAMP() AS detected_at
FROM {{VALIDATED_TABLE}}
WHERE ingested_date = DATE("{{RUN_DATE}}")
  AND amount <= 0;
