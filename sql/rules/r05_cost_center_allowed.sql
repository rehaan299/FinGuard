-- Rule: cost_center must exist in cost_center_master
INSERT INTO {{EXCEPTIONS_TABLE}}
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  t.transaction_id,
  "VALID_COST_CENTER" AS rule_name,
  "MEDIUM" AS severity,
  "Cost center not recognized" AS description,
  t.amount,
  t.vendor_id,
  t.invoice_id,
  CURRENT_TIMESTAMP() AS detected_at
FROM {{VALIDATED_TABLE}} t
LEFT JOIN {{COST_CENTER_MASTER}} c
ON t.cost_center = c.cost_center
WHERE t.ingested_date = DATE("{{RUN_DATE}}")
  AND t.cost_center IS NOT NULL
  AND c.cost_center IS NULL;
