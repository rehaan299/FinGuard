-- Rule: duplicate invoice_id for same vendor_id is suspicious
INSERT INTO {{EXCEPTIONS_TABLE}}
WITH dups AS (
  SELECT
    vendor_id,
    invoice_id,
    COUNT(*) AS cnt
  FROM {{VALIDATED_TABLE}}
  WHERE ingested_date = DATE("{{RUN_DATE}}")
    AND vendor_id IS NOT NULL
    AND invoice_id IS NOT NULL
  GROUP BY vendor_id, invoice_id
  HAVING COUNT(*) > 1
)
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  t.transaction_id,
  "DUPLICATE_INVOICE" AS rule_name,
  "HIGH" AS severity,
  "Duplicate invoice_id for same vendor detected" AS description,
  t.amount,
  t.vendor_id,
  t.invoice_id,
  CURRENT_TIMESTAMP() AS detected_at
FROM {{VALIDATED_TABLE}} t
JOIN dups d
ON t.vendor_id = d.vendor_id AND t.invoice_id = d.invoice_id
WHERE t.ingested_date = DATE("{{RUN_DATE}}");
