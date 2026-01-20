-- Build daily summary for this run_date
DELETE FROM {{SUMMARY_TABLE}}
WHERE run_date = DATE("{{RUN_DATE}}");

INSERT INTO {{SUMMARY_TABLE}}
WITH totals AS (
  SELECT COUNT(*) AS total_transactions
  FROM {{VALIDATED_TABLE}}
  WHERE ingested_date = DATE("{{RUN_DATE}}")
),
flagged AS (
  SELECT
    COUNT(DISTINCT transaction_id) AS flagged_transactions,
    SUM(CASE WHEN severity = "HIGH" THEN 1 ELSE 0 END) AS high_severity_count,
    SUM(CASE WHEN severity = "MEDIUM" THEN 1 ELSE 0 END) AS medium_severity_count,
    SUM(CASE WHEN severity = "LOW" THEN 1 ELSE 0 END) AS low_severity_count
  FROM {{EXCEPTIONS_TABLE}}
  WHERE run_date = DATE("{{RUN_DATE}}")
)
SELECT
  DATE("{{RUN_DATE}}") AS run_date,
  t.total_transactions,
  f.flagged_transactions,
  f.high_severity_count,
  f.medium_severity_count,
  f.low_severity_count,
  SAFE_DIVIDE(f.flagged_transactions, t.total_transactions) * 100 AS flagged_percent,
  CURRENT_TIMESTAMP() AS created_at
FROM totals t
CROSS JOIN flagged f;
