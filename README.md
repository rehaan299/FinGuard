# FinGuard — Automated Transaction Validation & Controls Platform

FinGuard is a small “financial controls” system that checks daily transaction data and flags problems early.

It is inspired by real enterprise workflows (like SAP S/4HANA / Ariba controls), but built using:
- Python (data generation + ingestion)
- BigQuery (storage + validation rules)
- Google Apps Script (email alerts)

---

## What this project does

Every day, FinGuard:
1) Generates a CSV of transactions (synthetic data for demo/testing)
2) Ingests it into BigQuery (`raw_transactions`)
3) Normalizes it into a clean table (`validated_transactions`)
4) Runs rule checks (missing fields, negative amounts, duplicates, etc.)
5) Writes violations into `transaction_exceptions`
6) Builds a daily summary table (`daily_validation_summary`)
7) (Optional) Sends an email alert if something looks risky

---

## Why this exists

In real finance systems, transaction data can have problems like:
- missing vendor IDs
- negative values
- duplicate invoices
- invalid cost centers
- missing approvals for high-value spending

FinGuard catches these issues automatically so:
- finance teams can review exceptions faster
- audits become easier (everything is logged)
- data quality stays consistent

---

## BigQuery tables

**raw_transactions**
- exactly what we ingested (raw + immutable)

**validated_transactions**
- cleaned version of raw data (standardized schema)

**transaction_exceptions**
- one row per violation (rule name, severity, description)

**daily_validation_summary**
- daily totals + flagged counts + severity breakdown

**vendor_master / cost_center_master**
- simple lookup lists (for “valid vendor/cost center” rules)

---

## Quickstart (local → BigQuery)

### 1) Install dependencies
```bash
pip install -r requirements.txt
