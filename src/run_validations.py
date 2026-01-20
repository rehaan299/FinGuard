import argparse
import glob
import os

from utils import (
    load_config,
    bq_client,
    dataset_table,
    read_sql,
    render_sql,
    parse_date,
)


def run_query(client, sql: str) -> None:
    job = client.query(sql)
    job.result()


def create_tables(client, cfg) -> None:
    project_id = cfg["gcp"]["project_id"]
    dataset_id = cfg["gcp"]["dataset_id"]

    sql_text = read_sql("sql/00_create_tables.sql")

    rendered = render_sql(
        sql_text,
        RAW_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["raw_transactions"]),
        VALIDATED_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["validated_transactions"]),
        EXCEPTIONS_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["transaction_exceptions"]),
        SUMMARY_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["daily_validation_summary"]),
        VENDOR_MASTER=dataset_table(project_id, dataset_id, cfg["tables"]["vendor_master"]),
        COST_CENTER_MASTER=dataset_table(project_id, dataset_id, cfg["tables"]["cost_center_master"]),
    )

    run_query(client, rendered)
    print("[OK] Tables created / ensured.")


def normalize(client, cfg, run_date: str) -> None:
    project_id = cfg["gcp"]["project_id"]
    dataset_id = cfg["gcp"]["dataset_id"]

    sql_text = read_sql("sql/10_normalize_transactions.sql")

    rendered = render_sql(
        sql_text,
        RUN_DATE=run_date,
        RAW_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["raw_transactions"]),
        VALIDATED_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["validated_transactions"]),
    )

    run_query(client, rendered)
    print(f"[OK] Normalized transactions for {run_date} → validated_transactions")


def run_rules(client, cfg, run_date: str) -> None:
    project_id = cfg["gcp"]["project_id"]
    dataset_id = cfg["gcp"]["dataset_id"]

    rules = sorted(glob.glob("sql/rules/*.sql"))
    if not rules:
        raise FileNotFoundError("No rule files found in sql/rules/")

    for rule_path in rules:
        sql_text = read_sql(rule_path)
        rendered = render_sql(
            sql_text,
            RUN_DATE=run_date,
            VALIDATED_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["validated_transactions"]),
            EXCEPTIONS_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["transaction_exceptions"]),
            VENDOR_MASTER=dataset_table(project_id, dataset_id, cfg["tables"]["vendor_master"]),
            COST_CENTER_MASTER=dataset_table(project_id, dataset_id, cfg["tables"]["cost_center_master"]),
            APPROVAL_THRESHOLD=str(cfg["settings"]["approval_threshold"]),
        )

        run_query(client, rendered)
        print(f"[OK] Ran rule: {os.path.basename(rule_path)}")


def build_summary(client, cfg, run_date: str) -> None:
    project_id = cfg["gcp"]["project_id"]
    dataset_id = cfg["gcp"]["dataset_id"]

    sql_text = read_sql("sql/90_build_daily_summary.sql")

    rendered = render_sql(
        sql_text,
        RUN_DATE=run_date,
        VALIDATED_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["validated_transactions"]),
        EXCEPTIONS_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["transaction_exceptions"]),
        SUMMARY_TABLE=dataset_table(project_id, dataset_id, cfg["tables"]["daily_validation_summary"]),
    )

    run_query(client, rendered)
    print(f"[OK] Built daily summary for {run_date}")


def main():
    parser = argparse.ArgumentParser(description="Run FinGuard validations in BigQuery.")
    parser.add_argument("--date", help="Run date in YYYY-MM-DD (required unless --create-tables)")
    parser.add_argument("--create-tables", action="store_true", help="Create/ensure BigQuery tables")
    args = parser.parse_args()

    cfg = load_config()
    client = bq_client(cfg["gcp"]["project_id"])

    if args.create_tables:
        create_tables(client, cfg)
        return

    if not args.date:
        raise ValueError("--date is required unless --create-tables is used.")

    run_date = parse_date(args.date)

    normalize(client, cfg, run_date)
    run_rules(client, cfg, run_date)
    build_summary(client, cfg, run_date)

    print("[DONE] FinGuard validation pipeline finished.")


if __name__ == "__main__":
    main()
