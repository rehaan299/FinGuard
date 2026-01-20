import argparse
import pandas as pd

from utils import load_config, bq_client, dataset_table, parse_date


def main():
    parser = argparse.ArgumentParser(description="Ingest daily CSV transactions into BigQuery raw table.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD")
    args = parser.parse_args()

    run_date = parse_date(args.date)
    cfg = load_config()

    project_id = cfg["gcp"]["project_id"]
    dataset_id = cfg["gcp"]["dataset_id"]
    raw_table_name = cfg["tables"]["raw_transactions"]

    csv_path = f"{cfg['settings']['output_dir']}/transactions_{run_date}.csv"

    df = pd.read_csv(csv_path)

    # Add ingestion metadata
    df["ingested_date"] = run_date

    client = bq_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.{raw_table_name}"

    job_config = None  # Let BigQuery auto-detect schema from dataframe

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print(f"[OK] Ingested {len(df)} rows into {table_ref}")


if __name__ == "__main__":
    main()
