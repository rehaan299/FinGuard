import os
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from google.cloud import bigquery


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Missing {config_path}. Copy config.example.yaml → config.yaml and edit it."
        )
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def bq_client(project_id: str) -> bigquery.Client:
    """
    Uses Application Default Credentials (ADC).
    For local dev:
      - gcloud auth application-default login
    For GitHub Actions:
      - use a service account + auth action
    """
    return bigquery.Client(project=project_id)


def dataset_table(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"`{project_id}.{dataset_id}.{table_name}`"


def read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def render_sql(sql_text: str, **kwargs) -> str:
    """
    Lightweight templating for SQL files.
    Example placeholder: {{RAW_TABLE}}
    """
    out = sql_text
    for k, v in kwargs.items():
        out = out.replace(f"{{{{{k}}}}}", str(v))
    return out


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def parse_date(date_str: str) -> str:
    """
    Validates YYYY-MM-DD and returns it.
    """
    datetime.strptime(date_str, "%Y-%m-%d")
    return date_str
