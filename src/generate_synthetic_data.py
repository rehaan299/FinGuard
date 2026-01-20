import argparse
import random
import uuid
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd

from utils import load_config, ensure_dir, parse_date


def make_vendor_master() -> List[str]:
    return [f"V{str(i).zfill(5)}" for i in range(1, 51)]  # 50 vendors


def make_cost_centers() -> List[str]:
    return [f"CC{str(i).zfill(4)}" for i in range(100, 140)]  # 40 cost centers


def make_gl_accounts() -> List[str]:
    return ["600100", "600200", "610000", "620000", "700100", "710500"]


def inject_error(tx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Intentionally breaks a transaction to simulate real-world messy data.
    """
    error_type = random.choice([
        "missing_vendor",
        "negative_amount",
        "duplicate_invoice",
        "bad_cost_center",
        "missing_invoice",
        "wrong_currency",
        "missing_approval",
    ])

    if error_type == "missing_vendor":
        tx["vendor_id"] = None

    elif error_type == "negative_amount":
        tx["amount"] = -abs(tx["amount"])

    elif error_type == "duplicate_invoice":
        # Make invoice_id look non-unique by reducing randomness
        tx["invoice_id"] = "INV-DUPLICATE"

    elif error_type == "bad_cost_center":
        tx["cost_center"] = "CC9999"

    elif error_type == "missing_invoice":
        tx["invoice_id"] = None

    elif error_type == "wrong_currency":
        tx["currency"] = random.choice(["USD", "EUR"])

    elif error_type == "missing_approval":
        tx["approval_status"] = "PENDING"

    return tx


def generate_transactions(run_date: str, n: int, error_rate: float, cfg: Dict[str, Any]) -> pd.DataFrame:
    vendors = make_vendor_master()
    cost_centers = make_cost_centers()
    gl_accounts = make_gl_accounts()

    rows = []
    for _ in range(n):
        amount = round(random.uniform(10, 15000), 2)

        tx = {
            "transaction_id": str(uuid.uuid4()),
            "posting_date": run_date,
            "vendor_id": random.choice(vendors),
            "amount": amount,
            "currency": cfg["settings"]["default_currency"],
            "cost_center": random.choice(cost_centers),
            "gl_account": random.choice(gl_accounts),
            "invoice_id": f"INV-{uuid.uuid4().hex[:10].upper()}",
            "approval_status": random.choice(["APPROVED", "PENDING"]),
            "source_system": random.choice(["SAP", "ARIBA", "MANUAL"]),
            "created_at": datetime.utcnow().isoformat(),
        }

        # If transaction is high value, approvals are more likely correct
        if tx["amount"] > cfg["settings"]["approval_threshold"]:
            tx["approval_status"] = random.choice(["APPROVED", "APPROVED", "APPROVED", "PENDING"])

        # Inject errors for testing
        if random.random() < error_rate:
            tx = inject_error(tx)

        rows.append(tx)

    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data CSV.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD")
    args = parser.parse_args()

    run_date = parse_date(args.date)
    cfg = load_config()

    out_dir = cfg["settings"]["output_dir"]
    ensure_dir(out_dir)

    n = int(cfg["settings"]["synthetic_rows_per_day"])
    error_rate = float(cfg["settings"]["synthetic_error_rate"])

    df = generate_transactions(run_date, n, error_rate, cfg)

    out_path = f"{out_dir}/transactions_{run_date}.csv"
    df.to_csv(out_path, index=False)

    print(f"[OK] Generated {len(df)} rows → {out_path}")


if __name__ == "__main__":
    main()
