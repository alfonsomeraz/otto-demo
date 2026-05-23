from airflow.decorators import dag, task
from datetime import datetime, timedelta
import json
import csv
from pathlib import Path

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="merchant_transactions_ingest",
    description="Ingest merchant transaction data for downstream fraud and reporting workflows.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meridianpay", "transactions", "ingest"],
)
def merchant_transactions_ingest():

    @task
    def extract_transactions():
        path = Path("/usr/local/airflow/include/sample_transactions.json")
        with open(path, "r") as f:
            transactions = json.load(f)
        print(f"Extracted {len(transactions)} transactions")
        return transactions

    @task
    def validate_schema(transactions):
        required_fields = {
            "transaction_id",
            "merchant_id",
            "amount",
            "currency",
            "status",
            "created_at",
        }
        for txn in transactions:
            missing = required_fields - txn.keys()
            if missing:
                raise ValueError(f"Transaction missing required fields: {missing}")
        print("Schema validation passed")
        return transactions

    @task
    def load_raw_transactions(transactions):
        output_path = Path("/usr/local/airflow/include/raw_transactions.csv")
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["transaction_id", "merchant_id", "amount", "currency", "status", "created_at"],
            )
            writer.writeheader()
            writer.writerows(transactions)
        print(f"Loaded {len(transactions)} raw transactions to {output_path}")

    @task
    def notify_success():
        print("Merchant transaction ingest completed successfully")

    transactions = extract_transactions()
    validated = validate_schema(transactions)
    load_raw_transactions(validated) >> notify_success()


merchant_transactions_ingest()
