from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import csv

DEFAULT_ARGS = {
    "owner": "risk-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="fraud_features_daily",
    description="Generate daily fraud model features from merchant transactions.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meridianpay", "fraud", "ml-features"],
)
def fraud_features_daily():

    @task
    def check_snowflake_connection_convention():
        """
        Intentional demo failure: MeridianPay standardizes production Snowflake access
        using conn_id='meridian_snowflake_prod', but this DAG uses the wrong ID.
        """
        expected_conn_id = "meridian_snowflake_prod"
        actual_conn_id = "snowflake_prod"

        if actual_conn_id != expected_conn_id:
            raise ValueError(
                f"Invalid Snowflake connection ID: {actual_conn_id}. "
                f"Expected company standard: {expected_conn_id}. "
                "This may prevent the fraud feature pipeline from accessing production transaction data."
            )

        print("Snowflake connection convention validated")

    @task
    def check_raw_transactions():
        input_path = Path("/usr/local/airflow/include/raw_transactions.csv")
        if not input_path.exists():
            raise FileNotFoundError(
                "raw_transactions.csv not found. "
                "Upstream merchant_transactions_ingest may not have completed."
            )
        print("Raw transactions file found")

    @task
    def transform_fraud_features():
        input_path = Path("/usr/local/airflow/include/raw_transactions.csv")
        output_path = Path("/usr/local/airflow/include/fraud_features.csv")

        with open(input_path, "r") as f:
            rows = list(csv.DictReader(f))

        features = [
            {
                "transaction_id": row["transaction_id"],
                "merchant_id": row["merchant_id"],
                "amount": float(row["amount"]),
                "is_high_value": int(float(row["amount"]) > 500),
                "is_flagged": int(row["status"] == "flagged"),
            }
            for row in rows
        ]

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["transaction_id", "merchant_id", "amount", "is_high_value", "is_flagged"],
            )
            writer.writeheader()
            writer.writerows(features)

        print(f"Wrote {len(features)} fraud features to {output_path}")

    @task
    def validate_feature_freshness():
        output_path = Path("/usr/local/airflow/include/fraud_features.csv")
        if not output_path.exists():
            raise FileNotFoundError("Fraud feature output was not created")
        print("Fraud feature freshness validation passed")

    @task
    def publish_features():
        print("Published fraud features to downstream risk scoring workflow")

    (
        check_snowflake_connection_convention()
        >> check_raw_transactions()
        >> transform_fraud_features()
        >> validate_feature_freshness()
        >> publish_features()
    )


fraud_features_daily()
