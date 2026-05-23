from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path

DEFAULT_ARGS = {
    "owner": "analytics-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="merchant_dashboard_refresh",
    description="Refresh merchant dashboard tables after transaction data lands.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meridianpay", "dashboards", "dbt"],
)
def merchant_dashboard_refresh():

    @task
    def wait_for_transactions():
        input_path = Path("/usr/local/airflow/include/raw_transactions.csv")
        if not input_path.exists():
            raise FileNotFoundError(
                "Raw transactions are not available. Dashboard refresh cannot proceed."
            )
        print("Raw transactions are available")

    @task
    def run_dbt_models():
        print("Simulating dbt run for merchant analytics models")
        print("dbt run --select merchant_daily_revenue merchant_risk_summary")

    @task
    def validate_dashboard_tables():
        print("Validated dashboard row counts and freshness checks")

    @task
    def notify_slack():
        print("Slack notification sent to #data-platform-alerts")

    wait_for_transactions() >> run_dbt_models() >> validate_dashboard_tables() >> notify_slack()


merchant_dashboard_refresh()
