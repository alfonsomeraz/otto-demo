from pathlib import Path

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    dag_folder = str(Path(__file__).parent.parent.parent / "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_merchant_transactions_ingest_loaded(dagbag):
    assert dagbag.get_dag("merchant_transactions_ingest") is not None


def test_fraud_features_daily_loaded(dagbag):
    assert dagbag.get_dag("fraud_features_daily") is not None


def test_merchant_dashboard_refresh_loaded(dagbag):
    assert dagbag.get_dag("merchant_dashboard_refresh") is not None


def test_merchant_transactions_ingest_task_ids(dagbag):
    dag = dagbag.get_dag("merchant_transactions_ingest")
    assert dag is not None, "DAG 'merchant_transactions_ingest' not found in DagBag"
    assert {t.task_id for t in dag.tasks} == {
        "extract_transactions",
        "validate_schema",
        "load_raw_transactions",
        "notify_success",
    }


def test_fraud_features_daily_task_ids(dagbag):
    dag = dagbag.get_dag("fraud_features_daily")
    assert dag is not None, "DAG 'fraud_features_daily' not found in DagBag"
    assert {t.task_id for t in dag.tasks} == {
        "check_snowflake_connection_convention",
        "check_raw_transactions",
        "transform_fraud_features",
        "validate_feature_freshness",
        "publish_features",
    }


def test_merchant_dashboard_refresh_task_ids(dagbag):
    dag = dagbag.get_dag("merchant_dashboard_refresh")
    assert dag is not None, "DAG 'merchant_dashboard_refresh' not found in DagBag"
    assert {t.task_id for t in dag.tasks} == {
        "wait_for_transactions",
        "run_dbt_models",
        "validate_dashboard_tables",
        "notify_slack",
    }
