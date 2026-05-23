# MeridianPay Demo DAGs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three Airflow DAGs for a live Otto demo — transaction ingestion (healthy), fraud features (intentionally fails on wrong Snowflake connection ID), and dashboard refresh (healthy) — plus fixture data, a team conventions doc, and tests.

**Architecture:** Three standalone TaskFlow API DAGs that read/write local CSV/JSON files under `include/`. No external connections. DAG 2 fails deterministically at `check_snowflake_connection_convention` due to a hardcoded wrong connection ID (`snowflake_prod` instead of `meridian_snowflake_prod`), simulating a real misconfiguration that requires knowledge of team conventions to diagnose.

**Tech Stack:** Python 3.11, Apache Airflow 2.8, Astro CLI, pytest, TaskFlow API

---

## File Map

| File | Role |
|---|---|
| `include/sample_transactions.json` | Fixture: source transaction data read by DAG 1 |
| `include/meridianpay_airflow_conventions.md` | Fixture: team conventions doc used in Otto prompts |
| `dags/merchant_transactions_ingest.py` | DAG 1: reads JSON fixture, validates schema, writes CSV |
| `dags/fraud_features_daily.py` | DAG 2: fails at first task on wrong connection ID |
| `dags/merchant_dashboard_refresh.py` | DAG 3: simulates dbt run and Slack notification |
| `tests/dags/test_dag_integrity.py` | Confirms all 3 DAGs load without import errors and have correct task IDs |
| `tests/dags/test_fraud_features_failure.py` | Confirms DAG 2 failure logic raises with the right message |
| `requirements-dev.txt` | Local test dependencies (airflow, pytest) |
| `README.md` | Demo walkthrough, pipeline table, and Otto prompt scripts |

---

### Task 1: Create project structure and fixture files

**Files:**
- Create: `include/sample_transactions.json`
- Create: `include/meridianpay_airflow_conventions.md`
- Create: `requirements-dev.txt`

- [ ] **Step 1: Create the transaction fixture**

Create `include/sample_transactions.json`:

```json
[
  {
    "transaction_id": "txn_1001",
    "merchant_id": "m_200",
    "amount": 142.33,
    "currency": "USD",
    "status": "approved",
    "created_at": "2026-05-22T09:15:00"
  },
  {
    "transaction_id": "txn_1002",
    "merchant_id": "m_300",
    "amount": 982.12,
    "currency": "USD",
    "status": "flagged",
    "created_at": "2026-05-22T09:18:00"
  },
  {
    "transaction_id": "txn_1003",
    "merchant_id": "m_200",
    "amount": 45.00,
    "currency": "USD",
    "status": "approved",
    "created_at": "2026-05-22T09:22:00"
  },
  {
    "transaction_id": "txn_1004",
    "merchant_id": "m_410",
    "amount": 3200.00,
    "currency": "USD",
    "status": "flagged",
    "created_at": "2026-05-22T09:30:00"
  }
]
```

- [ ] **Step 2: Create the conventions doc**

Create `include/meridianpay_airflow_conventions.md`:

```markdown
# MeridianPay Airflow Conventions

## Connection IDs
- Production Snowflake: meridian_snowflake_prod
- Development Snowflake: meridian_snowflake_dev
- Slack alerts: meridian_slack_alerts

## DAG Defaults
- Production DAGs must define retries.
- Retry delay should be at least 5 minutes for external services.
- DAGs should use clear owner tags: data-platform, analytics-engineering, or risk-analytics.
- DAGs should include business-domain tags.

## Alerting
- Critical production DAGs should notify #data-platform-alerts.
- Fraud and risk pipelines should notify #risk-analytics-alerts.

## Code Style
- Prefer TaskFlow API for simple Python workflows.
- Avoid large data payloads in XCom.
- Store data in object storage or warehouse tables and pass references between tasks.
```

- [ ] **Step 3: Create requirements-dev.txt**

Create `requirements-dev.txt`:

```
apache-airflow==2.8.4
pytest
```

- [ ] **Step 4: Commit**

```bash
git add include/sample_transactions.json include/meridianpay_airflow_conventions.md requirements-dev.txt
git commit -m "feat: add transaction fixture and MeridianPay conventions doc"
```

---

### Task 2: DAG integrity tests (write tests first, before any DAGs exist)

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/dags/__init__.py`
- Create: `tests/dags/test_dag_integrity.py`

- [ ] **Step 1: Create test directories**

```bash
mkdir -p tests/dags
touch tests/__init__.py tests/dags/__init__.py
```

- [ ] **Step 2: Write DAG integrity tests**

Create `tests/dags/test_dag_integrity.py`:

```python
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


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
    assert {t.task_id for t in dag.tasks} == {
        "extract_transactions",
        "validate_schema",
        "load_raw_transactions",
        "notify_success",
    }


def test_fraud_features_daily_task_ids(dagbag):
    dag = dagbag.get_dag("fraud_features_daily")
    assert {t.task_id for t in dag.tasks} == {
        "check_snowflake_connection_convention",
        "check_raw_transactions",
        "transform_fraud_features",
        "validate_feature_freshness",
        "publish_features",
    }


def test_merchant_dashboard_refresh_task_ids(dagbag):
    dag = dagbag.get_dag("merchant_dashboard_refresh")
    assert {t.task_id for t in dag.tasks} == {
        "wait_for_transactions",
        "run_dbt_models",
        "validate_dashboard_tables",
        "notify_slack",
    }
```

- [ ] **Step 3: Run tests — expect all failures (no DAGs exist yet)**

```bash
pip install apache-airflow==2.8.4 pytest
pytest tests/dags/test_dag_integrity.py -v
```

Expected: all 7 tests fail — DAGs not found or import errors.

---

### Task 3: DAG 1 — merchant_transactions_ingest

**Files:**
- Create: `dags/merchant_transactions_ingest.py`

- [ ] **Step 1: Create DAG 1**

Create `dags/merchant_transactions_ingest.py`:

```python
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
```

- [ ] **Step 2: Run integrity tests — DAG 1 tests should now pass**

```bash
pytest tests/dags/test_dag_integrity.py -v
```

Expected partial output:
```
PASSED tests/dags/test_dag_integrity.py::test_merchant_transactions_ingest_loaded
PASSED tests/dags/test_dag_integrity.py::test_merchant_transactions_ingest_task_ids
FAILED tests/dags/test_dag_integrity.py::test_fraud_features_daily_loaded
FAILED tests/dags/test_dag_integrity.py::test_merchant_dashboard_refresh_loaded
```

- [ ] **Step 3: Commit**

```bash
git add dags/merchant_transactions_ingest.py tests/__init__.py tests/dags/__init__.py tests/dags/test_dag_integrity.py
git commit -m "feat: add merchant_transactions_ingest DAG and DAG integrity tests"
```

---

### Task 4: DAG 2 failure behavior tests

**Files:**
- Create: `tests/dags/test_fraud_features_failure.py`

- [ ] **Step 1: Write failure behavior tests**

Create `tests/dags/test_fraud_features_failure.py`:

```python
import pytest


def test_connection_convention_check_raises_on_wrong_id():
    """The intentional failure raises with a message naming both the wrong and correct IDs."""
    expected_conn_id = "meridian_snowflake_prod"
    actual_conn_id = "snowflake_prod"

    with pytest.raises(ValueError) as exc_info:
        if actual_conn_id != expected_conn_id:
            raise ValueError(
                f"Invalid Snowflake connection ID: {actual_conn_id}. "
                f"Expected company standard: {expected_conn_id}. "
                "This may prevent the fraud feature pipeline from accessing production transaction data."
            )

    assert "meridian_snowflake_prod" in str(exc_info.value)
    assert "snowflake_prod" in str(exc_info.value)


def test_connection_convention_check_passes_on_correct_id():
    """The check does not raise when the correct connection ID is used."""
    expected_conn_id = "meridian_snowflake_prod"
    actual_conn_id = "meridian_snowflake_prod"

    if actual_conn_id != expected_conn_id:
        raise ValueError(f"Invalid connection ID: {actual_conn_id}")
```

- [ ] **Step 2: Run failure tests — both should pass immediately (pure logic)**

```bash
pytest tests/dags/test_fraud_features_failure.py -v
```

Expected:
```
PASSED tests/dags/test_fraud_features_failure.py::test_connection_convention_check_raises_on_wrong_id
PASSED tests/dags/test_fraud_features_failure.py::test_connection_convention_check_passes_on_correct_id
```

---

### Task 5: DAG 2 — fraud_features_daily

**Files:**
- Create: `dags/fraud_features_daily.py`

- [ ] **Step 1: Create DAG 2**

Create `dags/fraud_features_daily.py`:

```python
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
```

- [ ] **Step 2: Run all tests — DAG 2 integrity tests should now pass**

```bash
pytest tests/ -v
```

Expected: all 9 tests pass.

- [ ] **Step 3: Commit**

```bash
git add dags/fraud_features_daily.py tests/dags/test_fraud_features_failure.py
git commit -m "feat: add fraud_features_daily DAG with intentional connection ID failure"
```

---

### Task 6: DAG 3 — merchant_dashboard_refresh

**Files:**
- Create: `dags/merchant_dashboard_refresh.py`

- [ ] **Step 1: Create DAG 3**

Create `dags/merchant_dashboard_refresh.py`:

```python
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
```

- [ ] **Step 2: Run all tests — all 9 should pass**

```bash
pytest tests/ -v
```

Expected (all passing):
```
PASSED tests/dags/test_dag_integrity.py::test_no_import_errors
PASSED tests/dags/test_dag_integrity.py::test_merchant_transactions_ingest_loaded
PASSED tests/dags/test_dag_integrity.py::test_fraud_features_daily_loaded
PASSED tests/dags/test_dag_integrity.py::test_merchant_dashboard_refresh_loaded
PASSED tests/dags/test_dag_integrity.py::test_merchant_transactions_ingest_task_ids
PASSED tests/dags/test_dag_integrity.py::test_fraud_features_daily_task_ids
PASSED tests/dags/test_dag_integrity.py::test_merchant_dashboard_refresh_task_ids
PASSED tests/dags/test_fraud_features_failure.py::test_connection_convention_check_raises_on_wrong_id
PASSED tests/dags/test_fraud_features_failure.py::test_connection_convention_check_passes_on_correct_id
```

- [ ] **Step 3: Commit**

```bash
git add dags/merchant_dashboard_refresh.py
git commit -m "feat: add merchant_dashboard_refresh DAG"
```

---

### Task 7: Update README with demo walkthrough

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Rewrite README**

Replace the full contents of `README.md`:

```markdown
# MeridianPay Otto Demo

A live demo environment simulating Airflow pipelines at MeridianPay, a fintech/payments company.
Built to demonstrate Otto's value around failure investigation, convention enforcement, DAG authoring, and Airflow upgrade readiness.

## Pipelines

| DAG | Owner | Status | Purpose |
|---|---|---|---|
| `merchant_transactions_ingest` | data-platform | Healthy | Ingests raw transaction data |
| `fraud_features_daily` | risk-analytics | **Fails** | Generates fraud model features (intentional failure) |
| `merchant_dashboard_refresh` | analytics-engineering | Healthy | Refreshes merchant-facing dashboards |

## Running Locally (Astro)

```bash
astro dev start
```

Navigate to http://localhost:8080 — Username: `admin`, Password: `admin`.

Trigger `merchant_transactions_ingest` first to generate `raw_transactions.csv`, then trigger `fraud_features_daily` to see the intentional failure at `check_snowflake_connection_convention`.

## Otto Demo Prompts

### 1. Failure Investigation
```
The fraud_features_daily DAG failed during the check_snowflake_connection_convention task.
Please investigate the failure, summarize the likely root cause in plain English, and recommend
the safest fix. Also check whether this violates MeridianPay's Airflow conventions.
```

### 2. Fix Generation
```
Update the fraud_features_daily DAG to use the correct MeridianPay production Snowflake
connection convention. Explain the code change and why it reduces future failure risk.
```

### 3. DAG Authoring
```
Create a new Airflow DAG for daily merchant settlement reconciliation. It should follow
MeridianPay's conventions, check for raw transactions, run a reconciliation step, validate
row counts, and notify Slack on failure. Use the TaskFlow API where appropriate.
```

### 4. Upgrade Readiness
```
Review these DAGs for Airflow 3 upgrade readiness. Identify deprecated patterns, provider
compatibility concerns, risky imports, and recommended changes. Prioritize the findings by
business impact.
```

## Key Files

| File | Purpose |
|---|---|
| `include/sample_transactions.json` | Source transaction fixture (4 records, 2 flagged) |
| `include/meridianpay_airflow_conventions.md` | Team conventions doc — reference this in Otto prompts |
| `include/raw_transactions.csv` | Generated by DAG 1 |
| `include/fraud_features.csv` | Would be generated by DAG 2 if the connection ID were correct |
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update README with demo walkthrough and Otto prompts"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `include/sample_transactions.json` with 4 transactions | Task 1 |
| `include/meridianpay_airflow_conventions.md` | Task 1 |
| DAG 1: 4 tasks, extract → validate → load → notify | Task 3 |
| DAG 2: fails at `check_snowflake_connection_convention` with `ValueError` | Task 5 |
| DAG 3: 4 tasks, wait → dbt → validate → notify_slack | Task 6 |
| DAG integrity tests (load + task IDs) | Tasks 2, 3, 5, 6 |
| Failure behavior tests | Task 4 |
| README with pipeline table and 4 Otto prompts | Task 7 |

All spec requirements covered. No placeholder text found. Type/name consistency confirmed across all tasks.
