# MeridianPay Demo DAGs — Design Spec

**Date:** 2026-05-23
**Goal:** Build a live Otto demo environment for a fintech customer story (MeridianPay). Three Airflow DAGs — one of which intentionally fails — demonstrating Otto's value around failure investigation, convention enforcement, DAG authoring, and upgrade readiness.

---

## Demo Theme

**Customer:** MeridianPay — a fintech/payments company.

**Simulated stack:** Airflow 2.x, Snowflake, dbt, Databricks, Slack alerts, GitHub, Kubernetes.

**Pain points being demonstrated:**
- Failed DAGs and slow debugging
- Onboarding friction around team conventions
- Airflow upgrade risk

---

## Project Structure

```
otto-demo/
  dags/
    merchant_transactions_ingest.py
    fraud_features_daily.py
    merchant_dashboard_refresh.py
  include/
    sample_transactions.json          # source data (read-only fixture)
    raw_transactions.csv              # written by DAG 1, read by DAGs 2 & 3
    fraud_features.csv                # written by DAG 2
    merchant_dashboard.csv            # placeholder for DAG 3 output
    meridianpay_airflow_conventions.md  # team conventions doc (used in Otto prompts)
  docs/
    superpowers/specs/
      2026-05-23-meridianpay-demo-dags-design.md
  README.md
```

---

## DAG 1: `merchant_transactions_ingest`

**Owner:** data-platform  
**Schedule:** @daily  
**Tags:** meridianpay, transactions, ingest

**Purpose:** Simulate ingesting merchant transaction data into a raw table. This is the upstream source of truth for all downstream pipelines.

**Task flow:**
```
extract_transactions
  → validate_schema
  → load_raw_transactions
  → notify_success
```

**Behavior:**
- Reads `include/sample_transactions.json` (fixture, not generated)
- Validates required fields: `transaction_id`, `merchant_id`, `amount`, `currency`, `status`, `created_at`
- Writes `include/raw_transactions.csv`
- Prints success notification (simulates Slack/email alert)

**No external connections required.** Fully local and self-contained.

**Demo talking point:** "Transaction ingestion is the source of truth for downstream fraud features and merchant reporting. If this breaks, everything downstream becomes stale."

---

## DAG 2: `fraud_features_daily` ← INTENTIONAL FAILURE DAG

**Owner:** risk-analytics  
**Schedule:** @daily  
**Tags:** meridianpay, fraud, ml-features

**Purpose:** Generate daily fraud model features from transaction data. This DAG intentionally fails to demonstrate Otto's investigation and debugging capability.

**Task flow:**
```
check_snowflake_connection_convention  ← FAILS HERE
  → check_raw_transactions
  → transform_fraud_features
  → validate_feature_freshness
  → publish_features
```

**Intentional failure mechanism:**
The first task (`check_snowflake_connection_convention`) raises a `ValueError` because the DAG uses `snowflake_prod` instead of the MeridianPay standard `meridian_snowflake_prod`. This simulates a real-world misconfiguration that requires knowledge of team conventions to diagnose.

```python
# Wrong:
actual_conn_id = "snowflake_prod"
# Expected by MeridianPay convention:
expected_conn_id = "meridian_snowflake_prod"
```

**Downstream tasks (would run if connection check passed):**
- Reads `include/raw_transactions.csv`
- Computes `is_high_value` (amount > 500) and `is_flagged` (status == "flagged") per transaction
- Writes `include/fraud_features.csv`
- Validates output file exists
- Prints "published to downstream risk scoring workflow"

**Demo talking point:** "This is the exact kind of issue that becomes expensive at scale. The technical fix is small, but finding it requires knowing logs, DAG code, and internal conventions. That's where Otto becomes valuable."

---

## DAG 3: `merchant_dashboard_refresh`

**Owner:** analytics-engineering  
**Schedule:** @daily  
**Tags:** meridianpay, dashboards, dbt

**Purpose:** Simulate refreshing merchant-facing analytics dashboards after transaction data lands.

**Task flow:**
```
wait_for_transactions
  → run_dbt_models
  → validate_dashboard_tables
  → notify_slack
```

**Behavior:**
- Checks `include/raw_transactions.csv` exists before proceeding
- Simulates `dbt run --select merchant_daily_revenue merchant_risk_summary` via a print statement
- Simulates dashboard validation (row count / freshness check)
- Simulates Slack notification to `#data-platform-alerts`

**Demo talking point:** "If fraud features and dashboards depend on the same transaction data, orchestration failures are not isolated technical events. They impact risk, reporting, and customer trust."

---

## Include Files

### `include/sample_transactions.json` (fixture, checked in)
```json
[
  {"transaction_id": "txn_1001", "merchant_id": "m_200", "amount": 142.33, "currency": "USD", "status": "approved", "created_at": "2026-05-22T09:15:00"},
  {"transaction_id": "txn_1002", "merchant_id": "m_300", "amount": 982.12, "currency": "USD", "status": "flagged", "created_at": "2026-05-22T09:18:00"},
  {"transaction_id": "txn_1003", "merchant_id": "m_200", "amount": 45.00, "currency": "USD", "status": "approved", "created_at": "2026-05-22T09:22:00"},
  {"transaction_id": "txn_1004", "merchant_id": "m_410", "amount": 3200.00, "currency": "USD", "status": "flagged", "created_at": "2026-05-22T09:30:00"}
]
```

### `include/meridianpay_airflow_conventions.md`
Documents MeridianPay's internal standards:
- Connection IDs (e.g., `meridian_snowflake_prod`)
- DAG default requirements (retries, retry_delay, owner tags, domain tags)
- Alerting channels
- Code style rules (TaskFlow API, XCom payload size, object storage refs)

This file is used in Otto prompts to demonstrate the "customer context" capability.

---

## Otto Demo Prompts (prepared scripts)

### Prompt 1 — Failure investigation
```
The fraud_features_daily DAG failed during the check_snowflake_connection_convention task.
Please investigate the failure, summarize the likely root cause in plain English, and recommend
the safest fix. Also check whether this violates MeridianPay's Airflow conventions.
```

### Prompt 2 — Fix generation
```
Update the fraud_features_daily DAG to use the correct MeridianPay production Snowflake
connection convention. Explain the code change and why it reduces future failure risk.
```

### Prompt 3 — DAG authoring
```
Create a new Airflow DAG for daily merchant settlement reconciliation. It should follow
MeridianPay's conventions, check for raw transactions, run a reconciliation step, validate
row counts, and notify Slack on failure. Use the TaskFlow API where appropriate.
```

### Prompt 4 — Upgrade readiness
```
Review these DAGs for Airflow 3 upgrade readiness. Identify deprecated patterns, provider
compatibility concerns, risky imports, and recommended changes. Prioritize the findings by
business impact.
```

---

## What This Demo Proves

| Claim | Demo vehicle |
|---|---|
| Otto investigates production failures faster | Failed `fraud_features_daily` |
| Otto helps new engineers follow team conventions | DAG authoring with conventions doc |
| Otto reduces upgrade and migration risk | Airflow 3 readiness prompt |

---

## Technical Constraints

- **No external connections.** Everything runs locally with file I/O only.
- **No real dbt, Snowflake, Databricks, or Kafka.** All simulated via Python print statements.
- **Failure is deterministic.** The wrong connection ID always causes a `ValueError` — easy to trigger, easy to explain.
- **All paths use `/usr/local/airflow/include/`** — the standard Astro project container path.
