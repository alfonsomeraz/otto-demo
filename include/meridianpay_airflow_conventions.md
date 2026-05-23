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
