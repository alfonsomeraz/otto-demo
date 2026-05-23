import os
import pytest
import tempfile
from pathlib import Path

# Fix the xcom_backend before importing Airflow
os.environ["AIRFLOW__CORE__XCOM_BACKEND"] = "airflow.models.xcom.BaseXCom"

# Set AIRFLOW_HOME to a temporary directory for testing
airflow_home = tempfile.mkdtemp(prefix="airflow_test_")
os.environ["AIRFLOW_HOME"] = airflow_home

# Initialize the database after setting AIRFLOW_HOME
from airflow.utils.db import initdb
initdb()
