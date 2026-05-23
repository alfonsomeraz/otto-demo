import os
import tempfile
import pytest


@pytest.fixture(scope="session", autouse=True)
def airflow_test_env():
    with tempfile.TemporaryDirectory(prefix="airflow_test_") as tmp_dir:
        os.environ["AIRFLOW__CORE__XCOM_BACKEND"] = "airflow.models.xcom.BaseXCom"
        os.environ["AIRFLOW_HOME"] = tmp_dir
        os.environ["AIRFLOW__DATABASE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
        try:
            from airflow.utils.db import resetdb, initdb
            resetdb()
            initdb()
        except Exception as e:
            print(f"Warning: Database initialization failed: {e}")
        yield
