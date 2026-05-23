import os
import tempfile
import pytest


@pytest.fixture(scope="session", autouse=True)
def airflow_test_env():
    with tempfile.TemporaryDirectory(prefix="airflow_test_") as tmp_dir:
        os.environ["AIRFLOW__CORE__XCOM_BACKEND"] = "airflow.models.xcom.BaseXCom"
        os.environ["AIRFLOW_HOME"] = tmp_dir
        from airflow.utils.db import initdb
        initdb()
        yield
