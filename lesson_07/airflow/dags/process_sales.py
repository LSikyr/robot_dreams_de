import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models.param import Param

BASE_DIR = os.environ.get("BASE_DIR")


def extract_data(**kwargs):
    hook = HttpHook(
        method='POST',
        http_conn_id='extract-job'
    )
    ds = kwargs.get("ds")
    base_dir = kwargs.get("params")["base_dir"]
    response = hook.run('/', json=
    {
        "date": ds,
        "raw_dir": f"{base_dir}/raw/{ds}/"
    })
    assert response.status_code == 201, response.text


def convert_to_avro(**kwargs):
    hook = HttpHook(
        method='POST',
        http_conn_id='convert-job'
    )
    ds = kwargs.get("ds")
    base_dir = kwargs.get("params")["base_dir"]
    response = hook.run('/', json={
        "raw_dir": f"{base_dir}/raw/{ds}",
        "stg_dir": f"{base_dir}/stg/{ds}"
    })
    assert response.status_code == 201, response.text


with DAG(
        dag_id="process_sales",
        schedule="0 1 * * *",
        params={"base_dir": Param(default=BASE_DIR)},
        start_date=datetime.strptime("2022-08-09", "%Y-%m-%d"),
        end_date=datetime.strptime("2022-08-12", "%Y-%m-%d"),
        catchup=True,
        max_active_runs=1
) as dag:
    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=extract_data,
    )

    convert_to_avro = PythonOperator(
        task_id="convert_to_avro",
        python_callable=convert_to_avro,
    )

    extract_data_from_api >> convert_to_avro