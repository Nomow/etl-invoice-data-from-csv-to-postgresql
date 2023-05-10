from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
from src.tasks.clean_task import copy_and_rename_csv_to_processed_folder_task
from src.tasks.load_task import load_transformed_invoices_to_database_task
from src.tasks.transform_task import transform_invoice_data_task
from src.tasks.extract_task import csv_to_pandas_df_task
import os



with DAG(
    'invoice_etl_dag',
    description='etl pipeline for extracting data from csv file, cleaning the data and loading it in postgresql database table',
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=timedelta(minutes=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl'],
    params={"csv_path" : os.getenv("AIRFLOW_HOME") + "/data"},
) as dag:

    start_task = DummyOperator(task_id= "start_task")
    sensor_csv_task = FileSensor( task_id="check_if_csv_exists_task", poke_interval=5, filepath="{{params.csv_path}}/raw/invoices.csv")
    csv_to_dataframe_task = csv_to_pandas_df_task("{{params.csv_path}}/raw/", "invoices.csv")
    invoice_data_transform_task = transform_invoice_data_task(csv_to_dataframe_task)
    load_data_task = load_transformed_invoices_to_database_task(invoice_data_transform_task)
    copy_and_rename_data_task = copy_and_rename_csv_to_processed_folder_task("{{params.csv_path}}/raw/", "invoices.csv")
    stop_task = DummyOperator(task_id="stop_task")

    chain(
        start_task,
        sensor_csv_task,
        csv_to_dataframe_task,
        invoice_data_transform_task,
        load_data_task,
        copy_and_rename_data_task,
        stop_task,
    )


