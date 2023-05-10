from airflow.decorators import task
import pandas as pd
import logging
import os
import uuid
import shutil

logger = logging.getLogger("airflow.task")


@task()
def copy_and_rename_csv_to_processed_folder_task(path : str, file_name : str) -> None:
    full_path = os.path.join(path, file_name)
    new_path = path.replace('/raw', '/processed')
    new_file_name = file_name.replace('.csv', str(uuid.uuid4()) + ".csv")
    new_full_path = os.path.join(new_path, new_file_name)
    logger.info(f"Starting to move file raw data to processed data")
    shutil.copy(full_path, new_full_path)
    logger.info(f"Done. Moved from {full_path} to {new_full_path}")
