from airflow.decorators import task
import pandas as pd
import logging
import os

logger = logging.getLogger("airflow.task")


@task()
def csv_to_pandas_df_task(path : str, file_name : str) -> pd.DataFrame:
    full_path = os.path.join(path, file_name)
    logger.info(f"Reading CSV file to pandas dataframe from path: {full_path}")
    df = pd.read_csv(full_path)
    logger.info(f"Done. Reading CSV file to pandas dataframe")
    return df
