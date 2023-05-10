from airflow.decorators import task
import pandas as pd
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger("airflow.task")

@task()
def load_transformed_invoices_to_database_task(df : pd.DataFrame) -> None:
    postgres_hook = PostgresHook("db_postgres")
    psql_engine = postgres_hook.get_sqlalchemy_engine()

    logger.info("Started writing to PostgreSQL database")
    df.to_sql('invoices', con=psql_engine, index=False, if_exists='replace')
    logger.info("Done. Wrote to PostgreSQL database")


