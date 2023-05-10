from airflow.decorators import task
import pandas as pd
import logging
from src.transformers.invoice_transformer import InvoiceTransformer

logger = logging.getLogger("airflow.task")

@task()
def transform_invoice_data_task(df : pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Transforming invoice data.")
    invoice_transformer = InvoiceTransformer()
    transformed_df = invoice_transformer.execute(df)
    logger.info(f"Done. Transforming invoice data")
    return transformed_df
