from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


import src.gameone_scraper as gameone_scraper
import src.tipidpc_scraper as tipidpc_scraper
import src.gpuspecs_scraper as gpuspecs_scraper
import src.transform as transform


from datetime import date, datetime
import numpy as np
import os
from pathlib import Path
import pandas as pd


BASE_PATH = os.getcwd()
DATA_DIR = f"{BASE_PATH}data/processed"
RAW_DATA_DIR = f"{BASE_PATH}/data/unprocessed"
DATA_FILE = f"{DATA_DIR}/available_gpu.csv"
BQ_DATASET = "great_expectations_bigquery_example"
BQ_TABLE = "available-gpu"
GCP_BUCKET = "capstone-gpu-data"
GCP_DATA_DEST = "gpu_available_today/available_gpu.csv"
MY_CONN_ID = 'google-cloud-service-account'

with DAG(
    "gpu-pipeline",
    description="ETL dag for gpu data to bigquery",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    scrape_gpuspecs = PythonOperator(
        task_id = 'scrape_gpuspecs',
        python_callable = gpuspecs_scraper.main,
    )
    
    scrape_tipidpc = PythonOperator(
        task_id = 'scrape_tipidpc',
        python_callable = tipidpc_scraper.main,
    )

    scrape_gameone = PythonOperator(
        task_id = 'scrape_gameone',
        python_callable = gameone_scraper.main,
    )

    transform_data = PythonOperator(
        task_id = 'transform_step',
        python_callable = transform.main,
        trigger_rule = TriggerRule.ONE_SUCCESS
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_to_gcs',
        src=DATA_FILE,
        dst=GCP_DATA_DEST,
        bucket=GCP_BUCKET,
        gcp_conn_id=MY_CONN_ID,
    )

    transfer_to_BQ = GCSToBigQueryOperator(
        task_id="transfer_to_BQ",
        bucket=GCP_BUCKET,
        source_objects=[GCP_DATA_DEST],
        #bigquery_conn_id= MY_CONN_ID,
        #google_cloud_storage_conn_id= MY_CONN_ID,
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(BQ_DATASET, BQ_TABLE),
        schema_fields=[
            {"name": "item_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "item_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date_posted", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "ao_date", "type": "DATE", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        allow_jagged_rows=True,
    )

    def validate(ds=None, **kwargs):
        df1 = pd.read_csv(f'{RAW_DATA_DIR}/gameone-graphics-cards.csv')
        df2 = pd.read_csv(f'{RAW_DATA_DIR}/tipidpc-graphics-cards.csv')
        
        fail_flag = 0
        df1['item_price'] = df1.item_price.str.replace("₱","").str.replace(",","").astype(float)
        df2['item_price'] = df1.item_price.str.replace("P","").str.replace(",","").astype(float)

        if ((df1.item_price < 0).any() == True) or ((df1.item_price < 0).any()) == True:
            fail_flag = 1
            print('found negative price values')
        
        if (df1['item_name'].str.strip() == "") or (df1['item_name'] is np.nan):
            fail_flag = 1
            print('found empty item names')
        
        if (df2['item_name'].str.strip() == "") or (df2['item_name'] is np.nan):
            fail_flag = 1
            print('found empty item names')

        if fail_flag == 0:
            return validation_success
        else: return validation_fail

    def clean_data(ds=None, **kwargs):
        df1 = pd.read_csv(f'{RAW_DATA_DIR}/gameone-graphics-cards.csv')
        df2 = pd.read_csv(f'{RAW_DATA_DIR}/tipidpc-graphics-cards.csv')
        df1['item_price'] = df1.item_price.str.replace("₱","").str.replace(",","").astype(float)
        df2['item_price'] = df1.item_price.str.replace("P","").str.replace(",","").astype(float)

        df1 = df1.replace('', np.nan)
        df1.dropna(inplace=True)        
        
        df2 = df2.replace('', np.nan)

        df1 = df1[df1['item_price']<0]
        df2 = df2[df2['item_price']<0]

        df1 = pd.to_csv(f'{RAW_DATA_DIR}/gameone-graphics-cards.csv')
        df2 = pd.to_csv(f'{RAW_DATA_DIR}/tipidpc-graphics-cards.csv')

    def delete_files(ds=None, **kwargs):
        for f in os.listdir(DATA_DIR):
            os.remove(os.path.join(DATA_DIR,f))
        
        for f in os.listdir(RAW_DATA_DIR):
            os.remove(os.path.join(DATA_DIR,f))

        print(f"files in {DATA_DIR} and {RAW_DATA_DIR} deleted")

    validation_step = BranchPythonOperator(
        task_id = "validation_step",
        python_callable = validate,
    )

    clean_data = PythonOperator(
        task_id = 'clean',
        python_callable=clean_data,
    )

    delete_local_data = PythonOperator(
        task_id = 'delete_local_data',
        python_callable= delete_files,
    )

    start = DummyOperator(task_id = 'start_pipeline')
    end = DummyOperator(task_id = 'end')
    validation_fail = DummyOperator(task_id = 'validation_fail')
    validation_success = DummyOperator(task_id = 'validation_success')
    
    start >> [scrape_tipidpc, scrape_gameone] >> validation_step >> [validation_success, validation_fail]
    validation_success >> transform_data
    validation_fail >> clean_data >> transform_data
    transform_data >> upload_to_gcs >> transfer_to_BQ >> delete_local_data >> end