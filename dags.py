# todo: add dags task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import src.gameone_scraper as gameone_scraper
import src.tipidpc_scraper as tipidpc_scraper
import src.gpuspecs_scraper as gpuspecs_scraper


with DAG(
    "gpu-pipeline",
    description="ETL dag for gpu data to bigquery",
    schedule_interval='@once', #check run
    start_date=datetime(2022, 6, 24),
    catchup=False,
) as dag:

    scrape_gpuspecs = PythonOperator(
        task_id = 'scrape_gpuspecs',
        python_callable = gpuspecs_scraper.main(),
    )
    
    scrape_tipidpc = PythonOperator(
        task_id = 'scrape_tipidpc',
        python_callable = tipidpc_scraper.main(),
    )

    scrape_gameone = PythonOperator(
        task_id = 'scrape_gameone',
        python_callable = gameone_scraper.main()
    )


    start = DummyOperator(
        task_id = 'start_pipeline',
    )

    end = DummyOperator(
        task_id = 'end',
    )

start >> [scrape_gpuspecs, scrape_tipidpc, scrape_gameone] >> end

