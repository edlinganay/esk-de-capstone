from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
import src.gameone_scraper as gameone_scraper
import src.tipidpc_scraper as tipidpc_scraper
import src.gpuspecs_scraper as gpuspecs_scraper
from datetime import date, datetime

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
    
    get_html_text = BashOperator(
        task_id = 'get_gpu_specs_page',
        bash_command = 'curl "https://techpowerup.com/gpu-specs/" > /data/response.txt; ls /data/'
    )
    scrape_tipidpc = PythonOperator(
        task_id = 'scrape_tipidpc',
        python_callable = tipidpc_scraper.main,
    )

    scrape_gameone = PythonOperator(
        task_id = 'scrape_gameone',
        python_callable = gameone_scraper.main,
    )

    start = DummyOperator(task_id = 'start_pipeline')
    end = DummyOperator(task_id = 'end')
    #start >> [scrape_tipidpc,scrape_gameone] >> end
    start >> [get_html_text, scrape_tipidpc, scrape_gameone] >> end

