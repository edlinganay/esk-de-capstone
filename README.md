# ESK-DE-CAPSTONE

Capstone project containing end to end automated dashboard project.
Capstone is about creating an Airflow DAG pipeline that automatically updates data in [GPU Price Monitoring dashboard](https://datastudio.google.com/reporting/90b9f347-1a65-4ba1-a8df-4c0e45957918) 


## QUICK START GUIDE

1. install docker engine see (https://docs.docker.com/engine/install/)
2. create and edit docker-compose.yaml file 
3. create a dags folder and link it to this repo
4. run ```docker compose up```
5. go to your airflow webserver on localhost:8080

## SCHEDULE
Runs daily at 7 PM

## SOME ISSUES
* make sure to set up a GCP Cloud connection on Airflow with name 'google_cloud_default'
* when running in a VM use nginx to redirect localhost:8080 to VM external IP
* gpusecs_scraper fails when running on VM due to refused connection
* There is no task to create BQ dataset.  Before running DAG, on BQ create dataset gpu-data

## OUTPUT DATA SCHEMA
```
[
    {"name": "item_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "item_price", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_posted", "type": "DATETIME", "mode": "NULLABLE"},
    {"name": "ao_date", "type": "DATE", "mode": "NULLABLE"},
        ]
```
item_price: FLOAT > 0

## DATA SOURCES
* https://tipidpc.com/catalog.php?sec=s&cat=4
* https://gameone.ph/computer-parts/graphics-card.html
* https://www.techpowerup.com/gpu-specs/
