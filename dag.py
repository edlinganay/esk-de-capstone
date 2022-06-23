# todo: add dags task




#start_pipeline >> [scrape_gpuspecs, scrape_tipidpc, scrape_gameone] >> validate >> transform_task >> upload_to_gcs_task >> load_gcs_to_bq >> end