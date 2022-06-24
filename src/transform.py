# todo: tipidpc data should have should have product name to properly categorize item names into same GPUs example
# ASUS ROG strix RTX 3090 uses Nvidia's GeFORCE RTX 3090 ti

import pandas as pd
import pysqldf as ps
import numpy as np
import os
from datetime import date
from pathlib import Path

def main():
    home_dir = os.getcwd()
    datadir = f"{home_dir}/data/unprocessed"
    output_dir = f"{home_dir}/data/processed"
    #make directories if it doesnt exist yet
    Path(output_dir).mkdir(parents=True, exist_ok=True)


    
    

    def transform_gpu_specs():
        gpu = pd.read_csv(f"{datadir}/gpu-specs.csv")
        gpu['prodkey'] = gpu.product_name.str.split(' ').str[2:]
        gpu.replace("",np.nan)
        gpu['release_date'] = pd.to_datetime(gpu.release_date)
        gpu.to_csv(f'{output_dir}/gpu-specs.csv')
        print(f'File transformed. Available at: {output_dir}/gpu-specs.csv',index=False)

    def get_available_data():
        gameone = pd.read_csv(f'{datadir}/gameone-graphics-cards.csv')
        tipidpc = pd.read_csv(f"{datadir}/tipidpc-graphics-cards.csv")
        today = date.today().strftime("%Y-%m-%d")

        tipidpc['item_price'] = tipidpc['item_price'].str.replace("P","").astype(float)
        tipidpc['date_posted'] = pd.to_datetime(tipidpc['date_posted'])
        gameone['item_price'] = gameone['item_price'].str.replace('₱','').str.replace(',','').astype(float)
        tipidpc['ao_date'] = today
        sqldf = ps.SQLDF(locals())

        q1 = """
        SELECT item_name, item_price, item_url as url, date_posted, ao_date
        FROM tipidpc
        WHERE UPPER(item_name) NOT LIKE "%SOLD%"
        AND UPPER(item_name) NOT LIKE "%RESERVE%"
        """
        q2 = """
        SELECT item_name,item_price, item_url as url, NULL as date_posted, ao_date
        FROM gameone
        WHERE upper(item_availability) = "AVAILABLE"
        """
        q3 = """
        SELECT item_name,item_price,url,date_posted,ao_date FROM available_tipidpc
        UNION ALL
        SELECT item_name,item_price,url,date_posted,ao_date FROM available_gameone
        """

        available_tipidpc = sqldf.execute(q1)
        available_gameone = sqldf.execute(q2)
        sqldf = ps.SQLDF(locals())
        available_gpu = sqldf.execute(q3)        

        available_gpu.to_csv(f'{output_dir}/available_gpu.csv',index=False)
        print(f'Files transformed. Find at {output_dir}/available_gpu.csv')

    #transform_gpu_specs()
    get_available_data()

if __name__ == '__main__':
    main() 