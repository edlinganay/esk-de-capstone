# todo: tipidpc data should have should have product name to properly categorize item names into same GPUs example
# ASUS ROG strix RTX 3090 uses Nvidia's GeFORCE RTX 3090 ti

import pandas as pd
import pysqldf as ps
import numpy as np
import os

def main():
    home_dir = os.pardir
    datadir = f"{home_dir}/data/unprocessed"
    output_dir = f"{home_dir}/data/processed"
    
    gameone = pd.read_csv(f'{datadir}/gameone-graphics-cards.csv')
    tipidpc = pd.read_csv(f"{datadir}/tipidpc-graphics-cards.csv")
    gpu = pd.read_csv(f"{datadir}/gpu-specs.csv")
    
    def get_available_graphics_cards():
        #gameone
        #tipidpc

        gpu['product_name'] = gpu.product_name.str.split(' ').str[2:]
        gpu.replace("",np.nan)

if __name__ == '__main__':
    main() 