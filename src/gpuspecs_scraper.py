import pandas as pd
from bs4 import BeautifulSoup
#from regex import F
import requests
import lxml
import os
from pathlib import Path

def try_request(url):
    headers = {'user-agent': 'your-own-user-agent/0.0.1'}
    try:
        r = requests.get(url,headers = headers)
        r.raise_for_status()
        return r
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("Oops: Unknown Error",err)

def get_gpu_specs(url,d = {'product_name':[], 'gpu_chip':[], 'release_date':[], 'bus':[], 'memory':[], 'gpu_clock':[], 'memory_clock':[], 'shaders_tmus_rops':[]}):
    '''
    d dict that default instantiates dict with product name, gpu chip, rel date, bus, memory, gpu clock, mem clock, and shader/TMUs/ROPs
    '''


    response = try_request(url)
    soup = BeautifulSoup(response.text, 'lxml')    
    soup = soup.find('table', class_='processors')
    
    #print(soup)

    gpus = soup.find_all('tr')
    
    for gpu in gpus:
        row = gpu.find_all('td')
        if row is None or row == []:
            continue
        i = 0
        for key in d.keys():
            d[key].append(row[i].text.strip())
            i+=1

    df = pd.DataFrame(d)
    return df


def main():
    """This gets executed if `gpuspecs_scraper.py` gets called."""
    home_dir = os.getcwd()
    output_dir = f"{home_dir}/data/unprocessed"
    
    #make directories if it doesnt exist yet
    Path(output_dir).mkdir(parents=True, exist_ok=True) 
    
    url = 'http://www.techpowerup.com/gpu-specs/'
    print("running gpuspecs_scraper.py")
    df = get_gpu_specs(url)
    df.to_csv(f"{output_dir}/gpu-specs.csv", index = False)
    print(f"file at: {output_dir}/gpu-specs.csv")
if __name__ == '__main__':
    main() 



