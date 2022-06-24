import pandas as pd
from bs4 import BeautifulSoup
#from regex import F
import requests
import lxml
from random import randint
from time import sleep
import os
from pathlib import Path

def try_request(url):
    try:
        r = requests.get(url,timeout=3)
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

def get_items_data(sr, d):
    '''
    ### Appends data to dictionary
    sr : soup variable
    d : dict
    '''
    domain = 'https://tipidpc.com/'
    items = sr.find_all('li')

    for item in items:
        d['date_posted'].append(item.text.split('\n')[-1][3:].strip())
        d['item_name'].append(item.find(class_="item-name").text)
        d['item_price'].append(item.find(class_="catprice").text)
        d['item_url'].append(domain + item.find(class_="item-name").get('href'))


def scrape_all_pages(d = {'item_name':[],'item_price':[],'item_url':[],'date_posted':[]}):
    '''
    ### Cycles through all valid pages 
    '''
    url = 'https://tipidpc.com/catalog.php?sec=s&cat=4' 
    
    pageno = 1
    page = f"{url}&page={pageno}"
    print(page)
    
    while True:
        
        response = try_request(page)
        soup = BeautifulSoup(response.text, 'lxml')
        search_results = soup.find("ul",id="item-search-results")

        if search_results != None:
            get_items_data(search_results, d)
            print(f'page {pageno} done...')
        else:
            print('\n scraping completed!')
            break
        
        pageno+=1
        page = f"{url}&page={pageno}"
        sleep(randint(4,11))
    return d

def main():
    """This gets executed if `tipidpc_scraper.py` gets called."""
    
    
    home_dir = os.getcwd()
    output_dir = f'{home_dir}/data/unprocessed'
    
    #make directories if it doesnt exist yet
    Path("output_dir").mkdir(parents=True, exist_ok=True) 
    
    print('running tipidpc_scraper.py')
    d = scrape_all_pages()
    
    df = pd.DataFrame(d)
    df.to_csv(f'{output_dir}/tipidpc-graphics-cards.csv',index = False)

if __name__ == '__main__':
    main()