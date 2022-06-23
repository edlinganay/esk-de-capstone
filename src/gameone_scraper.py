import pandas as pd
from bs4 import BeautifulSoup
import requests
import lxml
from random import randint
from time import sleep
import os
from datetime import date





def try_request(url):
    try:
        r = requests.get(url)
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

def get_data(sr,dict):
    '''
    sr: soup file search results
    dict: dictionary with item name, price, url, and availability
    '''
    for item in sr.find_all('li'):
        item_details = item.find(class_="product details product-item-details")
        name = item_details.find(class_="product name product-item-name").text.strip()
        url = item_details.find(class_="product-item-link").get('href')
        price = item_details.find("span", class_="price").text.strip()
        oos_check = item_details.find(class_="stock unavailable")
        if oos_check is None or oos_check == []:
            availability = 'Available'
        else: availability = 'Out of Stock'

        row = [name, price, url, availability]
        i = 0
        for key in dict.keys():
            dict[key].append(row[i])
            i+=1

def scrape_all_pages(dict={"item_name":[],"item_price":[],"item_url":[],"item_availability":[]}):
    '''
    # when URL page exceeds available data, it will show the 1st page instead
    # to prevent endless run, check if 1st item is already in data dictionary
    dict: dictionary with item name price url and availability
    '''
    url = 'https://gameone.ph/computer-parts/graphics-card.html'
    pageno = 1
    
    page = f"{url}?p={pageno}"
    response = try_request(page)
    soup = BeautifulSoup(response.text, 'lxml')
    soup = soup.find('ol')
    first_item = soup.find(class_="product name product-item-name").text.strip()
    
    while True: 
        response = try_request(page)
        soup = BeautifulSoup(response.text, 'lxml')
        soup = soup.find('ol')
        check = soup.find(class_="product name product-item-name").text.strip()
        
        if check == first_item and pageno != 1: 
            break #if data is already looping, break 
            print('Scraping completed!')
        
        #print(f'this is page {pageno}')
        
        get_data(soup,dict)
        pageno+=1
        page = f"{url}?p={pageno}"
        sleep(randint(2,6))

    return dict

def main():
    """This gets executed if `gameone_scraper.py` gets called."""
    
    home_dir = os.pardir
    output_dir = f'{home_dir}/data/unprocessed'
    today = date.today().strftime("%Y-%m-%d")

    print("running gameone_scraper.py")
    d = scrape_all_pages()
    df = pd.DataFrame(d)
    df['ao_date'] = today 
    df.to_csv(f"{output_dir}/gameone-graphics-cards.csv", index = False)


if __name__ == '__main__':
    main() 

