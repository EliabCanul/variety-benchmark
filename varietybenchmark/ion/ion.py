import pandas as pd
from dotenv import find_dotenv, load_dotenv
from ..vis import vis as vis
from netadata.ion import query_rds, push_table

# add here the db name for the remote db in rds
remote_db_names = {
    'aurrera': 'products_variety_ba',
    'chedraui': 'products_variety_chedraui'
}

# Load connection credentials
load_dotenv(find_dotenv())

def read_scraped_file(store):
    """Read raw files from  scraper. Add store name and path if needed.

    :param store: store name
    :type store: str
    :raises ValueError: Invalid store name. It should be in run_params["IO"]["store"]
        from managers
    :return: dataframe with scraped data
    :rtype: dataframe
    """

    # This part can be connected  with the price-scraping module
    if store == 'aurrera':
        #scraped_file = '../../data/raw/14-March-2022 14_43_30 despensa_bodegaaurrera_data.xlsx'
        scraped_file = './data/raw/14-March-2022 14_43_30 despensa_bodegaaurrera_data.xlsx'
    elif store == 'chedraui':
        #scraped_file = '../../data/raw/08-March-2022 21_37_58 chedraui_mx_data.xlsx'
        scraped_file = './data/raw/08-March-2022 21_37_58 chedraui_mx_data.xlsx'
    else:
        raise ValueError('Invalid store name in run_params["IO"]["store"]. Check docstring for valid names.')
        
    return pd.read_excel(scraped_file)
    
def query_scraped_store(store, logging):
    """_summary_

    :param store: store name
    :type store: str
    :param logging: logger
    :type logging: class
    :return: dataframe of the resulting query
    :rtype: dataframe
    """
    
    logging.info(f'Querying scraper results from {store}...')    
    if store == 'aurrera':
        return query_rds('data', QUERY_BA_SCRAPER, False)
    
    if store == 'chedraui':
        return query_rds('data', QUERY_CHEDRAUI_SCRAPER, False)    

def save_aurrera(neta_aurrera, logging):
    """Save processed data locally and remotely for BA. Additionally, 
    split and save tables depending on the existence of products in BA or in Neta

    :param neta_aurrera: dataframe contaning prodcut match
    :type neta_aurrera: dataframe
    :param logging: logger 
    :type logging: class
    """
    
    # save plot of percentage of product coincidences
    vis.plot_ba_percentage_coincidences(neta_aurrera, logging)
    
    # push to Data BI db
    table_name = remote_db_names['aurrera']
    logging.info(f'Pushing table to destination in rds: {table_name}')
    push_table(neta_aurrera, table_name, 'data', if_exists = 'replace')
    
    # save final dataframe locally
    logging.info('Saving files locally...')
    neta_aurrera.to_excel('./data/processed/aurrera_neta_products.xlsx')
    
    # Coincidences with aurrera
    yes_from_aurrera = neta_aurrera[neta_aurrera.is_in=='ambos']
    yes_from_aurrera.to_excel('./data/interim/ba_products_coincidence.xlsx')
    
    # Products only in aurrera catalog
    no_from_aurrera =  neta_aurrera[neta_aurrera.is_in=='BA']
    no_from_aurrera.to_excel('./data/interim/ba_products_no_coincidence.xlsx')
    
    # Additional information about categories
    # print coincidences of categories
    # prep.match_ba_categories(yes_from_aurrera, no_from_aurrera)

    return

def save_chedraui(df, logging):
    """Save processed data locally and remotely for Chedraui

    :param df: final dataframe of products for chedraui
    :type df: dataframe
    :param logging: logger
    :type logging: class
    """    
    # push to Data BI db
    table_name = remote_db_names['chedraui']
    logging.info(f'Pushing table to destination in rds: {table_name}')
    push_table(df, table_name, 'data', if_exists = 'replace')
    
    # save final datafram locally
    logging.info('Saving files locally...')
    df.to_excel('./data/processed/chedraui_products.xlsx')
    
    return


QUERY_PRODUCTS_IN_HOMEPAGE = """select 
p.Id,
p.Sku,
p.Gtin,
p.Name,
p.Price,
pcm.Id as catId,
cat.Name as category

from netamx.Product p
    left join product_category_mapping pcm
    on p.Id = pcm.ProductId
    left join category cat
    on pcm.CategoryId = cat.Id



    where p.Sku not like '%F1%'
    and p.Sku not like '%F2%'
    and p.Sku not like '%SP%'
    and p.Sku not like '%LB%'
    and p.Deprecated != 1
    and p.Deleted != 1
    and p.Published = 1
    order by p.Id Desc;
    
"""


QUERY_BA_SCRAPER = """
select 

ean,
product_id,
product_name,
category,
subcategory,
normal_price,
current_price,
url,
scraping_datetime

from dataprocessing.benchmark_aurrera

order by scraping_datetime desc
"""

QUERY_CHEDRAUI_SCRAPER = """
select *

from dataprocessing.benchmark_chedraui
"""