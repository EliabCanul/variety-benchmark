import pandas as pd
from ..ion import ion
import datetime
import pickle
from ..preprocessing import preprocessing as prep
import logging
from netadata.ion import query_rds
import sys

class Manager:
    ## Main class managers inherit from. DO NOT use this as template, use ManagerTemplate below
    log = ''
    version = (0,0,0)

    run_params = {
                    'name':'Baseline Manager',
                    'description':"""""",
                    'data' : {},
                    'IO':{},
                    'version':{},
                    
                            }


    def __init__(self) -> None:

        # Overwrite
        pass


    def run(self):
        # Overwrite
        return 

    def save_run_file(self, path : str):
        """ Saves runfile to path
        Args:
            path (str): [description]
        """

        self.run_params['savetime'] = datetime.datetime.now().strptime('$Y-$m-$d')
        self.run_params['version'] = self.version
        pickle.dump(self.run_params, open(path, 'wb'))
        

    def load_run_file(self, path : str):
        """Loads runfile to manager

        Args:
            path (str): path to file
        """
        self.run_params = pickle.load(self.run_params, open(path, 'rb'))




class VarietyBenchmark(Manager): #Use this template to make your own experiment
    log = ''
    version = (0,1,4) # Everytime you change something in the code, please update this so we can keep track of changes
    run_params = {
                    'name':'', # To identify between run files
                    'description':"""A manager to read scraper files and return data frames of product's coincidences""",
                    'data' : {}, # Targets, features, models, etc....
                    'IO':{
                        # Specify store to analize: 'aurrera', 'chedraui', 'scorpion', 'walmart'
                        'store': "aurrera", 
                        }, # Path to DBs used, functions to load / save data
                    'version':{}, # Keep track of different versions of the package in case of debug/reproducibility
                    'log':{}
                            }


    def __init__(self, run_params = None ) -> None:

        if run_params:
            self.run_params = run_params
        pass

    def run(self):
        """Execute all the pipeline to get product coincidences with Neta and competitors.

        - Design your code so that everything that needs to be configured can be accessed
        through "run_params"

        """

        # IO: LOAD ----------------------------------------------------
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("logfile.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Store name
        store = self.run_params['IO']['store']

        logging.info('----------------------------')
        logging.info(f'Resolving for store: {store}')
        
        # Query to db of scraped store
        scrapeddf = ion.query_scraped_store(store, logging)
        
        # Read scraped files
        #logging.info('Reading raw scraped file...')
        #scrapeddf = ion.read_scraped_file(store)
        
        # Pre-processing:
        logging.info('Running preprocessing of data...')
        dfstore = prep.preprocess_df(store, scrapeddf, logging)
    
        # Get data from RDS NETA
        logging.info('Reading Neta catalog from RDS...')
        neta = query_rds('prod', ion.QUERY_PRODUCTS_IN_HOMEPAGE, False)    
        neta = neta[ neta['Gtin'].apply( lambda x: prep.is_int(x)) ]
        neta['Gtin'] = neta['Gtin'].apply(lambda x: prep.complete_13(x))
        neta_cols = [ "NETA_"+c for c in neta.columns.to_list()]
        neta.columns = neta_cols

        # IO: Your Work -----------------------------------------------
        
        # Bodega Aurrerá
        if str(store) == 'aurrera':
            
            logging.info(f'Matching products between Neta and Aurrera...')
            
            # Add display order, grouped by category
            # NOTE: This is different to the display_order in benchmark_aurrera, since that 
            # display_order is not refreshed each time the category changes.
            aur_display_groups = pd.DataFrame(data=None)
            for name, group in dfstore.groupby('BA_category'):
                group['BA_display_order'] = list(range(len(group)))
                aur_display_groups = pd.concat( [aur_display_groups, group] )
                
            aur_display_groups.sort_values(['BA_category','BA_display_order'], inplace=True)

            # Reorder columns
            aur_display_groups = aur_display_groups[['BA_ean', 'BA_product_name', 
                                                     'BA_display_order', 'BA_category', 
                                                     'BA_subcategory', 'BA_normal_price', 
                                                     'BA_current_price', 'BA_url']]
            # Merge tables
            neta_aurrera = pd.merge(aur_display_groups, neta, right_on='NETA_Gtin', left_on='BA_ean', how='outer', indicator=True)

            # Change labels
            neta_aurrera['_merge'] = neta_aurrera['_merge'].apply(lambda x: prep.say_if_its_in(x))

            neta_aurrera.rename(columns={'_merge':'is_in'}, inplace=True)

            # Fill Nan
            neta_aurrera['BA_display_order'] = neta_aurrera['BA_display_order'].fillna(-1)

            neta_aurrera['BA_display_order'] = neta_aurrera['BA_display_order'].astype(int)

            neta_aurrera.sort_values(['BA_category','BA_display_order'], inplace=True)


        elif store == 'chedraui':
            
            logging.info(f'Matching products between Neta and Chedraui...')
             
            ched_display_groups = pd.DataFrame(data=None)

            for name, group in dfstore.groupby('CH_input_category'):

                group['CH_display_order'] = list(range(len(group)))
                ched_display_groups = pd.concat( [ched_display_groups, group] )

            ched_display_groups.sort_values(['CH_input_category','CH_display_order'], inplace=True)
        
            ched_display_groups.reset_index(inplace=True, drop=True)

            ched_display_groups = ched_display_groups[['CH_upc','CH_product_name',
                                                       'CH_display_order','CH_input_category',
                                                       'CH_category','CH_sub_category_1',
                                                       'CH_sub_category_2','CH_normal_price',
                                                       'CH_url']]

        else: 
            logging.error(f'Impossible to match products for store: {store}')

        # IO: OUT -----------------------------------------------------

        # Save
        logging.info(f'Saving final tables for {store}...')
        
        if store == 'aurrera':
            ion.save_aurrera(neta_aurrera, logging)
            
        elif store == 'chedraui':
            ion.save_chedraui(ched_display_groups, logging)
            
        logging.info('Finished!')
            


