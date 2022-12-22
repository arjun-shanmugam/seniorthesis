"""
03_get_zillow_URLs.py

Merges eviction data with Zillow housing data.
"""
import pandas as pd
from zillow_utilities import get_zillow_url
from joblib import Parallel, delayed
from multiprocessing import Manager
if __name__ == '__main__':
    INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
    OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_urls.csv"
    N_JOBS = -1
    evictions_df = pd.read_csv(INPUT_DATA)

    # Get URL of Zillow page for each property
    manager = Manager()
    addresses_URL_tuples = manager.list()
    Parallel(n_jobs=-1)(delayed(get_zillow_url)(address, addresses_URL_tuples) for address in evictions_df['property_address_full'])
    result = pd.DataFrame(list(addresses_URL_tuples), columns=['property_address_full', 'zillow_url'])

    evictions_df = evictions_df.merge(result, on=['property_address_full'], how='inner', validate='1:1')
    evictions_df.to_csv(OUTPUT_DATA, index=False)
