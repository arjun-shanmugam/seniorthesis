"""
02_POST_batched_addresses.py

Sends POST requests containing batches of addresses.
"""

import csv
import os
import time
import pandas as pd
from build_utilities import batch_df
from zillow_utilities import POST_batch_ZPID_request, to_binary

INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
BATCHED_ADDRESSES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/batched_addresses"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/job_numbers.csv"
evictions_df = pd.read_csv(INPUT_DATA)

"""Get ZPIDs for each property."""
# Save eviction addresses in 100-row batches.
batched_addresses = batch_df(df=evictions_df['property_address_full'], batch_size=100)
filepaths = [os.path.join(BATCHED_ADDRESSES, f"batch_{i}.txt") for i, batched_address in enumerate(batched_addresses)]
[to_binary(filepath=filepath, series=batch, index=False, header=False) for filepath, batch in zip(filepaths, batched_addresses)]
job_numbers = [POST_batch_ZPID_request(filepath) for filepath in filepaths]
job_numbers = pd.Series(job_numbers)
job_numbers.to_csv(OUTPUT_DATA, index=False, header=False)