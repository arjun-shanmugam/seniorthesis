"""
04_GET_zpids_and_merge.py
"""
import pandas as pd
from zillow_utilities import GET_processed_ZPIDs
INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
INPUT_DATA_JOB_NUMBERS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/job_numbers.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_zpids.csv"
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
job_numbers = pd.read_csv(INPUT_DATA_JOB_NUMBERS, header=None)[0]

# Returns a DataFrame where each row corresponds to one eviction case and all of the ZPIDs associated with that eviction case's address.
addresses_with_zpids = pd.concat([GET_processed_ZPIDs(job_number) for job_number in job_numbers], axis=0).reset_index(drop=True)

# Merge that DataFrame with the correct case number.
case_numbers_with_ZPIDs = pd.concat([evictions_df['case_number'], addresses_with_zpids.drop(columns='property_address_full')], axis=1)

# Convert to long format.
case_numbers_with_ZPIDs = pd.wide_to_long(case_numbers_with_ZPIDs, stubnames='zpid', i='case_number', j='j').reset_index().drop(columns='j').reset_index(drop=True)

case_numbers_with_ZPIDs.to_csv(OUTPUT_DATA, index=False)
