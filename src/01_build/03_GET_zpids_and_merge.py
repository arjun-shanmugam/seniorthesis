"""
03_GET_zpids_and_merge.py
"""
import pandas as pd
from zillow_utilities import GET_processed_ZPIDs
INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
INPUT_DATA_JOB_NUMBERS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/job_numbers.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_zpids.csv"
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
job_numbers = pd.read_csv(INPUT_DATA_JOB_NUMBERS, header=None)[0]
addresses_with_zpids = pd.concat([GET_processed_ZPIDs(job_number) for job_number in job_numbers], axis=0).reset_index(drop=True)
addresses_with_zpids.to_csv(OUTPUT_DATA, index=False)
