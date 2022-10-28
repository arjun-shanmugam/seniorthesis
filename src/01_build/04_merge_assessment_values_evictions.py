"""
04_merge_assessment_values_evictions.py

Merges eviction data with assessment value data.
"""
import pandas as pd
INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
INPUT_DATA_EVICTIONS_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
INPUT_DATA_EVICTIONS_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"


# Merge assessment values with a restricted version of the evictions data.
assessment_df = pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES)
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS_RESTRICTED)

# Drop uneeded columns.
assessment_df = assessment_df.drop(columns=['OWNER1', 'OWN_ADDR', 'OWN_CITY', 'OWN_STATE', 'OWN_ZIP', 'OWN_CO', 'OBJECTID', 'SL'])



# Rename the street address and zip code columns to match the street address column in the evictions file.
assessment_df = assessment_df.rename(columns={'SITE_ADDR': 'property_address_street', 'ZIP': 'property_address_zip'})
e
