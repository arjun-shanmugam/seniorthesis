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

# Make the address columns in both datasets consistent.
evictions_df.loc[:, 'property_address_street'] = (evictions_df.loc[:, 'property_address_street']
                                                  .str.lower()
                                                  .str.split()
                                                  .str.join(" "))
assessment_df.loc[:, 'property_address_street'] = (assessment_df.loc[:, 'property_address_street']
                                                   .str.lower()
                                                   .str.split()
                                                   .str.join(" "))

# Make the zip code columns in both datasets consistent.
evictions_df.loc[:, 'property_address_zip'] = evictions_df.loc[:, 'property_address_zip'].str[0:5].astype(int)
assessment_df.loc[:, 'property_address_zip'] = assessment_df.loc[:, 'property_address_zip'].astype(int)


#
print(evictions_df[['property_address_street', 'property_address_zip']])
print(assessment_df[['property_address_street', 'property_address_zip']])

print(evictions_df.columns)
print(assessment_df.columns)

# Merge assessment values with an unrestricted version of the eviction data.
