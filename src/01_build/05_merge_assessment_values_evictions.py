"""
05_merge_assessment_values_evictions.py

Merges eviction data with assessment value data.
"""
import pandas as pd

INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
INPUT_DATA_EVICTIONS_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
INPUT_DATA_EVICTIONS_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"
OUTPUT_DATA_ASSESSOR_VALUES_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/assessor_values_restricted.csv"
OUTPUT_DATA_ASSESSOR_VALUES_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/assessor_values_unrestricted.csv"

# 1. Produce restricted version of assessor values file.
assessment_df = pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES, dtype={'ZIP': str})

evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS_RESTRICTED)

# Build date columns in eviction dataset.
evictions_df.loc[:, 'file_date'] = pd.to_datetime(evictions_df['file_date'])
evictions_df.loc[:, 'file_year'] = evictions_df['file_date'].dt.year
evictions_df.loc[:, 'file_month'] = evictions_df['file_date'].dt.strftime('%Y-%m')

# Build address columns in eviction dataset.
evictions_df = evictions_df.rename(columns={'parsed': 'full_geocoded_address'})
evictions_df.loc[:, 'full_geocoded_address'] = evictions_df['full_geocoded_address'].str.lower()
evictions_df.loc[:, 'geocoded_street_address'] = evictions_df['full_geocoded_address'].str.split(", ", regex=False).str[0]
evictions_df.loc[:, 'geocoded_zipcode'] = evictions_df['full_geocoded_address'].str.split(", ", regex=False).str[-1]

# Build date columns in assessment values dataset.
assessment_df.loc[:, 'assessment_value_decided_date'] = pd.to_datetime("10/01/" + (assessment_df['FY'] - 1).astype(str))

# Build address columns in assessment values dataset.
assessment_df.loc[:, 'full_geocoded_address'] = assessment_df['full_geocoded_address'].str.lower()
assessment_df.loc[:, 'geocoded_street_address'] = assessment_df['full_geocoded_address'].str.split(", ", regex=False).str[0]
assessment_df.loc[:, 'geocoded_zipcode'] = assessment_df['full_geocoded_address'].str.split(", ", regex=False).str[-1]

# Group assessor values dataset by address and year.
columns_to_keep = ['TOTAL_VAL', 'BLDG_VAL', 'OTHER_VAL', 'BLD_AREA', 'UNITS', 'LAND_VAL']
# Convert to float data type so that we can .groupby() and then .sum()
assessment_df.loc[:, 'BLDG_VAL'] = assessment_df['BLDG_VAL'].replace({"SAUGUS": 0}).astype(float)
assessment_df.loc[:, 'LAND_VAL'] = assessment_df['LAND_VAL'].replace({"MA": 0}).astype(float)
assessment_df = (assessment_df
                 .groupby(by=['geocoded_street_address', 'geocoded_zipcode', 'assessment_value_decided_date'])
                 .mean()[columns_to_keep]  # TODO: Should I be summing here?
                 .reset_index())

# Merge eviction in calendar year t with assessor record from fiscal year t+2
# Sort DataFrames
evictions_df = evictions_df.sort_values(by=['file_date'])
assessment_df = assessment_df.sort_values(by=['assessment_value_decided_date'])

eviction_zipcodes_in_assessment_data = evictions_df['geocoded_zipcode'].isin(assessment_df['geocoded_zipcode']).mean()
print(f"{eviction_zipcodes_in_assessment_data} percent of zipcodes in eviction data are in assessment data.")
eviction_street_addresses_in_assessment_data = evictions_df['geocoded_street_address'].isin(assessment_df['geocoded_street_address']).mean()
print(f"{eviction_street_addresses_in_assessment_data} percent of street addresses in eviction data are in assessment data")

print(assessment_df[['geocoded_street_address', 'geocoded_zipcode', 'assessment_value_decided_date']])
print(evictions_df[['geocoded_street_address', 'geocoded_zipcode', 'file_date']])

df = pd.merge_asof(left=evictions_df,
                   right=assessment_df,
                   by=['geocoded_street_address', 'geocoded_zipcode'],
                   left_on=['file_date'],
                   right_on=['assessment_value_decided_date'],
                   # Take the earliest row from right D.F. s.t. right[assessment_value_decided_date] < left[file_date]
                   direction='forward',
                   allow_exact_matches=False,
                   )

mask = ~(df['TOTAL_VAL'].isna())
df = df.loc[mask, :]

df.to_csv(OUTPUT_DATA_ASSESSOR_VALUES_RESTRICTED)
