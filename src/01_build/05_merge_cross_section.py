"""
05_merge_cross_section.py

Merges eviction data with assessment value data.
"""
import pandas as pd

INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data_geocoded.csv"
INPUT_DATA_EVICTIONS_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
INPUT_DATA_EVICTIONS_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"
OUTPUT_DATA_ASSESSOR_VALUES_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/cross_section_restricted.csv"
OUTPUT_DATA_ASSESSOR_VALUES_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/cross_section_unrestricted.csv"

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
assessment_df.loc[:, 'assessment_value_decided_date'] = pd.to_datetime("01/01/" + (assessment_df['FY'] - 1).astype(str))

# Build address columns in assessment values dataset.
assessment_df.loc[:, 'full_geocoded_address'] = assessment_df['full_geocoded_address'].str.lower()
assessment_df.loc[:, 'geocoded_street_address'] = assessment_df['full_geocoded_address'].str.split(", ", regex=False).str[0]
assessment_df.loc[:, 'geocoded_zipcode'] = assessment_df['full_geocoded_address'].str.split(", ", regex=False).str[-1]

# Check how many addresses in eviction data appear in assessment file.
eviction_full_addresses_in_assessment_data = evictions_df['full_geocoded_address'].isin(assessment_df['full_geocoded_address']).mean()
print(f"{eviction_full_addresses_in_assessment_data} percent of full addresses in eviction data are in assessment data")
eviction_zipcodes_in_assessment_data = evictions_df['geocoded_zipcode'].isin(assessment_df['geocoded_zipcode']).mean()
print(f"{eviction_zipcodes_in_assessment_data} percent of zipcodes in eviction data are in assessment data.")
eviction_street_addresses_in_assessment_data = evictions_df['geocoded_street_address'].isin(assessment_df['geocoded_street_address']).mean()
print(f"{eviction_street_addresses_in_assessment_data} percent of street addresses in eviction data are in assessment data")

# Write to a file the eviction rows which do not appear in the assessment file.
no_tax_record_mask = ~(evictions_df['full_geocoded_address'].isin(assessment_df['full_geocoded_address']))
evictions_df.loc[no_tax_record_mask, :].to_csv("~/Desktop/unmatched_eviction_records.csv")

# Group assessor values dataset by address and year.
columns_to_keep = ['TOTAL_VAL', 'BLDG_VAL', 'OTHER_VAL', 'BLD_AREA', 'UNITS', 'LAND_VAL']
# Convert to float data type so that we can .groupby() and then .sum()
assessment_df.loc[:, 'BLDG_VAL'] = assessment_df['BLDG_VAL'].replace({"SAUGUS": 0}).astype(float)
assessment_df.loc[:, 'LAND_VAL'] = assessment_df['LAND_VAL'].replace({"MA": 0}).astype(float)
assessment_df = (assessment_df
                 .groupby(by=['geocoded_street_address', 'geocoded_zipcode', 'lon', 'lat', 'assessment_value_decided_date'])
                 .sum()[columns_to_keep]
                 .reset_index())

# Sort DataFrames
evictions_df = evictions_df.sort_values(by=['file_date'])
assessment_df = assessment_df.sort_values(by=['assessment_value_decided_date'])





df = pd.merge_asof(left=evictions_df,
                   right=assessment_df,
                   by=['lon', 'lat'],
                   left_on=['file_date'],
                   right_on=['assessment_value_decided_date'],
                   # Take the earliest row from right D.F. s.t. right[assessment_value_decided_date] < left[file_date]
                   direction='forward',
                   allow_exact_matches=False,
                   )

mask = ~(df['TOTAL_VAL'].isna())
df = df.loc[mask, :]
print(f"Final dataset contains {len(df)} observations.")
df.to_csv(OUTPUT_DATA_ASSESSOR_VALUES_RESTRICTED)
