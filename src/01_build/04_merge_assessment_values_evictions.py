"""
04_merge_assessment_values_evictions.py

Merges eviction data with assessment value data.
"""
import pandas as pd

INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
INPUT_DATA_EVICTIONS_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
INPUT_DATA_EVICTIONS_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"
OUTPUT_DATA_ASSESSOR_VALUES_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/assessor_values_restricted.csv"
OUTPUT_DATA_ASSESSOR_VALUES_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/assessor_values_unrestricted.csv"
OUTPUT_DATA_LS_PRICE_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/ls_price_restricted.csv"
OUTPUT_DATA_LS_PRICE_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/ls_price_unrestricted.csv"

# 1. Produce restricted version of assessor values file.
assessment_df = pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES, dtype={'ZIP': str})
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS_RESTRICTED)

# Build the address and date columns to match on in both datasets.
evictions_df.loc[:, 'file_date'] = pd.to_datetime(evictions_df['file_date'])
evictions_df = evictions_df.rename(columns={'parsed': 'full_geocoded_address'})
evictions_df.loc[:, 'full_geocoded_address'] = evictions_df['full_geocoded_address'].str.lower()
evictions_df.loc[:, 'geocoded_street_address'] = evictions_df['full_geocoded_address'].str.split(", ", regex=False).str[0]
evictions_df.loc[:, 'geocoded_zipcode'] = evictions_df['full_geocoded_address'].str.split(", ", regex=False).str[-1]

assessment_df = assessment_df.rename(columns={'ZIP': 'geocoded_zipcode'})
assessment_df.loc[:, 'geocoded_street_address'] = (assessment_df['ADDR_NUM'].astype(str) +
                                                   " " +
                                                   assessment_df['FULL_STR'].str.lower())
assessment_df.loc[:, 'FY'] = pd.to_datetime("20" + assessment_df['FY']
                                            .dropna()
                                            .astype(int)
                                            .astype(str)).dt.year
assessment_df.loc[:, 'assessment_value_decided_date'] = pd.to_datetime("01/01/" +
                                                                       (assessment_df['FY'] - 1).astype(str))

# Group assessor values dataset by address and year.
columns_to_keep = ['TOTAL_VAL', 'BLDG_VAL', 'OTHER_VAL', 'BLD_AREA', 'UNITS', 'LAND_VAL']
# Convert to float data type so that we can .groupby() and then .sum()
assessment_df.loc[:, 'BLDG_VAL'] = assessment_df['BLDG_VAL'].replace({"SAUGUS": 0}).astype(float)
assessment_df.loc[:, 'LAND_VAL'] = assessment_df['LAND_VAL'].replace({"MA": 0}).astype(float)
assessment_df = (assessment_df
                 .groupby(by=['geocoded_street_address', 'geocoded_zipcode', 'assessment_value_decided_date'])
                 .sum()[columns_to_keep]
                 .reset_index())

# Merge TODO: Edit merge so that we are merging on fiscal year!
df = pd.merge_asof(left=assessment_df,
                   right=evictions_df,
                   by=['geocoded_street_address', 'geocoded_zipcode'],
                   left_on=['assessment_value_decided_date'],
                   right_on=['file_date'],
                   # Take the most recent row from right D.F. s.t. right[file_date] < left[assessment_value_decided_date]
                   direction='backward')
df = assessment_df.merge(evictions_df,
                         right_on=['geocoded_street_address', 'geocoded_zipcode', 'fiscal_year'],
                         left_on=['geocoded_street_address', 'geocoded_zipcode', 'calendar_year_plus_2'],
                         how='outer',
                         validate='1:m',
                         indicator='_merge2')

print(df[['geocoded_street_address', 'geocoded_zipcode']].loc[df['_merge2'] == 'right_only', :])
print(df['geocoded_street_address'].loc[df['_merge2'] == 'right_only'].str.contains("  ", regex=False).sum().sum())
print(df['_merge2'].value_counts())

mask = (df['_merge2'] == 'right_only')
unmerged_evictions = df.loc[mask, :]

df.to_csv(OUTPUT_DATA_ASSESSOR_VALUES_RESTRICTED)
