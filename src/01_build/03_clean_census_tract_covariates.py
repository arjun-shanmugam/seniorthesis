"""
clean_census_tract_covariates.py

Cleans census tract level data from Opportunity Insights.
"""
import pandas as pd

INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/tract_covariates.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/tracts.csv"

# Load data.
tracts_df = pd.read_csv(INPUT_DATA)

# Keep only rows corresponding to Massachusetts.
tracts_df = tracts_df.loc[tracts_df['state'] == 25, :]

# Generate a GEOID column.
tracts_df.loc[:, 'county'] = tracts_df['county'].astype(str).str.zfill(3)
tracts_df.loc[:, 'tract'] = tracts_df['tract'].astype(str).str.zfill(6)
tracts_df.loc[:, 'tract_geoid'] = tracts_df['state'].astype(str) + tracts_df['county'] + tracts_df['tract']


# Save data.
tracts_df.to_csv(OUTPUT_DATA)
#%%
