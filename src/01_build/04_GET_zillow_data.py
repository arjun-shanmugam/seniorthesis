"""
04_GET_zillow_data.py
"""
import pandas as pd
from zillow_utilities import GET_zestimate_history
INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_zpids.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"

# Get Zestimates and concatenate into a single DataFrame.
zpids_df = pd.read_csv(INPUT_DATA, dtype={'zpid': str}).fillna(" ")
zestimates_df = pd.concat([GET_zestimate_history(zpid, address) for zpid, address in zip(zpids_df['zpid'], zpids_df['property_address_full'])], axis=0)
# TODO: Resume here. Start by adapting GET_zestimate_history to the new way that ZPIDs are stored on disk.
zestimates_df = zestimates_df.reset_index()

# Produce month and year columns.
zestimates_df.loc[:, 'time'] = pd.to_datetime(zestimates_df['time'], unit='s')
zestimates_df.loc[:, 'month_year'] = zestimates_df['time'].dt.to_period('M')
zestimates_df = zestimates_df.drop(columns='time')

# Average Zestimates to the month-case level.
zestimates_df = zestimates_df.groupby(by=['case_number', 'month_year']).mean().reset_index()

# Reshape from long to wide.
zestimates_df = pd.pivot(zestimates_df, index=['case_number'], columns=['month_year'], values='zestimate')

# Save to CSV.
zestimates_df.to_csv(OUTPUT_DATA)