"""
05_GET_zillow_data.py
"""
import pandas as pd
from zillow_utilities import GET_zestimate_history
INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_zpids.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"

# Get Zestimates and concatenate into a single DataFrame.
zpids_df = pd.read_csv(INPUT_DATA, dtype={'zpid': str}).fillna(" ")
zestimates = [GET_zestimate_history(zpid, case_number) for zpid, case_number in zip(zpids_df['zpid'], zpids_df['case_number'])]
zestimates_df = pd.concat(zestimates, axis=0)
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