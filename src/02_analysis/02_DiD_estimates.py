"""
02_DiD_estimates.py

Run difference-in-differences estimates.
"""
import numpy as np
import pandas as pd
from differences import ATTgt
from matplotlib import pyplot as plt
INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/restricted.csv"
df = pd.read_csv(INPUT_DATA)

# Store column names containing Zestimates.
years = [str(year) for year in range(2013, 2023)]
months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
value_vars = ["2012-12"] + [str(year) + "-" + str(month) for year in years for month in months]
month_to_int_dictionary = {key: value for value, key in enumerate(value_vars)}
int_to_month_dictionary = {key: value for key, value in enumerate(value_vars)}

# Drop rows for which we have no Zestimates.
no_zestimates_mask = df[value_vars].notna().any(axis=1)  # True if any Zestimates are present, False otherwise.
print(f"Limiting sample to {no_zestimates_mask.sum()} evictions for which we have Zestimate data.")
df = df.loc[no_zestimates_mask, :]

# Drop rows for which we do not have the latest docket date.
no_latest_docket_date_mask = ~df['latest_docket_date'].isna()
print(f"Limiting sample to {no_latest_docket_date_mask.sum()} evictions for which the latest docket date is listed.")
df = df.loc[no_latest_docket_date_mask, :]

# Reshape from wide to long.
df = pd.melt(df,
             id_vars=['case_number', 'latest_docket_date', 'judgment_for_plaintiff'],
             value_vars=value_vars, var_name='month', value_name='zestimate')
df = df.sort_values(by=['case_number', 'month'])

# Get 'first treated' date for each observation.
df.loc[:, 'latest_docket_date'] = pd.to_datetime(df['latest_docket_date']).dt.strftime('%Y-%m')

# Convert months to integers.
df.loc[:, 'month'] = df['month'].replace(month_to_int_dictionary)
df.loc[:, 'latest_docket_date'] = df['latest_docket_date'].replace(month_to_int_dictionary)
never_treated_mask = (df['judgment_for_plaintiff'] == 0)
df.loc[never_treated_mask, 'latest_docket_date'] = np.NaN

# Set index to entity and time.
df = df.set_index(['case_number', 'month'])

# Run DiD.
att_gt = ATTgt(data=df, cohort_name='latest_docket_date', freq='M')
result = att_gt.fit(formula='zestimate', control_group="not_yet_treated", n_jobs=-1)
result.to_csv("~/Desktop/results.csv")
print(att_gt.aggregate('event'))
att_gt.aggregate('event').to_csv("~/Desktop/agg_results.csv")
att_gt.plot('event')
plt.show()
print("Done")