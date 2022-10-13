"""
02_clean_assessment_values.py

Cleans property assessment values from MassGIS.
"""
import censusgeocode
import geopandas as gpd
import pandas as pd
import numpy as np
import os
from src.utilities.dataframe_utilities import reduce_mem_usage, batch_df
import matplotlib.pyplot as plt
from src.utilities.figure_utilities import plot_pie_chart

INPUT_DATA_ASSESSOR = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/AllParcelData_SHP_20220810"
INPUT_DATA_ZIPCODES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zipcodes.csv"
INTERMEDIATE_DATA_GEOCODING = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/to_geocode"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
OUTPUT_FIGURES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/01_exploratory/figures"
town_folders = os.listdir(INPUT_DATA_ASSESSOR)

dfs = []
for town_folder in town_folders:
    if town_folder == '.DS_Store':  # ignore folder metadata
        continue
    data = town_folder.split("_")
    town_id = data[2]
    town_name = data[3]
    calendar_year = data[4][2:]
    fiscal_year = data[5][2:]
    assessor_filename = town_id + "Assess" + "_" + data[4] + "_" + data[5] + ".dbf"
    df = gpd.read_file(os.path.join(INPUT_DATA_ASSESSOR, town_folder, assessor_filename))
    df.loc[:, 'CY'] = int(calendar_year)  # save calendar year as its own column
    df.loc[:, 'TOWN_NAME'] = town_name
    df = df.drop(columns='geometry')  # drop the geometry column from the assessor database

    df, columns_containing_nans = reduce_mem_usage(df)

    # Convert object type columns to string.
    object_type_columns = df.columns[df.dtypes == "object"].tolist()
    df.loc[:, object_type_columns] = df[object_type_columns].astype("string")

    dfs.append(df)

# concatenate municipality-year level DataFrames.
assessor_data = pd.concat(dfs, axis=0).reset_index(drop=True)

# Drop rows where TOTAL_VAL == -1
mask = (assessor_data['TOTAL_VAL'] != -1)
fig, ax = plt.subplots(1, 1)  # Plot a pie chart giving the use code of properties with TOTAL_VAL == -1
plot_pie_chart(ax,
               x=assessor_data.loc[mask, 'USE_CODE'],
               title="Use Codes of Properties with TOTAL_VAL == -1")
plt.savefig(os.path.join(OUTPUT_FIGURES, "pie_USE_CODE_for_TOTAL_VAL_-1.png"), bbox_inches='tight')
plt.close(fig)
assessor_data = assessor_data.loc[mask, :]

# Drop rows where SITE_ADDR = NaN
mask = ~(assessor_data['SITE_ADDR'].isna())
fig, ax = plt.subplots(1, 1)  # Plot a pie chart giving the use code of properties with SITE_ADDR == NaN
plot_pie_chart(ax,
               x=assessor_data.loc[mask, 'USE_CODE'],
               title="Use Codes of Properties with SITE_ADDR == NaN")
plt.savefig(os.path.join(OUTPUT_FIGURES, "pie_USE_CODE_for_SITE_ADDR_NaN.png"), bbox_inches='tight')
plt.close(fig)
assessor_data = assessor_data.loc[mask, :]

# Select rows where zip code column does not contain a real Massachusetts zip code.
MA_zipcodes = pd.read_csv(INPUT_DATA_ZIPCODES, dtype={'zipcode': str})['zipcode']
correct_zipcode_mask = assessor_data['ZIP'].str[0:5].isin(MA_zipcodes)

# Drop those rows from the data.
assessor_data = assessor_data.loc[correct_zipcode_mask, :]

# save to CSV
assessor_data.to_csv(OUTPUT_DATA, index=False)
