"""
clean_assessment_values.py

Cleans property assessment values from MassGIS.
"""
import geopandas as gpd
import pandas as pd
import numpy as np
import os
from src.utilities.dataframe_utilities import reduce_mem_usage

INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/AllParcelData_SHP_20220810"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
town_folders = os.listdir(INPUT_DATA)

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
    df = gpd.read_file(os.path.join(INPUT_DATA, town_folder, assessor_filename))
    df.loc[:, 'CY'] = int(calendar_year)  # save calendar year as its own column
    df.loc[:, 'TOWN_NAME'] = town_name
    df = df.drop(columns='geometry')  # drop the geomtry columnn from the assessor database

    df, columns_containing_nans = reduce_mem_usage(df)

    # Convert object type columns to string.
    object_type_columns = df.columns[df.dtypes == "object"].tolist()
    df.loc[:, object_type_columns] = df[object_type_columns].astype("string")

    dfs.append(df)

# concatenate municipality-year level DataFrames.
assessor_data = pd.concat(dfs, axis=0)

# save to CSV
assessor_data.to_csv(OUTPUT_DATA, index=False)
