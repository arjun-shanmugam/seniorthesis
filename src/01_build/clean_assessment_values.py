"""
clean_assessment_values.py

Cleans property assessment values from MassGIS.
"""
import geopandas as gpd
import pandas as pd
import os
INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/AllParcelData_SHP_20220810"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.dta"
town_folders = os.listdir(INPUT_DATA)

dataframes = []
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
    df.loc[:, 'calendar_year'] = int(calendar_year)
    df.loc[:, 'fiscal_year'] = int(fiscal_year)
    df.loc[:, 'town_name'] = town_name
    dataframes.append(df)

# concatenate municipality-year level DataFrames.
assessor_data = pd.concat(dataframes, axis=0)

# save to CSV
assessor_data.to_csv(OUTPUT_DATA)