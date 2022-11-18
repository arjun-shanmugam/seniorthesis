"""
01_clean_assessment_values.py

Cleans property assessment values from MassGIS.
"""
import geopandas as gpd
import pandas as pd
import os
from build_utilities import reduce_mem_usage

INPUT_DATA_ASSESSOR = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/AllParcelData_SHP_20220810"
INPUT_DATA_ZIPCODES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zipcodes.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data2.csv"
town_folders = os.listdir(INPUT_DATA_ASSESSOR)

dfs = []
for town_folder in town_folders:
    if town_folder == '.DS_Store':  # ignore folder metadata
        continue
    data = town_folder.split("_")
    town_id = data[2]
    fiscal_year = data[5][2:]
    assessor_filename = town_id + "Assess" + "_" + data[4] + "_" + data[5] + ".dbf"
    df = gpd.read_file(os.path.join(INPUT_DATA_ASSESSOR, town_folder, assessor_filename))
    df = df.drop(columns=['geometry', 'OWNER1', 'OWN_ADDR', 'OWN_CITY', 'OWN_STATE', 'OWN_ZIP', 'OWN_CO', 'BLD_AREA', 'STYLE'])
    df, columns_containing_nans = reduce_mem_usage(df)

    # Convert object type columns to string.
    object_type_columns = df.columns[df.dtypes == "object"].tolist()
    df.loc[:, object_type_columns] = df[object_type_columns].astype("string")
    dfs.append(df)

assessor_data = pd.concat(dfs, axis=0)

# Select rows where fiscal year is malformed or doesn't make sense.
missing_fy_mask = ((assessor_data['FY'] == 0) | (assessor_data['FY'] == 19180))
# Drop those rows from the data.
assessor_data = assessor_data.loc[~missing_fy_mask, :]
print(f"Dropping {missing_fy_mask.sum()} rows where FY is malformed or doesn't make sense ({missing_fy_mask.sum() / len(assessor_data)}"
      f"percent of original dataset).")

# Drop rows where SITE_ADDR = NaN
missing_site_addr_mask = ((assessor_data['SITE_ADDR'].isna()) | (assessor_data['SITE_ADDR'] == ""))
assessor_data = assessor_data.loc[~missing_site_addr_mask, :]
print(f"Dropping {missing_site_addr_mask.sum()} rows where SITE_ADDR is missing ({missing_site_addr_mask.sum() / len(assessor_data)}"
      f"percent of original dataset).")

# Drop rows where LOC_ID is missing.
missing_LOC_ID_mask = ((assessor_data['LOC_ID'].isna()) | (assessor_data['LOC_ID'] == ""))
assessor_data = assessor_data.loc[~missing_LOC_ID_mask, :]
print(f"Dropping {missing_LOC_ID_mask.sum()} rows where LOC_ID is missing ({missing_LOC_ID_mask.sum() / len(assessor_data)}"
      f"percent of original dataset).")

# TODO: Aggregate according to LOC_ID

# save to CSV
assessor_data.to_csv(OUTPUT_DATA, index=False)
