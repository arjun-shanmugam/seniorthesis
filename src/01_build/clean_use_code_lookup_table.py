"""
clean_use_code_lookup_table.py

Cleans the use code lookup table from MassGIS's statewide tax parcel geodatabase.
"""
import os

import pandas as pd
import geopandas as gpd

TAX_PARCELS_GDB = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/MassGIS_L3_Parcels.gdb"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate"

gdf = gpd.read_file(TAX_PARCELS_GDB, layer='L3_UC_LUT')  # Read the use code table from the GDB.

print(gdf.head())

gdf.to_csv(os.path.join(OUTPUT_DATA, 'use_code_lookup_table.csv'))