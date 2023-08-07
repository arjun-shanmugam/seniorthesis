"""
02_clean_tax_parcels.py
"""
import pandas as pd
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
INPUT_DATA_TAX_PARCELS_EAST = "../../data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
INPUT_DATA_TAX_PARCELS_WEST = "../../data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
OUTPUT_DATA_TAX_PARCELS = "../../data/02_intermediate/tax_parcels.gpkg"

tax_parcel_columns_to_keep = ['LOC_ID', 'geometry']
tax_parcels_gdf = pd.concat([gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)[tax_parcel_columns_to_keep],
                             gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)[tax_parcel_columns_to_keep]], axis=0)
tax_parcels_gdf = tax_parcels_gdf.dissolve(by='LOC_ID')
tax_parcels_gdf.to_file(OUTPUT_DATA_TAX_PARCELS, driver='GPKG', layer='layer')
