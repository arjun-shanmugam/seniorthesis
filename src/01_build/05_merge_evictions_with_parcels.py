"""
05_merge_evictions_with_parcels.py

Merge eviction data with assessment values.
"""
import os.path
from typing import List

import matplotlib
import numpy as np
import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt
from build_utilities import sjoin_tax_parcels_with_evictions, merge_evictions_with_tax_parcels

matplotlib.rcParams['savefig.dpi'] = 500
from shapely.geometry import Point
import contextily as cx

if __name__ == '__main__':
    INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
    INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
    INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
    INPUT_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
    INTERMEDIATE_DATA_TAX_PARCELS_TO_GEOCODE = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/batched_tax_parcels_to_geocode"
    OUTPUT_FIGURES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/summary/figures"
    OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_merged_with_tax_parcels.csv"

    # Load evictions data.
    evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
    geometry = evictions_df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    evictions_gdf = gpd.GeoDataFrame(evictions_df, geometry=geometry)
    evictions_gdf = evictions_gdf.set_crs("EPSG:4326")  # crs attribute is currently unassigned.
    evictions_gdf = evictions_gdf.to_crs("EPSG:26986")


    # Load tax parcel files.
    tax_parcels_gdf = pd.concat([gpd.read_file(path_to_tax_parcel_east_gdf)[tax_parcel_columns_to_keep],
                                 gpd.read_file(path_to_tax_parcel_west_gdf)[tax_parcel_columns_to_keep]], axis=0)


    
    tax_parcels_gdf = merge_evictions_with_tax_parcels(INPUT_DATA_EVICTIONS,
                                                       INPUT_DATA_TAX_PARCELS_EAST,
                                                       INPUT_DATA_TAX_PARCELS_WEST,
                                                       INTERMEDIATE_DATA_TAX_PARCELS_TO_GEOCODE)

    tax_parcels_gdf.loc[:, 'file_date'] = pd.to_datetime(tax_parcels_gdf['file_date'])
    tax_parcels_gdf.loc[:, 'merge_year'] = tax_parcels_gdf['file_date'].dt.year

    assessment_values_df = pd.read_csv(INPUT_ASSESSMENT_VALUES)
    assessment_values_df.loc[:, 'BLDG_VAL'] = assessment_values_df['BLDG_VAL'].replace({"SAUGUS": 0}).astype(float)
    assessment_values_df.loc[:, 'LAND_VAL'] = assessment_values_df['LAND_VAL'].replace({"MA": 0}).astype(float)
    columns_to_keep = ['TOTAL_VAL', 'BLDG_VAL', 'OTHER_VAL', 'BLD_AREA', 'UNITS', 'LAND_VAL']
    assessment_values_df = assessment_values_df.groupby(by=['LOC_ID', 'FY']).sum()[columns_to_keep].reset_index()
    assessment_values_df.loc[:, 'merge_year'] = assessment_values_df['FY'] - 2

    output = assessment_values_df.merge(tax_parcels_gdf,
                                        how='inner',
                                        on=['LOC_ID'],
                                        validate='m:1')
    output.to_csv("~/Desktop/test1.csv")

    """
    # Load Western MA shapefiles.
    western_gdf = gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)
    western_gdf.loc[:, 'geometry'] = western_gdf['geometry'].buffer(0)
    # Drop unneeded columns.
    columns_to_drop = ['POLY_TYPE', 'MAP_NO', 'SOURCE', 'PLAN_ID', 'LAST_EDIT', 'BND_CHK', 'NO_MATCH', 'TOWN_ID', 'PROP_ID', 'BLDG_VAL',
                       'LAND_VAL', 'OTHER_VAL', 'TOTAL_VAL', 'FY', 'LOT_SIZE', 'LS_DATE', 'LS_PRICE', 'USE_CODE', 'ADDR_NUM',
                       'FULL_STR', 'LOCATION', 'CITY', 'OWNER1', 'OWN_ADDR', 'OWN_CITY', 'OWN_STATE', 'OWN_ZIP', 'OWN_CO', 'LS_BOOK',
                       'LS_PAGE', 'REG_ID', 'ZONING', 'YEAR_BUILT', 'BLD_AREA', 'UNITS', 'RES_AREA', 'STYLE', 'NUM_ROOMS', 'LOT_UNITS',
                       'STORIES']
    western_gdf = western_gdf.drop(columns=columns_to_drop)
    # Drop rows without a LOC_ID
    has_LOC_ID_mask = ~(western_gdf['LOC_ID'].isna() | western_gdf['LOC_ID'] == "")
    western_gdf = western_gdf.loc[has_LOC_ID_mask, :]
    # Drop rows where SITE_ADDR or ZIP are missing
    has_SITE_ADDR = ~(western_gdf['SITE_ADDR'].isna() | western_gdf['SITE_ADDR'] == "")
    has_ZIP = ~(western_gdf['ZIP'].isna() | western_gdf['ZIP'] == "")
    western_gdf = western_gdf.loc[has_SITE_ADDR & has_ZIP, :]
    # Dissolve so that every polygon has a single row.
    western_gdf = western_gdf.dissolve(by=['MAP_PAR_ID'])
    western_gdf = western_gdf.to_crs(evictions_gdf.crs)
    print("Joining shapefile with eviction data.")
    western_gdf = evictions_gdf.sjoin(western_gdf, how='inner', predicate='within')
    
    # Load Eastern MA shapefiles and join.
    eastern_gdf = gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)
    # Drop unneeded columns.
    eastern_gdf = eastern_gdf.drop(columns=columns_to_drop)
    # Drop rows without a LOC_ID
    has_LOC_ID_mask = ~(eastern_gdf['LOC_ID'].isna() | eastern_gdf['LOC_ID'] == "")
    eastern_gdf = eastern_gdf.loc[has_LOC_ID_mask, :]
    # Drop rows where SITE_ADDR or ZIP are missing
    has_SITE_ADDR = ~(eastern_gdf['SITE_ADDR'].isna() | eastern_gdf['SITE_ADDR'] == "")
    has_ZIP = ~(eastern_gdf['ZIP'].isna() | eastern_gdf['ZIP'] == "")
    eastern_gdf = eastern_gdf.loc[has_SITE_ADDR & has_ZIP, :]
    # Dissolve so that every polygon has a single row.
    eastern_gdf.loc[:, 'geometry'] = evictions_gdf['geometry'].buffer(0)
    eastern_gdf = eastern_gdf.dissolve(by=['MAP_PAR_ID'])
    eastern_gdf = eastern_gdf.to_crs(evictions_gdf.crs)
    print("Joining shapefile with eviction data.")
    eastern_gdf = evictions_gdf.sjoin(eastern_gdf, how='inner', predicate='within')
    
    # Combine Western and Eastern files.
    df = pd.concat([western_gdf, eastern_gdf], axis=0)
    columns_to_drop = ['SHAPE_Leng', 'SHAPE_Area', 'geometry', 'index_right']
    df = df.drop(columns=columns_to_drop)
    
    # Save data.
    df.to_csv(OUTPUT_DATA)
    """
