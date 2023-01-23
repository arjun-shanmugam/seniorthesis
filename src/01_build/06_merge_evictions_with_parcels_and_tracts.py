"""
06_merge_evictions_with_parcels_and_tracts.py
"""

import pandas as pd
import geopandas as gpd
import os
os.environ['USE_PYGEOS'] = '0'

INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
INPUT_DATA_TRACTS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/tracts.csv"
INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_parcels_and_tracts.csv"

VERBOSE = True

# Load evictions data.
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
evictions_df.loc[:, 'file_month'] = pd.to_datetime(evictions_df['file_date']).dt.strftime('%Y-%m')
evictions_df.loc[:, 'file_year'] = pd.to_datetime(evictions_df['file_date']).dt.year
original_N = len(evictions_df)

# Drop cases missing file_date.
mask = evictions_df['file_date'].notna()
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations where file_date is missing ({100 * (((~mask).sum()) / original_N):.3} percent "
        f"of original dataset).")
evictions_df = evictions_df.loc[mask, :]

# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
evictions_df.loc[:, "judgment_for_pdu"] = evictions_df.loc[:, "judgment_for_pdu"].replace(
    judgment_for_pdu_replacement_dict)

# Replace missing values in money judgment column with zeroes.
evictions_df.loc[:, 'judgment'] = evictions_df['judgment'].fillna(0)

# Rename duration to case_duration.
evictions_df = evictions_df.rename(columns={'duration': 'case_duration'})

# Drop malformed addresses.
evictions_df = evictions_df.loc[~evictions_df['property_address_full'].str.contains("span, span span"), :]

# Drop addresses without ZIP codes.
evictions_df = evictions_df.loc[evictions_df['property_address_zip'].notna(), :]

# Merge with census tract characteristics.
evictions_df = evictions_df.rename(columns={'Full FIPS (tract)': 'tract_geoid'})
evictions_df = evictions_df.merge(pd.read_csv(INPUT_DATA_TRACTS, dtype={'tract_geoid': float}),
                                  on='tract_geoid',
                                  how='left',
                                  validate='m:1')

# Merge with tax parcels.
evictions_df = gpd.GeoDataFrame(evictions_df,
                             geometry=gpd.points_from_xy(evictions_df['longitude'], evictions_df['latitude']))
evictions_df = evictions_df.set_crs("EPSG:4326")  # CRS attribute is not set initially set.
tax_parcel_columns_to_keep = ['MAP_PAR_ID', 'LOC_ID', 'POLY_TYPE', 'SITE_ADDR', 'SHAPE_Leng',
                              'SHAPE_Area',
                              'geometry']
tax_parcels_gdf = pd.concat([gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)[tax_parcel_columns_to_keep],
                             gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)[tax_parcel_columns_to_keep]], axis=0)
tax_parcels_gdf = tax_parcels_gdf.to_crs("EPSG:4326")
tax_parcels_gdf = tax_parcels_gdf.dissolve(by='LOC_ID').reset_index()
spatially_joined_columns_to_drop = ['index_right', 'geometry', 'MAP_PAR_ID', 'POLY_TYPE', 'SITE_ADDR',
                                    'SHAPE_Leng', 'SHAPE_Area']

merged_df = (gpd
             .sjoin(evictions_df, tax_parcels_gdf, how='left', predicate='within')
             .drop(columns=spatially_joined_columns_to_drop))
if VERBOSE:
    successfully_matched_observations = merged_df['LOC_ID'].notna().sum()
    print(f"Successfully matched {successfully_matched_observations} evictions "
          f"({100 * (successfully_matched_observations / original_N):.2f} percent of observations) to parcels.")

# Save unrestricted data file.
columns_to_drop = ['Accuracy Score', 'Accuracy Type', 'Number', 'Street', 'Unit Type', 'Unit Number',
                   'State', 'Zip', 'Country', 'Source', 'Census Year', 'State FIPS', 'County FIPS',
                   'Place Name', 'Place FIPS', 'Census Tract Code', 'Census Block Code', 'Census Block Group',
                   'Metro/Micro Statistical Area Code', 'Metro/Micro Statistical Area Type',
                   'Combined Statistical Area Code', 'Metropolitan Division Area Code']
merged_df.drop(columns=columns_to_drop).to_csv(OUTPUT_DATA_UNRESTRICTED, index=False)