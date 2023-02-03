"""
06_merge_evictions_with_parcels_and_tracts.py
"""

import pandas as pd
import os

os.environ['USE_PYGEOS'] = '0'  # Use Shapely vectorizations of GIS functions instead of PyGEOS.
import geopandas as gpd

INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
INPUT_DATA_TRACTS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/tracts.csv"
INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_parcels_and_tracts.csv"

VERBOSE = True

# Load evictions data.
with open(INPUT_DATA_EVICTIONS, 'r') as file:
    all_column_names = set(file.readline().replace("\"", "").replace("\n", "").split(","))
to_drop = {'Accuracy Score', 'Accuracy Type', 'Number', 'Street', 'Unit Type', 'Unit Number',
           'State', 'Zip', 'Country', 'Source', 'Census Year', 'State FIPS', 'County FIPS',
           'Place Name', 'Place FIPS', 'Census Tract Code', 'Census Block Code', 'Census Block Group',
           'Metro/Micro Statistical Area Code', 'Metro/Micro Statistical Area Type',
           'Combined Statistical Area Code', 'Metropolitan Division Area Code', 'court_location',
           'defendant', 'defendant_atty', 'defendant_atty_address_apt',
           'defendant_atty_address_city', 'defendant_atty_address_name', 'defendant_atty_address_state',
           'defendant_atty_address_street', 'defendant_atty_address_zip', 'docket_history', 'execution', 'judgment_for',
           'judgment_total', 'latest_docket_date', 'plaintiff', 'plaintiff_atty', 'plaintiff_atty_address_apt',
           'plaintiff_atty_address_city', 'plaintiff_atty_address_name', 'plaintiff_atty_address_state',
           'plaintiff_atty_address_street', 'plaintiff_atty_address_zip', 'Metropolitan Division Area Name',
           'property_address_city', 'property_address_state', 'property_address_street',
           'property_address_zip'}
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS, usecols=set(all_column_names) - set(to_drop))
# evictions_df = evictions_df.rename(columns={'Latitude': 'latitude', 'Longitude': 'longitude'})
original_N = len(evictions_df)
if VERBOSE:
    print(f"Beginning with {original_N} observations.")

# Drop cases missing file_date.
mask = evictions_df['file_date'].notna()
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations where file_date is missing ({100 * (((~mask).sum()) / original_N):.3} percent "
        f"of original dataset).")
evictions_df = evictions_df.loc[mask, :]

# Add file month and year to dataset.
evictions_df.loc[:, 'file_month'] = pd.to_datetime(evictions_df['file_date']).dt.strftime('%Y-%m')
evictions_df.loc[:, 'file_year'] = pd.to_datetime(evictions_df['file_date']).dt.year

# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
evictions_df.loc[:, "judgment_for_pdu"] = (evictions_df.loc[:, "judgment_for_pdu"]
                                           .replace(judgment_for_pdu_replacement_dict))

# Replace missing values in money judgment column with zeroes.
evictions_df.loc[:, 'judgment'] = evictions_df['judgment'].fillna(0)

# Rename duration to case_duration.
evictions_df = evictions_df.rename(columns={'duration': 'case_duration'})

# Drop malformed addresses.
if VERBOSE:
    print(f"Dropping {evictions_df['property_address_full'].str.contains('span, span span').sum()} observations which "
          f"have malformed addresses "
          f"({evictions_df['property_address_full'].str.contains('span, span span').sum() / original_N:.2f} "
          f"percent of observations).")
evictions_df = evictions_df.loc[~evictions_df['property_address_full'].str.contains("span, span span"), :]

# Drop addresses without latitude and longitude.
if VERBOSE:
    print(f"Dropping {evictions_df[['longitude', 'latitude']].isna().any(axis=1).sum()} evictions missing latitude "
          f"or longitude ({evictions_df[['longitude', 'latitude']].isna().any(axis=1).sum() / original_N:.2f}) "
          f"percent of observations.")
evictions_df = evictions_df.dropna(subset=['longitude', 'latitude'])

# Merge with census tract characteristics.
evictions_df = evictions_df.rename(columns={'Full FIPS (tract)': 'tract_geoid'})
evictions_df = evictions_df.merge(pd.read_csv(INPUT_DATA_TRACTS, dtype={'tract_geoid': float}),
                                  on='tract_geoid',
                                  how='left',
                                  validate='m:1')
if VERBOSE:
    print(f"Successfully merged {evictions_df['med_hhinc2016'].notna().sum()} observations "
          f"({evictions_df['med_hhinc2016'].notna().sum() / original_N:.2f} percent of observations) with census "
          f"tracts.")

# Merge with tax parcels.
evictions_df = gpd.GeoDataFrame(evictions_df,
                                geometry=gpd.points_from_xy(evictions_df['longitude'], evictions_df['latitude']))
evictions_df = evictions_df.set_crs("EPSG:4326")  # CRS attribute is not set initially set.

# TODO: Read tax_parcels_gdf
tax_parcels_gdf =

merged_df = (gpd
             .sjoin(evictions_df, tax_parcels_gdf, how='left', predicate='within')
             .drop(columns=spatially_joined_columns_to_drop)
             .drop(index=29305))  # Drop the one eviction which erroneously merges to two parcels.
if VERBOSE:
    successfully_matched_observations = merged_df['LOC_ID'].notna().sum()
    print(f"Successfully matched {successfully_matched_observations} evictions "
          f"({100 * (successfully_matched_observations / original_N):.2f} percent of observations) to parcels.")
merged_df.loc[:, 'LOC_ID'] = merged_df['LOC_ID'].fillna('UNMATCHED')


# Save unrestricted data file.
merged_df.to_csv(OUTPUT_DATA_UNRESTRICTED, index=False)
