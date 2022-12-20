"""
06_merge.py

Merge eviction data with assessment values.
"""
import pandas as pd
import geopandas as gpd


INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
INPUT_DATA_ZESTIMATES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"
INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_values.csv"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/unrestricted.csv"
OUTPUT_DATA_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/restricted.csv"
VERBOSE = True

# Load evictions data.
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
evictions_gdf = gpd.GeoDataFrame(evictions_df, geometry=gpd.points_from_xy(evictions_df['longitude'], evictions_df['latitude']))
evictions_gdf = evictions_gdf.set_crs("EPSG:4326")  # crs attribute is currently unassigned.

# Load tax parcel files.
tax_parcel_columns_to_keep = ['MAP_PAR_ID', 'LOC_ID', 'POLY_TYPE', 'SITE_ADDR', 'CITY', 'ZIP', 'SHAPE_Leng', 'SHAPE_Area',
                              'geometry']
tax_parcels_gdf = pd.concat([gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)[tax_parcel_columns_to_keep],
                             gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)[tax_parcel_columns_to_keep]], axis=0)
tax_parcels_gdf = tax_parcels_gdf.to_crs("EPSG:4326")
tax_parcels_gdf = tax_parcels_gdf.dissolve(by='LOC_ID').reset_index()

# Spatial join.
spatially_joined_columns_to_drop = ['index_right', 'geometry', 'MAP_PAR_ID', 'POLY_TYPE', 'SITE_ADDR', 'CITY', 'ZIP', 'SHAPE_Leng', 'SHAPE_Area']
tax_parcels_gdf = gpd.sjoin(evictions_gdf, tax_parcels_gdf, how='left', predicate='within').drop(columns=spatially_joined_columns_to_drop)
if VERBOSE:
    successfully_matched_observations = (~tax_parcels_gdf['LOC_ID'].isna()).sum()
    print(f"Successfully matched {successfully_matched_observations} evictions ({successfully_matched_observations / len(evictions_gdf)}"
          f" percent of observations) to parcels.")

tax_parcels_gdf.loc[:, 'file_month'] = pd.to_datetime(tax_parcels_gdf['file_date']).dt.month
tax_parcels_gdf.loc[:, 'file_year'] = pd.to_datetime(tax_parcels_gdf['file_date']).dt.year
tax_parcels_gdf.loc[:, 'latest_docket_year'] = pd.to_datetime(tax_parcels_gdf['latest_docket_date']).dt.year
tax_parcels_gdf.loc[:, 'latest_docket_month'] = pd.to_datetime(tax_parcels_gdf['latest_docket_date']).dt.month
tax_parcels_gdf.loc[:, 'next_fiscal_year'] = tax_parcels_gdf['latest_docket_year'] + 2
merged_df = tax_parcels_gdf.merge(pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES),
                                  how='left',
                                  right_on=['LOC_ID', 'FY'],
                                  left_on=['LOC_ID', 'next_fiscal_year'],
                                  validate='m:1').drop(index=29326)  # Drop one row which matched to two property parcels to eliminate duplicates.

# Merge with Zillow data.
zestimates_df = pd.read_csv(INPUT_DATA_ZESTIMATES)
for i, column in enumerate(zestimates_df.columns):
    if column == 'case_number':
        continue
    else:
        zestimates_df = zestimates_df.rename(columns={column: f'zestimate_period{str(i-1)}'})
merged_df = zestimates_df.merge(merged_df, on='case_number', how='right', validate='1:1')
merged_df.to_csv(OUTPUT_DATA_UNRESTRICTED, index=False)

original_N = len(merged_df)
# Drop cases which were resolved via mediation.
mask = merged_df['disposition_found'] != "Mediated"
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Mediated\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
mask = ~(merged_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", na=False, regex=False))
print(
    f"Dropping {(~mask).sum()} observations resolved through voluntary dismissal ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases where disposition_found is "Other".
mask = ~(merged_df['disposition_found'] == "Other")
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
merged_df.loc[:, "judgment_for_pdu"] = merged_df.loc[:, "judgment_for_pdu"].replace(judgment_for_pdu_replacement_dict)

# Drop rows which contain inconsistent values of disposition_found and judgment_for_pdu
# Case listed as a default, yet defendant listed as winning.
mask = ~((merged_df['disposition_found'] == "Defaulted") & (merged_df['judgment_for_pdu'] == "Defendant"))
print(
    f"Dropping {(~mask).sum()} observations disposition_found is \'Defaulted\' but judgment_for_pdu is \'Defendant\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]
# Case listed as dismissed, yet plaintiff listed as having won.
mask = ~((merged_df['disposition_found'] == "Dismissed") & (merged_df['judgment_for_pdu'] == "Plaintiff"))
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Dismissed\' but judgment_for_pdu is \'Plaintiff\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")

merged_df = merged_df.loc[mask, :]

# Generate a variable indicating judgement in favor of defendant.
merged_df.loc[:, 'judgment_for_defendant'] = 0
mask = (merged_df['disposition_found'] == "Dismissed") | (merged_df['judgment_for_pdu'] == "Defendant")
merged_df.loc[mask, 'judgment_for_defendant'] = 1

# Generate a variable indicating judgement in favor of plaintiff.
merged_df.loc[:, 'judgment_for_plaintiff'] = 1 - merged_df['judgment_for_defendant']
# Save restricted eviction data.
merged_df.to_csv(OUTPUT_DATA_RESTRICTED, index=False)
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
