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
if VERBOSE:
    successfully_matched_observations = (~merged_df['FY'].isna()).sum()
    print(f"Successfully matched {successfully_matched_observations} evictions ({100 * (successfully_matched_observations / len(evictions_gdf))} "
          f"percent of observations) to assessment records from the appropriate fiscal year.")

# Merge with Zillow data.
zestimates_df = pd.read_csv(INPUT_DATA_ZESTIMATES)
merged_df = zestimates_df.merge(merged_df, on='case_number', how='right', validate='1:1')
if VERBOSE:
    successfully_matched_observations = (~merged_df['2022-12'].isna()).sum()
    print(f"Successfully matched {successfully_matched_observations} evictions ({100 * (successfully_matched_observations / len(evictions_gdf))} "
          f"percent of observations) to Zestimates.")

original_N = len(merged_df)
mask = merged_df['latest_docket_date'].notna()
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations where latest_docket_date is missing ({100 * (((~mask).sum()) / original_N):.3} percent "
        f"of original dataset).")
merged_df = merged_df.loc[mask, :]
merged_df.to_csv(OUTPUT_DATA_UNRESTRICTED, index=False)

# Drop cases which were resolved via mediation.
mask = merged_df['disposition_found'] != "Mediated"
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations where disposition_found is \'Mediated\' ({100 * (((~mask).sum()) / original_N):.3} "
        f"percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
mask = ~(merged_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", na=False, regex=False))
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations resolved through voluntary dismissal ({100 * (((~mask).sum()) / original_N):.3} "
        f"percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases where disposition_found is "Other".
mask = ~(merged_df['disposition_found'] == "Other")
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100 * (((~mask).sum()) / original_N):.3} "
        f"percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
merged_df.loc[:, "judgment_for_pdu"] = merged_df.loc[:, "judgment_for_pdu"].replace(judgment_for_pdu_replacement_dict)

# Drop rows which contain inconsistent values of disposition_found and judgment_for_pdu
# Case listed as a default, yet defendant listed as winning.
mask = ~((merged_df['disposition_found'] == "Defaulted") & (merged_df['judgment_for_pdu'] == "Defendant"))
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations disposition_found is \'Defaulted\' but judgment_for_pdu is \'Defendant\' "
        f"({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Case listed as dismissed, yet plaintiff listed as having won.
mask = ~((merged_df['disposition_found'] == "Dismissed") & (merged_df['judgment_for_pdu'] == "Plaintiff"))
if VERBOSE:
    print(
        f"Dropping {(~mask).sum()} observations where disposition_found is \'Dismissed\' but judgment_for_pdu is "
        f"\'Plaintiff\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")

merged_df = merged_df.loc[mask, :]

# Generate a variable indicating judgement in favor of defendant.
merged_df.loc[:, 'judgment_for_defendant'] = 0
mask = (merged_df['disposition_found'] == "Dismissed") | (merged_df['judgment_for_pdu'] == "Defendant")
merged_df.loc[mask, 'judgment_for_defendant'] = 1

# Generate a variable indicating judgement in favor of plaintiff.
merged_df.loc[:, 'judgment_for_plaintiff'] = 1 - merged_df['judgment_for_defendant']
# Save restricted eviction data.
merged_df.to_csv(OUTPUT_DATA_RESTRICTED, index=False)
