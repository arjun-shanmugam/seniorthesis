"""
03_merge_cross_section.py

Merge eviction data with assessment values, producing a panel dataset..
"""
import pandas as pd
import geopandas as gpd


INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_values.csv"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/panel_unrestricted.csv"
OUTPUT_DATA_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/panel_restricted.csv"
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

tax_parcels_gdf.loc[:, 'file_year'] = pd.to_datetime(tax_parcels_gdf['file_date']).dt.year
tax_parcels_gdf.loc[:, 'next_fiscal_year'] = tax_parcels_gdf['file_year'] + 2
merged_df = tax_parcels_gdf.merge(pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES),
                                  how='inner',
                                  right_on=['LOC_ID'],
                                  left_on=['LOC_ID'],
                                  validate='m:m')  # Note: this is not producing any kind of validation.
print(f"Successfully matched {len(merged_df['case_number'].value_counts())} observations to assessment records ({100 * (len(merged_df) / len(tax_parcels_gdf))} "
      f"percent of observations).")
merged_df.to_csv(OUTPUT_DATA_UNRESTRICTED)

original_N = len(merged_df)
# Restrict to cases where court_person_type is 'judge'
mask = (merged_df['court_person_type'] == 'judge')
print(
    f"Dropping {(~mask).sum()} observations where court_person_type is not \'judge\' ({100 * ((~mask).sum() / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases which were resolved via mediation.
mask = merged_df['disposition_found'] != "Mediated"
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Mediated\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
mask = ~(merged_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", regex=False))
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
merged_df.to_csv(OUTPUT_DATA_RESTRICTED)