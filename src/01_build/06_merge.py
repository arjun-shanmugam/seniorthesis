"""
06_merge.py

Merge eviction data with assessment values.
"""
if __name__ == '__main__':
    import pandas as pd
    import geopandas as gpd
    from build_utilities import geocode_coordinates

    INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
    INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
    INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
    INPUT_DATA_TRACTS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/tracts.csv"
    INPUT_DATA_ZESTIMATES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"
    INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_values.csv"
    OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/unrestricted.csv"
    OUTPUT_DATA_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/restricted.csv"
    VERBOSE = True

    # Load evictions data.
    evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
    evictions_gdf = gpd.GeoDataFrame(evictions_df,
                                     geometry=gpd.points_from_xy(evictions_df['longitude'], evictions_df['latitude']))
    evictions_gdf = evictions_gdf.set_crs("EPSG:4326")  # crs attribute is currently unassigned.

    # Load tax parcel files.
    tax_parcel_columns_to_keep = ['MAP_PAR_ID', 'LOC_ID', 'POLY_TYPE', 'SITE_ADDR', 'CITY', 'ZIP', 'SHAPE_Leng',
                                  'SHAPE_Area',
                                  'geometry']
    tax_parcels_gdf = pd.concat([gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)[tax_parcel_columns_to_keep],
                                 gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)[tax_parcel_columns_to_keep]], axis=0)
    tax_parcels_gdf = tax_parcels_gdf.to_crs("EPSG:4326")
    tax_parcels_gdf = tax_parcels_gdf.dissolve(by='LOC_ID').reset_index()

    # Spatial join.
    spatially_joined_columns_to_drop = ['index_right', 'geometry', 'MAP_PAR_ID', 'POLY_TYPE', 'SITE_ADDR', 'CITY', 'ZIP',
                                        'SHAPE_Leng', 'SHAPE_Area']
    tax_parcels_gdf = gpd.sjoin(evictions_gdf, tax_parcels_gdf, how='left', predicate='within').drop(
        columns=spatially_joined_columns_to_drop)
    if VERBOSE:
        successfully_matched_observations = (~tax_parcels_gdf['LOC_ID'].isna()).sum()
        print(f"Successfully matched {successfully_matched_observations} evictions "
              f"({successfully_matched_observations / len(evictions_gdf)} percent of observations) to parcels.")

    tax_parcels_gdf.loc[:, 'file_month'] = pd.to_datetime(tax_parcels_gdf['file_date']).dt.strftime('%Y-%m')
    tax_parcels_gdf.loc[:, 'file_year'] = pd.to_datetime(tax_parcels_gdf['file_date']).dt.year
    tax_parcels_gdf = tax_parcels_gdf.sort_values(['file_year'])
    merged_df = pd.merge_asof(tax_parcels_gdf, pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES).sort_values('FY'),
                              by=['LOC_ID'],
                              left_on=['file_year'],
                              right_on=['FY'],
                              direction='backward',
                              # Drop one row which matched to two property parcels to eliminate duplicates.
                              ).drop(index=13373)
    if VERBOSE:
        successfully_matched_observations = (~merged_df['FY'].isna()).sum()
        print(f"Successfully matched {successfully_matched_observations} evictions "
              f"({100 * (successfully_matched_observations / len(evictions_gdf))} percent of observations) to assessment"
              f" records from a recent fiscal year.")

    # Merge with Zillow data.
    zestimates_df = pd.read_csv(INPUT_DATA_ZESTIMATES)
    merged_df.to_csv("~/Desktop/duplicates_check.csv")
    merged_df = zestimates_df.merge(merged_df, on='case_number', how='right', validate='1:1')
    if VERBOSE:
        successfully_matched_observations = (~merged_df['2022-12'].isna()).sum()
        print(
            f"Successfully matched {successfully_matched_observations} evictions ({100 * (successfully_matched_observations / len(evictions_gdf))} "
            f"percent of observations) to Zestimates.")

    original_N = len(merged_df)

    # Drop cases missing file_date.
    mask = merged_df['file_date'].notna()
    if VERBOSE:
        print(
            f"Dropping {(~mask).sum()} observations where file_date is missing ({100 * (((~mask).sum()) / original_N):.3} percent "
            f"of original dataset).")
    merged_df = merged_df.loc[mask, :]

    # Clean the values in the judgment_for_pdu variable.
    judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                         "plaintiff": "Plaintiff",
                                         "defendant": "Defendant"}
    merged_df.loc[:, "judgment_for_pdu"] = merged_df.loc[:, "judgment_for_pdu"].replace(judgment_for_pdu_replacement_dict)

    # Replace missing values in money judgment column with zeroes.
    merged_df.loc[:, 'judgment'] = merged_df['judgment'].fillna(0)

    # Rename duration to case_duration.
    merged_df = merged_df.rename(columns={'duration': 'case_duration'})

    # Drop malformed addresses.
    merged_df = merged_df.loc[~merged_df['property_address_full'].str.contains("span, span span"), :]

    # Drop addresses without ZIP codes.
    merged_df = merged_df.loc[merged_df['property_address_zip'].notna(), :]

    result = geocode_coordinates(merged_df.index, merged_df['latitude'], merged_df['longitude'], n_jobs=-1)
    merged_df = pd.concat([merged_df, result], axis=1)
    merged_df = merged_df.merge(pd.read_csv(INPUT_DATA_TRACTS, dtype={'tract_geoid': str}),
                                on='tract_geoid',
                                how='left',
                                validate='m:1')

    # Save unrestricted data file.
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
    print(
        f"Dropping {(~mask).sum()} observations resolved through voluntary dismissal ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
    merged_df = merged_df.loc[mask, :]

    # Drop cases where disposition_found is "Other".
    mask = ~(merged_df['disposition_found'] == "Other")
    if VERBOSE:
        print(
            f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100 * (((~mask).sum()) / original_N):.3} "
            f"percent of original dataset).")
    merged_df = merged_df.loc[mask, :]

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

    # Drop rows for which we are missing any Zestimates.
    years = [str(year) for year in range(2013, 2023)]
    months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
    value_vars = ["2012-12"] + [str(year) + "-" + str(month) for year in years for month in months]
    has_all_zestimates_mask = merged_df[value_vars].notna().all(axis=1)
    print(f"Limiting sample to {has_all_zestimates_mask.sum()} evictions for which we observe Zestimates at every month "
          f"from 2012-12 to 2022-12.")
    merged_df = merged_df.loc[has_all_zestimates_mask, :]

    # Save restricted eviction data.
    merged_df.to_csv(OUTPUT_DATA_RESTRICTED, index=False)
