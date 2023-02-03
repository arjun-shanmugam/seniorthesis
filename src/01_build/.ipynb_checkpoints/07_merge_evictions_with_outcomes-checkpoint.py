"""
07_merge_evictions_with_outcomes.py

Merge eviction data with assessment values.
"""

import os
import dask_geopandas
import geopandas as gpd
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster

if __name__ == '__main__':
    INPUT_DATA_EVICTIONS_PARCELS_AND_TRACTS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_parcels_and_tracts.csv"
    INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
    INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"
    INPUT_DATA_ZESTIMATES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"
    INPUT_DATA_CRIME = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/crime_incidents"
    OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/unrestricted.csv"
    OUTPUT_DATA_ZILLOW = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/zestimates_analysis.csv"

    VERBOSE = True
    client = Client(
        LocalCluster(
            n_workers=4,
            threads_per_worker=1
        )
    )

    # 1. Produce the unrestricted sample--------------------------------------------------------------------------------
    # 1a. Get number of crimes reported within each parcel.
    eviction_ddf = (dd.read_csv(INPUT_DATA_EVICTIONS_PARCELS_AND_TRACTS).set_index('LOC_ID'))
    tax_parcel_cols_to_keep = ['MAP_PAR_ID', 'LOC_ID', 'POLY_TYPE', 'SITE_ADDR', 'SHAPE_Leng',
                                  'SHAPE_Area',
                                  'geometry']
    eastern_dgdf = (dask_geopandas.from_geopandas(gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)[tax_parcel_cols_to_keep],
                                                  npartitions=4)
                    .dropna(subset='LOC_ID')
                    .dissolve(by='LOC_ID')
                    .compute())
    print("loaded eastern ")
    western_dgdf = (dask_geopandas.from_geopandas(gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)[tax_parcel_cols_to_keep],
                                                  npartitions=4)
                    .dropna(subset='LOC_ID')
                    .dissolve(by='LOC_ID')
                    .compute())
    tax_parcels_dgdf = dd.multi.concat([eastern_dgdf, western_dgdf], axis=0).compute()
    dgdf = tax_parcels_dgdf.merge(eviction_ddf, how='right', left_index=True, right_index=True).compute()
    breakpoint()
    eviction_gdf = dask_geopandas.from_geopandas(dgdf, npartitions=4)
    crime_gdf = (pd.concat([pd.read_csv(os.path.join(INPUT_DATA_CRIME, file)) for file in os.listdir(INPUT_DATA_CRIME)],
                           axis=0)  # Read crime data.
                 .dropna(subset=['Long', 'Lat', 'OCCURRED_ON_DATE'])
                 .rename(columns={'OCCURRED_ON_DATE': 'month_of_crime_incident'}))
    # Get month of each crime incident.
    crime_gdf.loc[:, 'month_of_crime_incident'] = (pd.to_datetime(crime_gdf['month_of_crime_incident'].str[:10])
                                                   .dt.to_period("M"))
    # Convert crime data to a GeoDataFrame.
    crime_gdf = (gpd
                 .GeoDataFrame(crime_gdf,
                               geometry=gpd.points_from_xy(crime_gdf['Long'], crime_gdf['Lat']))
                 .set_crs("EPSG:4326")
                 .to_crs("EPSG:26986")
                 .drop(columns=['Lat', 'Long', 'Location', 'UCR_PART', 'HOUR', 'DAY_OF_WEEK', 'YEAR', 'MONTH',
                               'REPORTING_AREA', 'DISTRICT', 'STREET', 'OFFENSE_CODE_GROUP', 'OFFENSE_DESCRIPTION']))
    crime_gdf = crime_gdf.iloc[:100]
    crime_gdf = dask_geopandas.from_geopandas(crime_gdf, npartitions=4)

    # Spatial join of crimes and evictions
    join = dask_geopandas.sjoin(crime_gdf,
                                eviction_gdf,
                                how='inner',
                                predicate='within')
    evictions_matched_with_crimes = join.compute()
    evictions_matched_with_crimes.to_csv('~/Desktop/parcel_join_results.csv')
    print(stophere)
    # TODO on this line: Aggregate and reshape so that each row to wide format so that gives a single eviction.
    # # Aggregate by case number and month of crime.
    # eviction_df = eviction_df.groupby(
    #     ['case_number', 'month_of_crime_incident', 'OFFENSE_CODE_GROUP']).agg(
    #     {eviction_df_columns: 'first',
    #      'INCIDENT_NUMBER': 'count',
    #      'SHOOTING': 'sum'})
    evictions_unmatched_with_crimes = eviction_gdf.loc[~eviction_gdf['case_number'].isin(evictions_matched_with_crimes['case_number']), :]
    eviction_gdf = pd.concat([evictions_matched_with_crimes, evictions_unmatched_with_crimes], axis=0)

    # Convert back to DataFrame, dropping the geometry column.
    eviction_df = pd.DataFrame(eviction_gdf.drop(columns='geometry'))

    # Merge with Zillow data------------------------------------------------------------------------------------------------
    merged_df = pd.read_csv(INPUT_DATA_ZESTIMATES).merge(eviction_df,
                                                         on='case_number',
                                                         how='right',
                                                         validate='1:1')
    if VERBOSE:
        successfully_matched_observations = (~merged_df['2022-12'].isna()).sum()
        print(
            f"Successfully matched {successfully_matched_observations} evictions "
            f"({100 * (successfully_matched_observations / len(merged_df)) :.2f} percent of observations) to "
            f"Zestimates.")

    # Rename columns containing Zestimates.
    years = [str(year) for year in range(2013, 2023)]
    months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
    value_vars = ["2012-12"] + [str(year) + "-" + str(month) for year in years for month in months]
    for value_var in value_vars:
        merged_df = merged_df.rename(columns={value_var: value_var + "_zestimate"})
    value_vars_new = [value_var + "_zestimate" for value_var in value_vars]
    value_vars = value_vars_new

    merged_df.to_csv(OUTPUT_DATA_UNRESTRICTED, index=False)

    # Generate the sample used in the Zestimates analysis-------------------------------------------------------------------
    # Drop cases which were resolved via mediation.
    zestimates_sample_df = merged_df.copy()
    mask = zestimates_sample_df['disposition_found'] != "Mediated"
    if VERBOSE:
        print(
            f"Dropping {(~mask).sum()} observations where disposition_found is \'Mediated\' ({100 * (((~mask).sum()) / len(merged_df)):.3} "
            f"percent of original dataset).")
    zestimates_sample_df = zestimates_sample_df.loc[mask, :]

    # Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
    mask = ~(
        zestimates_sample_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", na=False, regex=False))
    print(
        f"Dropping {(~mask).sum()} observations resolved through voluntary dismissal ({100 * (((~mask).sum()) / len(merged_df)):.3} percent of original dataset).")
    zestimates_sample_df = zestimates_sample_df.loc[mask, :]

    # Drop cases where disposition_found is "Other".
    mask = ~(zestimates_sample_df['disposition_found'] == "Other")
    if VERBOSE:
        print(
            f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' "
            f"({100 * (((~mask).sum()) / len(merged_df)):.3} "
            f"percent of original dataset).")
    zestimates_sample_df = zestimates_sample_df.loc[mask, :]

    # Drop rows which contain inconsistent values of disposition_found and judgment_for_pdu
    # Case listed as a default, yet defendant listed as winning.
    mask = ~((zestimates_sample_df['disposition_found'] == "Defaulted") &
             (zestimates_sample_df['judgment_for_pdu'] == "Defendant"))
    if VERBOSE:
        print(
            f"Dropping {(~mask).sum()} observations disposition_found is \'Defaulted\' but judgment_for_pdu is "
            f"\'Defendant\' ({100 * (((~mask).sum()) / len(merged_df)):.3} percent of original dataset).")
    zestimates_sample_df = zestimates_sample_df.loc[mask, :]
    # Case listed as dismissed, yet plaintiff listed as having won.
    mask = ~((zestimates_sample_df['disposition_found'] == "Dismissed") & (
            zestimates_sample_df['judgment_for_pdu'] == "Plaintiff"))
    if VERBOSE:
        print(
            f"Dropping {(~mask).sum()} observations where disposition_found is \'Dismissed\' but judgment_for_pdu is "
            f"\'Plaintiff\' ({100 * (((~mask).sum()) / len(merged_df)):.3} percent of original dataset).")
    zestimates_sample_df = zestimates_sample_df.loc[mask, :]

    # Generate a variable indicating judgement in favor of defendant.
    zestimates_sample_df.loc[:, 'judgment_for_defendant'] = 0
    mask = ((zestimates_sample_df['disposition_found'] == "Dismissed") |
            (zestimates_sample_df['judgment_for_pdu'] == "Defendant"))
    zestimates_sample_df.loc[mask, 'judgment_for_defendant'] = 1

    # Generate a variable indicating judgement in favor of plaintiff.
    zestimates_sample_df.loc[:, 'judgment_for_plaintiff'] = 1 - zestimates_sample_df['judgment_for_defendant']

    # Drop rows for which we are missing any Zestimates.
    has_all_zestimates_mask = zestimates_sample_df[value_vars].notna().all(axis=1)
    print(
        f"Limiting sample to {has_all_zestimates_mask.sum()} evictions for which we observe Zestimates at every month "
        f"from 2012-12 to 2022-12.")
    zestimates_sample_df = zestimates_sample_df.loc[has_all_zestimates_mask, :]

    # Save dataset for Zillow analysis.
    zestimates_sample_df.to_csv(OUTPUT_DATA_ZILLOW, index=False)

# %%
