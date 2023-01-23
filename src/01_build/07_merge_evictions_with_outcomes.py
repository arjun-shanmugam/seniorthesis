"""
07_merge_evictions_with_outcomes.py

Merge eviction data with assessment values.
"""

import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import pandas as pd

INPUT_DATA_EVICTIONS_PARCELS_AND_TRACTS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_with_parcels_and_tracts.csv"
INPUT_DATA_ZESTIMATES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"
INPUT_DATA_CRIME = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/crime_incidents"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/unrestricted.csv"
OUTPUT_DATA_ZILLOW = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/zestimates_analysis.csv"

VERBOSE = True

# Produce the unrestricted sample---------------------------------------------------------------------------------------
evictions_with_parcels_and_tracts_df = pd.read_csv(INPUT_DATA_EVICTIONS_PARCELS_AND_TRACTS).drop(index=29305)

# TODO: Merge with crime data-------------------------------------------------------------------------------------------
crime_df = pd.concat([pd.read_csv(os.path.join(INPUT_DATA_CRIME, file)) for file in os.listdir(INPUT_DATA_CRIME)],
                     axis=0)  # Read crime data.
crime_df = crime_df.dropna(subset=['Long', 'Lat', 'OCCURRED_ON_DATE'])
crime_df = (gpd  # Convert crime data to a GeoDataFrame.
            .GeoDataFrame(crime_df,
                          geometry=gpd.points_from_xy(crime_df['Long'], crime_df['Lat']))
            .set_crs("EPSG:4326")
            .to_crs("EPSG:26986")
            .drop(columns=['Lat', 'Long', 'Location', 'UCR_PART', 'HOUR', 'DAY_OF_WEEK', 'YEAR', 'MONTH',
                           'REPORTING_AREA', 'DISTRICT', 'STREET', 'OFFENSE_CODE', 'OFFENSE_DESCRIPTION']))
# Get month of each crime incident.
crime_df.loc[:, 'OCCURRED_ON_DATE'] = pd.to_datetime(crime_df['OCCURRED_ON_DATE'].str[:10]).dt.to_period("M")
crime_df = crime_df.rename(columns={'OCCURRED_ON_DATE': 'month_of_crime_incident'})

evictions_with_parcels_and_tracts_df = (gpd
             .GeoDataFrame(evictions_with_parcels_and_tracts_df,
                           geometry=gpd.points_from_xy(evictions_with_parcels_and_tracts_df['longitude'],
                                                       evictions_with_parcels_and_tracts_df['latitude']))
             .set_crs("EPSG:4326")
             .to_crs("EPSG:26986"))
evictions_with_parcels_and_tracts_df = evictions_with_parcels_and_tracts_df.dropna(subset=['longitude', 'latitude'])

boston_evictions = ['Boston', 'Brighton', 'Charlestown', 'Dorchester', 'Dorchester Center', 'East Boston']
print(len(evictions_with_parcels_and_tracts_df))
print(len(crime_df))
for radius in [1000]:
    # Temporarily re-assign the evictions a new geometry equal which has a buffer.
    evictions_with_parcels_and_tracts_df.geometry = evictions_with_parcels_and_tracts_df.geometry.buffer(radius)

    # Spatial join.
    evictions_with_parcels_and_tracts_df = gpd.sjoin(evictions_with_parcels_and_tracts_df,
                                                     crime_df,
                                                     how='left',
                                                     predicate='contains')
    print("join finished")

    # Convert back to DataFrame, dropping the geometry column containing buffered points.
    evictions_with_parcels_and_tracts_df = pd.DataFrame(evictions_with_parcels_and_tracts_df.drop(columns='geometry'))

    # Aggregate by case number and month of crime.
    evictions_with_parcels_and_tracts_df = evictions_with_parcels_and_tracts_df.groupby(['case_number', 'month_of_crime_incident', 'OFFENSE_CODE']).agg(
        {eviction_cols: 'first',
         'INCIDENT_NUMBER': 'count',
         'SHOOTING': 'sum'})

    # TODO: Reshape to wide format.
    print(evictions_with_parcels_and_tracts_df)


# Merge with Zillow data------------------------------------------------------------------------------------------------
merged_df = pd.read_csv(INPUT_DATA_ZESTIMATES).merge(evictions_with_parcels_and_tracts_df,
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

print(vfakevariable)

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
