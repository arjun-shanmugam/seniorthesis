"""
Defines useful helper functions for us with Pandas DataFrames.
"""
import os
from time import sleep

import pandas as pd
import numpy as np
import censusgeocode
from typing import List
from shapely.geometry import Point
from joblib import Parallel, delayed
from multiprocessing import Manager
import geopandas as gpd


def merge_evictions_with_tax_parcels(path_to_evictions_df: str,
                                     path_to_tax_parcel_east_gdf: str,
                                     path_to_tax_parcel_west_gdf: str,
                                     path_to_intermediate_data_geocoding: str):
    # Create evictions GeoDataFrame.
    evictions_gdf = pd.read_csv(path_to_evictions_df)
    geometry = evictions_gdf.apply(lambda row: Point(row['lon'], row['lat']), axis=1)
    evictions_gdf = gpd.GeoDataFrame(evictions_gdf, geometry=geometry)
    evictions_gdf = evictions_gdf.set_crs("EPSG:4326")  # crs attribute is currently unassigned.
    evictions_gdf = evictions_gdf.to_crs("EPSG:26986")



    # Create tax parcels GeoDataFrame.
    tax_parcel_columns_to_keep = ['MAP_PAR_ID', 'LOC_ID', 'POLY_TYPE', 'SITE_ADDR', 'CITY', 'ZIP', 'SHAPE_Leng', 'SHAPE_Area',
                                  'geometry']
    tax_parcels_gdf = pd.concat([gpd.read_file(path_to_tax_parcel_east_gdf)[tax_parcel_columns_to_keep],
                                 gpd.read_file(path_to_tax_parcel_west_gdf)[tax_parcel_columns_to_keep]], axis=0)

    tax_parcels_gdf = tax_parcels_gdf.dissolve(by='LOC_ID').reset_index()
    tax_parcels_gdf.to_file("~/Desktop/tax_parcels.shp")

    tax_parcels_gdf = evictions_gdf.sjoin_nearest(tax_parcels_gdf, how='left', max_distance=500, distance_col='distance')
    tax_parcels_gdf.to_csv("~/Desktop/sjoin_nearest_results.csv")

    columns_to_return = evictions_gdf.drop(columns='geometry').columns.tolist()
    return tax_parcels_gdf[['LOC_ID'] + columns_to_return]

    """# Geocode tax parcels.
    tax_parcels_gdf.loc[:, 'STATE'] = "MA"
    result = geocode_addresses(tax_parcels_gdf[['SITE_ADDR', 'CITY', 'STATE', 'ZIP']].fillna("na"),
                               path_to_intermediate_data_geocoding)
    result.loc[:, 'id'] = result['id'].astype(int)
    result = result.set_index('id').rename(columns={'parsed': 'full_geocoded_address'})
    print(f"Successfully geocoded {100 * (result['match'].sum() / len(tax_parcels_gdf)):.3} percent of records.")

    # Concatenate the geocoded data with the original evictions data.
    tax_parcels_gdf = gpd.GeoDataFrame(tax_parcels_gdf.merge(result,
                                                             right_index=True,
                                                             left_index=True,
                                                             how='inner',
                                                             validate='1:1')).dissolve(by='LOC_ID')

    tax_parcels_gdf = tax_parcels_gdf.drop_duplicates(subset='full_geocoded_address')"""



    """# TODO: Merge tax parcels with eviction data on street address when possible.
    successfully_geocoded_mask = tax_parcels_gdf['match']
   |{{{{{{"
    merged = tax_parcels_gdf[successfully_geocoded_mask].merge(evictions_gdf,
                                                               on='full_geocoded_address',
                                                               how='outer',
                                                               validate='1:m',
                                                               indicator='_string_merge')
    unmerged = pd.concat([merged.loc[(merged['_string_merge'] == "left_only"), :],
                          tax_parcels_gdf.loc[~successfully_geocoded_mask, :]],
                         axis=0)[tax_parcel_columns_to_keep]
    merged = merged.loc[(merged['_string_merge'] == "both"), :]

    unmerged.to_csv("~/Desktop/unmerged.csv")
    merged.to_csv("~/Desktop/merged.csv")"""

    # TODO: Merge the unmerged observations using a spatial join.


# Write utility function to join evictions data with East and West shapefiles.
def sjoin_tax_parcels_with_evictions(tax_parcels_gdf: gpd.GeoDataFrame,
                                     evictions_gdf: gpd.GeoDataFrame,
                                     columns_to_drop: List[str] = None):
    """

    :param tax_parcels_gdf: GeoDataFrame containing the tax parcels.
    :param evictions_gdf: GeoDataFrame containing the eviction records.
    :param columns_to_drop:
    :return:
    """
    tax_parcels_gdf = tax_parcels_gdf.to_crs(evictions_gdf.crs)
    print("Joining shapefile with eviction data.")
    gdf = evictions_gdf.sjoin(tax_parcels_gdf, how='inner', predicate='contains')
    if columns_to_drop is not None:
        gdf = gdf.drop(columns=columns_to_drop)
    return gdf


def geocode_single_address(row):
    """
    Function meant to be applied to a single row of a DataFrame.
    :param row:
    """
    sleep(3)  # Sleep to avoid over-pinging the API server.
    id_num = int(row['id'])
    tokenized_address = row['address'].split(", ")
    if len(tokenized_address) > 4:  # If there is an extra comma in the address tokenized address may have too many items.
        return row  # Abort if this happens.
    street_address = tokenized_address[0]
    city = tokenized_address[1]
    state = tokenized_address[2]
    zipcode = tokenized_address[3]
    if len(zipcode) > 10:
        return row  # Abort if zipcode is longer than 10 characters.
    result = censusgeocode.address(street_address, city=city, state=state, zip=zipcode)
    print(result)
    if len(result) > 0:  # If a match is found:
        row['parsed'] = result[0]['matchedAddress']
        row['match'] = True
        row['tigerlineid'] = result[0]['tigerLine']['tigerLineId']
        row['side'] = result[0]['tigerLine']['side']
        row['statefp'] = result[0]['geographies']['Census Tracts'][0]['STATE']
        row['countyfp'] = result[0]['geographies']['Census Tracts'][0]['COUNTY']
        row['tract'] = result[0]['geographies']['Census Tracts'][0]['TRACT']
        row['block'] = result[0]['geographies']['2020 Census Blocks'][0]['BLOCK']
        row['lat'] = result[0]['coordinates']['y']
        row['lon'] = result[0]['coordinates']['x']
    else:
        row['match'] = False
    print(row)
    return row


def geocode_helper(cg: censusgeocode.CensusGeocode,
                   master_list: List[pd.DataFrame],
                   filepath: str):
    """
    Helper function to allow parallelization of batch geocoding.
    :param cg: A CensusGeocode object.
    :param master_list: A list which will hold all the geocoded DataFrames.
    :param filepath: Path to a CSV file containing the records to geocode.
    """
    returned_from_api = cg.addressbatch(filepath)
    returned_from_api_as_df = pd.DataFrame(returned_from_api, columns=returned_from_api[0].keys())
    master_list.append(returned_from_api_as_df)
    print(f"Geocoded batch: {filepath[-7:-4]}")
    os.remove(filepath)


def geocode_addresses(df: pd.DataFrame,
                      path_to_intermediate_data: str,
                      n_jobs=-1):
    """
    Geocodes a DataFrame containing addresses.
    :param df: A DataFrame containing 4 columns, ID, street address, city, state, zip, in that order.
    :param n_jobs: Number of processors to use.
    :param path_to_intermediate_data: Location to store CSV files before sending to batch geocoder.
    """
    batched_df = batch_df(df=df, batch_size=9999)
    filepaths = []  # Save each batch as a separate CSV file.
    for i, batch in enumerate(batched_df):
        path = os.path.join(path_to_intermediate_data, f"batch{i}.csv")
        filepaths.append(path)
        batch.to_csv(path, header=False)
        print(f"Saving batch number to CSV: {i}")
    # Send CSVs to the census geocoder.
    cg = censusgeocode.CensusGeocode(benchmark='Public_AR_Current', vintage='Census2020_Current')

    manager = Manager()
    geocoded_results = manager.list()
    Parallel(n_jobs=n_jobs)(delayed(geocode_helper)(cg, geocoded_results, filepath) for filepath in filepaths)
    result = pd.concat(list(geocoded_results), axis=0)  # Store returned, geocoded data.

    return result


def batch_df(df: pd.DataFrame, batch_size: int):
    """

    :param df: DataFrame to create batches out of.
    :param batch_size: Number of observations in each batch.
    """
    num_batches = len(df) // batch_size
    batched_dataframes = []
    for batch in range(0, num_batches + 1):
        if batch < num_batches:
            start = batch * batch_size
            end = (batch + 1) * batch_size
        else:
            start = batch * batch_size
            end = len(df)

        current_batch = df.iloc[start:end]
        batched_dataframes.append(current_batch)
    return batched_dataframes


def reduce_mem_usage(df: pd.DataFrame):
    """
    :param df: a Pandas DataFrame whose columns will be downcast.
    :return: A downcasted DataFrame and a list of columns which contained NaN values.
    """
    start_mem_usg = df.memory_usage().sum() / 1024 ** 2
    print("Memory usage of properties dataframe is :", start_mem_usg, " MB")
    NAlist = []  # Keeps track of columns that have missing values filled in.
    for col in df.columns:
        if df[col].dtype != object:  # Exclude strings

            # Print current column type
            print("******************************")
            print("Column: ", col)
            print("dtype before: ", df[col].dtype)

            # make variables for Int, max and min
            IsInt = False
            mx = df[col].max()
            mn = df[col].min()

            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(df[col]).all():
                NAlist.append(col)
                df[col].fillna(mn - 1, inplace=True)

                # test if column can be converted to an integer
            asint = df[col].fillna(0).astype(np.int64)
            result = (df[col] - asint)
            result = result.sum()
            if result > -0.01 and result < 0.01:
                IsInt = True

            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if mn >= 0:
                    if mx < 255:
                        df[col] = df[col].astype(np.uint8)
                    elif mx < 65535:
                        df[col] = df[col].astype(np.uint16)
                    elif mx < 4294967295:
                        df[col] = df[col].astype(np.uint32)
                    else:
                        df[col] = df[col].astype(np.uint64)
                else:
                    if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)

                        # Make float datatypes 32 bit
            else:
                df[col] = df[col].astype(np.float32)

            # Print new column type
            print("dtype after: ", df[col].dtype)
            print("******************************")

    # Print final result
    print("___MEMORY USAGE AFTER COMPLETION:___")
    mem_usg = df.memory_usage().sum() / 1024 ** 2
    print("Memory usage is: ", mem_usg, " MB")
    print("This is ", 100 * mem_usg / start_mem_usg, "% of the initial size")
    return df, NAlist
