"""
Defines useful helper functions for us with Pandas DataFrames.
"""
import os
from time import sleep

import pandas as pd
import numpy as np
import censusgeocode as cg
from typing import List
from joblib import Parallel, delayed
from multiprocessing import Manager
import geopandas as gpd


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


def geocode_single_point(master_list: List, index: int, latitude: float, longitude: float):
    print(f"Geocoding row {index} at latitude {latitude} and longitude {longitude}.")
    if np.isnan(latitude) or np.isnan(longitude):
        master_list.append((index, np.nan))
    else:
        try:
            print(cg.coordinates(longitude, latitude))
            master_list.append((index, cg.coordinates(longitude, latitude)['Census Tracts'][0]['GEOID']))
        except ValueError:
            print("ValueError: Unable to parse response from Census")


def geocode_coordinates(indices: List[int], latitudes: List[float], longitudes: List[float], n_jobs: int = 1):
    manager = Manager()
    master_list = manager.list()
    data = zip(indices, latitudes, longitudes)
    Parallel(n_jobs=n_jobs)(
        delayed(geocode_single_point)(master_list, index, latitude, longitude) for index, latitude, longitude in data)
    result = pd.DataFrame(list(master_list))  # Store returned, geocoded data.
    result = result.rename(columns={0: 'index', 1: 'tract_geoid'}).set_index('index')
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
