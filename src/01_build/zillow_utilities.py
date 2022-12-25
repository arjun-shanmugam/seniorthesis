"""
zillow_utilities.py

Defines useful functions for pulling data from Zillow.
"""
import time
import json
from io import StringIO
from typing import List

import numpy as np
import pandas as pd
import requests
from googlesearch import search

API_KEY = "91ac630dccmshb123beb8366a49ap124b4djsn4bdf0c892172"


def GET_zestimate_history(zpid: int, case_number: str):
    """
    :param zpid: The Zillow property ID for which we want to fetch zestimates.
    :return: A DataFrame containing the past Zestimates associated with the provided ZPID.
    """
    import requests
    # Obey call frequency limits.
    time.sleep(0.34)
    print(f"Getting Zestimates for ZPID: {zpid}")
    url = "https://zillow-com1.p.rapidapi.com/zestimateHistory"

    querystring = {"zpid": str(zpid)}

    headers = {
        "X-RapidAPI-Key": "251c7df1e1msha0e9511aa2bea51p144754jsna6dcc56ce15d",
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    df = pd.DataFrame.from_dict(json.loads(response.text)).rename(columns={'t': 'time', 'v': 'zestimate'})
    if df.empty:
        print(f"Could not get Zestimates for ZPID: {zpid}")
    else:
        print(f"Successfully got Zestimates for ZPID: {zpid}")
        df.loc[:, 'case_number'] = case_number
    return df

def POST_batch_ZPID_request(path_to_addresses):
    """
    :param path_to_addresses: Path to a single .txt file containing one address per line and no more than 100 addresses.
    :return: A DataFrame containing a property address column and a ZPID column.
    """
    # Pause to obey call frequency limits.
    time.sleep(1)

    file = open(path_to_addresses, "rb")
    url = "https://zillow-com1.p.rapidapi.com/resolveAddressToZpid"
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
    }
    response = requests.post(url, files={"file": file}, headers=headers)
    if response.ok:
        job_number = response.text.split("\"")[-2]
    else:
        raise RuntimeError("Post request was unsuccessful.")
    file.close()

    current_batch = path_to_addresses.split("/")[-1]
    print(f"Submitted POST request for {current_batch}, assigned job number {job_number}.")
    return job_number


def GET_processed_ZPIDs(job_number):
    """

    :param job_number:
    :return:
    """

    # Pause to obey call frequency limits.
    time.sleep(1)

    # Submit a GET request using the job number.
    url = "https://zillow-com1.p.rapidapi.com/getJobResults"

    querystring = {"jobNumber": str(job_number)}
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    if "isn't complete!" in response.text:
        raise RuntimeError("The specified job_number was requested before it was completed on the server.")
    else:
        # Read in the text of the response as a CSV with a separator that isn't actually present in the text.
        # This ensures that we read in the text of the CSV as a single column.
        result = pd.read_csv(StringIO(response.text), header=None, sep="NOT_A_SEPARATOR")[0]  # Get the first and only column.

        split = result.str.split("\"")  # Split on the address's closing quotes.
        # Build one column containing addresses and another containing lists of ZPIDs.
        ZPIDs = split.str[2].str.split(",")
        for index, zpid_list in enumerate(ZPIDs):
            new_list = [element for element in zpid_list if element != ""]
            if len(new_list) == 0:
                ZPIDs.loc[index] = [np.nan]
            else:
                ZPIDs.loc[index] = new_list
        ZPIDs = pd.DataFrame(ZPIDs.tolist())
        ZPIDs.columns = "zpid" + (ZPIDs.columns + 1).astype(str)
        addresses = split.str[1]
        addresses.name = "property_address_full"

        # Return result.
        result = pd.concat([addresses, ZPIDs], axis=1)
        result = pd.wide_to_long(result, stubnames='zpid', i='address', j='j').reset_index().drop(columns='j').reset_index(drop=True)

        return result
