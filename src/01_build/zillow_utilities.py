"""
zillow_utilities.py

Defines useful functions for pulling data from Zillow.
"""
import time
import json
from io import StringIO
from typing import List
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

def get_zillow_url(property_address: str, master_list: List):
    """
    Finds the URL of a specified property's page on Zillow.
    :param master_list:
    :param property_address: The address of the property.
    :return:
    """
    # Avoid 429 error from Google.
    time.sleep(.5)

    # Get the URL of the first search result.
    result = search(f"zillow {property_address}", num_results=1)
    first_url = next(result)

    # Return the URL if it appears to the Zillow URL of the requested property
    print(first_url)
    if first_url.startswith("https://www.zillow.com/homedetails/"):
        print(f"Found Zillow URL for {property_address}.")
        master_list.append((property_address, first_url))
    else:
        print(f"Unable to find Zillow URL for {property_address}.")
        master_list.append((property_address, "Unable to find Zillow URL."))


def to_binary(filepath: str, series: pd.Series, index: bool, header: bool):
    """
    Write a pd.Series to a file in binary.
    :param filepath:
    :param series:
    :param index:
    :param header:
    """
    series_as_string = series.to_string(index=index, header=header)
    with open(filepath, 'wb') as file:
        file.write(bytes(series_as_string, encoding='utf-8'))


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
        result = pd.read_csv(StringIO(response.text), header=None, usecols=[0, 1], names=['property_address_full', 'zpid'])
        result.loc[:, 'property_address_full'] = result['property_address_full'].str.replace("\"", "", regex=False)
        return result