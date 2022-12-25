import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from figure_utilities import plot_scatter_with_error_bars

result = pd.read_csv("/Users/arjunshanmugam/Desktop/zpid_result_test.txt", header=None, sep="NOT_A_SEPARATOR")[0]  # Get the first and only column.

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
addresses.name = "address"

# Return result.
result = pd.concat([addresses, ZPIDs], axis=1)
result = pd.wide_to_long(result, stubnames='zpid', i='address', j='j').reset_index().drop(columns='j').reset_index(drop=True)