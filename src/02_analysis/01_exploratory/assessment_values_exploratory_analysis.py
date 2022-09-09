"""
assessment_values_exploratory_analysis.py

Performs exploratory analysis on the assessment values data.
"""
import pandas as pd
import matplotlib.pyplot as plt


from src.utilities.figure_utilities import plot_histogram
import numpy as np
from src.utilities import figure_and_table_constants
import os
import fiona
import geopandas as gpd
ASSESSOR_FILE = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
TAX_PARCELS_GDB = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/MassGIS_L3_Parcels.gdb"
OUTPUT_FIGURES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/01_exploratory/figures"
OUTPUT_TABLES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/01_exploratory/tables"

df = pd.read_csv(ASSESSOR_FILE)
fiona.listlayers(TAX_PARCELS_GDB)
gdf = gdf.read_file(TAX_PARCELS_GDB)

print(penis)
# Drop rows where TOTAL_VA  L == -1
mask = (df['TOTAL_VAL'] != -1)
print(len(mask))
df = df.loc[mask, :]

# Drop rows where SITE_ADDR = NaN
mask = ~(df['SITE_ADDR'].isna())
print(len(mask))
df = df.loc[mask, :]

"""
# Plot histogram of log-total property values.
fig, ax = plt.subplots(1, 1)
plot_histogram(ax=ax,
               x=np.log(df['TOTAL_VAL'][df['TOTAL_VAL'] >= 0] + 1),
               title="Histogram of Log(Total Values) of Massachusetts Properties",
               xlabel="Log of Total Value ($)")
plt.savefig(os.path.join(OUTPUT_FIGURES, "hist_TOTAL_VAL.png"), bbox_inches='tight')
plt.close(fig)
"""

# Aggregate data to city year level.
df_town_year_level = df.groupby(by=['CITY', 'TOWN_ID', 'CY'])['TOTAL_VAL'].mean()
print(df_town_year_level)
print(df_town_year_level.columns)
print(gdf.columns)

