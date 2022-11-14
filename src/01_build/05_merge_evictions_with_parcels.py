"""
05_merge_evictions_with_parcels.py

Merge eviction data with assessment values.
"""
import os.path
from typing import List

import matplotlib
import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt
from build_utilities import sjoin_tax_parcels_with_evictions

matplotlib.rcParams['savefig.dpi'] = 500
from shapely.geometry import Point
import contextily as cx

INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"
INPUT_DATA_TAX_PARCELS_EAST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_EAST.shp"
INPUT_DATA_TAX_PARCELS_WEST = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/Statewide_parcels_SHP/L3_TAXPAR_POLY_ASSESS_WEST.shp"

INPUT_DATA_ASSESSED_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
OUTPUT_FIGURES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/summary/figures"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_merged_with_tax_parcels.csv"

# Add Geometry to eviction dataset.
eviction_df = pd.read_csv(INPUT_DATA_EVICTIONS)
geometry = eviction_df.apply(lambda row: Point((row['lon'], row['lat'])), axis=1)
evictions_gdf = gpd.GeoDataFrame(eviction_df, geometry=geometry)
evictions_gdf = evictions_gdf.set_crs("EPSG:4326")  # crs attribute is currently unassigned.

# Plot evictions spatially.
fig, ax = plt.subplots()
evictions_gdf.plot(ax=ax, markersize=0.01, color='black')
cx.add_basemap(ax, crs=evictions_gdf.crs, source=cx.providers.CartoDB.Voyager)
ax.axis('off')
plt.savefig(os.path.join(OUTPUT_FIGURES, "map_individual_evictions.png"), bbox_inches='tight')
plt.close(fig)

# Load Western MA shapefiles.
western_gdf = gpd.read_file(INPUT_DATA_TAX_PARCELS_WEST)
western_gdf.loc[:, 'geometry'] = western_gdf['geometry'].buffer(0)
# Drop unneeded columns.
columns_to_drop = ['POLY_TYPE', 'MAP_NO', 'SOURCE', 'PLAN_ID', 'LAST_EDIT', 'BND_CHK', 'NO_MATCH', 'TOWN_ID', 'PROP_ID', 'BLDG_VAL',
                   'LAND_VAL', 'OTHER_VAL', 'TOTAL_VAL', 'FY', 'LOT_SIZE', 'LS_DATE', 'LS_PRICE', 'USE_CODE', 'ADDR_NUM',
                   'FULL_STR', 'LOCATION', 'CITY', 'OWNER1', 'OWN_ADDR', 'OWN_CITY', 'OWN_STATE', 'OWN_ZIP', 'OWN_CO', 'LS_BOOK',
                   'LS_PAGE', 'REG_ID', 'ZONING', 'YEAR_BUILT', 'BLD_AREA', 'UNITS', 'RES_AREA', 'STYLE', 'NUM_ROOMS', 'LOT_UNITS',
                   'STORIES']
western_gdf = western_gdf.drop(columns=columns_to_drop)
# Drop rows without a LOC_ID
has_LOC_ID_mask = ~(western_gdf['LOC_ID'].isna() | western_gdf['LOC_ID'] == "")
western_gdf = western_gdf.loc[has_LOC_ID_mask, :]
# Drop rows where SITE_ADDR or ZIP are missing
has_SITE_ADDR = ~(western_gdf['SITE_ADDR'].isna() | western_gdf['SITE_ADDR'] == "")
has_ZIP = ~(western_gdf['ZIP'].isna() | western_gdf['ZIP'] == "")
western_gdf = western_gdf.loc[has_SITE_ADDR & has_ZIP, :]
# Dissolve so that every polygon has a single row.
western_gdf = western_gdf.dissolve(by=['MAP_PAR_ID'])
western_gdf = western_gdf.to_crs(evictions_gdf.crs)
print("Joining shapefile with eviction data.")
western_gdf = evictions_gdf.sjoin(western_gdf, how='inner', predicate='within')


# Load Eastern MA shapefiles and join.
eastern_gdf = gpd.read_file(INPUT_DATA_TAX_PARCELS_EAST)
# Drop unneeded columns.
eastern_gdf = eastern_gdf.drop(columns=columns_to_drop)
# Drop rows without a LOC_ID
has_LOC_ID_mask = ~(eastern_gdf['LOC_ID'].isna() | eastern_gdf['LOC_ID'] == "")
eastern_gdf = eastern_gdf.loc[has_LOC_ID_mask, :]
# Drop rows where SITE_ADDR or ZIP are missing
has_SITE_ADDR = ~(eastern_gdf['SITE_ADDR'].isna() | eastern_gdf['SITE_ADDR'] == "")
has_ZIP = ~(eastern_gdf['ZIP'].isna() | eastern_gdf['ZIP'] == "")
eastern_gdf = eastern_gdf.loc[has_SITE_ADDR & has_ZIP, :]
# Dissolve so that every polygon has a single row.
eastern_gdf.loc[:, 'geometry'] = evictions_gdf['geometry'].buffer(0)
eastern_gdf = eastern_gdf.dissolve(by=['MAP_PAR_ID'])
eastern_gdf = eastern_gdf.to_crs(evictions_gdf.crs)
print("Joining shapefile with eviction data.")
eastern_gdf = evictions_gdf.sjoin(eastern_gdf, how='inner', predicate='within')

# Combine Western and Eastern files.
df = pd.concat([western_gdf, eastern_gdf], axis=0)
columns_to_drop = ['SHAPE_Leng', 'SHAPE_Area', 'geometry', 'index_right']
df = df.drop(columns=columns_to_drop)

# Save data.
df.to_csv(OUTPUT_DATA)
