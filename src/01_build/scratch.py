import numpy as np
import pandas as pd
from build_utilities import geocode_single_point

df = pd.read_csv("/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv")
df2 = pd.read_csv("/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/tracts.csv")
print(df['Full FIPS (tract)'].astype(str))
print(df2['tract_geoid'])

print(df2['tract_geoid'].isin(df['Full FIPS (tract)']).sum())
