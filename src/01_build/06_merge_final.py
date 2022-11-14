import pandas as pd
df = pd.read_csv("/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_merged_with_parcels/evictions_merged_with_eastern_parcels.csv")
df.to_csv()
assessor_data = pd.read_csv("/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv")
df = df.merge(assessor_data, how='inner', on='LOC_ID', validate='1:m')