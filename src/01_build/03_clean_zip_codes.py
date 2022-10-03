"""
03_clean_zip_codes.py

Cleans dataset of US zip codes.
"""
import pandas as pd

INPUT_PATH = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/US_ZIP_2014data.dta"
OUTPUT_PATH = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zipcodes.csv"

zipcodes_df = pd.read_stata(INPUT_PATH)

# Limit to Massachusetts zip codes.
mask = (zipcodes_df['STATE'] == "MA")
zipcodes_df = zipcodes_df.loc[mask, :].reset_index(drop=True)

# Keep only the zip code column.
zipcodes_df = zipcodes_df.drop(columns=["NAME", "STATE", "SQMI", "_ID"])

# Rename the zip code column.
zipcodes_df = zipcodes_df.rename(columns={"ZIP_CODE": "zipcode"})

# Save file.
zipcodes_df.to_csv(OUTPUT_PATH)