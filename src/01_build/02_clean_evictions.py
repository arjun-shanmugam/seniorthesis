"""
02_clean_evictions.py

Cleans eviction dataset from MassLandlords.
"""
import pandas as pd
from geocodio import GeocodioClient
from build_utilities import batch_df

INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/gather20221121_aug.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
GEOCODIO_API_KEY = "060167a66c7587887a81c38077996a71c963638"
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS, encoding='unicode_escape')
original_N = len(evictions_df)
VERBOSE = True

# Clean court division.
court_division_replacement_dict = {"central": "Central",
                                   "eastern": "Eastern",
                                   "metro_south": "Metro South",
                                   "northeast": "Northeast",
                                   "southeast": "Southeast",
                                   "western": "Western"}
evictions_df.loc[:, 'court_division'] = evictions_df.loc[:, 'court_division'].replace(court_division_replacement_dict)

# Drop BMC and District Court cases.
mask = evictions_df['court_division'].isin(["Central", "Eastern", "Metro South", "Southeast", "Northeast", "Western"])
evictions_df = evictions_df.loc[mask, :]

# Clean initiating actions.
initiating_action_replacement_dict = {"Efiled SP Summons and Complaint - Cause": "SP Summons and Complaint - Cause",
                                      "Efiled SP Summons and Complaint - Foreclosure": "SP Summons and Complaint - Foreclosure",
                                      "SP Summons and Complaint - Non-payment": "SP Summons and Complaint - Non-payment of Rent",
                                      "Efiled SP Summons and Complaint - Non-payment": "SP Summons and Complaint - Non-payment of Rent",
                                      "Efiled SP Summons and Complaint - Non-payment of Rent": "SP Summons and Complaint - Non-payment of Rent",
                                      "Efiled SP Summons and Complaint - No Cause": "SP Summons and Complaint - No Cause",
                                      "Poah Communities, Managing Agent For Poah Central Annex Preservation Associates, Lp": ""}
evictions_df.loc[:, 'initiating_action'] = evictions_df.replace(initiating_action_replacement_dict)

# Clean inconsistencies in judge names.
evictions_df.loc[:, 'court_person'] = evictions_df.loc[:, 'court_person'].str.replace("&#039;",
                                                                                      # Apostrophes represented as mojibake.
                                                                                      "",
                                                                                      regex=False)
name_replacement_dict = {"David D Kerman": "David Kerman",
                         "Del ": "Gustavo del Puerto",
                         "Diana H": "Diana Horan",
                         "Diana H Horan": "Diana Horan",
                         "Fairlie A Dalton": "Fairlie Dalton",
                         "Gustavo A": "Gustavo del Puerto",
                         "Gustavo A Del Puerto": "Gustavo del Puerto",
                         "III Joseph ": "Joseph Kelleher III",
                         "III Kelleher": "Joseph Kelleher III",
                         "Laura J Fenn": "Laura Fenn",
                         "Laura J. Fenn": "Laura Fenn",
                         "Michae Malamut": "Michael Malamut",
                         "Michael J Doherty": "Michael Doherty",
                         "Nickolas W Moudios": "Nickolas Moudios",
                         "Nickolas W. Moudios": "Nickolas Moudios",
                         "Robert G Fields": "Robert Fields",
                         "Sergio E Carvajal": "Sergio Carvajal",
                         "Timothy F Sullivan": "Timothy Sullivan",
                         "on. Donna Salvidio": "Donna Salvidio"}
evictions_df.loc[:, 'court_person'] = evictions_df['court_person'].replace(name_replacement_dict)

# Drop rows missing property address.
no_address_info_mask = (evictions_df['property_address_full'].isna())
if VERBOSE:
    print(
        f"Dropping {no_address_info_mask.sum()} rows missing property_address_full "
        f"({100 * (no_address_info_mask.sum() / original_N):.3} percent of original dataset).")
evictions_df = evictions_df.loc[~no_address_info_mask, :].reset_index(drop=True)

# Geocode addresses for matching with tax parcels.
client = GeocodioClient(GEOCODIO_API_KEY)
batched_addresses = batch_df(evictions_df['property_address_full'], batch_size=10000)
list_of_coordinates = []
for i, batch in enumerate(batched_addresses):
    list_of_coordinates += client.batch_geocode(batch.tolist()).coords
    if VERBOSE:
        print(f"Geocoded batch {i}")
coordinates = pd.DataFrame(list_of_coordinates, columns=['latitude', 'longitude'])
evictions_df = pd.concat([evictions_df, coordinates], axis=1)
geocoding_failed_mask = (evictions_df['longitude'] == 0) & (evictions_df['latitude'] == 0)
evictions_df = evictions_df.loc[~geocoding_failed_mask, :]
if VERBOSE:
    print(f"Successfully geocoded {len(evictions_df) - geocoding_failed_mask.sum()} evictions "
          f"({(len(evictions_df) - geocoding_failed_mask.sum()) / len(evictions_df)} percent of observations).")

# Save unrestricted eviction data.
if VERBOSE:
    print("Saving unrestricted evictions dataset.")
evictions_df.to_csv(OUTPUT_DATA, index=False)

# NOTE: After this file was run, I manually ran evictions.csv through Geocod.io to get the 2010 Census Tract
#       in which each property is located.