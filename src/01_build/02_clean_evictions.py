"""
02_clean_evictions.py

Cleans eviction dataset from MassLandlords.
"""
import os

import numpy as np
import pandas as pd
import censusgeocode

INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/shanmugam_2022_08_03_aug.csv"
INPUT_DATA_JUDGES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/judges.xlsx"
INPUT_DATA_ZIPCODES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zipcodes.csv"
INTERMEDIATE_DATA_GEOCODING = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/to_geocode"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
OUTPUT_DATA_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"

evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS, encoding='unicode_escape')
judges_df = pd.read_excel(INPUT_DATA_JUDGES)
zipcodes_df = pd.read_csv(INPUT_DATA_ZIPCODES, dtype={'zipcode': str})

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

# Clean inconsistencies in judge names.
evictions_df.loc[:, 'court_person'] = evictions_df.loc[:, 'court_person'].str.replace("&#039;",  # Apostrophes represented as mojibake.
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
evictions_df.loc[:, 'court_person'] = evictions_df.loc[:, 'court_person'].replace(name_replacement_dict)

# Generate zipcodes.



# Save as separate CSV files.
filepaths = []
for i, batch in enumerate(batched_data):
    path = os.path.join(INTERMEDIATE_DATA_GEOCODING, f"batch{i}.csv")
    filepaths.append(path)
    print(batch)
    batch.to_csv(path, header=False)

# Send CSVs to the census geocoder.
cg = censusgeocode.CensusGeocode(benchmark='Public_AR_Current', vintage='Census2020_Current')
k = cg.addressbatch(filepaths[0])
df = pd.DataFrame(k, columns=k[0].keys())
print(df)
df.to_csv("~/Desktop/results.csv")

# Save unrestricted eviction data.
evictions_df.to_csv(OUTPUT_DATA_UNRESTRICTED, index=False)

# Restrict to cases where court_person_type is 'judge'
mask = evictions_df['court_person_type'] == 'judge'
evictions_df = evictions_df.loc[mask, :]

# Drop cases which were resolved via mediation.
mask = evictions_df['disposition_found'] != "Mediated"
evictions_df = evictions_df.loc[mask, :]

# Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
mask = ~(evictions_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", regex=False))
evictions_df = evictions_df.loc[mask, :]

# Drop cases where disposition_found is "Other".
mask = ~(evictions_df['disposition_found'] == "Other")
evictions_df = evictions_df.loc[mask, :]

# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
evictions_df.loc[:, "judgment_for_pdu"] = evictions_df.loc[:, "judgment_for_pdu"].replace(judgment_for_pdu_replacement_dict)

# Drop rows which contain inconsistent values of disposition_found and judgment_for_pdu
# Case listed as a default, yet defendant listed as winning.
mask = ~((evictions_df['disposition_found'] == "Defaulted") & (evictions_df['judgment_for_pdu'] == "Defendant"))
evictions_df = evictions_df.loc[mask, :]
# Case listed as dismissed, yet plaintiff listed as having won.
mask = ~((evictions_df['disposition_found'] == "Dismissed") & (evictions_df['judgment_for_pdu'] == "Plaintiff"))
evictions_df = evictions_df.loc[mask, :]

# Generate a variable indicating judgement in favor of defendant.
evictions_df.loc[:, 'judgment_for_defendant'] = 0
mask = (evictions_df['disposition_found'] == "Dismissed") | (evictions_df['judgment_for_pdu'] == "Defendant")
evictions_df.loc[mask, 'judgment_for_defendant'] = 1

# Save restricted eviction data.
evictions_df.to_csv(OUTPUT_DATA_RESTRICTED, index=False)
