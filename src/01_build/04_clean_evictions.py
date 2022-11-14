"""
04_clean_evictions.py

Cleans eviction dataset from MassLandlords.
"""
import os

import numpy as np
import pandas as pd
import censusgeocode

from build_utilities import geocode_addresses

if __name__ == '__main__':
    INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/shanmugam_2022_08_03_aug.csv"
    INPUT_DATA_JUDGES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/judges.xlsx"
    INPUT_DATA_ZIPCODES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zipcodes.csv"
    INTERMEDIATE_DATA_GEOCODING = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/batched_evictions_to_geocode"
    OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_unrestricted.csv"
    OUTPUT_DATA_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_restricted.csv"

    evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS, encoding='unicode_escape')
    original_N = len(evictions_df)
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
    evictions_df.loc[:, 'court_person'] = evictions_df['court_person'].replace(name_replacement_dict)

    # Clean street addresses to remove unit numbers.


    # Drop rows missing street address, city, or state.
    has_address_info_mask = ~((evictions_df['property_address_street'].isna()) |
                              (evictions_df['property_address_city'].isna()) |
                              (evictions_df['property_address_state'].isna())
                              )
    print(f"Dropping {(~has_address_info_mask).sum()} rows missing street address, city, or state ({(~has_address_info_mask).sum() / original_N:.3} percent of original dataset).")
    evictions_df = evictions_df.loc[has_address_info_mask, :]


    # Geocode addresses for easy matching with assessor values.
    evictions_df = evictions_df.reset_index(drop=True)
    evictions_df.loc[:, 'property_address_street'] = evictions_df['property_address_street'].str.replace("&#039;",
                                                                                                         "\'",
                                                                                                         regex=False)
    columns_to_geocode = evictions_df[['property_address_street', 'property_address_city', 'property_address_state', 'property_address_zip']]
    print(f"Sending {len(columns_to_geocode)} observations to geocoder.")
    result = geocode_addresses(columns_to_geocode,
                               INTERMEDIATE_DATA_GEOCODING)
    result.loc[:, 'id'] = result['id'].astype(int)
    result = result.set_index('id')
    print(f"Successfully geocoded {100*(len(result) / len(evictions_df)):.3} percent of remaining dataset. Dropping failed rows.")

    # Concatenate the geocoded data with the original evictions data.
    evictions_df = evictions_df.merge(result,
                                      right_index=True,
                                      left_index=True,
                                      how='inner',
                                      validate='1:1')

    # Rename "parsed" column.
    evictions_df = evictions_df.rename(columns={'parsed': 'full_geocoded_address'})

    # Save unrestricted eviction data.
    print("Saving unrestricted evictions dataset.")
    evictions_df.to_csv(OUTPUT_DATA_UNRESTRICTED)


    # Restrict to cases where court_person_type is 'judge'
    mask = (evictions_df['court_person_type'] == 'judge')
    print(f"Dropping {(~mask).sum()} observations where court_person_type is not \'judge\' ({100*((~mask).sum() / original_N):.3} percent of original dataset).")
    evictions_df = evictions_df.loc[mask, :]

    # Drop cases which were resolved via mediation.
    mask = evictions_df['disposition_found'] != "Mediated"
    print(f"Dropping {(~mask).sum()} observations where disposition_found is \'Mediated\' ({100*(((~mask).sum()) / original_N):.3} percent of original dataset).")
    evictions_df = evictions_df.loc[mask, :]

    # Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
    mask = ~(evictions_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", regex=False))
    print(f"Dropping {(~mask).sum()} observations resolved through voluntary dismissal ({100*(((~mask).sum()) / original_N):.3} percent of original dataset).")
    evictions_df = evictions_df.loc[mask, :]

    # Drop cases where disposition_found is "Other".
    mask = ~(evictions_df['disposition_found'] == "Other")
    print(f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100*(((~mask).sum()) / original_N):.3} percent of original dataset).")
    evictions_df = evictions_df.loc[mask, :]

    # Clean the values in the judgment_for_pdu variable.
    judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                         "plaintiff": "Plaintiff",
                                         "defendant": "Defendant"}
    evictions_df.loc[:, "judgment_for_pdu"] = evictions_df.loc[:, "judgment_for_pdu"].replace(judgment_for_pdu_replacement_dict)

    # Drop rows which contain inconsistent values of disposition_found and judgment_for_pdu
    # Case listed as a default, yet defendant listed as winning.
    mask = ~((evictions_df['disposition_found'] == "Defaulted") & (evictions_df['judgment_for_pdu'] == "Defendant"))
    print(f"Dropping {(~mask).sum()} observations disposition_found is \'Defaulted\' but judgment_for_pdu is \'Defendant\' ({100*(((~mask).sum()) / original_N):.3} percent of original dataset).")
    evictions_df = evictions_df.loc[mask, :]
    # Case listed as dismissed, yet plaintiff listed as having won.
    mask = ~((evictions_df['disposition_found'] == "Dismissed") & (evictions_df['judgment_for_pdu'] == "Plaintiff"))
    print(f"Dropping {(~mask).sum()} observations where disposition_found is \'Dismissed\' but judgment_for_pdu is \'Plaintiff\' ({100*(((~mask).sum()) / original_N):.3} percent of original dataset).")
    print(f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100*(((~mask).sum()) / original_N):.3} percent of original dataset).")
    
    evictions_df = evictions_df.loc[mask, :]

    # Generate a variable indicating judgement in favor of defendant.
    evictions_df.loc[:, 'judgment_for_defendant'] = 0
    mask = (evictions_df['disposition_found'] == "Dismissed") | (evictions_df['judgment_for_pdu'] == "Defendant")
    evictions_df.loc[mask, 'judgment_for_defendant'] = 1

    # Generate a variable indicating judgement in favor of plaintiff.
    evictions_df.loc[:, 'judgment_for_plaintiff'] = 1 - evictions_df['judgment_for_defendant']
    print(evictions_df.columns)
    # Save restricted eviction data.
    evictions_df.to_csv(OUTPUT_DATA_RESTRICTED, index=False)
