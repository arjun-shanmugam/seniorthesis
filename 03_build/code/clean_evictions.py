"""
clean_evictions.py

Cleans eviction dataset from MassLandlords.
"""
import pandas as pd
INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/03_build/input/shanmugam_aug_v2.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/03_build/intermediate/evictions.dta"
evictions_df = pd.read_csv(INPUT_DATA)

# Clean inconsistencies in judge names.
evictions_df.loc[:, 'court_person'] = evictions_df.loc[:, 'court_person'].str.replace("&#039;",  # Apostrophes represented as mojibake.
                                                                                      "",
                                                                                      regex=False)
# TODO: Deal with rows where court_person_name == "Del"
# TODO: Deal with rows where court_person_name begins with "III"
# TODO: Deal with rows where court_person_name begins with "Jr"
name_replacement_dict = {"Alex Valderrama": "Alex J Valderrama",
                         "Caitlin; 01 Castillo": "Caitlin Castillo",
                         "Caitlin; 10 Castillo": "Caitlin Castillo",
                         "Caitlin; span Castillo": "Caitlin Castillo",
                         "Cesar A": "Cesar A Archilla",
                         "Claudia; 08 Abreau": "Claudia Abreau",
                         "Claudia; 11 Abreau": "Claudia Abreau",
                         "Claudia; span Abreau": "Claudia Abreau",
                         "Dunbar D": "Dunbar D Livingston",
                         "Eric; 07 Donovan": "Eric Donovan",
                         "Erica; 06 Colombo": "Erica Colombo",
                         "Gregory; span Bartlett": "Gregory Bartlett",
                         "Gustavo A": "Gustavo A Del Puerto",
                         "Gustavo del Puerto": "Gustavo A Del Puerto",
                         "Kara; span Cunha": "Kara Cunha",
                         "Michael; 02 Neville": "Michael T Neville",
                         "Robert T": "Robert T Santaniello",
                         "Sergio Carvajal": "Sergio E Carvajal",
                         "Shelly Sankar": "Shelly Ann Sankar",
                         "Stephen; 08 Poitrast": "Stephen Poitrast",
                         "Steven E  Thomas": "Steven E Thomas"}
evictions_df = evictions_df.replace(name_replacement_dict)

# Clean inconsistencies in judgement values.
judgement_replacement_dict = {"defendant": "Defendant",
                              "plaintiff": "Plaintiff",
                              "unknown": "Unknown"}


# # Clean dispositions
# # TODO: Deal with row 20793, where disposition is missing!
# evictions_df = evictions_df.drop(index=20793)
# # print(evictions_df.loc[evictions_df['disposition'].str.count("/") == 4]['disposition'])
#
# # If there was a voluntary dismissal according to disposition, specify that in disposition_found
# mask = (evictions_df['disposition'].str.contains("Voluntary Dismissal")) | (evictions_df['disposition'].str.contains("Voluntary Dismissa"))
# evictions_df.loc[mask, 'disposition_found'] = "Voluntary Dismissal"
#
# # If there was an involuntary dismissal according to disposition, specify that in disposition_found.
# print(evictions_df['disposition'].loc[evictions_df['disposition'] == ""])
# mask = evictions_df['disposition'].str.contains("Involuntary Dismissal")
# print(mask)
# print(mask[(mask != True) & (mask != False)])
# evictions_df.loc[mask, 'disposition_found'] = "Involuntary Dismissal"

evictions_df.to_stata(OUTPUT_DATA)
