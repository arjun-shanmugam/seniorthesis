"""
clean_evictions.py

Cleans eviction dataset from MassLandlords.
"""
import pandas as pd
INPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/shanmugam_aug_v2.csv"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.dta"
PLACBEO_OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions_placebo.dta"
RANDOM_STATE = 7
evictions_df = pd.read_csv(INPUT_DATA)

# Clean inconsistencies in judge names.
evictions_df.loc[:, 'court_person'] = evictions_df.loc[:, 'court_person'].str.replace("&#039;",  # Apostrophes represented as mojibake.
                                                                                      "",
                                                                                      regex=False)



# TODO: Deal with rows where court_person_name == "Del"
# evictions_df = evictions_df.drop(labels=(evictions_df['court_person'] == "Del").index)

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
                         "Del ": "Gustavo A Del Puerto",
                         "Kara; span Cunha": "Kara Cunha",
                         "Michael; 02 Neville": "Michael T Neville",
                         "Michae Malamut": "Michael Malamut",
                         "Robert T": "Robert T Santaniello",
                         "Sergio Carvajal": "Sergio E Carvajal",
                         "Shelly Sankar": "Shelly Ann Sankar",
                         "Stephen; 08 Poitrast": "Stephen Poitrast",
                         "Steven E  Thomas": "Steven E Thomas",
                         "III Joseph ": "Joseph Kelleher III",
                         "III, Kelleher": "Joseph Kelleher III"}
evictions_df.loc[:, 'court_person'] = evictions_df.loc[:, 'court_person'].replace(name_replacement_dict)

# Clean dispositions.
# In some rows, there are two dispositions listed. Choose only the last one.
rows_with_two_dispositions = evictions_df['disposition'].str.count("/") == 4  # Identify them as containing two dates.
evictions_df.loc[rows_with_two_dispositions, 'disposition'] = evictions_df.loc[rows_with_two_dispositions, 'disposition'].str.split(pat="\d+/\d+/\d+").str[-2]


# If there was a voluntary dismissal according to disposition, specify that in disposition_found
mask = (evictions_df['disposition'].str.contains("Voluntary Dismissal")) | (evictions_df['disposition'].str.contains("Voluntary Dismissa"))
evictions_df.loc[mask, 'disposition_found'] = "Voluntary Dismissal"

# Non-voluntary dismissals are considered involuntary.
mask = (evictions_df['disposition_found'] == "Dismissed")
evictions_df.loc[mask, 'disposition_found'] = "Involuntary Dismissal"


# Clean judgment_for_pdu.
evictions_df = evictions_df.rename(columns={'judgment_for_pdu': 'judgement_for_pdu'})
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
evictions_df.loc[:, "judgement_for_pdu"] = evictions_df.loc[:, "judgement_for_pdu"].replace(judgment_for_pdu_replacement_dict)

# Generate new column, defendant_victory.
evictions_df.insert(len(evictions_df.columns), column='defendant_victory', value=0)
mask = (
    (evictions_df['judgement_for_pdu'] == "Defendant") |
    (evictions_df['disposition_found'] == "Involuntary Dismissal")
)
evictions_df.loc[mask, 'defendant_victory'] = 1

# Clean court division.
court_division_replacement_dict = {"central": "Central",
                                   "eastern": "Eastern",
                                   "metro_south": "Metro South",
                                   "northeast": "Northeast",
                                   "southeast": "Southeast",
                                   "western": "Western"}

evictions_df.loc[:, 'court_division'] = evictions_df.loc[:, 'court_division'].replace(court_division_replacement_dict)

evictions_df.to_stata(OUTPUT_DATA, write_index=False)

