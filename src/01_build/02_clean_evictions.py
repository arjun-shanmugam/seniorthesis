"""
02_clean_evictions.py

Cleans eviction dataset from MassLandlords.
"""
import pandas as pd
INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/shanmugam_2022_08_03_aug.csv"
INPUT_DATA_JUDGES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/01_raw/judges.xlsx"
OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"

evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS, encoding='unicode_escape')
judges_df = pd.read_excel(INPUT_DATA_JUDGES)

# Restrict to cases where court_person_type is 'judge'
mask = evictions_df['court_person_type'] == 'judge'
evictions_df = evictions_df.loc[mask, :]

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
                         "Gustavo A ": "Gustavo del Puerto",
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

#
# # Clean dispositions.
# # In some rows, there are two dispositions listed. Choose only the last one.
# rows_with_two_dispositions = evictions_df['disposition'].str.count("/") == 4  # Identify them as containing two dates.
# evictions_df.loc[rows_with_two_dispositions, 'disposition'] = evictions_df.loc[rows_with_two_dispositions, 'disposition'].str.split(pat="\d+/\d+/\d+").str[-2]
#
#
# # If there was a voluntary dismissal according to disposition, specify that in disposition_found
# mask = (evictions_df['disposition'].str.contains("Voluntary Dismissal")) | (evictions_df['disposition'].str.contains("Voluntary Dismissa"))
# evictions_df.loc[mask, 'disposition_found'] = "Voluntary Dismissal"
#
# # Non-voluntary dismissals are considered involuntary.
# mask = (evictions_df['disposition_found'] == "Dismissed")
# evictions_df.loc[mask, 'disposition_found'] = "Involuntary Dismissal"
#
#
# # Clean judgment_for_pdu.
# evictions_df = evictions_df.rename(columns={'judgment_for_pdu': 'judgement_for_pdu'})
# judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
#                                      "plaintiff": "Plaintiff",
#                                      "defendant": "Defendant"}
# evictions_df.loc[:, "judgement_for_pdu"] = evictions_df.loc[:, "judgement_for_pdu"].replace(judgment_for_pdu_replacement_dict)
#
# # Generate new column, defendant_victory.
# evictions_df.insert(len(evictions_df.columns), column='defendant_victory', value=0)
# mask = (
#     (evictions_df['judgement_for_pdu'] == "Defendant") |
#     (evictions_df['disposition_found'] == "Involuntary Dismissal")
# )
# evictions_df.loc[mask, 'defendant_victory'] = 1
#
# # Clean court division.
# court_division_replacement_dict = {"central": "Central",
#                                    "eastern": "Eastern",
#                                    "metro_south": "Metro South",
#                                    "northeast": "Northeast",
#                                    "southeast": "Southeast",
#                                    "western": "Western"}
# evictions_df.loc[:, 'court_division'] = evictions_df.loc[:, 'court_division'].replace(court_division_replacement_dict)

evictions_df.to_csv(OUTPUT_DATA, index=False)

