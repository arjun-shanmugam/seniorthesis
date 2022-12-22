"""
05_merge_panel.py
"""
import pandas as pd

INPUT_DATA_ZESTIMATES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/zestimates.csv"
INPUT_DATA_EVICTIONS = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/evictions.csv"
OUTPUT_DATA_UNRESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/zillow_panel_unrestricted.csv"
OUTPUT_DATA_RESTRICTED = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/zillow_panel_restricted.csv"

zestimates_df = pd.read_csv(INPUT_DATA_ZESTIMATES)
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
merged_df = zestimates_df.merge(evictions_df, on='case_number', how='inner', validate='1:1')

merged_df.loc[:, 'file_year'] = pd.to_datetime(merged_df['file_date']).dt.year
merged_df.to_csv(OUTPUT_DATA_UNRESTRICTED)

original_N = len(merged_df)


# Drop cases which were resolved via mediation.
mask = (merged_df['disposition_found'] != "Mediated")
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Mediated\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases which were resolved by voluntary dismissal (dropped by plaintiff).
mask = ~(merged_df['disposition'].str.contains("R 41(a)(1) Voluntary Dismissal on", na=False, regex=False))
print(
    f"Dropping {(~mask).sum()} observations resolved through voluntary dismissal ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Drop cases where disposition_found is "Other".
mask = ~(merged_df['disposition_found'] == "Other")
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]

# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
merged_df.loc[:, "judgment_for_pdu"] = merged_df.loc[:, "judgment_for_pdu"].replace(judgment_for_pdu_replacement_dict)

# Drop rows which contain inconsistent values of disposition_found and judgment_for_pdu
# Case listed as a default, yet defendant listed as winning.
mask = ~((merged_df['disposition_found'] == "Defaulted") & (merged_df['judgment_for_pdu'] == "Defendant"))
print(
    f"Dropping {(~mask).sum()} observations disposition_found is \'Defaulted\' but judgment_for_pdu is \'Defendant\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
merged_df = merged_df.loc[mask, :]
# Case listed as dismissed, yet plaintiff listed as having won.
mask = ~((merged_df['disposition_found'] == "Dismissed") & (merged_df['judgment_for_pdu'] == "Plaintiff"))
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Dismissed\' but judgment_for_pdu is \'Plaintiff\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")
print(
    f"Dropping {(~mask).sum()} observations where disposition_found is \'Other\' ({100 * (((~mask).sum()) / original_N):.3} percent of original dataset).")

merged_df = merged_df.loc[mask, :]

# Generate a variable indicating judgement in favor of defendant.
merged_df.loc[:, 'judgment_for_defendant'] = 0
mask = (merged_df['disposition_found'] == "Dismissed") | (merged_df['judgment_for_pdu'] == "Defendant")
merged_df.loc[mask, 'judgment_for_defendant'] = 1

# Generate a variable indicating judgement in favor of plaintiff.
merged_df.loc[:, 'judgment_for_plaintiff'] = 1 - merged_df['judgment_for_defendant']
# Save restricted eviction data.
merged_df.to_csv(OUTPUT_DATA_RESTRICTED)